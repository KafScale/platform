<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
This project is supported and financed by Scalytics, Inc. (www.scalytics.io).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# KafScale Compatibility Guide

This document explains how this demo is configured to work optimally with KafScale's single-broker architecture.

## Understanding the Architecture Difference

### Apache Kafka (Multi-Broker)
```
Client connects to: kafka1:9092, kafka2:9092, kafka3:9092
├─ Broker ID 0: kafka1:9092
├─ Broker ID 1: kafka2:9092
└─ Broker ID 2: kafka3:9092

Metadata contains distinct endpoints for each broker
Client can route to specific brokers
```

### KafScale (Single Logical Broker)
```
Client connects to: 127.0.0.1:29092
└─ Logical Broker (infinite scaling behind single endpoint)
   ├─ Object storage for durability
   ├─ Stateless workers
   └─ Metadata abstraction

Metadata may reference broker IDs but all route through one address
Client must treat as single logical broker
```

## Why KafkaJS Requires Special Configuration

KafkaJS makes assumptions based on traditional Kafka:
1. **Stable broker IDs** - Each broker ID maps to a unique endpoint
2. **Direct broker routing** - Can connect to specific broker.id
3. **Static metadata** - Brokers don't disappear/reappear
4. **Multi-broker coordination** - Handles rebalancing across brokers

KafScale breaks assumption #1 and #2:
- Multiple broker IDs may map to the same endpoint
- Or broker IDs are abstracted entirely
- Routing is handled by the platform, not the client

**Result:** KafkaJS can get confused trying to route to broker.id = 1 when it only knows about 127.0.0.1:29092

## The Error You Saw

```
KafkaJSBrokerNotFound: Broker 1 not found in the cached metadata
TimeoutNegativeWarning: -1767887275900 is a negative number
```

### What Happened
1. KafkaJS connected to `127.0.0.1:29092` ✅
2. Received metadata referencing `broker.id = 1` ✅
3. Tried to route request to broker.id = 1 ❌
4. Could not resolve broker.id → endpoint mapping ❌
5. Crashed with `BrokerNotFound` error

### Why Java Clients Often Work
The official Kafka Java client:
- More tolerant of broker indirection
- Re-resolves metadata aggressively
- Has defensive fallback logic
- Better handles broker ID inconsistencies

KafkaJS is stricter and expects traditional Kafka behavior.

## Our Solution: KafScale-Compatible Configuration

See [src/kafka.js](src/kafka.js) for the complete implementation.

### Key Configuration Changes

#### 1. Frequent Metadata Refresh
```javascript
metadataMaxAge: 30000 // 30 seconds
```
Forces frequent metadata updates to handle KafScale's dynamic broker model.

#### 2. Conservative Retry Logic
```javascript
retry: {
  retries: 10,
  initialRetryTime: 300,
  maxRetryTime: 30000,
  multiplier: 2,
  factor: 0.2
}
```
Handles transient broker routing issues gracefully.

#### 3. Extended Timeouts
```javascript
connectionTimeout: 10000,
requestTimeout: 30000
```
Gives KafScale time to route requests through its abstraction layer.

#### 4. Single-Partition Concurrency
```javascript
// In consumer config
sessionTimeout: 30000,
rebalanceTimeout: 60000,
heartbeatInterval: 3000
```
Works best with KafScale's single logical broker model.

#### 5. Silenced Warnings
```javascript
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1'
```
Removes noise from KafkaJS v2 partitioner changes.

## Recommended Topic Configuration

For KafScale, use **fewer partitions** than you would with Apache Kafka:

```bash
# Good for KafScale
kafka-topics --create --topic agent_requests --partitions 3

# Overkill for KafScale (but works)
kafka-topics --create --topic agent_requests --partitions 50
```

Why? KafScale scales horizontally without needing many partitions. The platform handles distribution internally.

## Testing KafScale Compatibility

### Run the E2E Test
```bash
# Terminal 1: Start agent
make run-agent

# Terminal 2: Run test
make test-e2e
```

The test verifies:
- ✅ Producer can connect and send messages
- ✅ Consumer can subscribe and receive messages
- ✅ Full workflow completes end-to-end
- ✅ No broker routing errors
- ✅ Responses are properly correlated

### Expected Output
```
🧪 E2E Test: Agent Workflow
==================================================

📡 Connecting to Kafka...
✓ Connected

📥 Subscribing to: agent_responses
✓ Subscribed

📤 Sending test task...
   Correlation ID: abc-123-def
   Topic: agent_requests
✓ Task sent

⏳ Waiting for agent to process...

✓ Response received!
   Correlation ID: abc-123-def
   Time elapsed: 1234ms
   Has result: true
   Has error: false

📄 Result preview:
──────────────────────────────────────────────────
LLM RESPONSE (stub):
Prompt received and processed...
──────────────────────────────────────────────────

==================================================
✅ E2E TEST PASSED
==================================================
```

## Architecture Recommendations

### ✅ DO: Treat KafScale as Single Logical Broker
```javascript
// Client connects to one endpoint
brokers: ['127.0.0.1:29092']

// Platform handles scaling internally
// You don't need to manage broker IDs
```

### ✅ DO: Use Fewer Partitions
```bash
# 3 partitions is plenty
--partitions 3
```

### ✅ DO: Rely on Platform Scaling
Let KafScale handle horizontal scaling internally. You get "infinite brokers" behind one address.

### ❌ DON'T: Assume Multi-Broker Client Patterns
```javascript
// Don't try to route to specific broker IDs
// Don't assume 1:1 mapping of broker ID → endpoint
// Don't use rack-aware clients
```

### ❌ DON'T: Over-Partition Topics
You don't need 50+ partitions. KafScale scales differently than Apache Kafka.

## Client Compatibility Matrix

| Client | KafScale Compatible | Notes |
|--------|---------------------|-------|
| **KafkaJS** (this demo) | ✅ Yes* | Requires configuration tuning (see above) |
| **Kafka Java Client** | ✅ Yes | Usually works out of the box |
| **librdkafka** | ✅ Yes | C/C++/Python/Go - generally compatible |
| **confluent-kafka-python** | ✅ Yes | Built on librdkafka |
| **kafka-go** | ⚠️ Maybe | Depends on implementation |

*This demo includes all necessary configuration for KafScale compatibility.

## Debugging Tips

### If you see `BrokerNotFound` errors:

1. **Check metadata age:**
   ```javascript
   metadataMaxAge: 30000 // Increase if needed
   ```

2. **Verify single endpoint:**
   ```json
   {
     "brokers": ["127.0.0.1:29092"]
   }
   ```
   One address only!

3. **Enable debug logging:**
   ```bash
   KAFKA_LOG_LEVEL=5 make run-agent
   ```

4. **Test connectivity:**
   ```bash
   make check-kafka
   ```

### If you see timeout warnings:

This is usually metadata inconsistency. The fix:
- Increase `metadataMaxAge`
- Increase retry times
- Verify broker configuration

### If messages don't arrive:

1. **Check topics exist:**
   ```bash
   make topics
   ```

2. **Run E2E test:**
   ```bash
   make test-e2e
   ```

3. **Check agent is running:**
   ```bash
   # Should see: "🎧 Agent started, waiting for tasks..."
   make run-agent
   ```

## Summary

**The key insight:** KafScale is "one broker IP, infinite scaling behind it."

KafkaJS needs to be configured to treat it as such:
- Frequent metadata refresh
- Conservative timeouts
- Single-broker semantics

With these changes, KafkaJS works perfectly with KafScale! ✅

## Further Reading

- [KafkaJS Documentation](https://kafka.js.org/)
- [KafScale Architecture](../../README.md)
- [Our E2E Test](scripts/e2e-test.js)
- [Our Kafka Config](src/kafka.js)
