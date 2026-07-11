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

# Mixed Client Test Guide

This guide helps isolate issues by testing producer and consumer separately, mixing KafkaJS with native Kafka tools (kcat/kafka-console-*).

## Why Mixed Tests?

By mixing clients, we can determine:
- ✅ Is the **producer** working? (KafkaJS produce → kcat consume)
- ✅ Is the **consumer** working? (kcat produce → KafkaJS consume)
- ✅ Is the **broker** handling both directions?

This narrows down where the problem is.

---

## Test 1: KafkaJS Producer → kcat Consumer

**Purpose:** Test if KafkaJS can produce messages that native Kafka tools can read.

### Terminal 1: Start kcat consumer
```bash
kcat -b 127.0.0.1:39092 -t agent_requests -C
```

Alternative with kafka-console-consumer:
```bash
kafka-console-consumer \
  --bootstrap-server 127.0.0.1:39092 \
  --topic agent_requests \
  --from-beginning
```

### Terminal 2: Run KafkaJS producer test
```bash
node scripts/test-producer-only.js
```

### Expected Result
✅ **SUCCESS:** Terminal 1 shows the message

```json
{"task":"Test task from KafkaJS producer","spec":"This is a test message",...}
```

❌ **FAILURE:** No message appears in Terminal 1
- **Diagnosis:** KafkaJS producer cannot write to KafScale
- **Check:** Producer logs for errors

---

## Test 2: kcat Producer → KafkaJS Consumer

**Purpose:** Test if KafkaJS can consume messages from native Kafka producers.

### Terminal 1: Start KafkaJS consumer
```bash
node scripts/test-consumer-only.js
```

Should show:
```
🎧 Waiting for messages...
```

### Terminal 2: Send message with kcat
```bash
echo '{"test":"hello from kcat","timestamp":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}' | \
  kcat -b 127.0.0.1:39092 -t agent_requests -P
```

Alternative with kafka-console-producer:
```bash
echo '{"test":"hello from kafka-console"}' | \
  kafka-console-producer \
    --bootstrap-server 127.0.0.1:39092 \
    --topic agent_requests
```

### Expected Result
✅ **SUCCESS:** Terminal 1 shows:

```
📨 Message #1 received
──────────────────────────────────────────────────
Topic: agent_requests
Value: {"test":"hello from kcat","timestamp":"2026-01-08T..."}
```

❌ **FAILURE:** No message appears in Terminal 1
- **Diagnosis:** KafkaJS consumer cannot read from KafScale
- **Check:** Consumer logs for errors

---

## Test 3: kcat Producer → kcat Consumer

**Purpose:** Verify broker works with native tools (baseline test).

### Terminal 1: Start kcat consumer
```bash
kcat -b 127.0.0.1:39092 -t agent_requests -C
```

### Terminal 2: Send with kcat producer
```bash
echo '{"test":"native to native"}' | \
  kcat -b 127.0.0.1:39092 -t agent_requests -P
```

### Expected Result
✅ **SUCCESS:** Message appears immediately

❌ **FAILURE:** No message
- **Diagnosis:** Broker itself is broken
- **Action:** Check KafScale broker logs

---

## Interpreting Results

### Scenario A: All Tests Pass ✅
- KafkaJS producer works
- KafkaJS consumer works
- Broker is healthy
- **Issue:** Likely in agent logic or E2E test coordination

### Scenario B: Test 1 Fails, Test 2 Passes
- ❌ KafkaJS producer broken
- ✅ KafkaJS consumer works
- **Issue:** Producer configuration or broker's produce API

### Scenario C: Test 1 Passes, Test 2 Fails
- ✅ KafkaJS producer works
- ❌ KafkaJS consumer broken
- **Issue:** Consumer configuration or broker's fetch API

### Scenario D: Test 1 & 2 Fail, Test 3 Passes
- ❌ KafkaJS producer broken
- ❌ KafkaJS consumer broken
- ✅ Native tools work
- **Issue:** KafkaJS incompatibility with KafScale (protocol mismatch)

### Scenario E: All Tests Fail ❌
- Broker is completely broken
- **Action:** Restart/fix KafScale broker

---

## Quick Commands

### Install kcat (if needed)
```bash
# macOS
brew install kcat

# Linux
apt-get install kafkacat  # or kcat
```

### Check broker is listening
```bash
lsof -i :39092
# or
nc -zv 127.0.0.1 39092
```

### List topics
```bash
kcat -b 127.0.0.1:39092 -L
```

### Consume from beginning
```bash
kcat -b 127.0.0.1:39092 -t agent_requests -C -o beginning
```

### Produce with key
```bash
echo 'key1:{"value":"test"}' | kcat -b 127.0.0.1:39092 -t agent_requests -P -K:
```

---

## Adding to Makefile

You can add shortcuts to the Makefile:

```makefile
# Test KafkaJS producer (use with kcat consumer)
test-producer:
	@node scripts/test-producer-only.js

# Test KafkaJS consumer (use with kcat producer)
test-consumer:
	@node scripts/test-consumer-only.js

# Quick kcat consumer
kcat-consume:
	@kcat -b 127.0.0.1:39092 -t agent_requests -C

# Quick kcat producer (interactive)
kcat-produce:
	@echo "Type messages (Ctrl+D to finish):"
	@kcat -b 127.0.0.1:39092 -t agent_requests -P
```

---

## Debugging Tips

### Enable verbose logging

**KafkaJS:**
```bash
KAFKA_LOG_LEVEL=5 node scripts/test-producer-only.js
```

**kcat:**
```bash
kcat -b 127.0.0.1:39092 -t agent_requests -C -vvv
```

### Check message count
```bash
kcat -b 127.0.0.1:39092 -t agent_requests -C -e -o beginning | wc -l
```

### View topic metadata
```bash
kcat -b 127.0.0.1:39092 -t agent_requests -L
```

---

## Summary

Mixed tests help isolate the problem:

| Test | Producer | Consumer | What it tests |
|------|----------|----------|---------------|
| Test 1 | KafkaJS | kcat | KafkaJS → Broker write path |
| Test 2 | kcat | KafkaJS | Broker → KafkaJS read path |
| Test 3 | kcat | kcat | Broker baseline (sanity check) |

Run all three to pinpoint the exact failure point! 🎯
