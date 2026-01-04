# Java Kafka Client Demo (E10)

This example demonstrates a basic Java application using the standard `kafka-clients` library to interact with a KafScale cluster.

## Overview

The `SimpleDemo` application demonstrates the full Kafka client lifecycle:

1. **List Topics**: Displays all existing topics in the cluster
2. **Create Topic**: Checks if `demo-topic-1` exists and creates it if needed (1 partition, replication factor 1)
3. **Produce Messages**: Sends configurable number of messages (default: 25) with keys `key-0`, `key-1`, etc.
4. **Show Cluster Metadata**: Prints cluster ID, controller info, and node details
5. **Consume Messages**: Reads configurable number of messages (default: 5) with auto-offset-reset to `earliest`

## Quick Start

```bash
# 1. Start KafScale cluster from repo root
make demo

# 2. Run the demo
cd examples/E10_java-kafka-client-demo
mvn clean package exec:java
```

## Prerequisites

-   Java 17+
-   Maven 3.6+
-   A running KafScale cluster

## Configuration (Optional)

CLI args take precedence over environment variables:

### Connection & Topic
- `--bootstrap=...` (env: `KAFSCALE_BOOTSTRAP_SERVERS`, default: `127.0.0.1:39092`)
- `--topic=...` (env: `KAFSCALE_TOPIC`, default: `demo-topic-1`)
- `--timeout-ms=...` (env: `KAFSCALE_TIMEOUT_MS`, default: `2000`)

### Producer Settings
- `--produce-count=...` (env: `KAFSCALE_PRODUCE_COUNT`, default: `25`)
- `--acks=...` (env: `KAFSCALE_ACKS`, default: `0`)
- `--idempotence=true|false` (env: `KAFSCALE_ENABLE_IDEMPOTENCE`, default: `false`)
- `--retries=...` (env: `KAFSCALE_RETRIES`, default: `3`)

### Consumer Settings
- `--consume-count=...` (env: `KAFSCALE_CONSUME_COUNT`, default: `5`)
- `--consume-timeout-ms=...` (env: `KAFSCALE_CONSUME_TIMEOUT_MS`, default: `10000`)
- `--group-id=...` (env: `KAFSCALE_GROUP_ID`, default: random UUID)

### Example with Custom Config

```bash
mvn clean package exec:java -Dexec.args="--bootstrap=127.0.0.1:39092 --topic=demo-topic-1 --produce-count=10 --consume-count=10 --acks=all --idempotence=true"
```

## Expected Output

You should see logs indicating the progression of the demo:

```text
[INFO] Starting SimpleDemo...
[INFO] Topic demo-topic-1 already exists.
[INFO] Sent message: key=key-0 value=message-0 partition=0 offset=0
...
[INFO] Cluster Metadata:
[INFO]   Cluster ID: ...
[INFO]   Nodes (Advertised Listeners): ...
[INFO] Received message: key=key-0 value=message-0 partition=0 offset=0
...
[INFO] Successfully consumed 5 messages.
```

## Troubleshooting

### Normal Shutdown Messages
When the demo finishes or times out, you'll see consumer shutdown logs including:
- Partition revocation
- LeaveGroup request
- Coordinator reset
- Network disconnect

**This is expected behavior** indicating clean shutdown, not failure.

### Common Issues

**"Timed out waiting for messages"**
- The consumer timeout expired before receiving all messages. Increase `--consume-timeout-ms` or reduce `--produce-count`.

**"Topic does not exist" errors**
- The demo auto-creates the topic. If using a restricted Kafka setup, create the topic manually first.

**Connection refused**
- Verify KafScale is running on the configured bootstrap server address (default: `127.0.0.1:39092`).

**Only 5 messages consumed but 25 produced**
- By design. Set `--consume-count=25` to consume all produced messages.

## Limitations & Considerations

- **Weak delivery guarantees**: Producer defaults to `acks=0` and idempotence disabled. For production use, set `--acks=all --idempotence=true`.
- **No offset persistence**: Consumer uses a random group ID by default. Provide a fixed `--group-id` to persist offsets across runs.
- **Message mismatch**: By default, 25 messages are produced but only 5 are consumed. Adjust `--consume-count=25` to read all messages.
- **No security**: TLS/SASL authentication not demonstrated.
- **No schema evolution**: Uses simple string serialization only.
- **Single-process**: No resiliency, rebalancing, or horizontal scaling demonstrated.

## Alternative: Running the Shaded JAR

The Maven shade plugin creates a standalone executable JAR:

```bash
mvn clean package
java -jar target/java-kafka-client-demo-1.0-SNAPSHOT.jar --bootstrap=127.0.0.1:39092 --acks=all
```

## Next Level Extensions

- **Config file support**: Replace simple CLI parsing with a proper library (e.g., JCommander, picocli)
- **Production defaults**: Switch to `acks=all` with idempotence enabled by default
- **Schema evolution**: Add JSON/Avro/Protobuf serializers with Schema Registry integration
- **Offset management**: Demonstrate manual offset commits and consumer group coordination
- **Monitoring**: Add metrics collection (JMX, Micrometer) and observability
- **Error handling**: Implement retry logic, dead letter queues, and circuit breakers
