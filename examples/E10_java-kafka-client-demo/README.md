# Java Kafka Client Demo (E10)

This example demonstrates a basic Java application using the standard `kafka-clients` library to interact with a KafScale cluster.

## Overview

The `SimpleDemo` application performs:
1. **Admin**: checks if a topic exists and creates it if needed (`demo-topic-1`).
2. **Producer**: sends messages to the topic.
3. **Admin**: prints cluster metadata (cluster ID, controller, nodes).
4. **Consumer**: consumes a bounded number of messages or times out.

## Prerequisites

-   Java 17+
-   Maven 3.6+
-   A running KafScale cluster (e.g., `make demo` from the repo root)

## Step 1: Start KafScale

Make sure your KafScale cluster is running (e.g., via `make demo` in the repo root).

## Step 2: Configure (optional)

The demo supports env vars or CLI args (CLI takes precedence):

- `KAFSCALE_BOOTSTRAP_SERVERS` / `--bootstrap=...`
- `KAFSCALE_TOPIC` / `--topic=...`
- `KAFSCALE_PRODUCE_COUNT` / `--produce-count=...`
- `KAFSCALE_CONSUME_COUNT` / `--consume-count=...`
- `KAFSCALE_CONSUME_TIMEOUT_MS` / `--consume-timeout-ms=...`
- `KAFSCALE_ACKS` / `--acks=...`
- `KAFSCALE_ENABLE_IDEMPOTENCE` / `--idempotence=true|false`
- `KAFSCALE_RETRIES` / `--retries=...`
- `KAFSCALE_TIMEOUT_MS` / `--timeout-ms=...`
- `KAFSCALE_GROUP_ID` / `--group-id=...`

## Step 3: Run the demo

Run the application using Maven:

```bash
mvn clean package exec:java
```

Example with CLI overrides:

```bash
mvn clean package exec:java -Dexec.args="--bootstrap=127.0.0.1:39092 --topic=demo-topic-1 --produce-count=10 --consume-count=10 --acks=all --idempotence=true"
```

## Step 4: Verify output

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

## Info: Shutdown sequence

When the demo finishes or hits the timeout, it closes the Kafka consumer. The client logs a normal shutdown sequence:
partition revocation, LeaveGroup request, coordinator reset, and network disconnect. This is expected and indicates
the consumer is shutting down cleanly, not failing.

## Limitations

- Producer defaults to `acks=0` and idempotence disabled, so delivery guarantees are minimal unless overridden.
- Consumer uses a random group ID by default, so offsets are not persisted unless configured.
- No security (TLS/SASL) or schema evolution is demonstrated.
- This is a single-process demo; no resiliency or horizontal scaling is covered.

## Next Level Extensions

- Provide a config file and proper CLI parsing library.
- Switch defaults to `acks=all` with idempotence enabled for production-style runs.
- Add serializers (e.g., JSON/Avro/Protobuf) and a schema registry example.
- Add consumer group management and committed offsets.
