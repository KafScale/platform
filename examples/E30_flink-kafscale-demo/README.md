# Flink KafScale Word Count Demo (E30)

A streaming word count application using Apache Flink 1.18 to process Kafka messages from KafScale. Counts words from message headers, keys, and values separately, tracking statistics for missing fields.

## Quick Start (Standalone Mode)

```bash
# 1. Start KafScale from repo root
cd ../..
make demo

# 2. Run the Flink job locally (with Web UI at http://localhost:8091)
cd examples/E30_flink-kafscale-demo
./scripts/run-standalone-local.sh
```

The job will:
- Consume from `demo-topic-1`
- Count words in headers, keys, and values
- Print running counts to console
- Expose Flink Web UI at [http://localhost:8091](http://localhost:8091)

## What This Demo Does

This example demonstrates:
- **Flink Kafka Source**: Consuming from KafScale using Flink's Kafka connector
- **Stateful stream processing**: Maintaining running word counts with keyed state
- **Multiple field parsing**: Extracting and counting words from headers, keys, and values
- **Kafka sink**: Writing aggregated counts back to Kafka (optional, enabled by default)
- **Deployment flexibility**: Run standalone, in Docker, or on Kubernetes

## Prerequisites

- Java 11+ (Flink 1.18 requires Java 11 bytecode)
- Maven 3.6+
- Docker (optional; only needed for Docker or Kubernetes modes)
- KafScale running locally or in a cluster

## Deployment Modes

Choose your deployment mode based on your environment:

### Mode 1: Standalone Java (Recommended for Getting Started)

**Best for:** Local development and testing
**Flink Web UI:** [http://localhost:8091](http://localhost:8091)

**One-command setup:**
```bash
./scripts/run-standalone-local.sh
```

**Manual steps:**
```bash
# Build and run the JAR directly
make run-jar-standalone
```

**What it does:**
- Starts KafScale with `make demo`
- Builds the job JAR
- Runs Flink in local mode with embedded Web UI
- Seeds sample data to `demo-topic-1`

### Mode 2: Docker Standalone Cluster

**Best for:** Testing Flink cluster behavior locally
**Flink Web UI:** [http://localhost:8081](http://localhost:8081)

**One-command setup:**
```bash
./scripts/run-docker-local.sh
```

**Manual steps:**
```bash
# Start Flink JobManager and TaskManager in Docker
make up
make status

# Submit job to Docker Flink cluster
make submit-local  # if broker is on host (make demo)
# OR
make submit        # if broker is in Docker network
```

**Environment variables:**
- `KEEP_DEMO=1` - Keep demo running after job submission
- `FLINK_JOB_ONLY=1` - Skip `make demo`, only submit to Flink
- `BUILD_JAR=1` - Rebuild the JAR layer

### Mode 3: Kubernetes (kind cluster)

**Best for:** Testing production-like deployments
**Flink Logs:** `kubectl logs` commands

**One-command setup:**
```bash
./scripts/run-k8s-stack.sh
```

**Manual steps:**
```bash
# Build and load image into kind
docker build -t ghcr.io/novatechflow/kafscale-flink-demo:dev .
kind load docker-image ghcr.io/novatechflow/kafscale-flink-demo:dev --name kafscale-demo

# Deploy to Kubernetes
kubectl apply -f deploy/demo/flink-wordcount-app.yaml

# View logs
kubectl -n kafscale-demo logs deployment/flink-wordcount-app -f

# Clean up
kubectl -n kafscale-demo delete deployment flink-wordcount-app
```

**Environment variables:**
- `SKIP_BUILD=1` - Reuse existing Docker image

## Configuration

All settings can be overridden via environment variables:

### Connection & Topics
- `KAFSCALE_BOOTSTRAP_SERVERS` - Kafka bootstrap servers (default: `localhost:39092`)
- `KAFSCALE_TOPIC` - Input topic (default: `demo-topic-1`)
- `KAFSCALE_GROUP_ID` - Consumer group ID (default: `flink-wordcount-group`)
- `KAFSCALE_STARTING_OFFSETS` - Offset reset: `latest` or `earliest` (default: `latest`)

### Flink Runtime
- `KAFSCALE_FLINK_REST_PORT` - Flink Web UI port (default: `8091`)
- `KAFSCALE_STATE_BACKEND` - State backend: `hashmap` or `rocksdb` (default: `hashmap`)
- `KAFSCALE_RESTART_ATTEMPTS` - Max restart attempts (default: `3`)
- `KAFSCALE_RESTART_DELAY_MS` - Delay between restarts (default: `10000`)

### Checkpointing
- `KAFSCALE_COMMIT_ON_CHECKPOINT` - Commit Kafka offsets on checkpoint (default: `false`)
- `KAFSCALE_CHECKPOINT_INTERVAL_MS` - Checkpoint interval (default: `60000`)
- `KAFSCALE_CHECKPOINT_MIN_PAUSE_MS` - Min pause between checkpoints (default: `500`)
- `KAFSCALE_CHECKPOINT_TIMEOUT_MS` - Checkpoint timeout (default: `600000`)
- `KAFSCALE_CHECKPOINT_DIR` - Checkpoint storage directory (default: `file:///tmp/flink-checkpoints`)

### Kafka Consumer Tuning
- `KAFSCALE_CONSUMER_MAX_POLL_INTERVAL_MS` - Max poll interval (default: `600000` - 10 min)
- `KAFSCALE_CONSUMER_MAX_POLL_RECORDS` - Max records per poll (default: `100`)
- `KAFSCALE_CONSUMER_SESSION_TIMEOUT_MS` - Session timeout (default: `60000`)
- `KAFSCALE_CONSUMER_HEARTBEAT_INTERVAL_MS` - Heartbeat interval (default: `20000`)

### Kafka Sink (Output)
- `KAFSCALE_SINK_ENABLED` - Enable Kafka sink (default: `true`)
- `KAFSCALE_SINK_TOPIC` - Output topic (default: `demo-topic-1-counts`)
- `KAFSCALE_SINK_ENABLE_IDEMPOTENCE` - Enable idempotent producer (default: `false`)
- `KAFSCALE_SINK_DELIVERY_GUARANTEE` - Delivery guarantee: `none` or `at-least-once` (default: `none`)

### Profile Selection
- `KAFSCALE_PROFILE` or `KAFSCALE_SETUP_PROFILE` - Profile: `default`, `cluster`, or `local-lb`
- `KAFSCALE_CONFIG_DIR` - Config directory for container runs (default: `/app/config`)

**Profiles:**
- `default` - Local broker on `localhost:39092`
- `cluster` - In-cluster broker at `kafscale-broker:9092`
- `local-lb` - Remote broker via load balancer on `localhost:59092`

## Expected Output

### Console Output

The job prints running word counts grouped by field type:

```
header | authorization => 5
key | order => 12
value | widget => 9
stats | no-key => 3
stats | no-header => 7
stats | no-value => 2
```

**Field types:**
- `header | <word>` - Words found in Kafka message headers
- `key | <word>` - Words found in message keys
- `value | <word>` - Words found in message values
- `stats | no-*` - Counters for messages missing that field

### Kafka Sink Output

When `KAFSCALE_SINK_ENABLED=true` (default), counts are written to Kafka topic `demo-topic-1-counts`.

**Output message format:**
- **Key**: `<field>|<word>` (e.g., `header|authorization`)
- **Value**: Count as string (e.g., `"5"`)

**Sink configuration (KafScale compatible):**
- Delivery guarantee: `none` (no exactly-once, for compatibility)
- Idempotence: `false` (disabled for KafScale)
- Commits on checkpoint: `false` (disabled)

**Consumer tuning (optimized for stability):**
- Max poll interval: `600000ms` (10 minutes - prevents rebalances during slow processing)
- Max poll records: `100` (smaller batches reduce memory pressure)
- Session timeout: `60000ms` (1 minute)
- Heartbeat interval: `20000ms` (20 seconds)

## Monitoring & Verification

### Flink Web UI

**Standalone mode:** [http://localhost:8091](http://localhost:8091)
**Docker mode:** [http://localhost:8081](http://localhost:8081)

The Web UI provides:
- Job status and uptime
- Task metrics and parallelism
- Checkpoint statistics
- Exception history
- Task manager details

### REST API Checks

```bash
# Check Flink cluster status (standalone)
curl http://localhost:8091/overview

# Check Flink cluster status (Docker)
curl http://localhost:8081/overview

# List running jobs (Docker)
docker run --rm --network flink-net flink:1.18.1-scala_2.12 \
  flink list -m flink-jobmanager:8081
```

### Verify Sink Output

```bash
# Check if sink topic was created
docker exec kafscale-broker kafka-topics --list --bootstrap-server localhost:9092

# Consume from sink topic to see counts
docker exec kafscale-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic demo-topic-1-counts \
  --from-beginning \
  --property print.key=true
```

## Troubleshooting

### Job Fails to Start

**Symptom:** `Unable to connect to Kafka broker`

**Fix:**
1. Verify KafScale is running: `docker ps | grep kafscale`
2. Check bootstrap server config matches your profile
3. Test connectivity: `nc -zv localhost 39092`

### No Output in Console

**Symptom:** Job starts but no word counts appear

**Possible causes:**
- **Topic is empty**: Send test messages with the helper script
- **Starting offsets set to `latest`**: Change to `earliest` with `KAFSCALE_STARTING_OFFSETS=earliest`
- **Job hasn't processed records yet**: Check Flink Web UI for records consumed metric

### Consumer Rebalances Frequently

**Symptom:** Logs show `Rebalance triggered` messages

**Fix:** Increase `KAFSCALE_CONSUMER_MAX_POLL_INTERVAL_MS` (default is already 10 minutes, but increase if needed)

### Checkpoint Failures

**Symptom:** `Checkpoint expired before completing`

**Fix:**
- Increase `KAFSCALE_CHECKPOINT_TIMEOUT_MS` (default: 10 minutes)
- Check disk space for checkpoint directory
- Reduce `KAFSCALE_CHECKPOINT_INTERVAL_MS` to checkpoint less frequently

### Docker Network Issues

**Symptom:** Job in Docker can't reach broker on host

**Fix:** Use `make submit-local` instead of `make submit` (uses host network mode)

### Kubernetes Pod CrashLoopBackOff

**Symptom:** Pod restarts repeatedly

**Fix:**
1. Check logs: `kubectl -n kafscale-demo logs deployment/flink-wordcount-app`
2. Verify profile is set to `cluster`: `KAFSCALE_PROFILE=cluster`
3. Ensure KafScale service is accessible: `kubectl -n kafscale-demo get svc kafscale-broker`

## Limitations & Considerations

### Production Readiness Gaps

1. **Weak delivery guarantees**: Sink uses `delivery.guarantee=none` for KafScale compatibility
   - **Impact**: No exactly-once semantics, potential duplicate/lost counts
   - **Fix**: Use `at-least-once` or deploy with a Kafka that supports transactions

2. **Checkpoint commits disabled**: `commit-offsets-on-checkpoint=false` by default
   - **Impact**: Consumer offsets not committed, job restarts from configured starting offset
   - **Fix**: Enable with `KAFSCALE_COMMIT_ON_CHECKPOINT=true` for exactly-once with Kafka

3. **No partitioning strategy**: Sink writes to random partitions
   - **Impact**: No ordering guarantees for same keys
   - **Fix**: Implement custom `KafkaRecordSerializationSchema` with key-based partitioning

4. **Limited offset initialization**: Only supports `latest` or `earliest`
   - **Impact**: Can't resume from specific timestamp or committed offset
   - **Fix**: Extend to support `OffsetsInitializer.committedOffsets()` and `OffsetsInitializer.timestamp()`

5. **Preflight AdminClient check**: Job requires cluster metadata access on startup
   - **Impact**: Restricted Kafka clusters with ACLs may fail
   - **Fix**: Make preflight check optional with env flag

6. **Local Web UI for standalone**: Uses `createLocalEnvironmentWithWebUI()`
   - **Impact**: Not suitable for production remote clusters
   - **Fix**: Use proper Flink cluster (standalone, YARN, K8s) for production

### Architectural Limitations

- **Single parallelism**: Job runs with parallelism=1 for simplicity
- **No windowing**: Counts are global, not time-windowed
- **No watermarks**: No event-time processing or late data handling
- **HashMap state backend**: Default state backend doesn't support large state or savepoints
- **No external storage**: Counts only in Flink state + Kafka sink, no database persistence

## Next Level Extensions

### Reliability & Fault Tolerance
- **RocksDB state backend**: Enable with `KAFSCALE_STATE_BACKEND=rocksdb` for large state support
- **Savepoint workflows**: Implement manual and periodic savepoint triggers
- **Exactly-once delivery**: Configure transactional Kafka sink with idempotent producers
- **Commit on checkpoint**: Enable `KAFSCALE_COMMIT_ON_CHECKPOINT=true` for offset commits

### Processing & Analytics
- **Windowing**: Add tumbling/sliding windows for time-based aggregations (e.g., word counts per minute)
- **Watermarks**: Implement watermark strategies for event-time processing
- **Multiple aggregations**: Track min/max/avg in addition to counts
- **Pattern detection**: Use CEP library for complex event patterns

### Deployment & Operations
- **Flink Kubernetes Operator**: Deploy with FlinkDeployment CRD for auto-scaling and savepoint management
- **External sinks**: Add database sink (PostgreSQL, Cassandra) for long-term storage
- **Metrics & Monitoring**: Expose custom Flink metrics to Prometheus
- **Security**: Add SASL/SSL for Kafka connections, Kerberos authentication

### Scalability
- **Increase parallelism**: Scale to multiple task slots for higher throughput
- **Partitioned sink**: Implement key-based partitioning for ordered writes
- **Async I/O**: Use async Kafka sink for better throughput
- **State TTL**: Configure state time-to-live to prevent unbounded growth
