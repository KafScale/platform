# Flink KafScale Word Count Demo (E30)

This demo runs a Flink-based word count over Kafka records, counting words separately for headers, keys, and values. It also tracks missing fields in stats (`no-key`, `no-header`, `no-value`).

## Prerequisites

- Java 11+ (job jar must target Java 11 for Flink 1.18)
- Docker (optional; only for Docker or kind modes)
- KafScale local demo or platform demo

## Profiles

The demo uses lightweight profiles to select the broker address.

- `default`: local broker on `localhost:39092`
- `cluster`: in-cluster broker at `kafscale-broker:9092`
- `local-lb`: local app + remote broker via LB/port-forward on `localhost:59092`

Set the profile with `KAFSCALE_SETUP_PROFILE`, `KAFSCALE_PROFILE`, or `--profile=...`.

For container runs, configs are loaded from `KAFSCALE_CONFIG_DIR` (default: `/app/config`) and can override the jar defaults.

## Step 1: Start the local demo (for local runs)

Start the local demo:

```bash
make demo
```

The job listens on `demo-topic-1` by default. Override with `KAFSCALE_TOPIC` if needed.

## Step 2: Choose a run mode

### Option A: Standalone (no Docker)

Run the job directly from this repo (no Docker):

```bash
cd examples/E30_flink-kafscale-demo
make run-jar-standalone
```

Or use the helper script (includes the local demo + seed data):

```bash
./scripts/run-standalone-local.sh
```

This uses the local Web UI port from `kafscale.flink.rest.port` (default `8091`).

### Option B: Flink standalone in Docker

Bring up Flink in Docker and submit the job:

```bash
cd examples/E30_flink-kafscale-demo
make up
make status
make submit
```

Use `make submit-local` if your broker is on the host (`make demo`).

### One-command local flow (Docker)

Use the helper script to start `make demo`, bring up Flink standalone, and submit the job:

```bash
./scripts/run-docker-local.sh
```

Set `KEEP_DEMO=1` to keep the demo running after the job is submitted.
Set `FLINK_JOB_ONLY=1` to skip `make demo` and only submit to Flink.
Set `BUILD_JAR=1` to rebuild the jar layer when needed.
Override the submission target or profile like this:

```bash
FLINK_JOB_ONLY=1 BUILD_JAR=1 KAFSCALE_SETUP_PROFILE=default SUBMIT_TARGET=submit-local ./scripts/run-docker-local.sh
```

### Option C: Run inside the kind cluster

Build the container and load it into kind:

```bash
cd examples/E30_flink-kafscale-demo
docker build -t ghcr.io/novatechflow/kafscale-flink-demo:dev .
kind load docker-image ghcr.io/novatechflow/kafscale-flink-demo:dev --name kafscale-demo
```

Deploy the job into the cluster:

```bash
kubectl apply -f deploy/demo/flink-wordcount-app.yaml
```

Follow the logs:

```bash
kubectl -n kafscale-demo logs deployment/flink-wordcount-app -f
```

Clean up:

```bash
kubectl -n kafscale-demo delete deployment flink-wordcount-app
```

### One-command k8s flow

Use the helper script to run `make demo-guide-pf`, build/load the image, and deploy:

```bash
./scripts/run-k8s-stack.sh
```

Skip the image build and reuse an existing image:

```bash
SKIP_BUILD=1 ./scripts/run-k8s-stack.sh
```

## Configuration

You can override defaults with environment variables:

- `KAFSCALE_BOOTSTRAP_SERVERS`
- `KAFSCALE_TOPIC`
- `KAFSCALE_GROUP_ID`
- `KAFSCALE_STARTING_OFFSETS` (`latest` or `earliest`)
- `KAFSCALE_FLINK_REST_PORT`
- `KAFSCALE_COMMIT_ON_CHECKPOINT`
- `KAFSCALE_CONSUMER_MAX_POLL_INTERVAL_MS`
- `KAFSCALE_CONSUMER_MAX_POLL_RECORDS`
- `KAFSCALE_CONSUMER_SESSION_TIMEOUT_MS`
- `KAFSCALE_CONSUMER_HEARTBEAT_INTERVAL_MS`
- `KAFSCALE_CHECKPOINT_INTERVAL_MS`
- `KAFSCALE_CHECKPOINT_MIN_PAUSE_MS`
- `KAFSCALE_CHECKPOINT_TIMEOUT_MS`
- `KAFSCALE_CHECKPOINT_DIR`
- `KAFSCALE_STATE_BACKEND` (`hashmap` or `rocksdb`)
- `KAFSCALE_RESTART_ATTEMPTS`
- `KAFSCALE_RESTART_DELAY_MS`
- `KAFSCALE_SINK_ENABLED`
- `KAFSCALE_SINK_TOPIC`
- `KAFSCALE_SINK_ENABLE_IDEMPOTENCE`
- `KAFSCALE_SINK_DELIVERY_GUARANTEE` (`none` or `at-least-once`)

## Output format

The job prints running counts:

```
header | authorization => 5
key | order => 12
value | widget => 9
stats | no-key => 3
```

The counts are also written to Kafka when `KAFSCALE_SINK_ENABLED=true` (default). The default sink topic is
`demo-topic-1-counts`. For KafScale compatibility, the default delivery guarantee is `none`, commits on checkpoint
are disabled, and idempotence is off. The default consumer settings are tuned for fewer rebalances (larger poll
interval, smaller batch size).

## Verify the job

- Standalone local UI: `http://localhost:8091` (or `KAFSCALE_FLINK_REST_PORT`)
- Docker UI: `http://localhost:8081`
- REST check (local): `curl http://localhost:8091/overview`
- REST check (Docker): `curl http://localhost:8081/overview`
- List jobs (Docker): `docker run --rm --network flink-net flink:1.18.1-scala_2.12 flink list -m flink-jobmanager:8081`

## Limitations

- Kafka sink is simple and not partitioned by key (demo only).
- Starting offsets are limited to `latest`/`earliest` (no committed offsets or timestamps).
- Preflight AdminClient requires topic/cluster metadata access; restricted clusters may fail the job.
- Local Web UI env is used for the standalone Java run; production clusters should use a remote environment.

## Next Level Extensions

- Add checkpoints and a state backend (RocksDB) plus savepoint workflows.
- Add a partitioned Kafka sink or a database sink for long-term storage.
- Add windowing and watermark strategies for time-based metrics.
- Deploy via Flink Kubernetes Operator with proper configs and secrets.
