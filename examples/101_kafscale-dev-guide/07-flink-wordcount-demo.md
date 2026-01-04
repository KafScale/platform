# Flink Word Count Demo (E30)

This section adds a Flink-based word count job that consumes from KafScale and keeps separate counts for headers, keys, and values. It also tracks `no-key`, `no-header`, and `no-value` stats.

**What you'll learn**:
- How Apache Flink consumes from KafScale using the Kafka connector
- Stateful stream processing with keyed state for word counts
- Deploying Flink jobs in standalone, Docker, and Kubernetes modes
- Handling common Flink-Kafka integration issues (idempotence, offset commits)

> **Prerequisites**:
> - Java 11+ and Maven 3.6+
> - KafScale running via `make demo` (from [Chapter 2](02-quick-start.md) or restart now)

## Step 1: Run locally (make demo)

Start the local demo:

```bash
make demo
```

## Step 2: Run the Flink job

### Option A: Standalone (no Docker)

Run the job directly from this repo:

```bash
cd examples/E30_flink-kafscale-demo
make run-jar-standalone
```

Or use the helper script (starts `make demo`, seeds data, runs the job):

```bash
./scripts/run-standalone-local.sh
```

The local Web UI is available at `http://localhost:8091` by default.

### Option B: Flink standalone in Docker

Build the jar and submit via Docker Flink:

```bash
cd examples/E30_flink-kafscale-demo
make up
make status
make submit
```

If your KafScale broker is local (from `make demo`), submit with:

```bash
make submit-local
```

### One-command local flow (Docker)

```bash
./scripts/run-docker-local.sh
```

Set `KEEP_DEMO=1` to keep the local demo running.
Set `FLINK_JOB_ONLY=1` to skip `make demo` and only submit to Flink.
Set `BUILD_JAR=1` to rebuild the jar layer when needed.

By default it listens on `demo-topic-1` via `localhost:39092`.

## Step 3: Run inside the kind cluster

Build and load the container:

```bash
cd examples/E30_flink-kafscale-demo
docker build -t ghcr.io/novatechflow/kafscale-flink-demo:dev .
kind load docker-image ghcr.io/novatechflow/kafscale-flink-demo:dev --name kafscale-demo
```

Deploy into the cluster:

```bash
kubectl apply -f deploy/demo/flink-wordcount-app.yaml
```

Follow logs:

```bash
kubectl -n kafscale-demo logs deployment/flink-wordcount-app -f
```

Clean up:

```bash
kubectl -n kafscale-demo delete deployment flink-wordcount-app
```

### One-command k8s flow

```bash
./scripts/run-k8s-stack.sh
```

Skip the image build and reuse an existing image:

```bash
SKIP_BUILD=1 ./scripts/run-k8s-stack.sh
```

## Profiles and listener note

The Flink job uses the same three profiles as the Spring Boot app:

- `default`: local broker on `localhost:39092`
- `cluster`: in-cluster broker at `kafscale-broker:9092`
- `local-lb`: local app + remote broker via `localhost:59092`

Set the profile with `KAFSCALE_SETUP_PROFILE`, `KAFSCALE_PROFILE`, or `--profile=...`.

> **Note:** The demo exposes only a single listener, so pick one network context at a time.

## Output format

You will see running counts like:

```
header | authorization => 5
key | order => 12
value | widget => 9
stats | no-key => 3
```
