# Flink KafScale Word Count Demo (E30)

This demo runs a Flink-based word count over Kafka records, counting words separately for headers, keys, and values. It also tracks missing fields in stats (`no-key`, `no-header`, `no-value`).

## Prerequisites

- Java 11+ (job jar must target Java 11 for Flink 1.18)
- Docker (for Flink standalone runtime)
- KafScale local demo or platform demo

## Profiles

The demo uses lightweight profiles to select the broker address.

- `default`: local broker on `localhost:39092`
- `cluster`: in-cluster broker at `kafscale-broker:9092`
- `local-lb`: local app + remote broker via LB/port-forward on `localhost:59092`

Set the profile with `KAFSCALE_PROFILE` or `--profile=...`.

For container runs, configs are loaded from `KAFSCALE_CONFIG_DIR` (default: `/app/config`) and can override the jar defaults.

## Step 1: Run locally with the make demo setup

Start the local demo:

```bash
make demo
```

In a new terminal, run the Flink job (builds the jar with Java 11 and submits via Docker Flink):

```bash
cd examples/E30_flink-kafscale-demo
make up
make status
make submit
```

If your KafScale broker is running on the host (from `make demo`), submit with the local helper:

```bash
make submit-local
```

### One-command local flow

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

The job listens on `demo-topic-1` by default. Override with `KAFSCALE_TOPIC` if needed.

## Step 2: Run inside the kind cluster

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

## Output format

The job prints running counts:

```
header | authorization => 5
key | order => 12
value | widget => 9
stats | no-key => 3
```

## Verify the job

- Flink UI: `http://localhost:8081`
- REST check: `curl http://localhost:8081/overview`
- List jobs: `docker run --rm --network flink-net flink:1.18.1-scala_2.12 flink list -m flink-jobmanager:8081`
