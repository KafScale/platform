# Spark KafScale Word Count Demo (E40)

This demo runs a Spark Structured Streaming word count over Kafka records, counting words separately for headers, keys, and values. It also tracks missing fields in stats (`no-key`, `no-header`, `no-value`).

## Prerequisites

- Java 11+
- Apache Spark 3.5+ (set `SPARK_HOME` or have `spark-submit` on PATH)
- KafScale local demo or platform demo

## Profiles

The demo uses lightweight profiles to select the broker address.

- `default`: local broker on `127.0.0.1:39092`
- `cluster`: in-cluster broker at `kafscale-broker:9092`
- `local-lb`: local app + remote broker via LB/port-forward on `localhost:59092`

Set the profile with `KAFSCALE_SETUP_PROFILE` or `--profile=...`.

## Step 1: Start the local demo

Start the local demo:

```bash
make demo
```

## Step 2: Run the Spark job

In a new terminal, run:

```bash
cd examples/E40_spark-kafscale-demo
make run-jar-standalone
```

Override profile or Spark master:

```bash
KAFSCALE_SETUP_PROFILE=local-lb SPARK_MASTER=local[2] make run-jar-standalone
```

The job listens on `demo-topic-1` by default. Override with `KAFSCALE_TOPIC` if needed.

## Configuration

You can override defaults with environment variables:

- `KAFSCALE_BOOTSTRAP_SERVERS`
- `KAFSCALE_TOPIC`
- `KAFSCALE_GROUP_ID`
- `KAFSCALE_STARTING_OFFSETS` (`latest` or `earliest`)
- `KAFSCALE_INCLUDE_HEADERS`
- `KAFSCALE_SPARK_UI_PORT`
- `KAFSCALE_CHECKPOINT_DIR`
- `KAFSCALE_FAIL_ON_DATA_LOSS` (`true` or `false`)
- `KAFSCALE_DELTA_ENABLED` (`true` or `false`)
- `KAFSCALE_DELTA_PATH`

## Durable storage (Delta Lake)

By default, checkpoints are stored on the local filesystem. For longer runs or restarts, point the checkpoint
directory to durable storage (e.g., NFS, S3 via a mounted path):

```bash
KAFSCALE_CHECKPOINT_DIR=/mnt/kafscale-spark-checkpoints make run-jar-standalone
```

To write results to Delta Lake, enable the Delta sink:

```bash
KAFSCALE_DELTA_ENABLED=true KAFSCALE_DELTA_PATH=/mnt/kafscale-delta-wordcount make run-jar-standalone
```

When Delta is enabled, the console output is disabled and results are written to the Delta table.

## Handling data loss (Spark offset reset)

If the topic is recreated or offsets are trimmed, Spark can detect missing data and fail with:

```
Partition demo-topic-1-0's offset was changed from 78 to 0
```

You have two options:

1) **Continue (default, demo-friendly)**  
   Keep `kafscale.fail.on.data.loss=false` or set `KAFSCALE_FAIL_ON_DATA_LOSS=false` to allow Spark to continue from the earliest available offsets.

2) **Fail fast (safety)**  
   Set `kafscale.fail.on.data.loss=true` or `KAFSCALE_FAIL_ON_DATA_LOSS=true` to surface missing data.

## Output format

The job prints running counts:

```
header | authorization => 5
key | order => 12
value | widget => 9
stats | no-key => 3
```

## Step 3: Verify the job

- Spark UI: `http://localhost:4040` (or `KAFSCALE_SPARK_UI_PORT`)

## Limitations

- Console sink by default; Delta Lake output is optional and requires a durable checkpoint path.
- Checkpoints are local by default; restarting with the same checkpoint can trigger offset conflicts.
- Setting `kafka.group.id` in Structured Streaming is discouraged for multi-query use.
- Preflight AdminClient requires topic/cluster metadata access; restricted clusters may fail the job.

## Next Level Extensions

- Write output to Kafka, Delta, or Parquet with a durable checkpoint location.
- Add watermarking and windowed aggregations for time-based metrics.
- Add structured logging and metrics integration (Prometheus/Grafana).
- Package as a Spark application for cluster deployment (K8s/YARN).
