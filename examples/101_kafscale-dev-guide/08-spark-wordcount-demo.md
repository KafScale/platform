# Spark Word Count Demo (E40)

This section adds a Spark Structured Streaming word count job that consumes from KafScale and keeps separate counts for headers, keys, and values. It also tracks `no-key`, `no-header`, and `no-value` stats.

**What you'll learn**:
- How Spark Structured Streaming consumes from KafScale
- Micro-batch processing with stateful aggregations
- Using Delta Lake for durable word count storage
- Handling offset changes and data loss scenarios

> **Prerequisites**:
> - Java 11+, Apache Spark 3.5.0 installed (`SPARK_HOME` set or `spark-submit` on PATH)
> - KafScale running via `make demo` (from [Chapter 2](02-quick-start.md) or restart now)

## Step 1: Run locally with the make demo setup

Start the local demo:

```bash
make demo
```

Run the Spark job:

```bash
cd examples/E40_spark-kafscale-demo
make run-jar-standalone
```

Override profile or Spark master:

```bash
KAFSCALE_SETUP_PROFILE=local-lb SPARK_MASTER=local[2] make run-jar-standalone
```

## Profiles

The Spark job uses the same three profiles as the Spring Boot app:

- `default` → `127.0.0.1:39092`
- `cluster` → `kafscale-broker:9092`
- `local-lb` → `localhost:59092`

Set the profile with `KAFSCALE_SETUP_PROFILE` or `--profile=...`.

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

When Delta is enabled, console output is disabled and results are written to the Delta table.

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

## Verify the job

- Spark UI: `http://localhost:4040` (or `KAFSCALE_SPARK_UI_PORT`)
