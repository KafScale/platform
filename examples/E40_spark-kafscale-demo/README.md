# Spark KafScale Word Count Demo (E40)

A streaming word count application using Apache Spark Structured Streaming to process Kafka messages from KafScale. Counts words from message headers, keys, and values separately, tracking statistics for missing fields.

## Quick Start

```bash
# 1. Start KafScale from repo root
cd ../..
make demo

# 2. Run Spark job
cd examples/E40_spark-kafscale-demo
make run-jar-standalone
```

The job will:
- Consume from `demo-topic-1`
- Count words in headers, keys, and values
- Print running counts to console
- Expose Spark UI at [http://localhost:4040](http://localhost:4040)

## What This Demo Does

This example demonstrates:
- **Spark Structured Streaming**: Processing unbounded Kafka streams with micro-batch execution
- **Kafka source integration**: Reading from KafScale using Spark's Kafka connector
- **Stateful aggregations**: Maintaining running word counts across batches
- **Multi-field parsing**: Extracting words from headers, keys, and values
- **Delta Lake sink** (optional): Writing results to Delta tables for analytics

## Prerequisites

- Java 11+
- Apache Spark 3.5.0 (tested version - set `SPARK_HOME` or have `spark-submit` on PATH)
- Maven 3.6+
- KafScale running locally or in a cluster

## Documentation References

- [Spark Structured Streaming + Kafka Integration Guide](https://spark.apache.org/docs/3.5.0/structured-streaming-kafka-integration.html)
- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/3.5.0/structured-streaming-programming-guide.html)

## Running the Job

### Basic Execution

```bash
# Run with defaults (connects to localhost:39092)
make run-jar-standalone
```

### With Custom Profile

```bash
# Use cluster profile (for in-cluster Kafka)
KAFSCALE_SETUP_PROFILE=cluster make run-jar-standalone

# Use local-lb profile (for port-forwarded LB)
KAFSCALE_SETUP_PROFILE=local-lb make run-jar-standalone
```

### With Custom Spark Configuration

```bash
# Override Spark master and profile
KAFSCALE_SETUP_PROFILE=local-lb SPARK_MASTER=local[4] make run-jar-standalone
```

## Configuration

All settings can be overridden via environment variables:

### Connection & Topics
- `KAFSCALE_BOOTSTRAP_SERVERS` - Kafka bootstrap servers (default: `127.0.0.1:39092`)
- `KAFSCALE_TOPIC` - Input topic (default: `demo-topic-1`)
- `KAFSCALE_GROUP_ID` - Consumer group ID (default: `spark-kafscale-wordcount-${random-uuid}`)
- `KAFSCALE_STARTING_OFFSETS` - Offset reset: `latest` or `earliest` (default: `latest`)

### Spark Settings
- `KAFSCALE_INCLUDE_HEADERS` - Include Kafka headers in processing (default: `true`)
- `KAFSCALE_SPARK_UI_PORT` - Spark Web UI port (default: `4040`)
- `KAFSCALE_CHECKPOINT_DIR` - Checkpoint directory (default: `/tmp/kafscale-spark-checkpoints`)
- `KAFSCALE_FAIL_ON_DATA_LOSS` - Fail on offset gaps (default: `false`)

### Delta Lake Output
- `KAFSCALE_DELTA_ENABLED` - Enable Delta sink (default: `false`)
- `KAFSCALE_DELTA_PATH` - Delta table path (default: `/tmp/kafscale-delta-wordcount`)

### Profile Selection
- `KAFSCALE_SETUP_PROFILE` - Profile: `default`, `cluster`, or `local-lb`

**Profiles:**
- `default` - Local broker on `127.0.0.1:39092`
- `cluster` - In-cluster broker at `kafscale-broker:9092`
- `local-lb` - Remote broker via load balancer on `localhost:59092`

## Delta Lake Integration

### Durable Checkpoints

By default, checkpoints are stored in `/tmp/kafscale-spark-checkpoints`. For production or long-running jobs, use durable storage:

```bash
# NFS mount
KAFSCALE_CHECKPOINT_DIR=/mnt/nfs/kafscale-spark-checkpoints make run-jar-standalone

# S3 (with appropriate Spark S3 jars and credentials)
KAFSCALE_CHECKPOINT_DIR=s3a://my-bucket/spark-checkpoints make run-jar-standalone
```

**⚠️ Warning**: Using `/tmp` for checkpoints means:
- Job restarts lose state and restart from configured starting offsets
- Offset conflicts may occur if the topic has changed

### Delta Lake Sink

Enable Delta Lake to persist word counts for analytics:

```bash
KAFSCALE_DELTA_ENABLED=true \
  KAFSCALE_DELTA_PATH=/mnt/nfs/kafscale-delta-wordcount \
  make run-jar-standalone
```

**When Delta is enabled:**
- Console output is disabled
- Results are written to Delta table in append mode
- Table schema: `field STRING, word STRING, count LONG`

**Query Delta table:**
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format("delta").load("/mnt/nfs/kafscale-delta-wordcount")
df.show()
```

## Handling Offset Changes

### The Problem

Spark Structured Streaming tracks Kafka offsets in checkpoints. If the topic is recreated or offsets are trimmed, Spark detects the mismatch:

```
org.apache.spark.sql.streaming.StreamingQueryException:
Partition demo-topic-1-0's offset was changed from 78 to 0
```

This indicates **data loss** - messages between offset 0 and 78 are missing.

### Solutions

**Option 1: Continue (Default - Demo Mode)**

Set `KAFSCALE_FAIL_ON_DATA_LOSS=false` (default) to allow Spark to continue from the earliest available offset:

```bash
KAFSCALE_FAIL_ON_DATA_LOSS=false make run-jar-standalone
```

- **Use when**: Developing, testing, or when gaps are acceptable
- **Impact**: Silently skips missing data, counts may be incomplete

**Option 2: Fail Fast (Production Mode)**

Set `KAFSCALE_FAIL_ON_DATA_LOSS=true` to surface missing data immediately:

```bash
KAFSCALE_FAIL_ON_DATA_LOSS=true make run-jar-standalone
```

- **Use when**: Production jobs where data integrity is critical
- **Impact**: Job fails, requires manual intervention (delete checkpoint or fix topic)

**Option 3: Delete Checkpoint**

If you need to restart from scratch:

```bash
rm -rf /tmp/kafscale-spark-checkpoints
make run-jar-standalone
```

## Expected Output

### Console Output

The job prints running word counts grouped by field type:

```
-------------------------------------------
Batch: 5
-------------------------------------------
+------+--------------+-----+
| field|          word|count|
+------+--------------+-----+
|header|authorization|    5|
|   key|         order|   12|
| value|        widget|    9|
| stats|        no-key|    3|
| stats|     no-header|    7|
| stats|      no-value|    2|
+------+--------------+-----+
```

**Field types:**
- `header` - Words found in Kafka message headers
- `key` - Words found in message keys
- `value` - Words found in message values
- `stats` - Counters for messages missing that field (no-key, no-header, no-value)

## Monitoring & Verification

### Spark Web UI

Access at [http://localhost:4040](http://localhost:4040) (or configured `KAFSCALE_SPARK_UI_PORT`)

**The UI provides:**
- Streaming query status and progress
- Batch processing times and rates
- Input/output metrics (rows read, rows written)
- SQL query plans and DAG visualization
- Executor and storage information

### Key Metrics to Watch

- **Input Rate**: Messages/second consumed from Kafka
- **Process Rate**: Messages/second processed
- **Batch Duration**: Time to process each micro-batch
- **Scheduling Delay**: Time waiting for executor availability

### Check Streaming Status

```bash
# View Spark streaming metrics via REST
curl http://localhost:4040/api/v1/applications
```

## Troubleshooting

### Job Fails to Start

**Symptom:** `Connection refused` or `Failed to construct kafka consumer`

**Fix:**
1. Verify KafScale is running: `docker ps | grep kafscale`
2. Check bootstrap server matches profile (default: `127.0.0.1:39092`)
3. Test connectivity: `nc -zv 127.0.0.1 39092`

### No Output in Console

**Symptom:** Job starts but no word counts appear

**Possible causes:**
- **Topic is empty**: Send test messages to `demo-topic-1`
- **Starting offsets set to `latest`**: Change to `earliest` with `KAFSCALE_STARTING_OFFSETS=earliest`
- **Job hasn't processed a batch yet**: Wait for trigger interval

### Offset Conflict Error

**Symptom:** `Partition X's offset was changed from Y to Z`

**Fix:**
- **Option 1**: Set `KAFSCALE_FAIL_ON_DATA_LOSS=false` to continue
- **Option 2**: Delete checkpoint directory and restart
- **Option 3**: Fix the underlying issue (recreated topic, trimmed offsets)

### Slow Batch Processing

**Symptom:** Batch duration > 10 seconds

**Fix:**
- Increase Spark master parallelism: `SPARK_MASTER=local[8]`
- Reduce micro-batch size (configure trigger interval)
- Check Spark UI for slow stages

### Delta Lake Write Failures

**Symptom:** `Failed to write to Delta table`

**Fix:**
- Verify path is writable: `ls -la /mnt/nfs/kafscale-delta-wordcount`
- Check disk space
- Ensure Delta Lake jars are on classpath

## Limitations & Considerations

### Production Readiness Gaps

1. **No exactly-once semantics**: Spark Structured Streaming doesn't guarantee exactly-once end-to-end processing
   - **Impact**: Potential duplicate word counts on job restarts
   - **Fix**: Implement idempotent downstream processing or use Delta Lake merge operations
   - **Reference**: [Spark Streaming Fault Tolerance](https://spark.apache.org/docs/3.5.0/streaming-programming-guide.html#fault-tolerance-semantics)

2. **Local checkpoints by default**: Checkpoint directory defaults to `/tmp/kafscale-spark-checkpoints`
   - **Impact**: Job restarts lose state, offsets reset to configured starting position
   - **Fix**: Use durable storage (NFS, S3, HDFS) via `KAFSCALE_CHECKPOINT_DIR`

3. **Console sink limitations**: Default output goes to stdout
   - **Impact**: No persistence, no queryable history, difficult to monitor
   - **Fix**: Enable Delta Lake sink with `KAFSCALE_DELTA_ENABLED=true`

4. **Consumer group ID behavior**: Explicit `group.id` setting can interfere with Spark's internal offset management
   - **Impact**: Offset tracking may be unreliable across multiple streaming queries
   - **Note**: Spark manages offsets in checkpoints, not Kafka consumer groups ([SPARK-19552](https://issues.apache.org/jira/browse/SPARK-19552))
   - **Fix**: Let Spark auto-generate group IDs or use unique IDs per query

5. **Data loss handling**: Default `failOnDataLoss=false` silently skips missing offsets
   - **Impact**: Gaps in data go undetected unless explicitly monitored
   - **Fix**: Set `KAFSCALE_FAIL_ON_DATA_LOSS=true` for production to fail fast

6. **Preflight AdminClient check**: Requires Kafka cluster metadata access (DescribeCluster, ListTopics permissions)
   - **Impact**: Restrictive ACL configurations may prevent job startup
   - **Fix**: Grant necessary Kafka permissions or make preflight check optional

### Architectural Limitations

- **Micro-batch processing**: Inherent latency from batch intervals (not true streaming)
- **No windowing**: Counts are global, not time-windowed
- **No watermarks**: No event-time processing or late data handling
- **Simple aggregations**: Only count operations, no complex analytics
- **In-memory state**: Word counts stored in Spark executors, limited by memory

### Tested Configuration

- **Spark version**: 3.5.0
- **Kafka client**: 3.9.1 (via KafScale)
- **Delta Lake**: 3.0.0 (when enabled)
- **Java**: 11+
- **Scala**: 2.12

## Next Level Extensions

### Reliability & Fault Tolerance
- **Durable checkpoints**: Configure S3/HDFS checkpoint directory for production
- **Exactly-once with Delta**: Use Delta Lake merge operations for idempotent writes
- **Graceful shutdown**: Implement signal handlers for clean streaming query termination
- **Monitoring alerts**: Set up Prometheus/Grafana alerts for batch delays and failures

### Processing & Analytics
- **Windowing**: Add tumbling/sliding windows for time-based aggregations ([Spark Windowing Guide](https://spark.apache.org/docs/3.5.0/structured-streaming-programming-guide.html#window-operations-on-event-time))
- **Watermarks**: Implement watermark strategies for handling late-arriving data
- **Complex aggregations**: Track percentiles, distinct counts, moving averages
- **Stateful operations**: Add custom stateful processing with `mapGroupsWithState`

### Deployment & Operations
- **Cluster deployment**: Package for YARN or Kubernetes with proper resource allocation
- **External sinks**: Write to Kafka, Cassandra, or Elasticsearch for real-time serving
- **Schema evolution**: Handle evolving message schemas with Schema Registry integration
- **Multi-query coordination**: Run multiple streaming queries with shared checkpoints

### Scalability
- **Increase parallelism**: Configure shuffle partitions and executor cores
- **Optimize batch intervals**: Tune trigger intervals based on throughput requirements
- **Partitioning strategy**: Partition Delta table by date for efficient queries
- **Resource tuning**: Configure executor memory, cores, and dynamic allocation
