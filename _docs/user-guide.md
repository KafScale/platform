---
layout: doc
title: User Guide
description: How to interact with KafScale once it is deployed.
permalink: /user-guide/
nav_title: User Guide
nav_order: 3
---

# KafScale User Guide

KafScale is a Kafka-compatible, S3-backed message transport system. It keeps brokers stateless, stores data in S3, and relies on Kubernetes for scheduling and scaling. This guide covers how to interact with the platform once it is deployed.

## Before you start

**Latency expectations:** KafScale has ~500ms end-to-end latency due to S3 flush semantics. It's ideal for ETL, logs, CDC, and async events—not for sub-100ms use cases like real-time bidding or gaming.

**Authentication:** KafScale v1.x does not yet support SASL authentication or mTLS. Network-level security (Kubernetes NetworkPolicies, private VPCs) is the current isolation mechanism. See the [Security](/security/) page for the roadmap.

**TLS:** If TLS is enabled, it's configured at the Kubernetes Ingress or LoadBalancer level by your operator—not in KafScale itself. Check with your platform team for connection details.

**Idempotent producers:** KafScale does not support idempotent producers (Kafka API 22). Since Kafka clients 3.0+, idempotence is enabled by default. You **must** set `enable.idempotence=false` in all producer configurations.

## Concepts

| Concept | Description |
|---------|-------------|
| **Topics / Partitions** | Standard Kafka semantics. All Kafka client libraries work. |
| **Brokers** | Stateless pods accepting Kafka protocol on port 9092, metrics on 9093. |
| **Metadata** | Stored in etcd, encoded via protobufs (`kafscale.metadata.*`). |
| **Storage** | Message segments live in S3; brokers keep only in-memory caches. |
| **Operator** | Kubernetes controller that provisions brokers, topics, and wiring via CRDs. |

## Client Examples

Use this section to copy/paste minimal examples for your client. If you don't control client config (managed apps, hosted integrations), ask the operator team to confirm idempotence and transactions are disabled.

For install and bootstrap steps, see [Quickstart](/quickstart/).

### Java (plain)

Disable idempotence—KafScale does not support transactional semantics or idempotent producers.

```properties
# Java producer properties
bootstrap.servers=kafscale-broker:9092
enable.idempotence=false
acks=1
```

**Producer:**

```java
Properties props = new Properties();
props.put("bootstrap.servers", "kafscale-broker:9092");
props.put("enable.idempotence", "false");
props.put("acks", "1");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
    producer.send(new ProducerRecord<>("orders", "key-1", "value-1")).get();
}
```

**Consumer:**

```java
Properties props = new Properties();
props.put("bootstrap.servers", "kafscale-broker:9092");
props.put("group.id", "orders-consumer");
props.put("auto.offset.reset", "earliest");
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
    consumer.subscribe(Collections.singletonList("orders"));
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
    for (ConsumerRecord<String, String> record : records) {
        System.out.println(record.value());
    }
}
```

### Spring Boot

```yaml
# application.yml
spring:
  kafka:
    bootstrap-servers: kafscale-broker:9092
    producer:
      properties:
        enable.idempotence: false
        acks: 1
    consumer:
      group-id: orders-consumer
      auto-offset-reset: earliest
```

### Python (confluent-kafka)

```python
from confluent_kafka import Producer, Consumer

# Producer
producer = Producer({
    'bootstrap.servers': 'kafscale-broker:9092',
    'enable.idempotence': False
})
producer.produce('orders', key='key-1', value='value-1')
producer.flush()

# Consumer
consumer = Consumer({
    'bootstrap.servers': 'kafscale-broker:9092',
    'group.id': 'orders-consumer',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['orders'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue
    print(f"{msg.key()}: {msg.value()}")
```

### Go (franz-go)

Franz-go is the most feature-complete Kafka client in Go.

**Producer:**

```go
client, _ := kgo.NewClient(
    kgo.SeedBrokers("kafscale-broker:9092"),
    kgo.AllowAutoTopicCreation(),
    kgo.DisableIdempotentWrite(),
)
defer client.Close()

client.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("hello")})
```

**Consumer:**

```go
client, _ := kgo.NewClient(
    kgo.SeedBrokers("kafscale-broker:9092"),
    kgo.ConsumerGroup("orders-consumer"),
    kgo.ConsumeTopics("orders"),
)
defer client.Close()

fetches := client.PollFetches(ctx)
fetches.EachRecord(func(r *kgo.Record) {
    fmt.Println(string(r.Value))
})
```

### Go (kafka-go)

```go
// Producer
w := &kafka.Writer{
    Addr:  kafka.TCP("kafscale-broker:9092"),
    Topic: "orders",
}
defer w.Close()
w.WriteMessages(ctx, kafka.Message{Value: []byte("hello")})

// Consumer
r := kafka.NewReader(kafka.ReaderConfig{
    Brokers: []string{"kafscale-broker:9092"},
    GroupID: "orders-consumer",
    Topic:   "orders",
})
defer r.Close()
m, _ := r.ReadMessage(ctx)
fmt.Println(string(m.Value))
```

### Kafka CLI

```bash
# Produce
kafka-console-producer \
  --bootstrap-server kafscale-broker:9092 \
  --topic orders \
  --producer-property enable.idempotence=false

# Consume
kafka-console-consumer \
  --bootstrap-server kafscale-broker:9092 \
  --topic orders \
  --from-beginning
```

## Stream Processing Integration

KafScale is a transport layer—it doesn't include embedded stream processing. This is intentional; [data processing doesn't belong in the message broker](https://www.novatechflow.com/2025/12/data-processing-does-not-belong-in.html). Pair KafScale with external engines like [Apache Flink](https://flink.apache.org/) or [Apache Wayang](https://wayang.apache.org/) for stateful transformations, windowing, and analytics.

### Apache Flink

Flink's Kafka connector works with KafScale. Disable idempotence and exactly-once semantics since KafScale does not support transactions or idempotent producers.

**Maven dependency:**

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka</artifactId>
  <version>3.1.0-1.18</version>
</dependency>
```

**Source (read from KafScale):**

```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafscale-broker:9092")
    .setTopics("orders")
    .setGroupId("flink-orders")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<String> stream = env.fromSource(
    source, 
    WatermarkStrategy.noWatermarks(), 
    "KafScale Source"
);
```

**Sink (write to KafScale):**

```java
Properties producerConfig = new Properties();
producerConfig.setProperty("enable.idempotence", "false");

KafkaSink<String> sink = KafkaSink.<String>builder()
    .setBootstrapServers("kafscale-broker:9092")
    .setKafkaProducerConfig(producerConfig)
    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("orders-processed")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build())
    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build();

stream.sinkTo(sink);
env.execute("Order Processing");
```

> **Note:** You must explicitly set `enable.idempotence=false` via `setKafkaProducerConfig()`. Since Kafka 3.0, idempotence defaults to true, which requires API 22 that KafScale does not support.

### Apache Wayang

Wayang provides a platform-agnostic API that can run on Java or Spark backends. Kafka source/sink support was added in 2024.

**Maven dependencies:**

```xml
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-api-scala-java</artifactId>
  <version>1.0.0</version>
</dependency>
<dependency>
  <groupId>org.apache.wayang</groupId>
  <artifactId>wayang-java</artifactId>
  <version>1.0.0</version>
</dependency>
```

**Read from KafScale, process, write back:**

```java
Configuration configuration = new Configuration();
WayangContext wayangContext = new WayangContext(configuration)
    .withPlugin(Java.basicPlugin());

JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
    .withJobName("OrderProcessing")
    .withUdfJarOf(MyJob.class);

planBuilder
    .readKafkaTopic("orders").withName("Load from KafScale")
    .flatMap(line -> Arrays.asList(line.split("\\W+")))
    .filter(token -> !token.isEmpty())
    .map(word -> new Tuple2<>(word.toLowerCase(), 1))
    .reduceByKey(Tuple2::getField0, 
        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1()))
    .writeKafkaTopic("orders-counts", 
        d -> String.format("%s: %d", d.getField0(), d.getField1()),
        "wayang-job",
        LoadProfileEstimators.createFromSpecification(
            "wayang.java.kafkatopicsink.load", configuration));
```

To switch from Java to Spark backend, change one line:

```java
.withPlugin(Spark.basicPlugin());  // instead of Java.basicPlugin()
```

> **Important:** Wayang uses the Kafka Java client internally but does not expose producer configuration through its API. Since Kafka 3.0+ defaults to `enable.idempotence=true`, you must disable it via JVM argument:
> ```bash
> java -Denable.idempotence=false -jar your-wayang-app.jar
> ```
> Requires Java 17. See [wayang.apache.org](https://wayang.apache.org/blog/kafka-meets-wayang-2/) for full implementation details.

### Other compatible engines

| Engine | Notes |
|--------|-------|
| Spark Structured Streaming | Use `kafka` format, set `kafka.enable.idempotence=false` |
| Kafka Streams | Disable EOS: `processing.guarantee=at_least_once` |
| Redpanda Console / Kowl | Compatible for topic browsing |
| Conduktor | Compatible for admin UI |

## Monitoring

KafScale exposes Prometheus metrics on port 9093.

```bash
# Scrape metrics
curl http://kafscale-broker:9093/metrics

# Key metrics to watch
curl -s http://kafscale-broker:9093/metrics | grep -E "kafscale_(produce|fetch|s3)"
```

**Key metrics:**

| Metric | Description |
|--------|-------------|
| `kafscale_produce_requests_total` | Total produce requests |
| `kafscale_fetch_requests_total` | Total fetch requests |
| `kafscale_s3_upload_duration_ms` | S3 segment upload latency |
| `kafscale_s3_download_duration_ms` | S3 segment download latency |
| `kafscale_cache_hit_ratio` | LRU cache effectiveness |

Brokers also emit structured JSON logs. The operator exposes its own metrics for CRD reconciliation.

## Troubleshooting

**Test broker connectivity:**

```bash
kafka-broker-api-versions --bootstrap-server kafscale-broker:9092
```

**List topics:**

```bash
kafka-topics --bootstrap-server kafscale-broker:9092 --list
```

**Describe a topic:**

```bash
kafka-topics --bootstrap-server kafscale-broker:9092 --describe --topic orders
```

**Check consumer group lag:**

```bash
kafka-consumer-groups \
  --bootstrap-server kafscale-broker:9092 \
  --group orders-consumer \
  --describe
```

**Common issues:**

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `NOT_LEADER_FOR_PARTITION` | Normal during rebalance | Retry; client handles this |
| `CLUSTER_AUTHORIZATION_FAILED` | Idempotence enabled, API 22 not supported | Set `enable.idempotence=false` |
| High produce latency | S3 flush taking long | Check S3 health, increase buffer |
| Consumer lag growing | Slow processing or S3 reads | Scale consumers, check cache hit ratio |
| `UNKNOWN_TOPIC` | Topic not created | Create via CRD or enable auto-create |

## Scaling and Maintenance

The operator uses Kubernetes HPA and the BrokerControl gRPC API to safely drain partitions before restarts.

- **Scaling up:** Add replicas via `kubectl scale` or adjust the CRD; partitions rebalance automatically.
- **Scaling down:** The operator drains partitions before terminating pods.
- **Rolling restarts:** Use `kubectl rollout restart`; the operator coordinates graceful handoff.

## Limits and Non-Goals

KafScale intentionally does not support:

- **Transactions / exactly-once semantics** — use at-least-once with idempotent consumers
- **Idempotent producers (API 22)** — disable `enable.idempotence` in all clients
- **Log compaction** — out of scope for MVP
- **Embedded stream processing** — pair with Flink, Wayang, Spark, etc.
- **Sub-100ms latency** — S3 flush semantics add ~500ms

## Next steps

- [Configuration](/configuration/) — tune cache sizes, buffer thresholds, S3 settings
- [Operations](/operations/) — S3 health states, failure modes, multi-region CRR
- [Security](/security/) — current posture, TLS setup, auth roadmap
- [Architecture](/architecture/) — how data flows through the system