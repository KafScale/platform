<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
This project is supported and financed by Scalytics, Inc. (www.scalytics.io).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Kafscale User Guide

Kafscale is a Kafka-compatible, S3-backed message transport system. It keeps brokers stateless, stores data in S3, and relies on Kubernetes for scheduling and scaling. This guide summarizes how to interact with the platform once it is deployed.

## Concepts

- **Topics / Partitions**: match upstream Kafka semantics. All Kafka client libraries continue to work.
- **Brokers**: stateless pods accepting Kafka protocol traffic on port 9092 and Prometheus metrics on 9093. The control-plane gRPC listener uses `KAFSCALE_CONTROL_ADDR` (binary default `:19094`). The operator does not set that variable or expose the port on broker pods.
- **Metadata**: stored in etcd, encoded via protobufs (`kafscale.metadata.*`).
- **Storage**: message segments live in S3 buckets; brokers only keep in-memory caches.
- **Operator**: Kubernetes controller that provisions brokers, topics, and wiring based on CRDs.

## Local Demo

For a kind-based demo environment run `make demo-platform`. The Makefile applies the demo resources via `scripts/demo-platform.sh`; if your environment blocks inline heredocs, ensure the script is executable and run the target again.

## Client Examples

Use this section to copy/paste a minimal example for your client. Default nontransactional idempotent producers are supported. Do not set a `transactional.id`; transactions and `read_committed` remain unsupported.

For install + bootstrap steps, follow `docs/quickstart.md`.

### Java (plain)

Java producer defaults work without disabling idempotence. Use `acks=all` and leave `transactional.id` unset.
```properties
# Java producer properties
bootstrap.servers=kafscale-broker:9092
enable.idempotence=true
acks=all
```
This is the smallest working producer in Java:
```java
// Java producer
Properties props = new Properties();
props.put("bootstrap.servers", "kafscale-broker:9092");
props.put("enable.idempotence", "true");
props.put("acks", "all");
try (KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
    producer.send(new ProducerRecord<>("orders", "key-1", "value-1")).get();
}
```
Consumers can be as simple as:
```java
// Java consumer
Properties props = new Properties();
props.put("bootstrap.servers", "kafscale-broker:9092");
props.put("group.id", "orders-consumer");
props.put("auto.offset.reset", "earliest");
try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
    consumer.subscribe(Collections.singletonList("orders"));
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
    for (ConsumerRecord<String, String> record : records) {
        System.out.println(record.value());
    }
}
```

If you use Spring Boot, drop this into `application.yml`:
```yaml
# Spring Boot (application.yml)
spring:
  kafka:
    bootstrap-servers: kafscale-broker:9092
    producer:
      properties:
        enable.idempotence: true
        acks: all
    consumer:
      group-id: orders-consumer
      auto-offset-reset: earliest
```

### Go (franz-go)

Franz-go is the most feature-complete Kafka client in Go. These examples are minimal and production-safe for Kafscale.
```go
// franz-go producer
client, _ := kgo.NewClient(
	kgo.SeedBrokers("kafscale-broker:9092"),
	kgo.AllowAutoTopicCreation(),
)
defer client.Close()
client.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("hello")})
```
Consumer example:
```go
// franz-go consumer
consumer, _ := kgo.NewClient(
	kgo.SeedBrokers("kafscale-broker:9092"),
	kgo.ConsumerGroup("orders-consumer"),
	kgo.ConsumeTopics("orders"),
)
defer consumer.CloseAllowingRebalance()
fetches := consumer.PollFetches(ctx)
fetches.EachRecord(func(record *kgo.Record) {
	fmt.Println(string(record.Value))
})
```

### Go (kafka-go)

If you already use `segmentio/kafka-go`, these are the smallest working snippets:
```go
// kafka-go producer
w := &kafka.Writer{Addr: kafka.TCP("kafscale-broker:9092"), Topic: "orders"}
defer w.Close()
_ = w.WriteMessages(ctx, kafka.Message{Value: []byte("hello")})
```
Consumer example:
```go
// kafka-go consumer
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

For an existing topic, use the default producer settings. The Apache Kafka
distribution uses `.sh` script names:
```bash
kafka-console-producer.sh --bootstrap-server kafscale-broker:9092 --topic orders
# Type a value, then Ctrl-D to finish.
kafka-console-consumer.sh --bootstrap-server kafscale-broker:9092 --topic orders --from-beginning
```

To send a key and value separated by a comma:

```bash
printf 'k1,v1\n' | kafka-console-producer.sh \
  --bootstrap-server kafscale-broker:9092 --topic orders \
  --property parse.key=true --property key.separator=,
kafka-console-consumer.sh --bootstrap-server kafscale-broker:9092 --topic orders \
  --from-beginning --property print.key=true --property key.separator=,
```

An `UnsupportedVersionException` mentioning `INIT_PRODUCER_ID` on a default
producer indicates an older broker or proxy. Upgrade all brokers and proxies
before enabling idempotence in an existing deployment. During a rolling upgrade,
`--producer-property enable.idempotence=false` remains a compatibility option;
those writes can duplicate after retries. A `transactional.id`, transactional
batches and `read_committed` still return `UNSUPPORTED_VERSION`.
The separate error `non-nullable field recordErrors was serialized as null`
indicates a malformed broker Produce response; update older brokers.

Idempotent writes require `acks=all` and always flush to S3 before acknowledgement,
even when ordinary writes use buffered mode. See [producer retry guarantees](protocol.md#idempotent-producers)
for retry-window and recovery details.

## Monitoring

- Metrics via Prometheus on port 9093 (`/metrics`). See [Metrics and Dashboards](metrics.md).
- Structured JSON logs from brokers and the operator.
- Control-plane gRPC is defined in `proto/control/broker.proto` and listens on `KAFSCALE_CONTROL_ADDR`. The operator does not expose that port.

## Scaling / Maintenance

The operator creates the HPA `<cluster>-broker` with a minimum of `spec.brokers.replicas` (or 3 if unset or 0) and a maximum of four times that minimum. The operator keeps the replica count the HPA sets, within that range. Setting `spec.brokers.replicas` to 0 stops all brokers. New brokers appear in Kafka Metadata and take over the partitions whose preferred leader they are. On scale-down the HPA removes at most one broker per minute; a removed broker releases its partition leases on shutdown and the remaining brokers take them over. See [Scaling brokers](operations.md#scaling-brokers).

```bash
kubectl -n kafscale patch kafscalecluster demo --type merge -p '{"spec":{"brokers":{"replicas":3}}}'
```

The BrokerControl `DrainPartitions` and `TriggerFlush` RPCs are not implemented yet and return `Unimplemented`.

## Limits / Non-Goals

- No embedded stream processing features. Pair Kafscale with Flink, Wayang, Spark, etc.
- Transactions and log compaction are out of scope for KafScale. Supported and unsupported Kafka APIs are listed in [Kafka Protocol Support](protocol.md).

For deployment and operations, read `docs/operations.md`.
For deeper architectural details or development guidance, read `kafscale-spec.md` and `docs/development.md`.
