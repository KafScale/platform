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
- **Brokers**: stateless pods accepting Kafka protocol traffic on port 9092 and metrics + gRPC control on 9093.
- **Metadata**: stored in etcd, encoded via protobufs (`kafscale.metadata.*`).
- **Storage**: message segments live in S3 buckets; brokers only keep in-memory caches.
- **Operator**: Kubernetes controller that provisions brokers, topics, and wiring based on CRDs.

## Local Demo

For a kind-based demo environment run `make demo-platform`. The Makefile applies the demo resources via `scripts/demo-platform-apply.sh`; if your environment blocks inline heredocs, ensure the script is executable and run the target again.

## Client Examples

Use this section to copy/paste a minimal example for your client. If you do not control client config (managed apps, hosted integrations), ask the operator team to confirm idempotence/transactions are disabled for Kafscale.

For install + bootstrap steps, follow `docs/quickstart.md`.

### Java (plain)

Start with a minimal set of producer properties. We disable idempotence because Kafscale does not support transactional semantics.
```properties
# Java producer properties
bootstrap.servers=kafscale-broker:9092
enable.idempotence=false
acks=1
```
This is the smallest working producer in Java:
```java
// Java producer
Properties props = new Properties();
props.put("bootstrap.servers", "kafscale-broker:9092");
props.put("enable.idempotence", "false");
props.put("acks", "1");
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
        enable.idempotence: false
        acks: 1
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

If you just want to test from a shell:
```bash
# Kafka CLI
kafka-console-producer --bootstrap-server kafscale-broker:9092 --topic orders --producer-property enable.idempotence=false
kafka-console-consumer --bootstrap-server kafscale-broker:9092 --topic orders --from-beginning
```

## Monitoring

- Metrics via Prometheus on port 9093 (`/metrics`)
- Structured JSON logs from brokers/operators
- Control-plane queries via the gRPC service defined in `proto/control/broker.proto`

## Scaling / Maintenance

The operator uses Kubernetes HPA and the BrokerControl gRPC API to safely drain partitions before restarts. Users can request manual drains or flushes by invoking those RPCs (CLI tooling TBD).

## Limits / Non-Goals

- No embedded stream processing features—pair Kafscale with Flink, Wayang, Spark, etc.
- Transactions, idempotent producers, and log compaction are out of scope for KafScale.

For deployment and operations, read `docs/operations.md`.
For deeper architectural details or development guidance, read `kafscale-spec.md` and `docs/development.md`.
