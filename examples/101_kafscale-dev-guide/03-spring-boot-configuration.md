# Spring Boot Configuration

Configuring your Spring Boot application to use KafScale is straightforward. Since KafScale implements the Kafka wire protocol, **most of your existing configuration will work without changes**. You typically only need to update the bootstrap server address.

## Minimal Configuration Changes

The good news: **KafScale is Kafka-compatible**, so your existing Spring Boot + Kafka application will work with minimal changes!

### What Stays the Same

- ✅ Spring Kafka dependencies
- ✅ Serializers and deserializers
- ✅ Consumer group configuration
- ✅ Producer and consumer properties
- ✅ `@KafkaListener` annotations
- ✅ `KafkaTemplate` usage

### What Changes

- 🔄 **Bootstrap servers**: Point to KafScale instead of Kafka (`localhost:39092` for local demos)
- 🔄 **Topic creation**: May need to create topics manually (or enable auto-creation)

That's it! Everything else works as-is.

## Configuration Examples

### application.properties

Here's a complete example for `application.properties`:

```properties
# Kafka Bootstrap Servers - Point to KafScale
spring.kafka.bootstrap-servers=localhost:39092

# Producer Configuration
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
spring.kafka.producer.acks=all
spring.kafka.producer.retries=3
spring.kafka.producer.properties.linger.ms=10
spring.kafka.producer.properties.batch.size=16384

# Consumer Configuration
spring.kafka.consumer.group-id=my-consumer-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=*
spring.kafka.consumer.enable-auto-commit=true
spring.kafka.consumer.auto-commit-interval=1000

# Listener Configuration
spring.kafka.listener.ack-mode=batch
spring.kafka.listener.concurrency=3

# Admin Configuration (for topic creation)
spring.kafka.admin.properties.bootstrap.servers=localhost:39092
```

> **Tip:** The complete configuration file is available in [`examples/application.properties`](examples/application.properties).

### application.yml

If you prefer YAML configuration:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:39092
    
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3
      properties:
        linger.ms: 10
        batch.size: 16384
    
    consumer:
      group-id: my-consumer-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      enable-auto-commit: true
      auto-commit-interval: 1000
      properties:
        spring.json.trusted.packages: "*"
    
    listener:
      ack-mode: batch
      concurrency: 3
    
    admin:
      properties:
        bootstrap.servers: localhost:39092
```

> **Tip:** The complete YAML configuration is available in [`examples/application.yml`](examples/application.yml).

## Migrating from Existing Kafka Configuration

If you're migrating from an existing Kafka setup, here's what you need to change:

### Before (Traditional Kafka)

```properties
spring.kafka.bootstrap-servers=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092
# ... rest of your configuration
```

### After (KafScale)

```properties
spring.kafka.bootstrap-servers=localhost:39092
# ... rest of your configuration (unchanged!)
```

That's it! Just update the bootstrap servers.

## Common Configuration Patterns

### Error Handling

Add error handling configuration for production use:

```properties
# Error handling
spring.kafka.listener.ack-mode=manual
spring.kafka.consumer.enable-auto-commit=false

# Retry configuration
spring.kafka.producer.retries=3
spring.kafka.producer.properties.retry.backoff.ms=1000
```

### Custom Serializers

If you're using custom serializers (e.g., Avro, Protobuf):

```properties
# Avro serialization
spring.kafka.producer.value-serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
spring.kafka.consumer.value-deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
spring.kafka.properties.schema.registry.url=http://localhost:8081
```

> **Note:** KafScale doesn't include a schema registry. You'll need to run one separately if using Avro/Protobuf.

## Configuration Best Practices

### 1. Externalize Configuration

Use environment variables or Spring profiles for different environments:

```properties
# application-dev.properties
spring.kafka.bootstrap-servers=localhost:9092

# application-prod.properties
spring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP_SERVERS}
```

### 2. Use Meaningful Consumer Group IDs

Choose descriptive group IDs that indicate the purpose:

```properties
spring.kafka.consumer.group-id=order-processing-service
```

### 3. Configure Appropriate Batch Sizes

Tune batch sizes based on your message volume:

```properties
# For high-throughput scenarios
spring.kafka.producer.properties.batch.size=32768
spring.kafka.producer.properties.linger.ms=100

# For low-latency scenarios
spring.kafka.producer.properties.batch.size=1024
spring.kafka.producer.properties.linger.ms=0
```

### 4. Enable Compression for Large Messages

```properties
spring.kafka.producer.compression-type=snappy
```

## What's Not Supported

KafScale does not support some advanced Kafka features:

- ❌ **Transactions**: No exactly-once semantics
- ❌ **Idempotent producers**: `enable.idempotence=true` will be ignored
- ❌ **Log compaction**: Compacted topics not supported

If your application relies on these features, you'll need to use traditional Kafka or refactor your application.

## Example Application

A complete Spring Boot example application is available in [`examples/spring-boot-kafscale-demo/`](examples/spring-boot-kafscale-demo/). This application demonstrates:

- Producer and consumer configuration
- Message production and consumption
- Error handling
- Running with Maven

See the [Running Your Application](04-running-your-app.md) section for details on how to run the example.

## Next Steps

Now that you've configured your Spring Boot application, let's run it and verify it works with KafScale!

**Next**: [Running Your Application](04-running-your-app.md) →
