# Running Your Application

Now that you have KafScale running and your Spring Boot application configured, let's test everything together!

## Using the Example Application

We've provided a complete Spring Boot example application that demonstrates producing and consuming messages with KafScale.

### Step 1: Navigate to the Example Directory

```bash
cd docs/guide/examples/spring-boot-kafscale-demo
```

### Step 2: Build the Application

```bash
mvn clean package
```

### Step 3: Run the Application

```bash
mvn spring-boot:run
```

The application will start on `http://localhost:8080` and automatically connect to KafScale at `localhost:9092`.

### Step 4: Send Test Orders

Open a new terminal and send some test orders using curl:

```bash
# Send order 1
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "product": "Widget",
    "quantity": 5
  }'

# Send order 2
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "product": "Gadget",
    "quantity": 3
  }'

# Send order 3
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "product": "Doohickey",
    "quantity": 7
  }'
```

### Step 5: Observe the Logs

In the application logs, you should see:

1. **Producer logs** showing orders being sent:
```
INFO  OrderProducerService - Sending order to KafScale: Order{orderId='...', product='Widget', quantity=5}
INFO  OrderProducerService - Order sent successfully: ... to partition 0
```

2. **Consumer logs** showing orders being received:
```
INFO  OrderConsumerService - Received order from KafScale: Order{orderId='...', product='Widget', quantity=5}
INFO  OrderConsumerService - Processing order: ... for product: Widget (quantity: 5)
```

## Running Your Own Application

If you have your own Spring Boot + Kafka application, follow these steps:

### Step 1: Update Configuration

Update your `application.properties` or `application.yml` to point to KafScale:

```properties
spring.kafka.bootstrap-servers=localhost:9092
```

### Step 2: Ensure Topics Exist

Create the topics your application needs:

```bash
kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic your-topic-name \
  --partitions 3
```

Or enable auto-topic creation (not recommended for production).

### Step 3: Run Your Application

```bash
mvn spring-boot:run
# or
./gradlew bootRun
```

## Monitoring and Observability

### Check Consumer Group Status

View consumer group details:

```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe \
  --group kafscale-demo-group
```

You'll see output like:

```
GROUP                TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
kafscale-demo-group  orders          0          10              10              0
kafscale-demo-group  orders          1          8               8               0
kafscale-demo-group  orders          2          12              12              0
```

### View Data in MinIO

Open the MinIO console at [http://localhost:9001](http://localhost:9001) (username: `minioadmin`, password: `minioadmin`).

Navigate to the `kafscale` bucket and browse to see the log segments:

```
kafscale/
  └── default/
      └── orders/
          ├── 0/
          │   ├── segment-00000000000000000000.kfs
          │   └── segment-00000000000000000000.index
          ├── 1/
          └── 2/
```

### Check Broker Metrics

KafScale exposes Prometheus metrics on port 9093:

```bash
curl http://localhost:9093/metrics
```

## Performance Considerations

### Expected Latency

KafScale adds some latency compared to traditional Kafka due to S3 storage:

- **Produce latency**: ~10-50ms additional (depends on MinIO/S3 performance)
- **Consume latency**: Similar to traditional Kafka for recent data
- **Replay latency**: May be higher when consuming old data from S3

### Tuning for Performance

If you need better performance, tune these settings:

```properties
# Increase batch size for higher throughput
spring.kafka.producer.properties.batch.size=32768
spring.kafka.producer.properties.linger.ms=100

# Increase fetch size for consumers
spring.kafka.consumer.properties.fetch.min.bytes=1024
spring.kafka.consumer.properties.fetch.max.wait.ms=500

# Enable compression
spring.kafka.producer.compression-type=snappy
```

### When to Use KafScale vs Traditional Kafka

**Use KafScale when**:
- Cost is a concern (S3 storage is cheaper than provisioned disks)
- You need long-term retention for replay
- You're running in Kubernetes and want stateless brokers
- Development and testing environments

**Use Traditional Kafka when**:
- You need ultra-low latency (< 5ms)
- You need exactly-once semantics
- You need log compaction
- Very high throughput on single partitions

## Testing Message Flow

### Produce Messages via Kafka Tools

You can also produce messages using Kafka console tools:

```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic orders
```

Then type JSON messages:
```json
{"orderId":"test-001","product":"Test Product","quantity":1}
```

### Consume Messages via Kafka Tools

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning
```

## Stopping the Application

Press `Ctrl+C` in the terminal where the Spring Boot application is running.

## Next Steps

If you encounter any issues, check the [Troubleshooting](05-troubleshooting.md) section.

For production deployment guidance, see [Next Steps](06-next-steps.md).

**Next**: [Troubleshooting](05-troubleshooting.md) →
