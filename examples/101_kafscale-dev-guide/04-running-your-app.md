# Running Your Application

Now that the infrastructure is running (from [Chapter 2](02-quick-start.md)), let's run a client application. This covers the **Developer (DEV)** aspect.

We provide two examples:
1.  **Simple Java Client** (E10) - Basic text producer/consumer.
2.  **Spring Boot App** (E20) - Full REST API application.

## Option A: Simple Java Client (E10)

This is the simplest way to verify your cluster is working.

### 1. Run the Demo
```bash
cd examples/E10_java-kafka-client-demo
mvn clean package exec:java
```

### 2. What it does
- Connects to `localhost:39092`
- Creates a topic `demo-topic-1`
- Produces 25 messages
- Consumes 5 messages
- Prints cluster metadata

## Option B: Spring Boot Application (E20)

A more complete real-world example using Spring Boot.

### 1. Run the App using the Default Profile
The default profile is configured for local development (`localhost:39092`).

```bash
cd examples/E20_spring-boot-kafscale-demo
mvn spring-boot:run
```

The app will start on **[http://localhost:8083](http://localhost:8083)**.

### 2. Test via REST API

**Send an Order:**
```bash
curl -X POST http://localhost:8083/api/orders \
  -H "Content-Type: application/json" \
  -d '{"product": "Widget", "quantity": 5}'
```

**Check Health:**
```bash
curl http://localhost:8083/api/orders/health
```

### 3. Observe Logs
Check the terminal where you ran `mvn spring-boot:run`. You should see "Sending order..." and "Received order..." logs, confirming the full cycle.

## Connecting Your Own Application

To connect your own apps to the local KafScale cluster:

- **Bootstrap Server**: `localhost:39092`
- **Security Protocol**: `PLAINTEXT`

**Example `application.properties`:**
```properties
spring.kafka.bootstrap-servers=localhost:39092
```

**Next**: [Troubleshooting](05-troubleshooting.md) →
