# Spring Boot KafScale Demo

This is a complete Spring Boot application demonstrating how to use KafScale as a Kafka replacement.

## Features

- REST API for creating orders
- Kafka producer to send orders to KafScale
- Kafka consumer to process orders from KafScale
- JSON serialization/deserialization
- Proper error handling and logging

## Prerequisites

- Java 17+
- Maven 3.6+
- KafScale running locally (see [Quick Start Guide](../../02-quick-start.md))

## Running the Application

### 1. Ensure KafScale is Running

Make sure you have KafScale running with Docker Compose:

```bash
cd ../
docker-compose up -d
```

### 2. Build the Application

```bash
mvn clean package
```

### 3. Run the Application

You can run the application with different profiles depending on your environment.

#### Local Development (Default)

Connects to `localhost:39092` (Port-forwarded broker).

```bash
mvn spring-boot:run
```

#### Kubernetes Cluster

Connects to `kafscale-broker:9092` (Internal service). Use this when deploying the app as a Pod.

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=cluster
```

#### Local Load Balancer

Connects to `localhost:59092` (Nginx LB).

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=local-lb
```

The application will start on `http://localhost:8083`.

## Testing the Application

### Create an Order via REST API

```bash
curl -X POST http://localhost:8083/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "product": "Widget",
    "quantity": 5
  }'
```

You should see:
1. The producer logs showing the order was sent
2. The consumer logs showing the order was received and processed

### Check Application Health

```bash
curl http://localhost:8083/api/orders/health
```

## Project Structure

```
src/main/java/com/example/kafscale/
├── KafScaleDemoApplication.java     # Main application class
├── controller/
├── model/
└── service/

src/main/resources/
└── application.yml                  # Application configuration & Profiles
```

## Configuration & Profiles

The application uses Spring Profiles to switch between environments. See `src/main/resources/application.yml` for details.

### Available Profiles

| Profile | Description | Bootstrap Servers |
|---------|-------------|-------------------|
| `default` | Local development with port-forwarding | `localhost:39092` |
| `cluster` | Running inside Kubernetes | `kafscale-broker:9092` |
| `local-lb`| Local development via LoadBalancer | `localhost:59092` |

### Key Settings
- **Topic**: `orders-springboot`
- **Consumer Group**: `kafscale-demo-group-${random.uuid}` (Randomized for uniqueness)

## Logs

Watch the application logs to see messages being produced and consumed:

```
INFO  OrderProducerService - Sending order to KafScale: Order{orderId='...', product='Widget', quantity=5}
INFO  OrderProducerService - Order sent successfully: ... to partition 0
INFO  OrderConsumerService - Received order from KafScale: Order{orderId='...', product='Widget', quantity=5}
INFO  OrderConsumerService - Processing order: ... for product: Widget (quantity: 5)
```

## Next Steps

- Modify the `Order` model to include additional fields
- Add more REST endpoints
- Implement error handling and retry logic
- Add integration tests

See the [Running Your Application](../../04-running-your-app.md) guide for more details.
