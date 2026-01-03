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
- KafScale running locally (see [Quick Start Guide](../../examples/101_kafscale-dev-guide/02-quick-start.md))

## Running the Application

### 1. Ensure KafScale is running (default local demo)

Start the local demo:

```bash
cd ../..
make demo
```

### 2. Build the Application

```bash
mvn clean package
```

### 3. Run the Application (default profile)

You can run the application with different profiles depending on your environment.

#### Local Development (Default)

Profile for local app development with a local broker. Choose one of these two options:

- **Pure local (no containers)**: run a local broker + storage without containers, then use `localhost:39092`.
- **Local demo (MinIO helper container)**: run `make demo` and use `localhost:39092`.

```bash
mvn spring-boot:run
```

The application will start on `http://localhost:8093`.

#### Kubernetes Cluster

Profile for in-cluster deployments where the app and KafScale live in the same Kubernetes namespace. Uses the internal service name.

Connects to `kafscale-broker:9092` (Internal service). Use this when deploying the app as a Pod.

```bash
cd ../..
make demo-guide-pf
cd examples/E20_spring-boot-kafscale-demo
mvn spring-boot:run -Dspring-boot.run.profiles=cluster
```

#### Local Load Balancer

Profile for running the app locally while connecting to a remote KafScale cluster via a single exposed listener (for example, a port-forwarded LB or gateway).

Connects to `localhost:59092` (Nginx LB).

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=local-lb
```

## Testing the Application

### Create an Order via REST API

```bash
curl -X POST http://localhost:8093/api/orders \
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
curl http://localhost:8093/api/orders/health
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
| `default` | Local app + local broker (loopback) | `localhost:39092` |
| `cluster` | App and KafScale in the same cluster | `kafscale-broker:9092` |
| `local-lb`| Local app + remote cluster via single exposed listener | `localhost:59092` |

**Listener note:** The demo currently exposes a single listener. That means you pick one network context at a time: in-cluster DNS (`cluster`) or external access (`default`/`local-lb`).

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

## OpenTelemetry

Tracing is enabled via Spring Boot Actuator + OpenTelemetry. Set the OTLP endpoint with:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
```

Metrics are exposed at `http://localhost:8093/actuator/prometheus`.

## Next Steps

- Modify the `Order` model to include additional fields
- Add more REST endpoints
- Implement error handling and retry logic
- Add integration tests

See the [Running Your Application](../../examples/101_kafscale-dev-guide/04-running-your-app.md) guide for more details.

## Deep Dive: Advanced networking

For host mapping, port-forwarding, and Docker networking options, see `CONFIGURATION.md`.

## Limitations

- Consumer state is kept in memory only (no persistence or replay UI).
- `/config` and `/cluster-info` endpoints expose internal configuration details and should not be public.
- No authentication/authorization or TLS examples are included.
- No retry, DLQ, or backoff strategy for failed consumption/production.

## Next Level Extensions

- Add persistence (database) and pagination for orders.
- Add a DLQ topic and retry/backoff policy for failures.
- Add authn/authz (Spring Security) and TLS configuration samples.
- Add metrics/tracing and structured logging.
