# Spring Boot KafScale Demo (E20)

A production-style Spring Boot application demonstrating KafScale integration with REST API, Kafka producers/consumers, JSON serialization, and OpenTelemetry tracing.

## Quick Start

```bash
# 1. Start KafScale from repo root
cd ../..
make demo

# 2. Run the Spring Boot app
cd examples/E20_spring-boot-kafscale-demo
mvn spring-boot:run

# 3. Open the web UI
open http://localhost:8093
# Or visit http://localhost:8093 in your browser

# 4. (Alternative) Create an order via REST API
curl -X POST http://localhost:8093/api/orders \
  -H "Content-Type: application/json" \
  -d '{"product": "Widget", "quantity": 5}'
```

**The Web UI provides:**
- 📝 Order creation form
- 📊 Real-time view of received orders (auto-refreshes every 3s)
- ⚙️ Kafka producer/consumer configuration inspection
- 🌐 Cluster info with nodes and topics

## Features

- **Web UI** (Bootstrap 5) for interactive order management and monitoring
- **REST API** for creating and viewing orders
- **Kafka Producer** with JSON serialization to send orders
- **Kafka Consumer** with auto-commit to process orders
- **OpenTelemetry tracing** and Prometheus metrics
- **Multi-profile configuration** for local/cluster/load-balancer deployments
- **Admin endpoints** for cluster info and diagnostics

## Prerequisites

- Java 17+
- Maven 3.6+
- KafScale running locally (see [Quick Start Guide](../../examples/101_kafscale-dev-guide/02-quick-start.md))

## Running with Different Profiles

The application uses Spring Profiles to target different deployment environments. The default profile connects to `localhost:39092`.

### Profile: `default` (Local Development)

For local development with KafScale running on your machine:

```bash
# Start KafScale
make demo  # from repo root

# Run the app (connects to localhost:39092)
mvn spring-boot:run
```

Application starts on `http://localhost:8093`.

### Profile: `cluster` (Kubernetes In-Cluster)

For deploying as a Pod in the same Kubernetes namespace as KafScale:

```bash
# Port-forward KafScale (from repo root)
make demo-guide-pf

# Run the app (connects to kafscale-broker:9092)
mvn spring-boot:run -Dspring-boot.run.profiles=cluster
```

### Profile: `local-lb` (Remote via Load Balancer)

For local app connecting to remote KafScale via port-forwarded load balancer:

```bash
# Run the app (connects to localhost:59092)
mvn spring-boot:run -Dspring-boot.run.profiles=local-lb
```

## API Endpoints

### Order Management
- `POST /api/orders` - Create and send an order to KafScale
- `GET /api/orders` - List all consumed orders (in-memory)
- `GET /api/orders/health` - Simple health check

### Diagnostics & Monitoring
- `GET /api/orders/config` - View Kafka configuration (⚠️ should not be public in production)
- `GET /api/orders/cluster-info` - View cluster metadata and topics (⚠️ should not be public in production)
- `POST /api/orders/test-connection` - Test Kafka connectivity
- `GET /actuator/prometheus` - Prometheus metrics
- `GET /actuator/health` - Spring Boot health endpoint

## Testing the Application

### 1. Create an Order

```bash
curl -X POST http://localhost:8093/api/orders \
  -H "Content-Type: application/json" \
  -d '{"product": "Widget", "quantity": 5}'
```

**Expected response:**
```
Order sent: <uuid>
```

**Expected logs:**
```
INFO  OrderProducerService - Sending order to KafScale: Order{orderId='...', product='Widget', quantity=5}
INFO  OrderProducerService - Order sent successfully: ... to partition 0
INFO  OrderConsumerService - Received order from KafScale: Order{orderId='...', product='Widget', quantity=5}
INFO  OrderConsumerService - Processing order: ... for product: Widget (quantity: 5)
```

### 2. View Consumed Orders

```bash
curl http://localhost:8093/api/orders
```

### 3. Check Cluster Information

```bash
curl http://localhost:8093/api/orders/cluster-info
```

Returns cluster ID, controller, nodes, and topics.

## Web UI

The application includes a browser-based UI at [http://localhost:8093](http://localhost:8093).

### Features

**Business Logic Tab:**
- Create orders using a form (product name + quantity)
- View received orders in a table that auto-refreshes every 3 seconds
- See order IDs, products, and quantities

**Kafka Client Configs Tab:**
- View producer settings (bootstrap servers, acks, serializers)
- View consumer settings (group ID, offset reset, deserializers)
- Test Kafka connectivity with LED indicators (🟢 connected, 🔴 failed, 🟡 testing)
- Inspect full configuration dump including active Spring profile

**Cluster Infos Tab:**
- Display cluster ID and controller node
- Show all broker nodes with host:port information
- List all topics in the cluster

### UI Screenshots

The UI is built with Bootstrap 5 and provides a responsive, modern interface for:
- Creating orders without using curl
- Monitoring message flow in real-time
- Debugging configuration issues visually
- Exploring cluster topology

## Project Structure

```
src/main/java/com/example/kafscale/
├── KafScaleDemoApplication.java           # Main Spring Boot application
├── controller/
│   └── OrderController.java               # REST endpoints for orders & diagnostics
├── model/
│   └── Order.java                         # Order domain model (uses Lombok)
└── service/
    ├── OrderProducerService.java          # Kafka producer service
    └── OrderConsumerService.java          # Kafka consumer service (in-memory storage)

src/main/resources/
├── application.yml                        # Multi-profile Spring configuration
└── static/
    └── index.html                         # Web UI (Bootstrap 5, vanilla JS)

pom.xml                                     # Maven dependencies (Spring Boot 3.1.6, Kafka 3.9.1, OpenTelemetry)
```

**Key Dependencies:**
- Spring Boot 3.1.6
- Spring Kafka
- Lombok (for `@Data`, `@Slf4j` annotations)
- OpenTelemetry + Micrometer for tracing
- Spring Boot Actuator for metrics

## Configuration Details

See [application.yml](src/main/resources/application.yml) for complete configuration.

### Profile Summary

| Profile | Bootstrap Servers | Use Case |
|---------|------------------|----------|
| `default` | `localhost:39092` | Local development with `make demo` |
| `cluster` | `kafscale-broker:9092` | In-cluster Kubernetes deployment |
| `local-lb`| `localhost:59092` | Local app → remote cluster via LB |

### Key Kafka Settings

**Producer:**
- `acks: 0` - No acknowledgment (⚠️ weak delivery guarantees, may lose messages)
- `retries: 3`
- `enable.idempotence: false` - Disabled for KafScale compatibility
- `key-serializer: StringSerializer`
- `value-serializer: JsonSerializer`

**Consumer:**
- `group-id: kafscale-demo-group-${random.uuid}` - Random UUID (⚠️ offsets not persisted across restarts)
- `auto-offset-reset: earliest` - Read from beginning if no committed offset
- `enable-auto-commit: true` - Commits offsets every 1ms
- `key-deserializer: StringDeserializer`
- `value-deserializer: JsonDeserializer`

**Topic:**
- Name: `orders-springboot`
- Auto-created by KafScale if it doesn't exist

## Observability

### OpenTelemetry Tracing

Distributed tracing is pre-configured with OpenTelemetry (sampling probability: 100%). Configure the OTLP endpoint:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
mvn spring-boot:run
```

Default endpoint: `http://localhost:4318/v1/traces`

### Prometheus Metrics

Metrics are exposed for scraping:

```bash
curl http://localhost:8093/actuator/prometheus
```

Available metrics include JVM stats, HTTP requests, and Kafka producer/consumer metrics.

## Troubleshooting

### Connection Refused

**Symptom:** `Connection to node -1 (localhost:39092) could not be established`

**Fix:**
1. Verify KafScale is running: `docker ps | grep kafscale` or check process list
2. Confirm bootstrap server matches profile (default: `localhost:39092`)
3. Test connectivity via UI: Open [http://localhost:8093](http://localhost:8093) → "Kafka Client Configs" tab → "Test Connection" button
4. Or via API: `curl -X POST http://localhost:8093/api/orders/test-connection`

### No Messages Consumed

**Symptom:** Producer logs show messages sent, but consumer doesn't receive them

**Possible causes:**
- Consumer started before topic was created (restart the app)
- Random group ID means no offset tracking (check logs for `kafscale-demo-group-<uuid>`)
- Topic name mismatch (verify topic: `curl http://localhost:8093/api/orders/cluster-info`)

### Port Already in Use

**Symptom:** `Port 8093 already in use`

**Fix:** Change server port with: `mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8094`

### Profile Not Active

**Symptom:** App connects to wrong bootstrap server

**Fix:** Verify active profile with: `curl http://localhost:8093/api/orders/config | jq .activeProfiles`

## Advanced Configuration

For detailed networking setup including:
- Host name mapping and DNS resolution
- Port-forwarding strategies
- Docker networking and advertised listeners
- Multi-listener configuration

See `CONFIGURATION.md` (if available) or the [Running Your Application](../../examples/101_kafscale-dev-guide/04-running-your-app.md) guide.

## Limitations & Considerations

### Production Readiness Gaps

1. **Weak delivery guarantees**: Producer uses `acks=0`, meaning messages may be lost without acknowledgment. For production, set `acks=all`.

2. **No offset persistence**: Consumer group ID uses random UUID, so offset tracking resets on every restart. Orders are re-consumed from `earliest`.
   - **Impact**: No resume capability, potential duplicate processing
   - **Fix**: Use a fixed group ID (e.g., `kafscale-demo-group`)

3. **In-memory storage only**: Consumed orders are stored in a `CopyOnWriteArrayList` with no persistence or size limits.
   - **Impact**: All data lost on restart, potential memory exhaustion
   - **Fix**: Add database persistence with pagination

4. **Security exposure**: Diagnostic endpoints (`/config`, `/cluster-info`) expose sensitive configuration and should be disabled or secured in production.
   - **Fix**: Add Spring Security with role-based access control

5. **No error handling**: Failed messages are logged but not retried or sent to a Dead Letter Queue.
   - **Impact**: Messages may be silently lost on consumer errors
   - **Fix**: Implement DLQ topic and retry backoff strategy

6. **No TLS/SASL**: All connections are unencrypted and unauthenticated.

### Single-Listener Limitation

The demo exposes only one Kafka listener per deployment. You must choose your network context:
- In-cluster DNS: `kafscale-broker:9092` (profile: `cluster`)
- External access: `localhost:39092` or `localhost:59092` (profiles: `default`, `local-lb`)

For dual connectivity, configure multiple advertised listeners in KafScale.

## Next Level Extensions

### Reliability & Resilience
- **DLQ implementation**: Add Dead Letter Queue topic with `@RetryableTopic` and `@DltHandler`
- **Circuit breaker**: Integrate Resilience4j for producer/consumer fault tolerance
- **Exactly-once semantics**: Enable idempotence and transactional producers

### Data Management
- **Database persistence**: Replace in-memory list with JPA/Hibernate + PostgreSQL
- **Pagination**: Add Spring Data pageable endpoints for large order lists
- **Event sourcing**: Store order state changes as event log

### Security
- **Authentication**: Add Spring Security with OAuth2/JWT
- **Authorization**: Role-based access control for admin endpoints
- **TLS**: Configure SSL for Kafka connections and REST endpoints

### Observability (Enhanced)
- **Structured logging**: Replace plain logs with JSON format (Logstash encoder)
- **Custom metrics**: Add Micrometer counters for business KPIs (orders/sec, avg processing time)
- **Distributed tracing**: Add TraceID to all log statements and HTTP responses
- **Alerting**: Configure Prometheus alerts for consumer lag and error rates
