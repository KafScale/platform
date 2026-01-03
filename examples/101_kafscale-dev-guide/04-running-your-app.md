# Running Your Application: Platform Demo + E20

Now that you have verified the local demo in [Chapter 2](02-quick-start.md), we will run the full platform demo on kind and use the Spring Boot demo app (E20).

## Step 1: Launch the Platform Demo

This command builds the local images, creates a kind cluster, installs the Helm chart, and deploys the Spring Boot demo app:

```bash
make demo-guide-pf
```

When it finishes, you should see messages about port-forwarding:
- **KafScale Console**: [http://localhost:8080/ui](http://localhost:8080/ui)
- **Spring Boot Demo (E20)**: [http://localhost:8093](http://localhost:8093)
- **Broker**: `localhost:39092`

Logs are written to `/tmp/kafscale-demo-*.log`.

![KafScale Demo App - Kafka Client Configs](images/image-02.png)

## Step 2: Exercise the E20 Demo API

**Send an Order:**
```bash
curl -X POST http://localhost:8093/api/orders \
  -H "Content-Type: application/json" \
  -d '{"product": "Widget", "quantity": 5}'
```

**Check Health:**
```bash
curl http://localhost:8093/api/orders/health
```

You should see "Sending order..." and "Received order..." in the Spring Boot logs at `/tmp/kafscale-demo-spring.log`.

![KafScale Demo App - Cluster Infos](images/image-03.png)

## Clean Up

When you are done:

```bash
make demo-guide-pf-clean
```

If you plan to re-run the demo, wait for port-forwards to exit before starting again.

## Connecting Your Own Application

To connect your own apps to the platform demo:

- **Bootstrap Server**: `localhost:39092`
- **Security Protocol**: `PLAINTEXT`

> **Note:** The demo currently exposes a single listener, so choose one network context at a time (in-cluster or local port-forward/LB). That’s why the Spring Boot app uses distinct profiles.

**Example `application.properties`:**
```properties
spring.kafka.bootstrap-servers=localhost:39092
```

## Which Deployment Mode Fits?

Use these questions to decide which Spring Boot profile (or deployment path) fits best:

1. Do you want the app and KafScale in the same Kubernetes cluster for staging/production?
2. Are you developing locally and want the simplest loopback setup to a local broker?
3. Do you need to run the app locally but connect to a remote cluster or different network?
4. Are you limited to a single exposed listener and must pick one access path?
5. Do you need in-cluster DNS names like `kafscale-broker:9092`, or external access via port-forward/LB?

**Next**: [Troubleshooting](05-troubleshooting.md) →
