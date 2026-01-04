# Quick Start: Local Demo + E10

This section guides you through the local demo using `make demo`. It runs the broker and console as local Go processes and uses a small MinIO helper container for S3-compatible storage. No Docker Compose is used.

## Overview

We'll run a complete KafScale demo stack:

- **Embedded etcd**: Metadata storage (started by the demo)
- **MinIO**: S3-compatible object storage (helper container)
- **KafScale Broker**: Stateless broker (local process)
- **KafScale Console**: UI for inspecting the cluster

## Step 1: Clone the Repository

```bash
git clone https://github.com/novatechflow/kafscale.git
cd kafscale
```

## Step 2: Build and Run the Local Demo

We provide a `Makefile` to automate building docker images and starting the cluster.

This command starts the local demo:

```bash
make demo
```

> **Note:** The demo starts local Go processes and a MinIO helper container. The first run may take a few minutes.

### System Check

Once running, you should see logs streaming. You can verify the MinIO helper container in a separate terminal:

```bash
docker ps
```

You should see:
- `kafscale-minio` (Port 9000, 9001)

## Step 3: Run the E10 Java Client Demo

**Estimated time**: 5-10 minutes (first-time build may take longer)

In another terminal:

```bash
cd examples/E10_java-kafka-client-demo
mvn clean package exec:java
```

What it does:
- Connects to `localhost:39092`
- Creates a topic `demo-topic-1`
- Produces 25 messages
- Consumes 5 messages
- Prints cluster metadata

**Verify success**:
You should see:
- ✅ "Sent message: key=key-0 value=message-0 partition=0 offset=0"
- ✅ "Received message: key=key-0 value=message-0 partition=0 offset=0"
- ✅ "Successfully consumed 5 messages."

If you see connection errors, check [Troubleshooting](05-troubleshooting.md).

## Step 4: Managing the Demo

### Stopping the Demo Cleanly
Press `Ctrl+C` in the terminal running `make demo` to stop the local broker and console. Wait for the test process to exit before starting another demo.

If you see ports still in use (39092/39093/39094), run:
```bash
make stop-containers
```
This stops the MinIO helper and frees broker ports.

### Accessing Interfaces

- **KafScale Console**: [http://localhost:48080/ui](http://localhost:48080/ui) (local demo uses port 48080)
- **MinIO Console**: [http://localhost:9001](http://localhost:9001) (User/Pass: `minioadmin`)
- **Prometheus Metrics**: [http://localhost:39093/metrics](http://localhost:39093/metrics)

> **Note**: The platform demo (Chapter 4) uses port 8080 for the console instead of 48080 to avoid conflicts with common development ports.

## Troubleshooting

If `make demo` fails, check:
1.  **Ports**: Ensure ports `39092`, `39093`, `39094`, `48080`, `9000` are free.
2.  **Docker Resource**: Ensure Docker has enough memory for the MinIO helper (recommended: 2GB+).

## Next Steps

Next, we'll run the platform demo on kind and use the Spring Boot demo app (E20).

**Next**: [Running Your Application](04-running-your-app.md) →
