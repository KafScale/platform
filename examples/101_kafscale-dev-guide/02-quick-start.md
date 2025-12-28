# Quick Start with Docker

This section will guide you through setting up the KafScale infrastructure locally using Docker Compose. This covers the **Operations (OPS)** aspect of getting the cluster running.

## Overview

We'll set up a complete KafScale stack:

- **etcd**: Metadata storage
- **MinIO**: S3-compatible object storage
- **KafScale Broker**: The stateless broker

## Step 1: Clone the Repository

```bash
git clone https://github.com/novatechflow/kafscale.git
cd kafscale
```

## Step 2: Build and Run (The Easy Way)

We provide a `Makefile` to automate building docker images and starting the cluster.

### Option A: Complete Demo (Build + Run)

This command builds the local developer images and starts the cluster:

```bash
make demo
```

> **Note:** The first build may take a few minutes as it compiles Go code and builds Docker images.

### System Check

Once running, you should see logs streaming. You can verify the components in a separate terminal:

```bash
docker ps
```

You should see:
- `kafscale-broker` (Port 9092, 9093)
- `kafscale-etcd` (Port 2379)
- `kafscale-minio` (Port 9000, 9001)

## Step 3: Managing the Cluster

### Stopping the Cluster
Press `Ctrl+C` in the terminal running `make demo` to stop and clean up environment.

### Accessing Interfaces

- **MinIO Console**: [http://localhost:9001](http://localhost:9001) (User/Pass: `minioadmin`)
- **Prometheus Metrics**: [http://localhost:9093/metrics](http://localhost:9093/metrics)

## Troubleshooting

If `make demo` fails, check:
1.  **Ports**: Ensure ports `9092`, `8080`, `2379`, `9000` are free.
2.  **Docker Resource**: Ensure Docker Desktop has enough memory (recommended: 4GB+).

## Next Steps

Now that the **Infrastructure** is ready, let's run a **Developer** application to interact with it.

**Next**: [Running Your Application](04-running-your-app.md) →
