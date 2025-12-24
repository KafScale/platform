# Quick Start with Docker

This section will guide you through setting up KafScale locally using Docker Compose. You'll have a fully functional KafScale environment running in just a few minutes!

## Overview

We'll set up a complete KafScale stack with:

- **etcd**: Metadata storage for topics and consumer offsets
- **MinIO**: S3-compatible object storage for log segments
- **KafScale Broker**: The Kafka-compatible broker

```
Docker Compose Environment
┌────────────────────────────────────────┐
│                                        │
│  ┌────────┐  ┌────────┐  ┌──────────┐ │
│  │  etcd  │  │ MinIO  │  │ KafScale │ │
│  │ :2379  │  │ :9000  │  │  Broker  │ │
│  │        │  │ :9001  │  │  :9092   │ │
│  └────────┘  └────────┘  └──────────┘ │
│                                        │
└────────────────────────────────────────┘
              ▲
              │
      Your Spring Boot App
```

## Step 1: Clone the KafScale Repository

First, clone the KafScale repository to get access to the Makefile and build tools:

```bash
git clone https://github.com/novatechflow/kafscale.git
cd kafscale
```

## Step 2: Build KafScale Docker Images

KafScale provides a Makefile to build all necessary Docker images:

```bash
make docker-build
```

This command builds:
- `ghcr.io/novatechflow/kafscale-broker:dev` - The KafScale broker
- `ghcr.io/novatechflow/kafscale-operator:dev` - The Kubernetes operator (not needed for Docker setup)
- `ghcr.io/novatechflow/kafscale-console:dev` - The web console (optional)

> **Note:** The build process may take 5-10 minutes the first time as it compiles the Go code and creates the images.

## Step 3: Run the App Locally (Optional)

If you prefer to run the Spring Boot application locally (instead of in Docker), you can start it using Maven.

> **Important:** The application needs a running KafScale broker to function. You will set up the broker in the next step.

```bash
cd docs/guide/examples/spring-boot-kafscale-demo
mvn clean spring-boot:run
```

Once started, the application will be available at [http://localhost:8083](http://localhost:8083).

## Step 4: Create Docker Compose Configuration

Create a `docker-compose.yml` file in the `docs/guide/examples/` directory (or anywhere you prefer):

```yaml
version: '3.8'

services:
  # etcd - Metadata storage
  etcd:
    image: quay.io/coreos/etcd:v3.5.9
    container_name: kafscale-etcd
    environment:
      - ETCD_NAME=etcd0
      - ETCD_INITIAL_ADVERTISE_PEER_URLS=http://etcd:2380
      - ETCD_LISTEN_PEER_URLS=http://0.0.0.0:2380
      - ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379
      - ETCD_ADVERTISE_CLIENT_URLS=http://etcd:2379
      - ETCD_INITIAL_CLUSTER=etcd0=http://etcd:2380
      - ETCD_INITIAL_CLUSTER_STATE=new
      - ETCD_INITIAL_CLUSTER_TOKEN=kafscale-cluster
    ports:
      - "2379:2379"
    networks:
      - kafscale-network
    healthcheck:
      test: ["CMD", "etcdctl", "endpoint", "health"]
      interval: 10s
      timeout: 5s
      retries: 5

  # MinIO - S3-compatible storage
  minio:
    image: quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z
    container_name: kafscale-minio
    command: server /data --console-address :9001
    environment:
      - MINIO_ROOT_USER=minioadmin
      - MINIO_ROOT_PASSWORD=minioadmin
    ports:
      - "9000:9000"  # S3 API
      - "9001:9001"  # Web Console
    volumes:
      - minio-data:/data
    networks:
      - kafscale-network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Create MinIO bucket
  minio-setup:
    image: quay.io/minio/mc:latest
    container_name: kafscale-minio-setup
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set myminio http://minio:9000 minioadmin minioadmin;
      mc mb myminio/kafscale || true;
      mc mb myminio/kafscale-snapshots || true;
      exit 0;
      "
    networks:
      - kafscale-network

  # KafScale Broker
  kafscale-broker:
    image: ghcr.io/novatechflow/kafscale-broker:dev
    container_name: kafscale-broker
    depends_on:
      etcd:
        condition: service_healthy
      minio:
        condition: service_healthy
      minio-setup:
        condition: service_completed_successfully
    environment:
      - KAFSCALE_BROKER_ID=0
      - KAFSCALE_BROKER_HOST=kafscale-broker
      - KAFSCALE_BROKER_PORT=9092
      - KAFSCALE_ETCD_ENDPOINTS=http://etcd:2379
      - KAFSCALE_S3_BUCKET=kafscale
      - KAFSCALE_S3_REGION=us-east-1
      - KAFSCALE_S3_ENDPOINT=http://minio:9000
      - KAFSCALE_S3_PATH_STYLE=true
      - KAFSCALE_S3_ACCESS_KEY=minioadmin
      - KAFSCALE_S3_SECRET_KEY=minioadmin
      - KAFSCALE_S3_NAMESPACE=default
      - KAFSCALE_LOG_LEVEL=info
    ports:
      - "9092:9092"  # Kafka protocol
      - "9093:9093"  # Metrics + gRPC
    networks:
      - kafscale-network
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "9092"]
      interval: 10s
      timeout: 5s
      retries: 10

networks:
  kafscale-network:
    driver: bridge

volumes:
  minio-data:
```

> **Tip:** The complete `docker-compose.yml` file is available in [`examples/docker-compose.yml`](examples/docker-compose.yml).

## Step 5: Start the KafScale Stack

Navigate to the directory containing your `docker-compose.yml` and start all services:

```bash
cd docs/guide/examples
docker-compose up -d
```

This will:
1. Start etcd for metadata storage
2. Start MinIO for S3-compatible storage
3. Create the necessary buckets in MinIO
4. Start the KafScale broker

## Step 6: Verify Data in S3

If you want to verify persistence:

1. Access the MinIO Console at [http://localhost:9001](http://localhost:9001).
2. Login with `minioadmin` / `minioadmin`.
3. Browse the `kafscale` bucket to see the stored log segments.

## Step 7: Managing the Stack

To stop the entire stack, simply go to your `make demo` terminal and press `Ctrl+C`.

This will clean up the Docker containers (broker, console, MinIO) automatically.

### View Logs

Logs are streamed directly to your terminal when running `make demo`.

## Troubleshooting

### Port Conflicts

If `make demo` fails, ensure ports `9092`, `8080`, `2379`, and `9000-9001` are free.

### Connection Refused

If the Spring Boot app cannot connect:
1. Ensure `make demo` is running and healthy.
2. Check that `KAFSCALE_BROKER_ADDR` in the demo output matches `localhost:39092`.
3. Try clicking "Test Connection" in the UI again.

## Next Steps

🎉 **Congratulations!** You now have a fully functional KafScale environment running locally.

**Next**: [Spring Boot Configuration](03-spring-boot-configuration.md) → Learn how to configure your Spring Boot application to connect to KafScale.
