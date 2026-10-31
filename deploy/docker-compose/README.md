<!--
Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
This project is supported and financed by Scalytics, Inc. (www.scalytics.io).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# KafScale Docker Compose

Run a local KafScale platform with Docker Compose. Verified with Docker Compose v2.29.7 and the `v1.6.3` images.

## Prerequisites

You need Docker and Docker Compose v2.

By default the compose file pulls `ghcr.io/kafscale/kafscale-broker`, `kafscale-proxy` and `kafscale-console` at tag `v1.6.3`. These images are public. MinIO is built from `test/e2e/minio.Dockerfile`.

### Use a Local Registry

Push images to your own registry, then set `REGISTRY` and `TAG`. Images are pulled from `<registry>/kafscale/<image>:<tag>`.

```bash
REGISTRY=<registry> TAG=<tag> docker compose up -d
```

If the registry uses plain HTTP, add it to `insecure-registries` in the Docker daemon config.

## Quick Start

```bash
cd deploy/docker-compose

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop all services
docker-compose down
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| **etcd** | 2379 | Coordination store |
| **minio** | 9000, 9001 | S3 storage (API, Console) |
| **broker** | 9092 | KafScale Kafka broker |
| **proxy** | 8080, 9093, 9094 | LFS HTTP API, Kafka protocol, health |
| **console** | 3080 | Web management console |
| **e72-browser-demo** | 3072 | Browser LFS demo, `demo` profile only |

## Access Points

| Service | URL |
|---------|-----|
| LFS HTTP API | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| KafScale Console | http://localhost:3080 |
| E72 Browser Demo | http://localhost:3072 |
| Health Check | http://localhost:9094/readyz |

## Testing

### Broker Advertised Address

The broker must advertise its container hostname so other services can connect.
Docker Compose sets:

- `KAFSCALE_BROKER_HOST=broker`
- `KAFSCALE_BROKER_PORT=9092`

### Proxy Kafka Port

The proxy advertises `localhost:9093`. Use it from the host only. Clients inside the compose network connect to `broker:9092`.

### Health Check

```bash
curl http://localhost:9094/readyz
```

### LFS Upload

```bash
# Upload a file
curl -X POST http://localhost:8080/lfs/produce \
  -H "X-Kafka-Topic: test-uploads" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @myfile.bin

# Upload with key
curl -X POST http://localhost:8080/lfs/produce \
  -H "X-Kafka-Topic: test-uploads" \
  -H "X-Kafka-Key: $(echo -n 'my-key' | base64)" \
  -H "Content-Type: video/mp4" \
  --data-binary @video.mp4
```

### LFS Download

```bash
# Stream a verified object through the proxy.
# Use bucket/key/sha256/size from the LFS envelope returned by /lfs/produce.
curl -X POST http://localhost:8080/lfs/download \
  -H "Content-Type: application/json" \
  -d '{
    "bucket": "kafscale",
    "key": "default/@lfs/test-uploads/2026/02/05/obj-...",
    "mode": "stream",
    "integrity": {
      "sha256": "<envelope-sha256>",
      "checksum_alg": "sha256",
      "size": 1024
    }
  }' \
  -o downloaded-blob.bin
```

## Traceability

Traceability is enabled in the compose file by default. It consists of:
- **LFS Ops Tracker** events emitted by the proxy to `__lfs_ops_state`
- **Console LFS Dashboard** consuming those events and exposing APIs/UI

### Where to see it

1) **Console UI**
   - Open http://localhost:3080
   - Navigate to the **LFS** tab for objects, topics, live events, and S3 browser.

2) **Raw events from Kafka**
```bash
kcat -b localhost:9092 -C -t __lfs_ops_state -o beginning
```

### Key settings (compose)

Proxy tracker:
- `KAFSCALE_LFS_PROXY_BACKENDS=broker:9092` turns the tracker on. Without it the proxy logs `lfs ops tracker: no brokers configured, tracker disabled`.
- The tracker writes to `__lfs_ops_state`, the default `KAFSCALE_LFS_TRACKER_TOPIC`.

Console LFS dashboard:
- `KAFSCALE_CONSOLE_LFS_ENABLED=true`
- `KAFSCALE_CONSOLE_KAFKA_BROKERS=broker:9092`
- `KAFSCALE_CONSOLE_LFS_S3_*` set to MinIO credentials

### Kafka (via kcat)

```bash
# List topics
kcat -b localhost:9092 -L

# Produce message (goes through regular broker, not LFS)
echo "hello" | kcat -b localhost:9092 -P -t test-topic

# Consume messages
kcat -b localhost:9092 -C -t test-topic -o beginning
```

## Configuration

### Environment Variables

Edit `.env` to customize:

```bash
# Registry settings
REGISTRY=ghcr.io
TAG=v1.6.3

# MinIO credentials
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
```

### Console Port Configuration

The console listens on `KAFSCALE_CONSOLE_HTTP_ADDR` (default `:8080`). In the compose file
we set it to `:3080` and map `3080:3080`.

### Console Login

The console UI requires credentials. Compose sets:
- `KAFSCALE_UI_USERNAME=kafscaleadmin`
- `KAFSCALE_UI_PASSWORD=kafscale`

Override these in `docker-compose.yaml` or via your own `.env.local` if needed.

### Run the E72 Browser Demo

`e72-browser-demo` runs only with the `demo` profile. Its image `kafscale-e72-browser-demo` is not published and the repository has no Dockerfile for it. Push it to your registry first, then run:

```bash
REGISTRY=<registry> TAG=<tag> docker compose --profile demo up -d
```

`<registry>` and `<tag>` must hold all KafScale images, including `kafscale-e72-browser-demo`. Without the profile, `docker compose up -d` starts every service except the demo.

### Override Registry/Tag

```bash
REGISTRY=my-registry.local:5000 TAG=v1.6.3 docker-compose up -d
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Docker Compose Network                       │
│                                                                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │   etcd   │    │  minio   │    │  broker  │    │ console  │  │
│  │  :2379   │    │ :9000/01 │    │  :9092   │    │  :3080   │  │
│  └────┬─────┘    └────┬─────┘    └────┬─────┘    └──────────┘  │
│       │               │               │                         │
│       └───────────────┼───────────────┘                         │
│                       │                                          │
│               ┌───────┴───────┐                                  │
│               │     proxy     │                                  │
│               │ :8080 (HTTP)  │                                  │
│               │ :9092 (Kafka) │                                  │
│               └───────────────┘                                  │
└─────────────────────────────────────────────────────────────────┘
                        │
              ┌─────────┴─────────┐
              │   Host Machine    │
              │                   │
              │ localhost:8080    │ ← LFS HTTP API
              │ localhost:9092    │ ← Kafka Broker
              │ localhost:9093    │ ← Kafka Proxy
              │ localhost:9001    │ ← MinIO Console
              │ localhost:3080    │ ← KafScale Console
              └───────────────────┘
```

## Troubleshooting

### Services not starting

```bash
# Check service status
docker-compose ps

# View logs for specific service
docker-compose logs proxy

# Restart a service
docker-compose restart proxy
```

### Image pull fails

```bash
# Check that the default image pulls
docker pull ghcr.io/kafscale/kafscale-broker:v1.6.3

# For a local registry, check the Docker daemon config
docker info | grep -A5 "Insecure Registries"
```

### LFS upload fails

```bash
# Check proxy logs
docker-compose logs proxy

# Verify MinIO is healthy
curl http://localhost:9000/minio/health/live

# Check bucket exists
docker-compose exec minio ls /data
```

### Reset everything

```bash
# Stop and remove all containers, volumes
docker-compose down -v

# Start fresh
docker-compose up -d
```

## Volumes

| Volume | Purpose |
|--------|---------|
| `etcd-data` | etcd persistent storage |
| `minio-data` | MinIO object storage |
| `broker-data` | Kafka broker data |

To persist data across restarts, volumes are used. To reset:

```bash
docker-compose down -v
```
