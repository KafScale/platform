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

Local development platform using Docker Compose with images from a local registry.

## Prerequisites

1. Docker and Docker Compose installed
2. Images pushed to local registry (`192.168.0.131:5100`)
3. Docker configured for insecure registry (see below)

### Configure Insecure Registry

Docker Desktop → Settings → Docker Engine:

```json
{
  "insecure-registries": ["192.168.0.131:5100"]
}
```

### Push Images to Local Registry

```bash
# From repository root
make stage-release STAGE_REGISTRY=192.168.0.131:5100 STAGE_TAG=dev
```

### Verify Images

```bash
curl http://192.168.0.131:5100/v2/_catalog
```

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
| **lfs-proxy** | 8080, 9093 | LFS HTTP API, Kafka protocol |
| **console** | 3080 | Web management console |
| **e72-browser-demo** | 3072 | Browser LFS demo (optional) |

## Access Points

| Service | URL |
|---------|-----|
| LFS HTTP API | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| KafScale Console | http://localhost:3080 |
| E72 Browser Demo | http://localhost:3072 |
| Prometheus Metrics | http://localhost:9095/metrics |
| Health Check | http://localhost:9094/readyz |

## Testing

### Broker Advertised Address

The broker must advertise its container hostname so other services can connect.
Docker Compose sets:

- `KAFSCALE_BROKER_HOST=broker`
- `KAFSCALE_BROKER_PORT=9092`

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

### Large Uploads (Beast Mode)

Docker Compose ships with a large-upload profile:

- `KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE=7516192768` (7 GB)
- `KAFSCALE_LFS_PROXY_CHUNK_SIZE=16777216` (16 MB parts)
- `KAFSCALE_LFS_PROXY_HTTP_READ_TIMEOUT_SEC=1800`
- `KAFSCALE_LFS_PROXY_HTTP_WRITE_TIMEOUT_SEC=1800`
- `KAFSCALE_LFS_PROXY_HTTP_IDLE_TIMEOUT_SEC=120`

These settings allow 6+ GB streaming uploads without hitting default limits.

### LFS Download

```bash
# Get presigned URL
curl -X POST http://localhost:8080/lfs/download \
  -H "Content-Type: application/json" \
  -d '{"bucket":"kafscale","key":"default/test-uploads/lfs/...","mode":"presign"}'
```

## Traceability

Traceability is enabled in the compose file by default. It consists of:
- **LFS Ops Tracker** events emitted by the LFS proxy to `__lfs_ops_state`
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

LFS proxy tracker:
- `KAFSCALE_LFS_TRACKER_ENABLED=true`
- `KAFSCALE_LFS_TRACKER_TOPIC=__lfs_ops_state`
- `KAFSCALE_LFS_TRACKER_BATCH_SIZE=100`
- `KAFSCALE_LFS_TRACKER_FLUSH_MS=100`
- `KAFSCALE_LFS_TRACKER_ENSURE_TOPIC=true`
- `KAFSCALE_LFS_TRACKER_PARTITIONS=3`
- `KAFSCALE_LFS_TRACKER_REPLICATION_FACTOR=1`

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
REGISTRY=192.168.0.131:5100
TAG=dev

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

### Override Registry/Tag

```bash
REGISTRY=my-registry.local:5000 TAG=v1.5.0 docker-compose up -d
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
│               │   lfs-proxy   │                                  │
│               │ :8080 (HTTP)  │                                  │
│               │ :9093 (Kafka) │                                  │
│               └───────────────┘                                  │
└─────────────────────────────────────────────────────────────────┘
                        │
              ┌─────────┴─────────┐
              │   Host Machine    │
              │                   │
              │ localhost:8080    │ ← LFS HTTP API
              │ localhost:9092    │ ← Kafka Broker
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
docker-compose logs lfs-proxy

# Restart a service
docker-compose restart lfs-proxy
```

### Image pull fails

```bash
# Verify registry is accessible
curl http://192.168.0.131:5100/v2/_catalog

# Check Docker daemon config
docker info | grep -A5 "Insecure Registries"
```

### LFS upload fails

```bash
# Check LFS proxy logs
docker-compose logs lfs-proxy

# Verify MinIO is healthy
curl http://localhost:9000/minio/health/live

# Check bucket exists
docker-compose exec minio mc ls local/
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
