# KafScale with Synology NAS S3 Storage

This deployment uses external MinIO running on a Synology NAS for S3 storage instead of a local container.

## Prerequisites

1. **Synology NAS MinIO** running at `192.168.0.131:9100`
   - API endpoint: `http://192.168.0.131:9100`
   - Console: `http://192.168.0.131:9101`

2. **Bucket**: Auto-created by the `s3-init` service on startup

## Quick Start

```bash
cd deploy/synology-s3
docker-compose up -d
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| s3-init | - | Creates bucket on Synology (runs once) |
| etcd | 2379 | Metadata store |
| broker | 9092 | KafScale broker |
| lfs-proxy | 8080 | HTTP API + Swagger UI |
| lfs-proxy | 9093 | Kafka protocol (LFS) |
| console | 3080 | Operations console |
| e72-browser-demo | 3072 | Browser SDK demo |

## Access Points

- **Console**: http://localhost:3080 (user: `kafscaleadmin`, pass: `kafscale`)
- **Swagger UI**: http://localhost:8080/swagger
- **MinIO Console**: http://192.168.0.131:9101

## Test LFS Upload

```bash
# Upload a file
echo "Hello Synology S3!" > test.txt
curl -X POST http://localhost:8080/lfs/produce \
  -H "X-Kafka-Topic: synology-test" \
  -H "Content-Type: text/plain" \
  --data-binary @test.txt

# Upload a larger file
curl -X POST http://localhost:8080/lfs/produce \
  -H "X-Kafka-Topic: synology-uploads" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @largefile.bin
```

## Configuration

S3 settings are configured via YAML anchors at the top of `docker-compose.yaml`:

```yaml
x-s3-endpoint: &s3-endpoint "http://192.168.0.131:9100"
x-s3-bucket: &s3-bucket "kafscale"
x-s3-region: &s3-region "us-east-1"
x-s3-access-key: &s3-access-key "minioadmin"
x-s3-secret-key: &s3-secret-key "minioadmin"
```

To change credentials or bucket, update these anchors.

## Advantages of Synology NAS Storage

- **Persistent storage** across container restarts
- **RAID protection** for data durability
- **Large capacity** for storing big files
- **Network-shared** access from multiple hosts
- **Built-in backup** integration with Synology features
