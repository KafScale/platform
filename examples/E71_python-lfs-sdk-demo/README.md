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

# Python LFS SDK Video Upload Demo (E71)

This demo showcases the Python LFS SDK for video uploads with different file sizes:
- **Small** (1 MB) - Quick validation
- **Midsize** (50 MB) - Typical web video
- **Large** (200 MB) - High-quality content

## Prerequisites

1. **Bring up the LFS demo stack** (keeps the cluster running for E70/E71):

```bash
make lfs-demo-video
```

2. **Port-forward the services** (separate terminal):

```bash
kubectl -n kafscale-video port-forward svc/lfs-proxy 8080:8080 &
kubectl -n kafscale-video port-forward svc/kafscale-broker 9092:9092 &
kubectl -n kafscale-video port-forward svc/minio 9000:9000 &
```

3. **Install the Python SDK locally**:

```bash
cd lfs-client-sdk/python
python -m venv .venv
. .venv/bin/activate
pip install -e .
```

## Run the Demo

```bash
cd examples/E71_python-lfs-sdk-demo

# Run all video sizes (default)
python demo.py

# Run specific sizes
VIDEO_SIZES=small python demo.py
VIDEO_SIZES=small,midsize python demo.py
VIDEO_SIZES=large python demo.py
```

Or use Makefile targets:

```bash
make run-small      # 1 MB test
make run-midsize    # 50 MB test
make run-large      # 200 MB test
make run-all        # All sizes
```

## Video Size Presets

| Preset | File Name | Size | Use Case |
|--------|-----------|------|----------|
| `small` | `small-clip.mp4` | 1 MB | Quick validation, CI tests |
| `midsize` | `promo-video.mp4` | 50 MB | Typical promotional video |
| `large` | `full-feature.mp4` | 200 MB | Full-length content |

## Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `LFS_HTTP_ENDPOINT` | `http://localhost:8080/lfs/produce` | LFS proxy HTTP endpoint |
| `LFS_TOPIC` | `video-raw` | Kafka topic for pointer records |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap address |
| `S3_BUCKET` | `kafscale-lfs` | Bucket used by the LFS proxy |
| `S3_ENDPOINT` | `http://localhost:9000` | MinIO endpoint |
| `S3_REGION` | `us-east-1` | S3 region |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` | MinIO secret key |
| `VIDEO_SIZES` | `small,midsize,large` | Comma-separated sizes to test |

## Expected Output

```
=== Python LFS SDK Video Upload Demo ===
Endpoint: http://localhost:8080/lfs/produce
Topic: video-raw
Bucket: kafscale-lfs
Videos to test: ['small-clip.mp4', 'promo-video.mp4', 'full-feature.mp4']

--- Testing small-clip.mp4 (1.0 MB) ---
  Generating 1048576 bytes...
  Generated in 0.01s, sha256=a1b2c3d4e5f6...
  Uploading to LFS proxy...
  Upload completed in 0.15s
  Envelope: key=lfs/..., sha256=a1b2c3d4e5f6...
  Consuming from topic video-raw...
  Resolved: is_envelope=True, payload_bytes=1048576
  SUCCESS: Payload verified (1048576 bytes)

--- Testing promo-video.mp4 (50.0 MB) ---
  ...

--- Testing full-feature.mp4 (200.0 MB) ---
  ...

=== Test Summary ===
  small-clip.mp4: PASS
  promo-video.mp4: PASS
  full-feature.mp4: PASS

All video upload tests passed!
```

## Python SDK Features

The Python LFS SDK (`kafscale-lfs-sdk`) provides:

- **LfsProducer**: HTTP client with retry/backoff for reliable uploads
- **LfsResolver**: S3 blob resolution with checksum validation
- **LfsEnvelope**: Structured envelope parsing
- **produce_lfs()**: Convenience function for one-shot uploads

### Example Usage

```python
from lfs_sdk import LfsProducer, LfsResolver
import boto3

# Upload
with LfsProducer("http://localhost:8080/lfs/produce") as producer:
    envelope = producer.produce(
        topic="video-raw",
        payload=video_bytes,
        key=b"my-video.mp4",
        headers={"Content-Type": "video/mp4"},
    )

# Resolve
s3 = boto3.client("s3", endpoint_url="http://localhost:9000")
resolver = LfsResolver(bucket="kafscale-lfs", s3_client=s3)
record = resolver.resolve(kafka_message.value())
if record.is_envelope:
    video_data = record.payload
```

## Cleanup

Stop port-forwards when done. The cluster remains running until you delete it or run cleanup scripts.

## Files

| File | Description |
|------|-------------|
| `demo.py` | Main video upload demo script |
| `Makefile` | Build and run targets |
| `README.md` | This documentation |
