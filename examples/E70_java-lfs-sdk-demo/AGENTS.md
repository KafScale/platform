# E70 Java LFS SDK Demo - Agent Requirements

This document tracks the requirements and dependencies for the E70 Java LFS SDK Demo.

## Overview

The E70 demo demonstrates the complete LFS (Large File Support) workflow using the Java SDK:
1. Produce large blobs via HTTP to the LFS Proxy
2. Consume pointer records from Kafka
3. Resolve blobs from S3/MinIO using the pointer metadata

## Dependencies

### Build Dependencies

| Dependency | Version | Purpose |
| --- | --- | --- |
| Java | 17+ | Runtime and compilation |
| Maven | 3.x | Build tool |
| Docker | latest | Container builds |
| kubectl | latest | Kubernetes CLI |
| kind | latest | Local Kubernetes clusters |

### Java Dependencies

| Artifact | Version | Scope |
| --- | --- | --- |
| `org.kafscale:lfs-sdk` | 0.2.0-SNAPSHOT | LFS client SDK (local install) |
| `org.apache.kafka:kafka-clients` | 4.1.1 | Kafka consumer |
| AWS SDK v2 (via lfs-sdk) | 2.31.5 | S3 client |
| Jackson (via lfs-sdk) | 2.17.2 | JSON serialization |

### Infrastructure Dependencies

| Component | Purpose | Port |
| --- | --- | --- |
| LFS Proxy | HTTP produce endpoint | 8080 |
| LFS Proxy Health | Readiness checks | 9094 |
| Kafka Broker | Pointer record storage | 9092 |
| MinIO | S3-compatible blob storage | 9000 |

## Source Directories

### SDK Sources
- `lfs-client-sdk/java/src/` - Java SDK source files
- `lfs-client-sdk/java/pom.xml` - SDK build configuration

### Proxy Sources
- `cmd/lfs-proxy/` - LFS Proxy Go source files
- `pkg/lfs/` - Shared LFS Go packages

### Demo Sources
- `examples/E70_java-lfs-sdk-demo/src/` - Demo Java files
- `examples/E70_java-lfs-sdk-demo/pom.xml` - Demo build configuration

## Build Requirements

### Incremental Build Triggers

The Makefile uses stamp files to track when rebuilds are needed:

| Stamp File | Triggers Rebuild When |
| --- | --- |
| `.build/sdk.stamp` | SDK Java sources or pom.xml change |
| `.build/proxy.stamp` | Proxy Go sources or pkg/lfs change |
| `.build/demo.stamp` | Demo Java sources or pom.xml change |

### Build Order

1. **SDK** - Must be built first and installed to local Maven repo
2. **LFS Proxy** - Docker image built and loaded into kind cluster
3. **Demo** - Compiled against locally installed SDK

## Runtime Requirements

### Kubernetes Cluster

- Kind cluster named `kafscale-demo`
- Namespace `kafscale-demo` with:
  - LFS Proxy deployment
  - Kafka broker (kafscale-broker)
  - MinIO deployment

### Port Forwards

All services must be port-forwarded for local access:

```bash
kubectl -n kafscale-demo port-forward svc/lfs-proxy 8080:8080
kubectl -n kafscale-demo port-forward svc/lfs-proxy 9094:9094
kubectl -n kafscale-demo port-forward svc/kafscale-broker 9092:9092
kubectl -n kafscale-demo port-forward svc/minio 9000:9000
```

## Environment Configuration

### Required for Demo

| Variable | Default | Required |
| --- | --- | --- |
| `LFS_HTTP_ENDPOINT` | `http://localhost:8080/lfs/produce` | No |
| `LFS_TOPIC` | `lfs-demo-topic` | No |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | No |
| `S3_BUCKET` | `kafscale` | No |
| `S3_ENDPOINT` | `http://localhost:9000` | No |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | No |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` | No |

### Optional Tuning

| Variable | Default | Description |
| --- | --- | --- |
| `LFS_PAYLOAD_SIZE` | 524288 | Test payload size (bytes) |
| `LFS_INLINE_THRESHOLD` | 102400 | Inline vs LFS threshold |

## Functional Requirements

### Producer Flow

1. Generate test payload (configurable size)
2. POST payload to LFS Proxy HTTP endpoint
3. Receive LFS envelope response with:
   - S3 key where blob is stored
   - SHA256 checksum
   - Size and metadata

### Consumer Flow

1. Subscribe to Kafka topic
2. Poll for records containing LFS pointers
3. Detect LFS envelope in record value
4. Resolve blob from S3 using envelope metadata
5. Return resolved payload bytes

### Verification

- Produced envelope key matches expected format
- Consumed record is identified as LFS envelope
- Resolved payload size matches original

## Test Scenarios

### Basic Flow
```bash
make run
```
- Produces 512KB blob
- Consumes pointer from Kafka
- Resolves blob from MinIO

### Custom Payload Size
```bash
LFS_PAYLOAD_SIZE=10485760 make run  # 10MB payload
```

### Full Stack Test
```bash
make run-all  # Includes port-forward setup
```

## Maintenance Notes

### Version Sync

The demo `pom.xml` must reference the correct SDK version:
- Check `lfs-client-sdk/java/pom.xml` for current version
- Update demo `pom.xml` property `<lfs.sdk.version>` to match

### Image Updates

When proxy sources change:
1. Docker image is rebuilt: `make -C ../.. docker-build-lfs-proxy`
2. Image is loaded to kind: `kind load docker-image ...`
3. Deployment is restarted: `kubectl rollout restart`

### Clean State

To reset all build state:
```bash
make clean-all
```

This removes:
- `.build/` stamp directory
- `target/` Maven output
- `.tmp/` port-forward PIDs
- SDK target directory
