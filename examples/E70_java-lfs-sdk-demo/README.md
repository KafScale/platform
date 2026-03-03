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

# Java LFS SDK Demo (E70)

This demo shows how to use the Java LFS SDK to:
- Produce a blob via the LFS proxy HTTP API.
- Consume the pointer record via Kafka.
- Resolve the blob from S3/MinIO.

## Prerequisites

1. **Bring up the LFS demo stack once** (from repo root, keeps the cluster running):

```bash
LFS_DEMO_CLEANUP=0 make lfs-demo
```

2. **Port-forward the services** (separate terminal):

```bash
kubectl -n kafscale-demo port-forward svc/lfs-proxy 8080:8080 &
kubectl -n kafscale-demo port-forward svc/lfs-proxy 9094:9094 &
kubectl -n kafscale-demo port-forward svc/kafscale-broker 9092:9092 &
kubectl -n kafscale-demo port-forward svc/minio 9000:9000 &
```

Or use `make port-forward` from this directory.

## Run the Demo

### Quick Start (with manual port-forwards)

```bash
cd examples/E70_java-lfs-sdk-demo
make run
```

### All-in-One (starts port-forwards automatically)

```bash
make run-all
```

This starts background port-forwards, waits for readiness, and runs the demo.

## Incremental Builds

The Makefile uses stamp files for incremental builds. Components only rebuild when their sources change:

| Component | Rebuilds When |
| --- | --- |
| SDK | `lfs-client-sdk/java/src/**/*.java` or `pom.xml` changes |
| LFS Proxy | `cmd/lfs-proxy/*.go` or `pkg/lfs/*.go` changes |
| Demo | `src/**/*.java` or local `pom.xml` changes |

### Force Rebuild

```bash
make install-sdk     # Force rebuild SDK
make refresh-proxy   # Force rebuild and redeploy proxy
make clean           # Remove local build stamps
make clean-all       # Remove all build artifacts including SDK
```

## Makefile Targets

| Target | Description |
| --- | --- |
| `run` | Incremental build + run (needs port-forwards) |
| `run-all` | Build and run with background port-forwards |
| `install-sdk` | Force install SDK (always rebuild) |
| `install-sdk-if-changed` | Install SDK only if sources changed |
| `refresh-proxy` | Force rebuild and redeploy LFS proxy |
| `refresh-proxy-if-changed` | Rebuild proxy only if sources changed |
| `build-demo` | Build demo JAR |
| `start-stack` | Start the LFS demo stack (keeps running) |
| `start-stack-if-missing` | Start stack only if not already running |
| `port-forward` | Port-forward all services (foreground) |
| `wait-ready` | Wait for LFS proxy /readyz endpoint |
| `wait-http` | Wait for LFS proxy HTTP port |
| `list-pods` | List pods and services in demo namespace |
| `check-deps` | Check required tools (kubectl, kind, mvn, docker) |
| `clean` | Remove local build artifacts |
| `clean-all` | Remove all build artifacts including SDK |
| `help` | Show all available targets |

## Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `LFS_HTTP_ENDPOINT` | `http://localhost:8080/lfs/produce` | LFS proxy HTTP endpoint |
| `LFS_TOPIC` | `lfs-demo-topic` | Kafka topic for pointer records |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap address (broker) |
| `KAFKA_CONSUMER_BOOTSTRAP` | `localhost:9092` | Override consumer broker address |
| `S3_BUCKET` | `kafscale` | Bucket used by the LFS proxy |
| `S3_ENDPOINT` | `http://localhost:9000` | MinIO endpoint |
| `S3_REGION` | `us-east-1` | S3 region |
| `S3_PATH_STYLE` | `true` | Use path-style S3 addressing |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` | MinIO secret key |
| `LFS_PAYLOAD_SIZE` | `524288` | Payload size in bytes (512KB default) |
| `LFS_INLINE_THRESHOLD` | `102400` | Inline threshold in bytes (100KB default) |

### Makefile Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `DEMO_NAMESPACE` | `kafscale-demo` | Kubernetes namespace |
| `KIND_CLUSTER` | `kafscale-demo` | Kind cluster name |
| `LFS_PROXY_IMAGE` | `ghcr.io/kafscale/kafscale-lfs-proxy:dev` | LFS proxy image |

## Expected Output

```
==> Running E70 Java LFS Demo...
Produced envelope: key=lfs/lfs-demo-topic/abc123... sha256=...
Resolved record: isEnvelope=true payloadBytes=524288
```

## Architecture

```
┌─────────────────┐     HTTP POST      ┌─────────────────┐
│  E70 Demo       │ ─────────────────► │  LFS Proxy      │
│  (LfsProducer)  │    /lfs/produce    │                 │
└─────────────────┘                    └────────┬────────┘
                                                │
                    ┌───────────────────────────┼───────────────────────────┐
                    │                           │                           │
                    ▼                           ▼                           │
            ┌───────────────┐           ┌───────────────┐                   │
            │   MinIO/S3    │           │    Kafka      │                   │
            │  (blob data)  │           │  (pointer)    │                   │
            └───────────────┘           └───────┬───────┘                   │
                    ▲                           │                           │
                    │                           ▼                           │
                    │                   ┌───────────────┐                   │
                    │ ◄─────────────────│  E70 Demo     │                   │
                    │    fetch blob     │  (LfsConsumer)│                   │
                    │                   └───────────────┘                   │
                    └───────────────────────────────────────────────────────┘
```

## Cleanup

Stop port-forwards when done. The cluster remains running until you delete it.

```bash
# Kill port-forwards (if using run-all, they stop automatically)
pkill -f "port-forward.*kafscale-demo"

# Remove local build stamps
make clean
```

## Troubleshooting

### ConnectException

Usually means the local port-forward is not running or the proxy is not ready.

```bash
# Check if port-forwards are running
pgrep -f "port-forward"

# Check pod status
make list-pods

# Use run-all which waits for readiness
make run-all
```

### SDK Version Mismatch

If you see dependency errors, ensure the SDK version in `pom.xml` matches the installed SDK:

```bash
# Check SDK version
cat ../../lfs-client-sdk/java/pom.xml | grep '<version>'

# Force reinstall SDK
make install-sdk
```

### Proxy Not Ready

```bash
# Wait for proxy readiness
make wait-ready

# Check proxy logs
kubectl -n kafscale-demo logs -l app=lfs-proxy
```

## Development Workflow

1. **Edit SDK code** in `lfs-client-sdk/java/src/`
2. **Edit Proxy code** in `cmd/lfs-proxy/` or `pkg/lfs/`
3. **Run `make run`** - only changed components rebuild
4. **Force rebuild** with `make install-sdk` or `make refresh-proxy` if needed

The incremental build system tracks changes using stamp files in `.build/`:
- `.build/sdk.stamp` - SDK build timestamp
- `.build/proxy.stamp` - Proxy build timestamp
- `.build/demo.stamp` - Demo build timestamp
