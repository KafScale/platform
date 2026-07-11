<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# Kafscale E2E Harness

These tests spin up a full cluster (via [kind](https://kind.sigs.k8s.io)), install the local Helm chart, and hit the console API over a port-forward. They are opt-in because they require Docker/kind/helm on the host and take several minutes.

## Prerequisites

1. Docker daemon (Colima, Docker Desktop, etc.)
2. `kind`, `kubectl`, and `helm` binaries on your `$PATH`
3. Internet access to pull the Bitnami `etcd` chart (the harness installs a single-node etcd for the operator)

## Test Categories and Dependencies

Tests have different dependency requirements. Tests will **skip gracefully** if their dependencies aren't available.

| Category | MinIO | Docker | Kind | Make Target |
|----------|-------|--------|------|-------------|
| Console tests | No | No | No | `go test -run Console` |
| Consumer group tests | No | No | No | `make test-consumer-group` |
| Ops API tests | No | No | No | `make test-ops-api` |
| MCP tests | No | No | No | `make test-mcp` |
| LFS proxy tests | No (fake S3) | No | No | `make test-lfs-proxy-broker` |
| Produce/consume tests | **Yes** | Yes | No | `make test-produce-consume` |
| Multi-segment durability | **Yes** | No | No | `make test-multi-segment-durability` |
| Kind cluster tests | No | Yes | Yes | Requires `KAFSCALE_E2E_KIND=1` |

### MinIO Dependency

Tests that require MinIO (produce/consume, durability) will automatically skip if MinIO isn't available:

```
=== RUN   TestFranzGoProduceConsume
    franz_test.go:42: MinIO not available at http://127.0.0.1:9000; run 'make ensure-minio' first or use 'make test-produce-consume'
--- SKIP: TestFranzGoProduceConsume (0.00s)
```

To run MinIO-dependent tests:

```bash
# Option 1: Use make targets (automatically starts MinIO)
make test-produce-consume

# Option 2: Start MinIO manually, then run tests
make ensure-minio
KAFSCALE_E2E=1 go test -tags=e2e ./test/e2e -run TestFranzGoProduceConsume -v
```

## Running

```bash
KAFSCALE_E2E=1 go test -tags=e2e ./test/e2e -v
```

## LFS Go SDK (Kind Cluster)

This test validates the Go SDK against a running Kind cluster (LFS proxy + Kafka + MinIO).

Required environment:
- `KAFSCALE_E2E=1`
- `KAFSCALE_E2E_KIND=1`
- `KAFSCALE_E2E_BROKER_ADDR` (host:port for broker)
- `LFS_PROXY_HTTP_URL` (full URL), or `LFS_PROXY_SERVICE_HOST` + `LFS_PROXY_HTTP_PORT` + `LFS_PROXY_HTTP_PATH`
- `KAFSCALE_LFS_PROXY_S3_BUCKET`
- `KAFSCALE_LFS_PROXY_S3_REGION`
- `KAFSCALE_LFS_PROXY_S3_ENDPOINT`
- `KAFSCALE_LFS_PROXY_S3_ACCESS_KEY`
- `KAFSCALE_LFS_PROXY_S3_SECRET_KEY`
- `KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE` (optional)

Run:
```bash
KAFSCALE_E2E=1 KAFSCALE_E2E_KIND=1 \
KAFSCALE_E2E_BROKER_ADDR=127.0.0.1:9092 \
LFS_PROXY_HTTP_URL=http://127.0.0.1:8080 \
KAFSCALE_LFS_PROXY_S3_BUCKET=kafscale \
KAFSCALE_LFS_PROXY_S3_REGION=us-east-1 \
KAFSCALE_LFS_PROXY_S3_ENDPOINT=http://127.0.0.1:9000 \
KAFSCALE_LFS_PROXY_S3_ACCESS_KEY=minioadmin \
KAFSCALE_LFS_PROXY_S3_SECRET_KEY=minioadmin \
go test -tags=e2e ./test/e2e -run TestLfsSDKKindE2E -v
```

**Note:** Running all tests with `go test` will skip tests whose dependencies aren't available. For complete test coverage, use `make test-full`.

For local developer workflows, prefer the Makefile targets:

```bash
make test-consumer-group          # embedded etcd + in-memory S3
make test-ops-api                 # embedded etcd + in-memory S3
make test-mcp                     # MCP server tests
make test-lfs-proxy-broker        # LFS proxy with fake S3
make test-multi-segment-durability # embedded etcd + MinIO
make test-produce-consume         # MinIO-backed produce/consume suite
make test-full                    # unit tests + local e2e suites
```

### Kind LFS SDK Helper Makefile

The Kind-based SDK test uses `lfs-client-sdk/Makefile` to orchestrate:
- `lfs-demo-up`: start the LFS demo stack on Kind (keeps it running)
- `pf-start`: port-forward broker, LFS proxy HTTP, and MinIO
- `test-lfs-sdk-kind`: run the Go SDK E2E test

Run all:
```bash
make -C lfs-client-sdk run-all
```

Stop port-forwards:
```bash
make -C lfs-client-sdk pf-stop
```

If you already ran `make lfs-demo`, ensure port-forwards are up before running the SDK test:
```bash
make -C lfs-client-sdk pf-start
make -C lfs-client-sdk test-lfs-sdk-kind
```

## Optional Environment Variables

| Variable | Description |
|----------|-------------|
| `KAFSCALE_KIND_CLUSTER` | Reuse an existing kind cluster without creating/deleting one |
| `KAFSCALE_S3_ENDPOINT` | MinIO endpoint (default: `http://127.0.0.1:9000`) |
| `KAFSCALE_E2E_DEBUG` | Enable verbose logging |
| `KAFSCALE_TRACE_KAFKA` | Enable Kafka protocol tracing |
| `KAFSCALE_E2E_OPEN_UI` | Open console UI in browser after test |

The harness installs everything into the `kafscale-e2e` namespace and removes it after the test (unless you reused a cluster).
