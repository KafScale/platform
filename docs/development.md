# Development Guide

This document tracks the steps needed to work on Kafscale. It complements the architecture spec in `kscale-spec.md`.

## Prerequisites

- Go 1.22+ (the module currently targets Go 1.25)
- `buf` (https://buf.build/docs/installation/) for protobuf builds
- `protoc` plus the `protoc-gen-go` and `protoc-gen-go-grpc` plugins (installed automatically by `buf` if you use the managed mode)
- Docker + Kubernetes CLI tools if you plan to iterate on the operator

## Repository Layout

- `cmd/broker`, `cmd/operator`: binary entry points
- `pkg/`: Go libraries (protocol, storage, broker, operator)
- `proto/`: protobuf definitions for metadata and internal control plane APIs
- `pkg/gen/`: auto-generated protobuf + gRPC Go code (ignored until `buf generate` runs)
- `docs/`: specs and this guide
- `test/`: integration + e2e suites
- `docs/storage.md`: deeper design notes for the storage subsystem, including S3 client expectations

Refer to `kscale-spec.md` for the detailed package-by-package breakdown.

## Generating Protobuf Code

We use `buf` to manage protobuf builds. All metadata schemas and control-plane RPCs live under `proto/`.

```bash
brew install buf      # or equivalent
make proto            # runs `buf generate`
```

The generated Go code goes into `pkg/gen/{metadata,control}`. Do not edit generated files manually—re-run `make proto` whenever the `.proto` sources change.

## Common Tasks

```bash
make build        # compile all Go binaries
make test         # run go test ./...
make docker-build # build broker/operator/console images (run locally whenever their code changes; CI builds on release tags)
make test-e2e         # run the minio/franz + operator e2e suites (images from docker-build are reused)
make test-e2e-debug   # same as above but with broker trace logging enabled
KAFSCALE_E2E=1 go test -tags=e2e ./test/e2e -run TestOperatorManagedEtcdResources -v # operator envtest (no kind)
KAFSCALE_E2E=1 KAFSCALE_E2E_KIND=1 go test -tags=e2e ./test/e2e -run TestOperatorEtcdSnapshotKindE2E -v # kind + helm integration
make test-operator-kind # same as above, wrapped in Makefile (set KAFSCALE_KIND_RECREATE=1 for clean clusters)
make demo-platform # kind-based platform demo (operator HA + managed etcd + console)
make docker-clean # delete dev images and prune Docker caches when you need a fresh slate
make stop-containers # stop leftover kafscale-minio/kind containers from previous e2e runs
make help         # list available Makefile targets
```

The Makefile defines these as `.PHONY`: `proto`, `build`, `test`, `tidy`, `lint`, `generate`, `docker-build`, `docker-build-broker`, `docker-build-operator`, `docker-build-console`, `docker-clean`, `ensure-minio`, `start-minio`, `stop-containers`, `release-broker-ports`, `test-e2e`, `test-e2e-debug`, `test-operator-kind`, `demo`, `demo-platform`, `help`.

### Development/Test Environment Variables

- `KAFSCALE_E2E` – Enable e2e tests.
- `KAFSCALE_E2E_KIND` – Enable kind-based e2e.
- `KAFSCALE_KIND_CLUSTER` – Kind cluster name.
- `KAFSCALE_KIND_RECREATE` – Force kind cluster recreation.
- `KAFSCALE_E2E_DEMO` – Run demo stack test.
- `KAFSCALE_E2E_OPEN_UI` – Open UI during demo test.
- `KAFSCALE_E2E_DEBUG` – Enable debug output in e2e tests.
- `KAFSCALE_BROKER_IMAGE`, `KAFSCALE_OPERATOR_IMAGE`, `KAFSCALE_CONSOLE_IMAGE` – Image overrides for kind e2e.
- `KAFSCALE_DEMO_BROKER_ADDR`, `KAFSCALE_DEMO_TOPICS`, `KAFSCALE_DEMO_MESSAGES_PER_SEC`, `KAFSCALE_DEMO_GROUP` – Demo workload tuning.
- `KAFSCALE_LOCAL_FRANZ` – Use local franz-go build in tests.

## Kafka Compatibility Tracking

To stay Kafka-compatible we track every protocol key + version that upstream exposes. Upstream Kafka 3.7.0 currently advertises the following highest ApiVersions (see `kafka-protocol` docs):

| API Key | Name | Kafka 3.7 Version | Kafscale Status |
|---------|------|-------------------|-----------------|
| 0 | Produce | 9 | ✅ Implemented |
| 1 | Fetch | 13 | ✅ Implemented |
| 2 | ListOffsets | 7 | ✅ Implemented |
| 3 | Metadata | 12 | ✅ Implemented |
| 4 | LeaderAndIsr | 5 | ❌ Not needed (internal) |
| 5 | StopReplica | 3 | ❌ Not needed (internal) |
| 6 | UpdateMetadata | 7 | ❌ Not needed (internal) |
| 7 | ControlledShutdown | 3 | ❌ Replaced by Kubernetes rollouts |
| 8 | OffsetCommit | 8 | ✅ Implemented |
| 9 | OffsetFetch | 8 | ✅ Implemented |
| 10 | FindCoordinator | 4 | ✅ Implemented |
| 11 | JoinGroup | 9 | ✅ Implemented |
| 12 | Heartbeat | 4 | ✅ Implemented |
| 13 | LeaveGroup | 5 | ✅ Implemented |
| 14 | SyncGroup | 5 | ✅ Implemented |
| 15 | DescribeGroups | 5 | 🔜 Planned |
| 16 | ListGroups | 5 | 🔜 Planned |
| 17 | SaslHandshake | 1 | ❌ Authentication not in scope yet |
| 18 | ApiVersions | 3 | ✅ Implemented |
| 19 | CreateTopics | 7 | ✅ Implemented |
| 20 | DeleteTopics | 6 | ✅ Implemented |
| 21 | DeleteRecords | 2 | ❌ Rely on S3 lifecycle |
| 22 | InitProducerId | 4 | ❌ Transactions out of scope |
| 23 | OffsetForLeaderEpoch | 3 | 🔜 Needed for catch-up tooling |
| 24 | AddPartitionsToTxn | 3 | ❌ Transactions out of scope |
| 25 | AddOffsetsToTxn | 3 | ❌ Transactions out of scope |
| 26 | EndTxn | 3 | ❌ Transactions out of scope |
| 27 | WriteTxnMarkers | 0 | ❌ Transactions out of scope |
| 28 | TxnOffsetCommit | 3 | ❌ Transactions out of scope |
| 29 | DescribeAcls | 1 | ❌ Auth not in v1 |
| 30 | CreateAcls | 1 | ❌ Auth not in v1 |
| 31 | DeleteAcls | 1 | ❌ Auth not in v1 |
| 32 | DescribeConfigs | 4 | ⚠️ Read-only subset |
| 33 | AlterConfigs | 1 | 🔜 After admin API hardening |
| 34 | AlterReplicaLogDirs | 1 | ❌ Not relevant (S3 backed) |
| 35 | DescribeLogDirs | 1 | ❌ Not relevant (S3 backed) |
| 36 | SaslAuthenticate | 2 | ❌ Auth not in v1 |
| 37 | CreatePartitions | 3 | 🔜 Requires S3 layout changes |
| 38 | CreateDelegationToken | 2 | ❌ Auth not in v1 |
| 39 | RenewDelegationToken | 2 | ❌ Auth not in v1 |
| 40 | ExpireDelegationToken | 2 | ❌ Auth not in v1 |
| 41 | DescribeDelegationToken | 2 | ❌ Auth not in v1 |
| 42 | DeleteGroups | 2 | ✅ Implemented |

We revisit this table each milestone. Anything marked 🔜 or ❌ has a pointer in the spec backlog so we can track when to bring it online (e.g., DescribeGroups/ListGroups for Kafka UI parity, OffsetForLeaderEpoch for catch-up tooling).
make tidy         # clean go.mod/go.sum
make lint         # run golangci-lint (requires installation)

### Local MinIO / S3 setup

`make test-e2e` assumes there is an S3 endpoint in front of the broker; we keep a MinIO container (`kafscale-minio`) running locally so the e2e and operator suites exercise a production-like S3 stack. When the broker starts without overriding `KAFSCALE_USE_MEMORY_S3=1`, it points at MinIO at `http://127.0.0.1:9000`, bucket `kafscale`, region `us-east-1`, and uses path-style addressing by default. Set `KAFSCALE_S3_BUCKET`, `KAFSCALE_S3_REGION`, `KAFSCALE_S3_ENDPOINT`, `KAFSCALE_S3_PATH_STYLE`, `KAFSCALE_S3_KMS_ARN`, `KAFSCALE_S3_ACCESS_KEY`, `KAFSCALE_S3_SECRET_KEY`, and `KAFSCALE_S3_SESSION_TOKEN` to target a different S3-compatible endpoint, or flip `KAFSCALE_USE_MEMORY_S3=1` to skip MinIO entirely (the broker then uses the in-memory S3 client for faster, more deterministic runs). Keep `make stop-containers` handy to stop the MinIO / kind helper containers before you restart the suite. If you need to inspect every protocol message, run `make test-e2e-debug`; it sets `KAFSCALE_LOG_LEVEL=debug` and `KAFSCALE_TRACE_KAFKA=true` before chaining into the standard target so the extra noise stays opt-in.

Need an interactive run? `make demo` chains into the same helpers, boots embedded etcd plus the broker + console, opens `http://127.0.0.1:48080/ui/`, and keeps everything running until you hit `Ctrl+C`. It’s the quickest way to click around the UI while real messages flow through the broker/MinIO stack. That target already wires `KAFSCALE_CONSOLE_BROKER_METRICS_URL=http://127.0.0.1:39093/metrics` so the console scrapes the broker’s Prometheus endpoint and populates the S3/metrics cards with live data. Any other process starting `cmd/console` can set the same env var (e.g., `go run ./cmd/console`), and the UI will render the broker-reported S3 state/latency instead of the mock placeholders.

The broker exports live throughput gauges (`kafscale_produce_rps` / `kafscale_fetch_rps`) so the UI can show messages-per-second alongside S3 latency. The sliding window defaults to 60 seconds; override it with `KAFSCALE_THROUGHPUT_WINDOW_SEC` before starting the broker if you want a shorter (spikier) or longer (smoother) view.

### Operator envtest (no kind)

`TestOperatorManagedEtcdResources` uses controller-runtime envtest to validate operator reconciliation without spinning up kind. Install envtest assets with `setup-envtest` or set `KUBEBUILDER_ASSETS` to a directory containing `kube-apiserver` and `etcd` binaries.

### Broker logging levels

The broker reads `KAFSCALE_LOG_LEVEL` at start-up. If the variable is unset we operate in warning-and-above mode, which keeps regular e2e/test runs quiet. Set `KAFSCALE_LOG_LEVEL=info` or `debug` (optionally together with `KAFSCALE_TRACE_KAFKA=true`) when you need additional visibility; the `test-e2e-debug` target wires those env vars up for you.

## Coding Standards

- Keep all new code documented in `kscale-spec.md` or cross-link back to the spec
- Favor context-rich structured logging (zerolog) and Prometheus metrics
- Protobufs should remain backward compatible; prefer adding optional fields over rewriting existing ones
- No stream processing primitives in the broker—hand those workloads off to Flink/Wayang or equivalent engines
- Every change must land with unit tests, smoke/integration coverage, and regression tests where appropriate; skipping tests requires an explicit TODO anchored to a tracking issue.
- Secrets live only in Kubernetes; never write S3 or etcd credentials into source control or etcd. Reference them via `credentialsSecretRef` and let the operator project them at runtime.
- When testing against etcd locally, set `KAFSCALE_ETCD_ENDPOINTS` (comma-separated), plus `KAFSCALE_ETCD_USERNAME` / `KAFSCALE_ETCD_PASSWORD` if auth is enabled. The broker will fall back to the in-memory store when those vars are absent.
