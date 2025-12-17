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
make test-e2e     # run the minio/franz + operator e2e suites (images from docker-build are reused)
make docker-clean # delete dev images and prune Docker caches when you need a fresh slate
make stop-containers # stop leftover kafscale-minio/kind containers from previous e2e runs

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
```

## Coding Standards

- Keep all new code documented in `kscale-spec.md` or cross-link back to the spec
- Favor context-rich structured logging (zerolog) and Prometheus metrics
- Protobufs should remain backward compatible; prefer adding optional fields over rewriting existing ones
- No stream processing primitives in the broker—hand those workloads off to Flink/Wayang or equivalent engines
- Every change must land with unit tests, smoke/integration coverage, and regression tests where appropriate; skipping tests requires an explicit TODO anchored to a tracking issue.
- Secrets live only in Kubernetes; never write S3 or etcd credentials into source control or etcd. Reference them via `credentialsSecretRef` and let the operator project them at runtime.
- When testing against etcd locally, set `KAFSCALE_ETCD_ENDPOINTS` (comma-separated), plus `KAFSCALE_ETCD_USERNAME` / `KAFSCALE_ETCD_PASSWORD` if auth is enabled. The broker will fall back to the in-memory store when those vars are absent.
