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
make build   # compile all Go binaries
make test    # run go test ./...
make tidy    # clean go.mod/go.sum
make lint    # run golangci-lint (requires installation)
```

## Coding Standards

- Keep all new code documented in `kscale-spec.md` or cross-link back to the spec
- Favor context-rich structured logging (zerolog) and Prometheus metrics
- Protobufs should remain backward compatible; prefer adding optional fields over rewriting existing ones
- No stream processing primitives in the broker—hand those workloads off to Flink/Wayang or equivalent engines
- Every change must land with unit tests, smoke/integration coverage, and regression tests where appropriate; skipping tests requires an explicit TODO anchored to a tracking issue.
- Secrets live only in Kubernetes; never write S3 or etcd credentials into source control or etcd. Reference them via `credentialsSecretRef` and let the operator project them at runtime.
