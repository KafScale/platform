---
layout: doc
title: Contributing
description: Development setup, testing, and PR process for KafScale.
---

# Contributing

## Development setup

Prerequisites:

- Go 1.22+ (module targets Go 1.25)
- `buf` for protobuf builds
- `protoc` and plugins (`protoc-gen-go`, `protoc-gen-go-grpc`)
- Docker + Kubernetes CLI tools if iterating on the operator

## Running tests

```bash
make build
make test
make test-produce-consume
make test-consumer-group
```

## Code style / linting

```bash
make lint
```

## PR process

- Add or extend unit tests for non-trivial logic.
- Run relevant e2e suites (broker changes should run `make test-produce-consume`).
- Add regression coverage when fixing bugs.

## Roadmap and open issues

See `/roadmap` and the GitHub issue tracker.
