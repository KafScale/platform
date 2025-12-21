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

# KafScale

[![CI](https://github.com/novatechflow/kafscale/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/novatechflow/kafscale/actions/workflows/ci.yml)
[![CodeQL](https://img.shields.io/badge/codeql-enabled-2ea44f?logo=github)](https://github.com/novatechflow/kafscale/security/code-scanning)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/novatechflow/kafscale/badge)](https://securityscorecards.dev/viewer/?uri=github.com/novatechflow/kafscale)
[![Release](https://img.shields.io/github/v/release/novatechflow/kafscale?include_prereleases&sort=semver)](https://github.com/novatechflow/kafscale/releases)
[![GHCR Broker](https://img.shields.io/badge/ghcr.io-kafscale--broker-blue)](https://github.com/novatechflow/kafscale/pkgs/container/kafscale-broker)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Go Report Card](https://img.shields.io/badge/go%20report-A%2B-brightgreen)](https://goreportcard.com/report/github.com/novatechflow/kafscale)

Kafscale™ is a Apache Kafka-protocol compatible streaming platform built for durable message transport without the operational complexity of stateful Kafka clusters. It is open source under the Apache 2.0 license and implemented in Go. (Kafscale is not a registered trademark.)

## Why KafScale Exists

Most Kafka deployments act as durable pipes: producers write, consumers read, teams rely on replay when something breaks. Very few workloads require sub-millisecond latency, exactly-once transactions, or compacted topics, yet traditional Kafka clusters still demand stateful brokers, disk management, and continuous operational effort.

Kafscale targets the common case by keeping brokers stateless and putting durability in S3 while remaining compatible with existing Kafka clients and tooling.

Kafscale is used in production environments, but there are no warranties or guarantees.

## Design Scope

In scope:
- Kafka wire protocol for core produce, fetch, and consumer group APIs
- Immutable S3-backed segment storage
- Stateless broker pods
- etcd-backed metadata, offsets, and consumer group state
- Kubernetes-native operation via the operator

Explicit non-goals:
- Exactly-once semantics and transactions
- Compacted topics
- Kafka internal replication protocols
- Embedded stream processing inside the broker

Stream processing is expected to run in external compute engines such as Apache Flink (https://flink.apache.org) or Apache Wayang (https://wayang.apache.org).

## Architecture at a Glance

- Brokers handle Kafka protocol traffic and buffer segments in memory.
- S3 stores immutable log segments and index files (source of truth).
- etcd stores metadata, offsets, and consumer group state.

For the technical specification and data formats, see `kafscale-spec.md`.

## Kafka Protocol Support (Broker-Advertised)

Versions below reflect what the broker advertises in ApiVersions today.

Supported:
- Produce: v0-9
- Fetch: v11-13
- ListOffsets: v0
- Metadata: v0-12
- FindCoordinator: v3
- JoinGroup / SyncGroup / Heartbeat / LeaveGroup: v4
- OffsetCommit: v3
- OffsetFetch: v5
- DescribeGroups: v5
- ListGroups: v5
- OffsetForLeaderEpoch: v3
- DescribeConfigs: v4
- AlterConfigs: v1
- CreatePartitions: v0-3
- DeleteGroups: v0-2
- CreateTopics: v0
- DeleteTopics: v0

Explicitly unsupported:
- Transactions and KRaft APIs
- Replica management internals (LeaderAndIsr, UpdateMetadata, etc.)

## Quickstart

See `docs/quickstart.md` for installation, `docs/user-guide.md` for runtime behavior, and `docs/development.md` for developer workflows.

Common local commands:

```bash
make build
make test
make test-produce-consume
make test-consumer-group
```

## Documentation Map

- `kafscale-spec.md` - technical specification (architecture + data formats)
- `docs/overview.md` - product overview and non-goals
- `docs/quickstart.md` - install the operator and create your first cluster
- `docs/architecture.md` - component responsibilities and data flow
- `docs/protocol.md` - Kafka protocol support matrix
- `docs/security.md` - security posture and roadmap
- `docs/roadmap.md` - completed work and open gaps
- `docs/user-guide.md` - running the platform
- `docs/development.md` - dev workflow and test targets
- `docs/operations.md` - ops guidance and etcd/S3 requirements
- `docs/ops-api.md` - ops/admin API surface and examples
- `docs/metrics.md` - Prometheus metrics and dashboards
- `docs/storage.md` - S3 layout and segment/index details

A detailed architecture overview and design rationale are available here:
https://www.novatechflow.com/p/kafscale.html

## Community

- License: Apache 2.0 (`LICENSE`)
- Contributing: `CONTRIBUTING.md`
- Code of Conduct: `CODE_OF_CONDUCT.md`
- Security: `SECURITY.md`
