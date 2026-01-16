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

# Kafscale Overview

Kafscale™ is a Kubernetes-native, S3-backed Kafka-compatible streaming platform focused on durable message delivery without the operational overhead of a full Kafka cluster. (Kafscale is not a registered trademark.)

## Project Philosophy

Most Kafka deployments serve as durable pipes moving data from point A to points B through N. They do not require sub-millisecond latency, exactly-once transactions, or compacted topics. They need messages in, messages out, consumer tracking, and the confidence that data will not be lost.

Kafscale trades latency for operational simplicity. Brokers are stateless. S3 is the source of truth. Kubernetes handles scaling and failover. The result is a system that can scale down cleanly and does not require dedicated ops expertise.

## What Kafscale Optimizes For

- Operational simplicity on Kubernetes.
- Durable storage via S3.
- Compatibility with common Kafka client workflows.
- Clear operational boundaries (broker compute vs S3 durability).

## Non-Goals (Current)

- Transactions or exactly-once semantics.
- Log compaction.
- KRaft or Kafka-internal replication APIs.

## Where to Go Next

- `docs/quickstart.md` for installation.
- `docs/architecture.md` for system internals.
- `docs/protocol.md` for Kafka protocol coverage.
- `docs/security.md` for security posture and roadmap.
- `kafscale-spec.md` for the technical specification.
