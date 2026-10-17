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

**Kafka-compatible streaming that keeps your log in S3. Stateless brokers, standard Kafka clients, Apache 2.0.**

[![CI](https://github.com/KafScale/platform/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/KafScale/platform/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/KafScale/platform/graph/badge.svg)](https://codecov.io/gh/KafScale/platform)
[![Go Reference](https://pkg.go.dev/badge/github.com/KafScale/platform.svg)](https://pkg.go.dev/github.com/KafScale/platform)
[![CodeQL](https://img.shields.io/badge/codeql-enabled-2ea44f?logo=github)](https://github.com/KafScale/platform/security/code-scanning)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/KafScale/platform/badge)](https://securityscorecards.dev/viewer/?uri=github.com/KafScale/platform)
[![Release](https://img.shields.io/github/v/release/KafScale/platform?include_prereleases&sort=semver)](https://github.com/KafScale/platform/releases)
[![GHCR Broker](https://img.shields.io/badge/ghcr.io-kafscale--broker-blue)](https://github.com/KafScale/platform/pkgs/container/kafscale-broker)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

KafScale is a Kafka-protocol compatible streaming platform built around a simple premise:  
**durable logs belong in object storage, not in stateful brokers**.

Traditional Kafka couples durability, replication, and compute into long-lived broker processes.  
This made sense in a disk-centric world. It becomes an operational and economic liability once object storage is available.

KafScale separates concerns cleanly. Brokers are stateless and ephemeral. Durability lives in S3.  
The Kafka protocol remains intact.

KafScale is open source under the Apache 2.0 license and implemented in Go.  
(KafScale is not a registered trademark.)

---

## Why KafScale Exists

Most Kafka deployments are not event-driven transaction engines.  
They are durable pipes: producers write, consumers read, teams rely on replay when something breaks.

Despite this, traditional Kafka clusters still require:
- Stateful brokers with local disks
- Replica rebalancing and leader movement
- Overprovisioning for peak load
- Continuous operational intervention

These costs are structural, not accidental.

Object storage changes the failure model, the scaling model, and the economics of durable logs.  
Once durability is externalized, broker state stops being an asset and becomes a liability.

KafScale targets this common case by:
- Keeping brokers stateless
- Writing immutable segments directly to S3
- Preserving compatibility with existing Kafka clients and tooling

KafScale is used in production environments, but there are no warranties or guarantees.

---

## Kafka Brokers Are a Legacy Artifact

Kafka brokers were designed in a world where durable storage meant local disks and failure meant replacing machines. In that model, brokers had to own both compute and data. Replication, leader election, and rebalancing were necessary to protect durability.

Cloud object storage changes this entirely.

Object storage already provides durability, availability, and cost efficiency at a level that broker-local disks cannot match. Once log segments live in object storage, brokers no longer need to be long-lived, stateful processes. They become protocol endpoints and transient compute.

Stateful brokers introduce complexity without adding resilience:
- Local disks require replication and rebalancing.
- Leader movement causes operational churn.
- Overprovisioning is needed to survive failure and peak load.
- Recovery time is tied to broker state, not data availability.

Stateless brokers backed by immutable object storage invert this model:
- Data durability is external and stable.
- Brokers can scale, restart, or disappear without data movement.
- Failure becomes a scheduling event, not an operational incident.

KafScale is built on the assumption that broker state is a historical constraint, not a requirement. The Kafka protocol remains valuable. The broker-centric storage model does not.

---

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

---

## Architecture at a Glance

- Brokers handle Kafka protocol traffic and buffer segments in memory.
- S3 stores immutable log segments and index files (source of truth).
- etcd stores metadata, offsets, and consumer group state.

For the technical specification and data formats, see `kafscale-spec.md`.

A detailed architecture overview and design rationale are available here:
https://www.novatechflow.com/p/kafscale.html

## Back up your Kafka to S3 in 10 minutes

KafScale speaks the Kafka protocol and writes immutable segments to S3. Point
[kaf-mirror](https://github.com/scalytics/kaf-mirror) at your existing cluster
and KafScale as the target, and you get a separate, replayable copy of your
topics in object storage. This walkthrough runs everything locally.

Prerequisites: Go, Docker (for the local MinIO bucket), `make`, and a reachable
source Kafka cluster.

**1. Start KafScale with a local S3 bucket (MinIO)**

```bash
git clone https://github.com/KafScale/platform.git && cd platform
make demo
```

This starts MinIO, a broker on `127.0.0.1:39092` and the console on
`http://127.0.0.1:48080/ui/` (user `kafscaleadmin`, password `kafscale`).
Segments land in the MinIO bucket `kafscale`.

**2. Build and start kaf-mirror**

In a second terminal:

```bash
git clone https://github.com/scalytics/kaf-mirror.git && cd kaf-mirror
mkdir -p bin
go build -o bin/kaf-mirror ./cmd/kaf-mirror
go build -o bin/mirror-cli ./cmd/mirror-cli
KAFMIRROR_DISABLE_IDEMPOTENT_WRITES=true ./bin/kaf-mirror
```

KafScale does not implement `InitProducerId`, so idempotent writes must be off
when KafScale is the target. The environment variable above does that for the
whole process. On first start kaf-mirror creates `admin@localhost` and prints a
random password to its log (`logs/kaf-mirror.log`).

**3. Add both clusters and start a job**

```bash
./bin/mirror-cli login admin@localhost --password <password-from-log>
./bin/mirror-cli clusters add   # your source cluster
./bin/mirror-cli clusters add   # target: brokers 127.0.0.1:39092
./bin/mirror-cli jobs add       # pick source, target and topics
./bin/mirror-cli jobs start
./bin/mirror-cli jobs status full
```

`clusters add` and `jobs add` are interactive. Once the job runs, read the copy
back with any Kafka consumer against `127.0.0.1:39092`, and look at the
segments in the MinIO bucket.

For a real deployment, install KafScale with the Helm chart and a real S3
bucket (see `docs/quickstart.md`), and give the backup its own bucket,
credentials and keys so production credentials cannot reach it.

## Use Cases

- **Backup, not just replication.** Replication copies deletes, bad writes and
  compromised-credential damage to the standby. A copy on S3 with separate
  credentials (and S3 Object Lock, if you enable it on the bucket) survives
  them. See [Kafka backup vs replication](https://www.scalytics.io/kafka-backup-vs-replication).
- **DORA and NIS2 evidence.** Regulated teams need backups kept apart from
  production and restore tests they can show. See the
  [Kafka DORA and NIS2 checklist](https://www.scalytics.io/kafka-dora-nis2-checklist).
- **Long retention and replay.** Keep months of history at object storage
  prices and replay it with standard Kafka consumers, without load on the
  production cluster.

More on the backup pattern: [KafShield, Kafka backup and disaster recovery on S3](https://www.scalytics.io/kafshield).

## Examples

- Quickstart guide: `examples/101_kafscale-dev-guide/README.md`
- Spring Boot app demo (E20): `examples/E20_spring-boot-kafscale-demo/README.md`
- Flink demo (E30): `examples/E30_flink-kafscale-demo/README.md`
- Spark demo (E40): `examples/E40_spark-kafscale-demo/README.md`

## Need it managed or supported?

KafScale is the open-source base of [Lascaris](https://www.scalytics.io/lascaris).
Scalytics can run it for you in your own cloud, or support your team running it
on-prem or air-gapped.

## Community

- License: Apache 2.0 (`LICENSE`)
- Contributing: `CONTRIBUTING.md`
- Code of Conduct: `CODE_OF_CONDUCT.md`
- Security: `SECURITY.md`
