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

[![CI](https://github.com/KafScale/platform/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/KafScale/platform/actions/workflows/ci.yml)
[![CodeQL](https://img.shields.io/badge/codeql-enabled-2ea44f?logo=github)](https://github.com/KafScale/platform/security/code-scanning)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/KafScale/platform/badge)](https://securityscorecards.dev/viewer/?uri=github.com/KafScale/platform)
[![Release](https://img.shields.io/github/v/release/KafScale/platform?include_prereleases&sort=semver)](https://github.com/KafScale/platform/releases)
[![GHCR Broker](https://img.shields.io/badge/ghcr.io-kafscale--broker-blue)](https://github.com/KafScale/platform/pkgs/container/kafscale-broker)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Go Report Card](https://img.shields.io/badge/go%20report-A%2B-brightgreen)](https://goreportcard.com/report/github.com/KafScale/platform)

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

## Community

- License: Apache 2.0 (`LICENSE`)
- Contributing: `CONTRIBUTING.md`
- Code of Conduct: `CODE_OF_CONDUCT.md`
- Security: `SECURITY.md`
