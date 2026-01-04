# KafScale Quickstart Guide

**Get Your Spring Boot + Kafka Application Running on KafScale in 30 Minutes**

Welcome to the KafScale Quickstart Guide! This tutorial will help you quickly set up KafScale locally using the Makefile flows and connect your existing Spring Boot + Kafka application to it.

## What You'll Learn

By the end of this guide, you will:

- ✅ Understand what KafScale is and when to use it
- ✅ Run a local KafScale demo with `make demo` (no Docker Compose)
- ✅ Run a full platform demo on kind with `make demo-guide-pf`
- ✅ Configure your Spring Boot application to connect to KafScale
- ✅ Produce and consume messages successfully
- ✅ Troubleshoot common issues

## Prerequisites

Before you begin, ensure you have:

- **Docker Desktop** (or Docker Engine) v20.10+ running
- **Java 11+** (`java -version`)
- **Maven 3.6+** (`mvn -version`)
- **kubectl** v1.28+ (`kubectl version --client`) for Kubernetes demos
- **kind** v0.20+ for local Kubernetes cluster
- **helm** v3.12+ for installing KafScale charts
- **make** command available
- **curl** for API testing
- **Git** (to clone the KafScale repository)
- Basic understanding of Kafka concepts (topics, producers, consumers)

**Verify Docker is running**:
```bash
docker ps
```

**Time commitment**:
- **Core tutorial** (Chapters 1-4 with E10 + E20): 45-60 minutes
- **With stream processing** (add E30 or E40): +20-30 minutes each
- **Minimal path** (Chapters 1-2 with E10 only): 25-30 minutes

## Glossary

**Core Concepts:**
- **[Stateless Broker](https://kafscale.io/architecture/)**: Broker pod that doesn't retain data persistently, enabling horizontal scaling from 0→N instances instantly
- **[S3 (Object Storage)](https://kafscale.io/architecture/)**: Amazon S3 or compatible storage (MinIO) serving as the source of truth for immutable segment files with 11 nines durability (99.999999999% for S3 Standard, single region)
- **[etcd](https://kafscale.io/architecture/)**: Distributed key-value store for cluster metadata including topic configuration, consumer offsets, and group assignments
- **[Segment](https://kafscale.io/storage-format/)**: Immutable log file (~4MB default, configurable via `KAFSCALE_SEGMENT_BYTES`) containing batched records with headers, data, and checksums, stored as `segment-{offset}.kfs` in S3
- **[Wire Protocol](https://kafscale.io/protocol/)**: Kafka-compatible client-server communication protocol enabling existing Kafka clients to connect without modification

**Configuration & Deployment:**
- **Profile**: Configuration preset that determines how clients connect to KafScale brokers across different deployment scenarios:
  - `default` (localhost:39092) - Local app connects to local demo broker; use for development with `make demo`
  - `cluster` (kafscale-broker:9092) - In-cluster app connects to broker via service DNS; use for apps deployed inside the same Kubernetes cluster
  - `local-lb` (localhost:59092) - Local app connects to remote broker via port-forward; use for development against a remote kind cluster

  Choose based on: Where is your app running? Where is the broker running? See [Running Your Application](04-running-your-app.md) for the decision checklist.
- **Bootstrap Server**: Initial broker address used by Kafka clients to discover the cluster (e.g., `localhost:39092`)
- **[MinIO](https://kafscale.io/configuration/)**: S3-compatible object storage server used for local development instead of AWS S3

**Limitations:**
- **[No Transactions](https://kafscale.io/protocol/)**: KafScale doesn't support Kafka transactions (`InitProducerId`, `EndTxn`, etc.)
- **[No Compaction](https://kafscale.io/protocol/)**: Log compaction is not available; S3 lifecycle policies handle retention instead

**Learn More**: See the [KafScale Documentation](https://kafscale.io/docs/) for comprehensive guides on [Architecture](https://kafscale.io/architecture/), [Configuration](https://kafscale.io/configuration/), [Protocol](https://kafscale.io/protocol/), and [Operations](https://kafscale.io/operations/).

**Claims Registry**: Technical claims throughout this tutorial reference verified statements in [examples/claims/](../claims/README.md). This ensures accuracy and traceability of all architectural and compatibility statements.

## Guide Structure

**Core Tutorial** (Required):
1. [**Introduction**](01-introduction.md) - What is KafScale and why use it?
2. [**Quick Start**](02-quick-start.md) - Run the local demo with `make demo` + E10
3. [**Spring Boot Configuration**](03-spring-boot-configuration.md) - Configure your application
4. [**Running Your Application**](04-running-your-app.md) - Platform demo with E20
5. [**Troubleshooting**](05-troubleshooting.md) - Common issues and solutions
6. [**Next Steps**](06-next-steps.md) - Production deployment and advanced topics

**Optional Stream Processing Demos**:
- [**Flink Word Count Demo (E30)**](07-flink-wordcount-demo.md) - Stateful stream processing with Apache Flink
- [**Spark Word Count Demo (E40)**](08-spark-wordcount-demo.md) - Micro-batch processing with Apache Spark

## Picking a Deployment Mode

Not sure which mode fits? The guide includes a short question list to help you choose:

- **Local app + local broker** (default)
- **In-cluster app + broker** (cluster)
- **Local app + remote broker** (local-lb)

See [Running Your Application](04-running-your-app.md) for the decision checklist.

## Getting Started

Ready to begin? Head over to the [Introduction](01-introduction.md) to learn about KafScale, or jump straight to the [Quick Start](02-quick-start.md) if you're already familiar with the concepts.

---

> **Note:** This guide focuses on local development and testing using the Makefile demos. For production deployments, see the [Next Steps](06-next-steps.md) section and the main [KafScale documentation](../quickstart.md).
