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

- **Docker Desktop** (or Docker Engine) running
- **Java 11+** (`java -version`)
- **Maven 3.6+** (`mvn -version`)
- **kubectl** (`kubectl version --client`) for Kubernetes demos
- **kind** for local Kubernetes cluster
- **helm** for installing KafScale charts
- **make** command available
- **curl** for API testing
- **Git** (to clone the KafScale repository)
- Basic understanding of Kafka concepts (topics, producers, consumers)

**Verify Docker is running**:
```bash
docker ps
```

**Time commitment**: 30 minutes

## Guide Structure

1. [**Introduction**](01-introduction.md) - What is KafScale and why use it?
2. [**Quick Start**](02-quick-start.md) - Run the local demo with `make demo` + E10
3. [**Spring Boot Configuration**](03-spring-boot-configuration.md) - Configure your application
4. [**Running Your Application**](04-running-your-app.md) - Platform demo with E20
5. [**Flink Word Count Demo**](07-flink-wordcount-demo.md) - Stream processing with E30
6. [**Spark Word Count Demo**](08-spark-wordcount-demo.md) - Stream processing with E40
7. [**Troubleshooting**](05-troubleshooting.md) - Common issues and solutions
8. [**Next Steps**](06-next-steps.md) - Production deployment and advanced topics

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
