---
layout: doc
title: Onboarding Guide
description: Progressive tutorial from basic Kafka clients to stream processing with KafScale.
permalink: /onboarding-guide/
nav_title: Onboarding Guide
nav_order: 4
---

<!--
Copyright 2025-2026 Alexander Alten (novatechflow), KafScale (novatechflow.com).
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

# Onboarding Guide

A progressive tutorial collection that takes you from basic Kafka client operations to full stream-processing pipelines on KafScale.

## Who this is for

- Developers new to KafScale who already know the basics of Apache Kafka
- Teams evaluating KafScale as a drop-in Kafka replacement
- Anyone wanting hands-on experience with KafScale's stateless-broker architecture

## Prerequisites

- Docker 20.10+, Java 11+, Maven 3.6+
- kubectl 1.28+, kind 0.20+, helm 3.12+
- make, curl, git
- Basic understanding of Kafka concepts (topics, producers, consumers, consumer groups)

## Learning path

Work through the examples in order. Each builds on concepts from the previous one.

| Example | Framework | What you learn | Skill level | Time |
|---------|-----------|----------------|-------------|------|
| **E10** | Pure Java | Produce/consume basics, KafScale-specific config | Beginner | 15 min |
| **E20** | Spring Boot | REST API integration, profiles, Docker deployment | Beginner | 20 min |
| **E30** | Apache Flink | Stateful streaming, 3 deployment modes | Intermediate | 30 min |
| **E40** | Apache Spark | Structured streaming, DataFrame API, checkpoints | Intermediate | 30 min |
| **E50** | JavaScript/Node.js | Web UI, agent integration, KafkaJS client | Intermediate | 20 min |

Total time: ~2 hours for the full path, or ~35 minutes for the minimal path (E10 + E20).

---

## E10: Java Kafka Client

**Directory:** [`examples/E10_java-kafka-client-demo/`](https://github.com/KafScale/platform/tree/main/examples/E10_java-kafka-client-demo)

The starting point. Uses the standard `kafka-clients` library to list topics, create a topic, produce 25 messages, and consume 5 messages.

**Key KafScale configuration:**
- `acks=0` — KafScale brokers are stateless; acknowledgement semantics differ from traditional Kafka
- `enable.idempotence=false` — KafScale does not support idempotent producers
- Bootstrap server: `127.0.0.1:39092` (default local dev port)

```bash
cd examples/E10_java-kafka-client-demo
mvn clean package
java -jar target/kafka-client-demo-*.jar
```

---

## E20: Spring Boot

**Directory:** [`examples/E20_spring-boot-kafscale-demo/`](https://github.com/KafScale/platform/tree/main/examples/E20_spring-boot-kafscale-demo)

A REST API that produces and consumes orders through KafScale, with a Bootstrap 5 web UI.

**REST endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/orders` | Create and send an order to Kafka |
| `GET` | `/api/orders` | List consumed orders (in-memory) |
| `GET` | `/api/orders/cluster-info` | View cluster metadata and topics |
| `POST` | `/api/orders/test-connection` | Test Kafka connectivity |
| `GET` | `/actuator/prometheus` | Prometheus metrics |

**Deployment profiles:** see [Common configuration](#common-configuration) below.

```bash
cd examples/E20_spring-boot-kafscale-demo
mvn clean package
java -jar target/kafscale-demo-*.jar
# Or with Docker:
docker build -t kafscale-demo-e20 .
docker run -p 8080:8080 kafscale-demo-e20
```

---

## E30: Apache Flink

**Directory:** [`examples/E30_flink-kafscale-demo/`](https://github.com/KafScale/platform/tree/main/examples/E30_flink-kafscale-demo)

A Flink streaming job that performs word counts across message headers, keys, and values separately, tracking statistics for missing fields.

**Three deployment modes:**

1. **Standalone Java** — runs locally with embedded Flink mini-cluster (Web UI at `localhost:8091`)
2. **Docker standalone cluster** — Flink JobManager + TaskManager containers (Web UI at `localhost:8081`)
3. **Kubernetes/kind cluster** — full K8s deployment

```bash
cd examples/E30_flink-kafscale-demo
mvn clean package
# Standalone:
make run-standalone
# Docker:
make run-docker
# Kubernetes:
make run-k8s
```

---

## E40: Apache Spark

**Directory:** [`examples/E40_spark-kafscale-demo/`](https://github.com/KafScale/platform/tree/main/examples/E40_spark-kafscale-demo)

Structured streaming with micro-batch execution. Groups word counts by field type (header, key, value) using the DataFrame API.

**Checkpointing:**
- Default location: `/tmp/kafscale-spark-checkpoints`
- Supports durable storage (NFS, S3) for production
- `failOnDataLoss` flag controls behavior on offset conflicts

```bash
cd examples/E40_spark-kafscale-demo
mvn clean package
make run
```

---

## E50: JavaScript / Node.js

**Directory:** [`examples/E50_JS-kafscale-demo/`](https://github.com/KafScale/platform/tree/main/examples/E50_JS-kafscale-demo)

A Node.js application using KafkaJS with an interactive Kanban board UI (drag-and-drop task management) and real-time WebSocket monitoring.

**Agent architecture:** queue-driven with Kafka topics for orchestration — requests flow through `agent.requests` to the agent service, responses return on `agent.responses`.

```bash
cd examples/E50_JS-kafscale-demo
npm install
npm start
# Open http://localhost:3000
```

---

## Common configuration

All Java examples (E10–E40) use Spring-style deployment profiles:

| Profile | Bootstrap server | Use case |
|---------|-----------------|----------|
| `default` | `localhost:39092` | Local development |
| `cluster` | `kafscale-broker:9092` | In-cluster Kubernetes |
| `local-lb` | `localhost:59092` | Remote via load balancer |

Activate a profile with `--spring.profiles.active=cluster` or the `SPRING_PROFILES_ACTIVE` environment variable.

Environment variables common across examples:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:39092` | Kafka/KafScale broker address |
| `KAFKA_TOPIC` | *(varies by example)* | Target topic name |

---

## Developer guide

The [`101_kafscale-dev-guide/`](https://github.com/KafScale/platform/tree/main/examples/101_kafscale-dev-guide) contains an 8-chapter written guide covering:

1. **Introduction** — what KafScale is, architecture overview, limitations
2. **Quick Start** — run the local demo with `make demo`
3. **Spring Boot Configuration** — configure your application for KafScale
4. **Running Your Application** — platform demo walkthrough
5. **Troubleshooting** — common issues and fixes
6. **Next Steps** — production deployment guidance
7. **Flink Word Count Demo** — stream processing with Flink (E30)
8. **Spark Word Count Demo** — stream processing with Spark (E40)

Core path (chapters 1–6): ~45–60 minutes. Each stream processing chapter adds 20–30 minutes.

## Next steps

- [Quickstart](/quickstart/) — install the KafScale operator and create your first topic
- [User Guide](/user-guide/) — full reference for operating KafScale
- [Architecture](/architecture/) — how stateless brokers and S3 storage work together
