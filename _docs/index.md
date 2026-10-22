---
layout: doc
title: "KafScale Documentation: Stateless Kafka on S3"
description: Guides, reference, and architecture docs for KafScale.
permalink: /docs/
nav_title: Overview
nav_order: 1
---

# Docs

Everything you need to run, operate, and extend KafScale.

KafScale keeps brokers stateless and moves processing into add-on services, so you can scale processors independently and keep broker fleets lean. See the [Iceberg Processor](/processors/iceberg/) for a concrete example.

## Get started

- [Project overview](/overview/)
- [Quickstart](/quickstart/)
- [User guide](/user-guide/)
- [Onboarding Guide](/onboarding-guide/): E10–E50 progressive tutorial
- [Installation](/installation/)
- [Architecture](/architecture/)

## Configure and operate

- [Runtime Settings](/configuration/)
- [MCP](/mcp/)
- [Ops MCP](/ops-mcp/)
- [Operations](/operations/)
- [Security](/security/)

## Reference

- [Protocol](/protocol/)
- [Ops API](/ops-api/)
- [Metrics](/metrics/)
- [Grafana](/grafana/)
- [Latency and batching](/latency/)
- [Benchmarks](/benchmarks/)
- [Helm](/helm/)
- [MCP Server](/mcp-server/)
- [Storage format](/storage-format/)
- [Storage layer](/storage/)
- [S3 health](/s3-health/)
- [Release notes](/releases/)

## Large File Support (LFS)

- [LFS Proxy](/lfs-proxy/): claim-check proxy for large binary payloads via S3 and Kafka
- [LFS Helm Deployment](/lfs-helm/): Kubernetes deployment and configuration
- [LFS Client SDKs](/lfs-sdks/): Java, Python, JS, and browser SDKs
- [LFS Demos](/lfs-demos/): runnable demos from local IDoc processing to full Kubernetes pipelines

## Processors

- [Iceberg Processor](/processors/iceberg/)
- [SQL Processor (KAFSQL)](/processors/sql/)

## Project

- [Roadmap](/roadmap/)
- [FAQ](/faq/)
- [Comparison](/comparison/)
- [Rationale](/rationale/)
- [Supported storage](/supported/)
- [Development](/development/)
- [Contributing](/contributing/)
