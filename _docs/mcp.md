---
layout: doc
title: MCP
description: Model Context Protocol service overview and usage for KafScale.
permalink: /mcp/
nav_title: MCP
nav_order: 5
---

<!--
Copyright 2025 Alexander Alten (novatechflow), KafScale (novatechflow.com).
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

# MCP

This document sketches a Model Context Protocol (MCP) service for KafScale.
The goal is to enable safe, structured, agent-friendly operations without
embedding MCP into brokers or widening the broker attack surface.

For deployment details and configuration options, see [MCP Server](/mcp-server/).

## Goals

- Provide read-only observability for ops debugging and reporting.
- Offer a path to mutation tools once broker/admin auth exists.
- Ship as a separate service (`kafscale-mcp`), not embedded in brokers.

## Non-Goals

- No direct embedding in the broker process.
- No unauthenticated mutation tools.
- No replacement for Kafka protocol or admin APIs.

## Architecture

- Standalone MCP service deployed alongside KafScale.
- Talks to KafScale via:
  - Metadata store access (read-only).
  - Prometheus metrics scraping.
  - Optional gRPC control plane (future, gated).
- Deployed as an optional Helm chart and disabled by default.
- Helm defaults to a dedicated namespace for the MCP service.

## Service Shape

- HTTP endpoint: `/mcp` using MCP streamable HTTP transport (SSE).
- Health check: `/healthz`.
- Read-only tools only; mutation tools are not registered.

For configuration and environment variables, see [MCP Server](/mcp-server/).

## Enable MCP

Enable the service via Helm, then configure auth and endpoints:

```yaml
mcp:
  enabled: true
  auth:
    token: "<bearer-token>"
  etcdEndpoints:
    - "http://etcd.kafscale.svc:2379"
  metrics:
    brokerMetricsURL: "http://kafscale-broker.kafscale.svc:9093/metrics"
```

For the full values and environment variable reference, see [MCP Server](/mcp-server/).

## Connect a client

Point your MCP-capable client at the `/mcp` endpoint and include the bearer token
if auth is enabled. See [MCP Server](/mcp-server/) for examples.

## v1 Tool Surface

Read-only tools (default):

- `cluster_status` (summary view; similar to console status).
- `cluster_metrics` (S3 latency, produce/fetch RPS, admin error rates).
- `list_topics` / `describe_topics`.
- `list_groups` / `describe_group`.
- `fetch_offsets` (consumer group offsets).
- `describe_configs` (topic configs).

Mutation tools (future, gated by auth + RBAC):

- `create_topic` / `delete_topic`.
- `create_partitions`.
- `update_topic_config`.
- `delete_group`.
- Broker control actions (drain/flush) via gRPC control plane.

## Security and Guardrails

KafScale currently does not enforce auth on broker/admin APIs. For MCP, we
must ship secure-by-default to avoid "one prompt away from prod changes".

Requirements:

- Strong auth (OIDC or short-lived tokens). No static shared secrets.
- RBAC and allowlist tool sets (separate observe vs mutate).
- Audit logs for every tool call (who/what/when + diff).
- Dry-run mode for all mutations.
- Environment fences (production requires explicit break-glass or approval).

See [Security](/security/) for the current security posture and roadmap; MCP should
not enable any operations that bypass the auth/authorization roadmap there.