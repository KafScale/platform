---
layout: doc
title: MCP Server
description: Deployment and configuration reference for the KafScale MCP service.
permalink: /mcp-server/
nav_title: MCP Server
nav_order: 5
nav_group: References
---

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

# MCP Server

This page documents how to deploy and configure the KafScale MCP service. For the
service overview and tool surface, see [MCP](/mcp/).

## Endpoints

- `/mcp` for MCP streamable HTTP (SSE)
- `/healthz` for basic health checks

## Helm values

```yaml
mcp:
  enabled: true
  namespace:
    name: kafscale-mcp
    create: true
  image:
    repository: ghcr.io/KafScale/platform-mcp
    tag: v0.1.0
  auth:
    token: "<bearer-token>"
  etcdEndpoints:
    - "http://etcd.kafscale.svc:2379"
  metrics:
    brokerMetricsURL: "http://kafscale-broker.kafscale.svc:9093/metrics"
  sessionTimeout: 10m
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_MCP_HTTP_ADDR` | `:8090` | HTTP listen address |
| `KAFSCALE_MCP_AUTH_TOKEN` | unset | Optional bearer token for auth |
| `KAFSCALE_MCP_ETCD_ENDPOINTS` | unset | Comma-separated etcd endpoints |
| `KAFSCALE_MCP_ETCD_USERNAME` | unset | Optional etcd username |
| `KAFSCALE_MCP_ETCD_PASSWORD` | unset | Optional etcd password |
| `KAFSCALE_MCP_BROKER_METRICS_URL` | unset | Broker metrics endpoint |
| `KAFSCALE_MCP_SESSION_TIMEOUT` | `10m` | Session timeout (duration string) |

Notes:

- Metadata tools require `KAFSCALE_MCP_ETCD_ENDPOINTS`.
- Metrics tools require `KAFSCALE_MCP_BROKER_METRICS_URL`.

## Auth token

The MCP server expects a bearer token string. Generate a strong random token and
configure it via `KAFSCALE_MCP_AUTH_TOKEN`.

Example:

```bash
openssl rand -base64 32
```

Then set the value in Helm:

```yaml
mcp:
  auth:
    token: "<generated-token>"
```

## Quick test

```bash
kubectl -n kafscale-mcp get svc
kubectl -n kafscale-mcp port-forward svc/<mcp-service> 8090:80
```

Connect an MCP client to `http://127.0.0.1:8090/mcp`.

## Local run

```bash
export KAFSCALE_MCP_AUTH_TOKEN="<token>"
export KAFSCALE_MCP_ETCD_ENDPOINTS="http://127.0.0.1:2379"
export KAFSCALE_MCP_BROKER_METRICS_URL="http://127.0.0.1:9093/metrics"
./kafscale-mcp
```
