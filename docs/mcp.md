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

# MCP Service

This document sketches a Model Context Protocol (MCP) service for Kafscale.
The goal is to enable safe, structured, agent-friendly operations without
embedding MCP into brokers or widening the broker attack surface.

For an ops-focused guide and integration examples, see `docs/ops-mcp.md`.

## Goals

- Provide read-only observability for ops debugging and reporting.
- Offer a path to mutation tools once broker/admin auth exists.
- Ship as a separate service (`kafscale-mcp`), not embedded in brokers.

## Non-Goals

- No direct embedding in the broker process.
- No unauthenticated mutation tools.
- No replacement for Kafka protocol or admin APIs.

## Architecture

- Standalone MCP service deployed alongside Kafscale.
- Talks to Kafscale via:
  - Metadata store access (read-only).
  - Prometheus metrics scraping.
  - Optional gRPC control plane (future, gated).
- Deployed as an optional Helm chart and disabled by default.
- Helm defaults to a dedicated namespace for the MCP service.

## Service Shape

- HTTP endpoint: `/mcp` using MCP streamable HTTP transport (SSE).
- Health check: `/healthz`.
- Read-only tools only; mutation tools are not registered.

Environment variables:

- `KAFSCALE_MCP_HTTP_ADDR` (default `:8090`)
- `KAFSCALE_MCP_ETCD_ENDPOINTS` (comma-separated)
- `KAFSCALE_MCP_ETCD_USERNAME`
- `KAFSCALE_MCP_ETCD_PASSWORD`
- `KAFSCALE_MCP_BROKER_METRICS_URL` (Prometheus metrics endpoint)
- `KAFSCALE_MCP_SESSION_TIMEOUT` (optional, duration string)
- `KAFSCALE_MCP_AUTH_TOKEN` (optional bearer token; if unset, server is open)

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

Kafscale v1.5 introduces basic broker ACLs, but MCP remains read-only for security reasons.
For MCP, we ship secure-by-default to avoid "one prompt away from prod changes".

Requirements:

- Strong auth (OIDC or short-lived tokens). No static shared secrets.
- RBAC and allowlist tool sets (separate observe vs mutate).
- Audit logs for every tool call (who/what/when + diff).
- Dry-run mode for all mutations.
- Environment fences (production requires explicit break-glass or approval).

See `docs/security.md` for the current security posture and roadmap; MCP should
not enable any operations that bypass the auth/authorization roadmap there.

## Rollout Plan

- Phase 1: Read-only MCP server with auth and audit logging.
- Phase 2: Mutations behind RBAC + dry-run + approval workflows.

Current status:

- Read-only MCP server is implemented.
- Optional bearer token auth is supported; stronger auth and audit logging remain pending.

## Open Questions

- Which auth provider(s) to standardize (OIDC provider and token format).
- Where to store audit logs (stdout, file, or external sink).
- Whether to expose broker control actions in v1 or v2.
