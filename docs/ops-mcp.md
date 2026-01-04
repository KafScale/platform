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

# Ops MCP Guide (EXPERIMENTAL)

This document describes the Kafscale MCP service from an operations perspective:
what it is for, how to configure it, and how to use it with MCP-enabled tools.

## What This Service Is For

The Kafscale MCP service exposes a read-only, structured view of Kafscale
cluster metadata and broker metrics over MCP streamable HTTP. It is intended
for ops workflows such as:

- Debugging consumer lag and topic health.
- Generating reports (topic layout, partitions, consumer groups).
- Querying S3 and broker health summaries without direct Kafka admin calls.
- Integrating Kafscale visibility into MCP-capable assistants or observability
  workflows.

The MCP service is **not** embedded in the broker and does **not** expose any
mutation tools in v1. For security posture and roadmap, see `docs/security.md`
and `docs/mcp.md`.

## What It Exposes (v1)

Read-only tools exposed by the MCP service:

- `cluster_status`
- `cluster_metrics`
- `list_topics`
- `describe_topics`
- `list_groups`
- `describe_group`
- `fetch_offsets`
- `describe_configs`

### Response shapes

Some tools return wrapper objects (not raw arrays):

- `list_topics` -> `{ "topics": [...] }`
- `describe_topics` -> `{ "topics": [...] }`
- `list_groups` -> `{ "groups": [...] }`
- `describe_configs` -> `{ "configs": [...] }`

Field notes:

- `cluster_status` includes `cluster_name` when available; clients can use it as the primary label and fall back to `cluster_id`.

## Deployment and Configuration

### Helm (recommended)

Enable the MCP service and deploy it into a dedicated namespace:

```yaml
mcp:
  enabled: true
  namespace:
    name: kafscale-mcp
    create: true
  image:
    repository: ghcr.io/novatechflow/kafscale-mcp
    tag: v0.1.0
  auth:
    token: "<bearer-token>"
  etcdEndpoints:
    - "http://etcd.kafscale.svc:2379"
  metrics:
    brokerMetricsURL: "http://kafscale-broker.kafscale.svc:9093/metrics"
  sessionTimeout: 10m
```

### Environment Variables

```bash
KAFSCALE_MCP_HTTP_ADDR=:8090
KAFSCALE_MCP_AUTH_TOKEN=<bearer-token>
KAFSCALE_MCP_ETCD_ENDPOINTS=http://etcd.kafscale.svc:2379
KAFSCALE_MCP_ETCD_USERNAME=
KAFSCALE_MCP_ETCD_PASSWORD=
KAFSCALE_MCP_BROKER_METRICS_URL=http://kafscale-broker.kafscale.svc:9093/metrics
KAFSCALE_MCP_SESSION_TIMEOUT=10m
```

Notes:

- Metadata tools require `KAFSCALE_MCP_ETCD_ENDPOINTS`.
- Metrics require `KAFSCALE_MCP_BROKER_METRICS_URL`.

### Creating a bearer token

The MCP server expects a static bearer token string (for now). Generate a strong random
token and configure it via `KAFSCALE_MCP_AUTH_TOKEN`.

Example (base64, 32 bytes):

```bash
openssl rand -base64 32
```

Then set the value in Helm:

```yaml
mcp:
  auth:
    token: "<generated-token>"
```

Or export it for local runs:

```bash
export KAFSCALE_MCP_AUTH_TOKEN="<generated-token>"
```

### Quick Test (port-forward)

```bash
kubectl -n kafscale-mcp get svc
kubectl -n kafscale-mcp port-forward svc/<mcp-service> 8090:80
```

You can then connect an MCP client to `http://127.0.0.1:8090/mcp`.

### Local run (no Helm)

```bash
export KAFSCALE_MCP_AUTH_TOKEN="<token>"
export KAFSCALE_MCP_ETCD_ENDPOINTS="http://127.0.0.1:2379"
export KAFSCALE_MCP_BROKER_METRICS_URL="http://127.0.0.1:9093/metrics"
./kafscale-mcp
```

## Using MCP-Enabled Tools

MCP clients typically support multiple servers at once. That means you can
combine Kafscale MCP with other MCP servers to create a holistic ops view.

### MCP-enabled observability servers in the ecosystem

These public projects indicate there are MCP servers for observability stacks:

- Observe MCP server: https://www.observeinc.com/blog/use-observe-mcp-server-with-claude-cursor-augment
- Datadog MCP server: https://docs.datadoghq.com/bits_ai/mcp_server/
- Grafana MCP server: https://github.com/grafana/mcp-grafana

### Example: Claude Desktop (via mcp-remote)

Claude Desktop is stdio-based. Use `mcp-remote` to bridge HTTP endpoints:

```json
{
  "mcpServers": {
    "kafscale": {
      "command": "npx",
      "args": [
        "mcp-remote@latest",
        "http://127.0.0.1:8090/mcp",
        "--header",
        "Authorization:${KAFSCALE_AUTH}"
      ],
      "env": {
        "KAFSCALE_AUTH": "Bearer <token>"
      }
    },
    "grafana": {
      "command": "npx",
      "args": [
        "mcp-remote@latest",
        "https://<grafana-mcp-endpoint>",
        "--header",
        "Authorization:${GRAFANA_AUTH}"
      ],
      "env": {
        "GRAFANA_AUTH": "Bearer <token>"
      }
    }
  }
}
```

### Example: Cursor or VS Code agent (multiple MCP servers)

If your client supports MCP HTTP transport directly, point it at the `/mcp`
endpoint and add any other MCP observability servers you rely on:

```json
{
  "mcpServers": {
    "kafscale": {
      "transport": "http",
      "url": "http://kafscale-mcp.kafscale-mcp.svc.cluster.local/mcp",
      "headers": {
        "Authorization": "Bearer <token>"
      }
    },
    "observe": {
      "command": "npx",
      "args": [
        "mcp-remote@latest",
        "https://<customerid>.observeinc.com/v1/ai/mcp",
        "--header",
        "Authorization:${OBSERVE_AUTH}"
      ],
      "env": {
        "OBSERVE_AUTH": "Bearer <customerid> <token>"
      }
    }
  }
}
```

### Example prompts

- "Show me cluster status and list all topics with partition counts."
- "Fetch committed offsets for group `payments-worker` and summarize lag risk."
- "Describe topic configs for `orders` and report retention."
- "Correlate Kafscale topic partitions with Grafana dashboard X panels."

## Security Notes

- The MCP service is read-only by default.
- Use `KAFSCALE_MCP_AUTH_TOKEN` and network policies for all deployments.
- Do not expose the MCP endpoint publicly without auth.
- `/healthz` is intentionally unauthenticated.
- Align with the security roadmap in `docs/security.md`.
