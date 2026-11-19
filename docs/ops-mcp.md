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
mutation tools. For design limits and the current security posture, see
[Design and limits](#design-and-limits) and `docs/security.md`.

## What It Exposes (v1)

Read-only tools exposed by the MCP service:

- `cluster_status`: no arguments.
- `cluster_metrics`: no arguments. Needs `KAFSCALE_MCP_BROKER_METRICS_URL`.
- `list_topics`: no arguments. Returns every topic.
- `describe_topics`: optional `names`, an array of topic names. Omit it to describe every topic.
- `list_groups`: no arguments.
- `describe_group`: required `group_id`.
- `fetch_offsets`: required `group_id`, optional `topics` (an array of topic names). Omit `topics` to fetch offsets for every topic in the group.
- `describe_configs`: optional `topics`, an array of topic names. Omit it to describe every topic.

A call that omits a required argument returns the validator error, for example `describe_group` without `group_id` returns `invalid params: validating "arguments": validating root: required: missing properties: ["group_id"]`. `cluster_metrics` without a metrics URL returns `metrics provider not configured`.

### Response shapes

Some tools return wrapper objects (not raw arrays):

- `list_topics` -> `{ "topics": [...] }`
- `describe_topics` -> `{ "topics": [...] }`
- `list_groups` -> `{ "groups": [...] }`
- `describe_configs` -> `{ "configs": [...] }`

Field notes:

- `cluster_status` includes `cluster_name` when available; clients can use it as the primary label and fall back to `cluster_id`.
- `describe_group` includes an `offsets` array sorted by topic and partition, with `topic`, `partition`, `offset` and `metadata` for each committed offset. A group with commits but no group metadata is returned with state `empty`, protocol type `consumer` and no members, matching `list_groups`. A group with neither metadata nor offsets is reported as not found.

## Design and limits

The MCP service is a standalone process (`kafscale-mcp`). It is not embedded in the broker. It reads cluster metadata from etcd and scrapes Prometheus metrics from brokers. It is not a replacement for the Kafka protocol or the ops admin APIs.

The Helm chart deploys it only when `mcp.enabled` is true (disabled by default) and defaults to the dedicated namespace `kafscale-mcp`.

The HTTP API is:

- `/mcp`: MCP streamable HTTP transport (SSE).
- `/healthz` and `/readyz`: unauthenticated probes, described under [Security Notes](#security-notes).

Only the read-only tools listed above are registered. Mutation tools such as `create_topic`, `delete_topic`, `create_partitions`, `update_topic_config`, `delete_group`, and broker drain or flush actions are not implemented.

Auth is an optional static bearer token, `KAFSCALE_MCP_AUTH_TOKEN`. If it is unset, the server is unauthenticated and logs a warning. OIDC, short-lived tokens, RBAC tool allowlists, and audit logging are not implemented.

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
    repository: ghcr.io/kafscale/kafscale-mcp
  auth:
    token: "<bearer-token>"
  etcdEndpoints:
    # Operator-managed etcd for KafscaleCluster demo in namespace kafscale.
    # The Service is <cluster>-etcd-client.<namespace>.svc.cluster.local:2379.
    - "http://demo-etcd-client.kafscale.svc.cluster.local:2379"
  metrics:
    # Broker metrics Service: <cluster>-broker.<namespace>.svc.cluster.local:9093
    brokerMetricsURL: "http://demo-broker.kafscale.svc.cluster.local:9093/metrics"
  sessionTimeout: 10m
```

The chart runs the image tag that matches the chart `appVersion` unless you set `mcp.image.tag`.

### Environment Variables

```text
KAFSCALE_MCP_HTTP_ADDR=:8090
# Read KAFSCALE_MCP_AUTH_TOKEN as shown under "Creating a bearer token" below.
KAFSCALE_MCP_ETCD_ENDPOINTS=http://demo-etcd-client.kafscale.svc.cluster.local:2379
KAFSCALE_MCP_ETCD_USERNAME=
KAFSCALE_MCP_ETCD_PASSWORD=
KAFSCALE_MCP_BROKER_METRICS_URL=http://demo-broker.kafscale.svc.cluster.local:9093/metrics
KAFSCALE_MCP_SESSION_TIMEOUT=10m
```

Notes:

- Metadata tools require `KAFSCALE_MCP_ETCD_ENDPOINTS`.
- Metrics require `KAFSCALE_MCP_BROKER_METRICS_URL`.
- With `KAFSCALE_MCP_ETCD_ENDPOINTS` set, `cluster_metrics` and the metrics part of `cluster_status` read every broker registered in etcd, at the broker host with the scheme, port and path of `KAFSCALE_MCP_BROKER_METRICS_URL`. They sum produce and fetch RPS and the admin request counters, average S3 latency, S3 error rate and admin latency, and report the worst S3 state. Each average includes every broker reporting that metric, including zero. Missing, malformed and non-finite samples are excluded from that metric’s average. S3 latency is averaged before converting to integer milliseconds. Brokers that do not answer are skipped. If etcd lists no brokers or none answers, they read the URL itself. Without etcd they read only the URL.
- See [Environment variable index](operations.md#environment-variable-index) for defaults.
- The examples use the quickstart cluster `demo` in namespace `kafscale`. Operator-managed etcd is `<cluster>-etcd-client.<namespace>.svc.cluster.local:2379`. Broker metrics are on `<cluster>-broker.<namespace>.svc.cluster.local:9093`. Use the full `.svc.cluster.local` name. See [Service names](operations.md#service-names).

### Creating a bearer token

The MCP server expects a static bearer token string (for now). Generate a strong random
token and configure it via `KAFSCALE_MCP_AUTH_TOKEN`. Only one token is valid at a time.

Example (base64, 32 bytes):

```bash
openssl rand -base64 32
```

Then set the value in Helm. The chart stores it in a Secret named after the rendered
MCP Deployment with `-auth` appended and sets `KAFSCALE_MCP_AUTH_TOKEN` from that Secret.
Both names use the chart's `kafscale.componentName` helper, so `nameOverride`,
`fullnameOverride`, and name truncation affect them. With release `kafscale` and no
overrides, the Deployment is `kafscale-mcp` and the Secret is `kafscale-mcp-auth`.
Changing `mcp.auth.token` rolls the MCP pod. The pod annotation
`kafscale.io/mcp-auth-checksum` is a SHA-256 hash of the Secret's token data. Anyone
who can read the Deployment can test guesses against it, so a short or guessable token
can be recovered. Prefer `mcp.auth.existingSecret` and a long random token if you do not
want a token-derived hash on the Deployment. With `mcp.auth.token`, the token also stays
in the Helm release values (`helm get values`).

```yaml
mcp:
  auth:
    token: "<generated-token>"
```

To use a Secret you already created, set `mcp.auth.existingSecret` to its name and
`mcp.auth.existingSecretKey` to the key (default `token`). The chart does not create a
Secret in that case, and `mcp.auth.token` is ignored. The existing Secret must already
exist in `mcp.namespace.name` (default `kafscale-mcp`) before install or upgrade, or the
pod gets `CreateContainerConfigError`. Setting `existingSecretKey` without
`existingSecret` fails the render.

```yaml
mcp:
  auth:
    existingSecret: my-mcp-token
    existingSecretKey: mcp-token
```

Changing the token inside an existing Secret does not restart MCP. After you
update the Secret, replace the release value below with your Helm release name and
use your `mcp.namespace.name` if you changed it. Select the Deployment by its release
and component labels so the command also works with name overrides:

```bash
release='<release>'
namespace='kafscale-mcp'
kubectl -n "$namespace" rollout restart deployment \
  --selector="app.kubernetes.io/instance=$release,app.kubernetes.io/component=mcp"
```

There is no overlap: only one token is valid at a time. To rotate: update the Secret
and run the rollout restart above, or set `mcp.auth.token` and run `helm upgrade`,
which rolls the pod. Then update every client right away. During the rollout clients
using either token can get 401s. Once the old pod is gone the old token is rejected,
so there is no separate revoke step.

For local runs without Helm, read and export the token. `read -rs` keeps the token out of your shell history:

```bash
read -rs KAFSCALE_MCP_AUTH_TOKEN
export KAFSCALE_MCP_AUTH_TOKEN
```

### Quick Test (port-forward)

```bash
kubectl -n kafscale-mcp get svc
kubectl -n kafscale-mcp port-forward svc/kafscale-mcp 8090:80
```

The Service is `kafscale-mcp` for the quickstart release name `kafscale`. For another release name, use the name `kubectl get svc` lists.

You can then connect an MCP client to `http://127.0.0.1:8090/mcp`.

Claude Desktop and IDE examples follow. From a shell, the MCP Inspector
CLI talks to the same endpoint. Keep the token off the command line: read it
without echo, write it to a config file only you can read, and delete the
file when you are done:

```bash
read -rs KAFSCALE_MCP_AUTH_TOKEN
(umask 077; cat > mcp-inspector.json <<EOF
{"mcpServers":{"kafscale":{"type":"streamable-http","url":"http://127.0.0.1:8090/mcp","headers":{"Authorization":"Bearer ${KAFSCALE_MCP_AUTH_TOKEN}"}}}}
EOF
)
npx -y @modelcontextprotocol/inspector --cli --config mcp-inspector.json --server kafscale --method tools/list
npx -y @modelcontextprotocol/inspector --cli --config mcp-inspector.json --server kafscale --method tools/call --tool-name describe_group --tool-arg 'group_id=<group>'
rm mcp-inspector.json
```

### Local run (no Helm)

```bash
read -rs KAFSCALE_MCP_AUTH_TOKEN
export KAFSCALE_MCP_AUTH_TOKEN
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
- `/healthz` and `/readyz` are intentionally unauthenticated. `/healthz` checks process liveness; `/readyz` checks live access to configured etcd with a one-second deadline and returns HTTP 503 on failure. Neither response exposes backend details. Metrics-only mode without a metadata store remains ready.
- Align with the security roadmap in `docs/security.md`.
