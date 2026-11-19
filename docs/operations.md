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

# KafScale production operations guide

This guide explains how to install the Kafscale control plane with Helm, configure S3 and etcd, and expose the operations console. It assumes you already have a Kubernetes cluster and the prerequisites listed below.

## Prerequisites

- Kubernetes 1.26+
- Helm 3.12+
- etcd for metadata. The operator can run a managed etcd for you (the default), or you can point it at an existing cluster with `operator.etcdEndpoints`
- An S3 bucket per environment plus IAM credentials with permission to list/create objects
- Optional: an ingress controller for the console UI

## Installing with Helm

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set 'operator.etcdEndpoints[0]=http://etcd.kafscale.svc:2379' \
  --set operator.image.tag=v1.6.3 \
  --set console.image.tag=v1.6.3
```

The chart ships the `KafscaleCluster` and `KafscaleTopic` CRDs so the operator can immediately reconcile resources.  Create a Kubernetes secret that contains your S3 access/secret keys, reference it inside a `KafscaleCluster` resource (see `config/samples/`), and the operator will launch broker pods with the right IAM credentials.

By default, image tags follow the chart `appVersion`. Every image key below uses that tag when `tag` is empty, unless `useLatest` is true, in which case the tag is `latest` and `imagePullPolicy` is `Always`:

- `operator.image`
- `operator.brokerImage`
- `operator.etcdSnapshotEtcdctlImage`
- `operator.lfsProxyImage`
- `console.image`
- `proxy.image`
- `schemaRegistry.image`
- `mcp.image`

Override `tag` on any of those keys to pin a specific release (for example, `operator.image.tag=v1.6.3`). If you install from a checkout of `main`, that tag may be older than the branch; set `useLatest=true` on each image the install runs.

The operator-managed LFS proxy image comes from `spec.lfsProxy.image` if that field is set, otherwise from the operator's `LFS_PROXY_IMAGE` env (the chart sets it from `operator.lfsProxyImage`, which follows `appVersion` like the other image keys). If neither is set, it falls back to `ghcr.io/kafscale/kafscale-proxy:dev`. The same order applies to the pull policy: `spec.lfsProxy.imagePullPolicy` if that field is set, otherwise `LFS_PROXY_IMAGE_PULL_POLICY` (the chart sets it to `Always` when `operator.lfsProxyImage.useLatest` is true, and to `IfNotPresent` otherwise). If that env is also unset, the pull policy is `IfNotPresent`.

The operator also reads `KAFSCALE_OPERATOR_ETCD_IMAGE` and `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE`. The chart sets them from `operator.etcdImage` and `operator.etcdSnapshotImage`. Those keys default to the pinned upstream images the operator already uses when the env is unset, `gcr.io/etcd-development/etcd:v3.6.14@sha256:a491baeaa0cb0c9cd89c0062ac44ece53886e3e5bddad18d2daf36678ce665b6` and `amazon/aws-cli:2.15.0`. They do not follow `appVersion`.

### Per-cluster broker image

`spec.brokers.image` overrides the operator's `BROKER_IMAGE` default for that cluster:

```yaml
spec:
  brokers:
    image:
      repository: ghcr.io/kafscale/kafscale-broker
      tag: v1.6.3
```

Omitting the image keeps the operator default. A repository without a tag is used
as supplied, including a complete `repository@sha256:...` digest reference. A tag
without a repository replaces the tag or digest in the operator default. When both
are set, the tag replaces any tag or digest in the supplied repository reference.
The operator's image pull policy still applies. Updating the image changes the
broker StatefulSet pod template, triggering its normal rolling update; removing
the override returns that cluster to the operator default.

### Values to pay attention to

| Value | Purpose |
|-------|---------|
| `operator.replicaCount` | Number of operator replicas (default `1`). Set `2` or more for HA; replicas elect a leader with a Kubernetes lease. Console etcd snapshot panel: Only the leader exports the `kafscale_operator_etcd_snapshot_*` gauges, and the operator metrics Service balances console scrapes across the replicas. When a scrape reaches a standby replica, the console reuses the last values it read from the leader for up to 3 minutes. It adds the time since that read to the snapshot age, and shows the snapshot as stale once that age passes `kafscale_operator_etcd_snapshot_threshold_seconds`, which every replica exports from `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_STALE_AFTER_SEC`. Before the first leader read, and 3 minutes after the last one, the panel shows n/a. Snapshot access shows n/a when the operator exports no `kafscale_operator_etcd_snapshot_access_ok` gauge, as with `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SKIP_PREFLIGHT=true`. For etcd that the operator does not manage, the operator takes no snapshots and sets access to 0, so the panel shows access FAIL, age 0, stale NO, and n/a for last success and last schedule. |
| `operator.leaderKey` | Kubernetes leader election lock name. Use a DNS-1123 compatible name. |
| `console.service.*` | Type/port used to expose the UI. Combine with `.console.ingress` to publish via an ingress controller. |
| `console.auth.*` | Console login credentials. Set `console.auth.existingSecret` to a Secret with `username` and `password` keys, or set both `console.auth.username` and `console.auth.password`, to enable the UI. |
| `imagePullSecrets` | Provide if your container registry (e.g., GHCR) is private. |

## Post-install steps

1. Apply a `KafscaleCluster` custom resource describing the S3 bucket, etcd endpoints, cache sizes, and credentials secret.
2. Apply any required `KafscaleTopic` resources.  The operator writes the desired metadata to etcd and the brokers begin serving Kafka clients right away.
3. Expose the console UI (optional) by enabling ingress in `values.yaml` or by creating a LoadBalancer service.

### Scaling brokers

The operator creates a HorizontalPodAutoscaler named `<cluster>-broker` for the broker StatefulSet. Its minimum is `spec.brokers.replicas` (or 3 if that field is unset or 0) and its maximum is four times that minimum. It targets 70% CPU and 80% memory utilization, so set CPU and memory requests in `spec.brokers.resources`; without requests the HPA cannot compute utilization and does not scale. The HPA also needs a metrics server in the cluster. CRDs from releases before 2.0 have no schema for `spec.brokers.resources.requests` and `limits`, so the API server drops them. Apply the new CRD before you set them (see [Upgrades & rollbacks](#upgrades--rollbacks)), then reapply the cluster.

- **Creating the cluster**: the operator creates the StatefulSet with `spec.brokers.replicas` brokers.
- **After that**: the operator keeps the replica count the HPA set. It only moves the count back inside the HPA range: a count below the minimum is raised to the minimum and a count above the maximum is lowered to the maximum.
- **Raising `spec.brokers.replicas`**: raises the HPA minimum, and the operator raises the StatefulSet to the new minimum right away.
- **Lowering `spec.brokers.replicas`**: lowers the HPA minimum. The operator does not remove brokers itself; the HPA removes them when load allows, as described under scale-down.
- **Without the HPA**: if the `<cluster>-broker` HPA is missing or is not owned by the cluster, the operator sets the StatefulSet replicas to `spec.brokers.replicas` on every reconcile. Note that the operator recreates its own HPA on the next reconcile.
- Setting `spec.brokers.replicas` to 0 stops all brokers. The HPA does not scale a StatefulSet with 0 replicas.

```bash
kubectl -n kafscale patch kafscalecluster demo --type merge -p '{"spec":{"brokers":{"replicas":3}}}'
```

Metadata follows the live broker count. The operator publishes the metadata snapshot with as many brokers as the StatefulSet replicas (`<cluster>-broker-0` up to the last ordinal), and each partition's preferred leader is its partition number modulo that count. Metadata responses from the brokers list every broker in the snapshot. A partition that a broker currently holds through its etcd lease reports that broker as leader; an unowned partition reports its preferred leader.

This placement applies to both `KafscaleTopic` resources and topics created through Kafka. At zero replicas, the snapshot has no brokers and partitions report `LEADER_NOT_AVAILABLE`. If the operator cannot read the StatefulSet, it leaves the last published snapshot in place and retries rather than publishing a guessed count.

Partitions move to new brokers:

- Every 10 seconds (`KAFSCALE_PARTITION_REBALANCE_INTERVAL_MS`), each broker checks the partitions it holds. When the preferred leader is another broker in the snapshot that accepts TCP connections, the old owner blocks new acquisitions, lets its active append finish, and then releases the lease. It does not take that partition back for three intervals (30 seconds by default), measured after release. The next produce or fetch goes to the preferred leader, which takes the lease.
- The proxy sends produce and fetch requests for unowned partitions to the preferred leader and falls back to any broker when it cannot connect. On an ownership rejection it refreshes the current owner and retries the affected partitions internally, up to three attempts. Requests can still receive a retryable error while the old owner is draining or the destination is unavailable.
- Brokers only hand partitions over this way with `KAFSCALE_PRODUCE_SYNC_FLUSH=true` (the default). With sync flush off, partitions stay where they are until their broker restarts.
- A broker whose advertised port is not reachable from the other brokers (for example an external `advertisedPort`) never receives partitions this way; it only gets partitions that are unowned when a client asks for them.

Scale-down:

- The HPA removes at most one broker per 60 seconds, and only after the load has been low enough for 300 seconds (the `scaleDown` rules on the HPA).
- Scale-up uses a 60 second stabilization window, so a short CPU spike, for example while brokers take over partitions after a scale-down, does not add brokers. After that it adds up to 100% of the brokers or 4 brokers per 15 seconds, whichever is more (the Kubernetes default policies).
- The StatefulSet removes the highest ordinal first. On SIGTERM the broker stops accepting connections, releases all of its partition and group leases, and waits for requests in flight. A produce that arrives after the release is rejected with `NOT_LEADER_OR_FOLLOWER`, and the client retries on the new owner.
- The operator then publishes the snapshot without the removed broker, and its partitions get a preferred leader among the remaining brokers, which take the leases on the next request.
- With `KAFSCALE_PRODUCE_SYNC_FLUSH=true` (the default) a produce is acknowledged only after its data is in S3, so removing a broker loses no acknowledged write. With `KAFSCALE_PRODUCE_SYNC_FLUSH=false`, records the removed broker acknowledged but had not yet flushed (up to `KAFSCALE_FLUSH_INTERVAL_MS` of data) are lost, as on any broker restart in that mode.
- Keep client retries enabled for retryable failures. A write rejected before append can be rerouted. If records reached S3 but ownership or offset publication could not be confirmed, the broker returns `REQUEST_TIMED_OUT`; the proxy does not replay that uncertain write automatically. Producer idempotency is not supported, so client retries after uncertain writes or lost responses can still duplicate records. Routing through a planned rebalance does not provide exactly-once delivery.

Console and MCP use `/healthz` for liveness. Their Helm readiness probes use `/readyz`, which performs a live etcd read with a one-second deadline and returns HTTP 503 when configured etcd is unreachable, even if metadata is cached. The next successful probe restores readiness without restarting the process. The Kubernetes probe timeout is two seconds. Standalone or metrics-only mode without a metadata store remains ready.

Operator-managed brokers use `/readyz` on the metrics port for Kubernetes readiness. Each check performs a live metadata read with a one-second deadline and checks the broker’s S3 health state. Unreachable etcd or an unavailable S3 state returns HTTP 503; degraded S3 remains ready. Kubernetes uses a two-second HTTP timeout and checks every five seconds, marking the broker NotReady after three failures. A successful check restores readiness. `/healthz` continues to report process liveness independently of etcd.

## Broker partition restore

On first access after a broker restart, a partition rebuilds its in-memory segment
catalog from S3 before serving requests for that partition. Restore uses up to 16
workers per partition to read segment footers and indexes. These requests also
share the broker-wide `KAFSCALE_S3_CONCURRENCY` limit with other S3 operations.

Broker logs report restore start, the listed segment count, progress every five
seconds during downloads, and completion with restored/skipped counts, last offset,
and duration. Failures include processed counts and the error. A failed restore
cancels the remaining workers and does not install a partial catalog.

Restore still lists all objects and reads a footer and index per eligible segment;
large partitions can still take time. This change does not merge small segments or
add a persistent manifest. Requests for the restoring partition wait for completion.

## Security & hardening

- **RBAC**: The Helm chart creates a scoped service account and RBAC role so the operator only touches its CRDs, Secrets, and Deployments inside the release namespace.
- **S3 credentials**: Credentials live in user-managed Kubernetes secrets. The operator never writes them to etcd. Snapshot jobs map `KAFSCALE_S3_ACCESS_KEY`/`KAFSCALE_S3_SECRET_KEY` into the AWS CLI env vars (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`) automatically.
- **Console auth**: The UI requires `KAFSCALE_UI_USERNAME` and `KAFSCALE_UI_PASSWORD`. There are no defaults; if unset, the login screen shows a warning and the API blocks access. In Helm, point `console.auth.existingSecret` at a Secret whose keys are `username` and `password`, or set `console.auth.username` and `console.auth.password` directly. For example:

```bash
printf 'Console password: '
read -rs CONSOLE_PASSWORD
echo
printf '%s' "$CONSOLE_PASSWORD" | kubectl -n kafscale create secret generic kafscale-console-auth \
  --from-literal=username=kafscaleadmin --from-file=password=/dev/stdin
unset CONSOLE_PASSWORD

helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale \
  --set console.auth.existingSecret=kafscale-console-auth
```

Keep the other `--set` flags your install already uses, because an upgrade with only this flag resets them.

- **TLS**: For Kafka clients, use the proxy mTLS listener (see [Kafka proxy authentication](kafka-authentication.md)) or terminate TLS at a LoadBalancer (see [Proxy TLS via LoadBalancer](#proxy-tls-via-loadbalancer)). Brokers and the console have no TLS listener of their own; terminate TLS at your ingress or service mesh.
- **Admin APIs**: Create/Delete Topics are enabled by default. Set `KAFSCALE_ALLOW_ADMIN_APIS=false` on broker pods to disable them, and gate external access via mTLS, ingress auth, or network policies.
- **Topic deletion**: DeleteTopics keeps the topic's data in S3.
  - Each broker drops its cached partition logs for the topic. A topic re-created with the same name starts empty at offset 0 under a new prefix.
  - A topic that was never deleted stores segments under `<namespace>/<topic>/<partition>/`. After the Nth delete, the next topic with that name stores them under `<namespace>/<topic>@<N>/<partition>/`. `<namespace>` is `KAFSCALE_S3_NAMESPACE`.
  - LFS objects live under their own prefix: `<namespace>/@lfs/<topic>/` for generation 0 and `<namespace>/@lfs/<topic>@<N>/` after the Nth delete. See [LFS proxy](#lfs-proxy).
- **Find retained topic data**: Each delete writes a record to etcd at `/kafscale/deleted-topics/<topic>/<generation>` with `name`, `topic_id`, `generation`, `prefix` and `deleted_at`. The current generation of a topic name is at `/kafscale/topic-generations/<topic>`. List the records with `etcdctl get --prefix /kafscale/deleted-topics/`. The data under each `prefix` stays in S3 until you remove it.
- **Network policies**: If your cluster enforces policies, allow the operator + brokers to reach etcd and S3 endpoints and lock everything else down.
- **Pod Security (restricted)**: Chart-templated workloads (operator, proxy, console, MCP) ship PSA `restricted`-compatible `securityContext` defaults.
  - The defaults are `runAsNonRoot`, dropped capabilities, `readOnlyRootFilesystem`, `seccompProfile: RuntimeDefault` and UID/GID `10001`.
  - Override per component via `<component>.podSecurityContext` and `<component>.containerSecurityContext`.
  - The proxy mounts a writable `/tmp` `emptyDir` for LFS verify temp files.
- **Pod placement**: Operator-managed broker and etcd StatefulSets, and the chart proxy Deployment (when `proxy.affinity` is empty), use soft (`preferredDuringSchedulingIgnoredDuringExecution`) pod anti-affinity on `kubernetes.io/hostname`.
  - Replicas spread across nodes on multi-node clusters and still schedule on single-node dev clusters.
  - Set `proxy.affinity` explicitly to replace the chart default.
  - For operator-managed brokers and etcd, use `spec.brokers.affinity` and `spec.etcd.affinity` as described below.
- **ACLs**: Optional, basic ACL enforcement is available at the broker.
  - With proxy mTLS enabled, identity comes from the client certificate mapping. Without it, identity comes from Kafka `client.id` or network identity, which is not authentication.
  - Configure via `KAFSCALE_ACL_ENABLED` plus either `KAFSCALE_ACL_JSON` or `KAFSCALE_ACL_FILE`.
  - Use `KAFSCALE_ACL_FAIL_OPEN=true` to allow traffic when the ACL config is missing/invalid (default is fail-closed).
  - **Authenticated proxy**: With `KAFSCALE_PROXY_IDENTITY_KEY_FILE` configured,
    the broker requires the proxy identity handshake and ignores legacy principal
    sources. See [Kafka proxy authentication](kafka-authentication.md).
  - **Principal source**: Set `KAFSCALE_PRINCIPAL_SOURCE` to `client_id` (default), `remote_addr`, or `proxy_addr`. Use `proxy_addr` with `KAFSCALE_PROXY_PROTOCOL=true` to derive principals from a trusted TCP proxy (PROXY protocol v1/v2).
  - **Auth denials**: Broker logs emit a rate-limited `authorization denied` entry with principal/action/resource context.
  - **Trust boundary**: Only enable `proxy_addr`/PROXY protocol when brokers are reachable *only* through a trusted LB or sidecar that injects the header. Do not expose brokers directly, or clients can spoof identity.
  - **Fail-closed**: When `KAFSCALE_PROXY_PROTOCOL=true`, brokers reject connections that do not include a valid PROXY header.
  - **Header limits**: PROXY v1 headers are capped at 256 bytes; oversized headers are rejected.
  - **Health checks**: PROXY v2 `LOCAL` connections are accepted (no identity); ensure LB health checks don’t rely on ACL-protected operations.

Example ACL values (Helm operator):
```yaml
operator:
  acl:
    enabled: true
    configJson: |
      {"default_policy":"deny","principals":[
        {"name":"analytics","allow":[{"action":"fetch","resource":"topic","name":"orders-*"}]}
      ]}
  auth:
    principalSource: "proxy_addr"
    proxyProtocol: true
```
- **Health / metrics**: Prometheus can scrape `/metrics` on the brokers and operator for early detection of S3 pressure or degraded nodes. The operator exposes metrics on port `8080` and the Helm chart can create a metrics Service, ServiceMonitor, and PrometheusRule.
- **Startup gating**: Before opening listeners, brokers check metadata access and S3 conditional writes. They write a unique `__health/` probe key twice and require the second `PutObject` with `If-None-Match: *` to return `412 Precondition Failed`; ignored or rejected conditions fail startup with an error. A first write that fails with `501` or the error code `NotImplemented` fails startup at once with `backend rejects If-None-Match: *`. A first write that fails with the error code `InvalidArgument` or `InvalidRequest` fails startup at once with `backend rejected the conditional PutObject (If-None-Match: *)`. Other initial S3 write failures retry until `KAFSCALE_STARTUP_TIMEOUT_SEC` expires. Probe objects are deleted after each attempt with a bounded cleanup request; cleanup failures are logged and need `s3:DeleteObject` on `__health/`.
- **Leader IDs**: Each broker advertises a numeric `NodeID` in etcd. In the single-node demo you’ll always see `Leader=0` in the Console’s topic detail because the only broker has ID `0`. In real clusters those IDs align with the broker addresses the operator published; if you see `Leader=3`, look for the broker with `NodeID 3` in the metadata payload.

### Proxy TLS via LoadBalancer

For per-client authentication and broker ACLs, use the native proxy mTLS listener
with TLS passthrough. See [Kafka proxy authentication](kafka-authentication.md).
The external termination configuration below provides transport security but does
not forward certificate identities.

The proxy is the external Kafka entrypoint. To terminate TLS outside the proxy,
do it at the cloud LoadBalancer by supplying Service annotations in the Helm values.
This keeps broker traffic plaintext inside the cluster while enabling HTTPS/TLS
for clients.

Example (provider-specific annotations omitted here):

```yaml
proxy:
  enabled: true
  service:
    type: LoadBalancer
    port: 9092
    annotations:
      # Add your cloud provider TLS annotations here (ACM / GCP / Azure, etc.)
    loadBalancerSourceRanges:
      - 203.0.113.0/24
```

If you need in-cluster ACME/Let’s Encrypt support, document it as an optional
stack (Traefik or another TCP-capable gateway + cert-manager). Keep it off by
default to avoid extra operational dependencies.

#### Provider examples (verify with your cloud)

These snippets show common patterns; annotation keys vary by provider and
feature. Always validate against your cloud provider docs.

AWS NLB TLS (example):
```yaml
proxy:
  service:
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
      service.beta.kubernetes.io/aws-load-balancer-ssl-cert: "arn:aws:acm:REGION:ACCOUNT:certificate/ID"
      service.beta.kubernetes.io/aws-load-balancer-ssl-ports: "9092"
      service.beta.kubernetes.io/aws-load-balancer-backend-protocol: "tcp"
```

GCP / Azure:
- L4 LoadBalancers typically do TCP pass-through; TLS termination often requires
  a provider gateway/ingress (or a TCP-capable ingress controller).
- If you terminate TLS outside the Service, keep the proxy Service as plain TCP.

## S3 credentials secret

The secret named by `spec.s3.credentialsSecretRef` can hold either key pair:

| Pair | Optional token |
|------|----------------|
| `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` | `KAFSCALE_S3_SESSION_TOKEN` |
| `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` | `AWS_SESSION_TOKEN` |

The brokers, the managed etcd snapshot upload job and the restore init container
accept either pair.

**Blank values and whitespace**

- Every consumer treats a key whose value is empty or only whitespace as unset,
  and trims whitespace around other values. This covers the operator
  `S3Credentials` condition and snapshot check, the brokers, the LFS proxy, the
  schema registry and the etcd snapshot containers.
- Whitespace means space, tab, newline, carriage return, vertical tab and form
  feed. Unicode spaces such as U+00A0 are kept as part of the value.
- A `spec.s3.credentialsSecretRef` that is empty or only whitespace means no
  secret.

**Which pair is used**

- When a secret holds both pairs, the brokers and the snapshot containers use
  `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY`. The LFS proxy uses
  `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
- The snapshot containers and the operator snapshot check use the
  `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` pair when both keys are
  set, otherwise the `AWS_*` pair.
- With the `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` pair they use `KAFSCALE_S3_SESSION_TOKEN` when the
  secret holds that key (a blank value means no token), and `AWS_SESSION_TOKEN`
  otherwise. With the `AWS_*` pair they use `AWS_SESSION_TOKEN`.
- A missing key does not stop a pod from starting. Without a complete pair, the
  proxy uses the default AWS credential chain if one is available.

**Rotation**

The broker and LFS proxy pod templates carry a
`kafscale.io/s3-credentials-checksum` annotation, computed over the credential
keys present in the secret. When one of those keys changes, those pods roll,
also while the etcd snapshot check fails. Other keys in the secret do not roll
them.

The Helm chart passes only the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
keys of the Secret named by `schemaRegistry.s3.credentialsSecret` to the schema
registry, as environment variables. It passes no session token, so temporary
credentials from that Secret are not supported. The registry reads the keys
once at startup.

When the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` keys in that Secret
change, the registry does not roll. Pointing `schemaRegistry.s3.credentialsSecret`
at a different Secret changes the pod template, so `helm upgrade` rolls the
registry without a manual restart.

Until the restart the registry keeps using the old keys. Once those keys are
revoked, deleted or expire, every request that has to read or write S3 fails
with HTTP 500, error code 50001 and `Error in the backend data store: <S3 error>`.
The S3 error depends on the provider and on how the key was retired, for example
`InvalidAccessKeyId` or `SignatureDoesNotMatch`. Requests that fail include:

- Registering a schema that gets a new id. The registry writes an id marker to
  S3 for every new id, and the schema text too when it is above
  `KAFSCALE_SCHEMA_REGISTRY_INLINE_LIMIT_BYTES`.
- Reading a schema above `KAFSCALE_SCHEMA_REGISTRY_INLINE_LIMIT_BYTES` when the
  pod does not have it in its cache, because its text is stored in S3.
- Schema lookup (`POST /subjects/{subject}`) and compatibility checks
  (`POST /compatibility/subjects/{subject}/versions` and
  `POST /compatibility/subjects/{subject}/versions/{version}`). They load the
  existing versions of the subject and fail when one of those versions is stored
  in S3 and not cached.
- Loading a schema stored inline in etcd when one of its references is stored in
  S3 and not cached.

Requests that need no S3 read or write keep working, such as reads of schemas
whose text and references are inline in etcd or in the pod cache, and
registering such a schema again when it is the latest version of the subject.

Rotate the keys in this order. The order matters.

1. Update `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the Secret.
2. Find the registry Deployment by its labels and restart it:

   ```bash
   DEPLOY=$(kubectl get deployment -n '<namespace>' -l 'app.kubernetes.io/instance=<release>,app.kubernetes.io/component=schema-registry' -o name)
   kubectl rollout restart "$DEPLOY" -n '<namespace>'
   ```

3. Check that the rollout finished:

   ```bash
   kubectl rollout status "$DEPLOY" -n '<namespace>'
   ```

4. Only then revoke the old keys.

During the restart each old pod leaves the Service endpoints when its deletion
starts, and keeps serving for a short drain period while kube-proxy and load
balancers catch up, before it stops listening. See [Shutdown](schema-registry.md#shutdown).

`<namespace>` is the namespace of the Helm release and `<release>` is the
release name. The labels do not depend on `nameOverride` or `fullnameOverride`.

**The `S3Credentials` condition**

The operator reports the secret state on the KafscaleCluster:

| Status | Reason | Meaning |
|--------|--------|---------|
| `True` | `SecretKeysFound` | A complete pair was found. |
| `True` | `SecretNotReferenced` | No secret is set. |
| `False` | `SecretNotFound` | The referenced secret does not exist. |
| `False` | `SecretReadFailed` | The operator could not read or verify the secret, for example because of RBAC. It says nothing about the keys the pods receive. |
| `False` | `SecretKeysMissing` | The secret holds no complete pair. When it holds part of a pair, the message names the keys it found. |
| `False` | `SecretKeysMixed` | See below. |

`SecretKeysMixed` flags a likely mistake in two cases:

- The secret holds the `AWS_*` pair plus one but not both of
  `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY`, or a
  `KAFSCALE_S3_SESSION_TOKEN` without that pair, with values that are not blank.
  Every workload then uses the `AWS_*` pair and ignores those keys.
- The secret holds the `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` pair and `AWS_SESSION_TOKEN` but no
  `KAFSCALE_S3_SESSION_TOKEN`. The etcd snapshot containers and the operator
  snapshot check would then sign with a token from the other pair.

What happens with each failure:

- With `SecretNotFound`, or `SecretKeysMissing` and no credential keys at all,
  workloads get no static keys and use the default AWS credential chain if one is
  available.
- With only part of a pair, the brokers and the LFS proxy use the default AWS
  credential chain if one is available. The etcd snapshot jobs run the AWS CLI,
  which fails on an access key without a secret key instead of falling back.
- The snapshot jobs disable the EC2 instance metadata service, so their default
  chain has no node role.
- The operator logs a warning once each time the condition turns `False`.
- The condition covers the brokers and snapshot jobs too, and never stops a
  reconcile on its own. A missing secret still fails the managed etcd snapshot
  check, as before.

At startup the proxy logs `lfs s3 credentials` with the source it uses: static
keys or the default AWS credential chain. For static keys it also logs a boolean
`session_token` flag. No key or token values are logged. The flag is left out for
the default chain because its provider may resolve credentials later.

## Ops API examples

Kafscale exposes Kafka admin APIs for operator workflows (consumer group visibility,
config inspection, cleanup). Command examples, API coverage, and config whitelists live in [Ops API](ops-api.md#ops-examples).

## etcd availability & storage

Kafscale depends on etcd for metadata + offsets. Treat etcd as a production datastore:

- Run a dedicated etcd cluster (do not share the Kubernetes control-plane etcd).
- Use SSD-backed disks for data and WAL volumes; avoid networked storage when possible.
- Deploy an odd number of members (3 for most clusters, 5 for higher fault tolerance).
- Spread members across zones/racks to survive single-AZ failures.
- Enable compaction/defragmentation and monitor fsync/proposal latency.
- HA requires a stable client access layer: use a Kafka-aware proxy or per-broker endpoints so bootstrap addresses and leader routing survive broker churn.

### Operator-managed etcd (default path)

If no etcd endpoints are supplied, the operator will provision a 3-node etcd StatefulSet for you. Recommended settings:

- Use an SSD-capable StorageClass for the etcd PVCs (`storageClassName`), with enough IOPS headroom.
- Set a PodDisruptionBudget so only one etcd pod can be evicted at a time.
- Pin etcd pods across zones with topology spread or anti-affinity.
- Enable snapshot backups to a dedicated S3 bucket and retain at least 7 days of snapshots.
- Monitor leader changes, fsync latency, and disk usage; alert on slow or flapping members.

The operator tunes managed etcd automatically:

- In-process periodic compaction (5m retention) and a 4 GiB backend quota on the etcd StatefulSet.
- `<cluster>-etcd-snapshot` CronJob (hourly, `0 * * * *`): writes `etcd-snapshots/<timestamp>.db` and a matching `.sha256` into the snapshot bucket (default `kafscale-etcd-<namespace>-<cluster>`, prefix `etcd-snapshots`). Override the schedule with `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SCHEDULE`.
- `<cluster>-etcd-maintenance` CronJob (every 6h): per-member defrag and NOSPACE alarm disarm.
- `<cluster>-etcd-maintenance-check` CronJob (every 15m): runs maintenance only when backend size crosses 75% of quota or a NOSPACE alarm is active.

To take a snapshot now, replace the namespace and cluster values below and start a Job from the snapshot CronJob:

```bash
namespace='<namespace>'
cluster='<cluster>'
kubectl -n "$namespace" create job --from=cronjob/"$cluster"-etcd-snapshot snapshot-now
kubectl -n "$namespace" wait --for=condition=complete --timeout=5m job/snapshot-now
kubectl -n "$namespace" logs job/snapshot-now
```

Use a new Job name for each manual snapshot. Verify the manual Job's `Complete` condition and its logs, which show the snapshot and checksum uploads. If the wait times out, inspect the Job with `kubectl -n "$namespace" describe job/snapshot-now` and check its logs before relying on the snapshot. A manual Job created with `--from=cronjob` does not update the CronJob's `status.lastSuccessfulTime`, so it does not refresh the cluster's `EtcdSnapshot` condition.

The `KafscaleCluster` condition `EtcdSnapshot` tracks scheduled snapshots through the CronJob's `status.lastSuccessfulTime`. A recent scheduled success sets `EtcdSnapshot=True` with reason `SnapshotHealthy`; no recorded success or an older success produces `SnapshotNeverSucceeded` or `SnapshotStale`. With operator-managed etcd, the operator watches CronJob status changes and requests a recheck at most one minute later, but reconciliation delays can postpone the condition update.

The snapshot bucket defaults to `kafscale-etcd-<namespace>-<cluster>`. The CronJob is created only when etcd is operator-managed. External etcd gets `EtcdSnapshot=False` with reason `SnapshotNotManaged` on the next reconcile.

Override schedules and thresholds via the operator etcd env vars listed below.

### etcd endpoint resolution

The operator resolves etcd endpoints in this order:

1. `KafscaleCluster.spec.etcd.endpoints`
2. `KAFSCALE_OPERATOR_ETCD_ENDPOINTS`
3. Managed etcd (operator creates a 3-node StatefulSet)

### Service names

On an operator-managed cluster, Kubernetes Service names come from the
`KafscaleCluster` name (`<cluster>`) and its namespace (`<namespace>`):

- etcd client: `<cluster>-etcd-client.<namespace>.svc.cluster.local:2379`
- broker (Kafka): `<cluster>-broker.<namespace>.svc.cluster.local:9092`
- broker (metrics): `<cluster>-broker.<namespace>.svc.cluster.local:9093`
- per-broker DNS: `<cluster>-broker-<n>.<cluster>-broker-headless.<namespace>.svc.cluster.local:9092`

The quickstart cluster `demo` in namespace `kafscale` uses
`demo-etcd-client.kafscale.svc.cluster.local:2379` and
`demo-broker.kafscale.svc.cluster.local:9092` (metrics on port 9093).

Helm values such as `proxy.etcdEndpoints`, `schemaRegistry.etcdEndpoints`,
`mcp.etcdEndpoints` and `mcp.metrics.brokerMetricsURL` must use these
Services when etcd is operator-managed. Examples that use
`etcd.kafscale.svc` are for an external etcd you already run, not for
managed etcd.

### etcd schema direction

Kafscale uses a snapshot-based metadata schema today: the operator publishes a full metadata snapshot to etcd and brokers consume it. We avoid per-key writes for broker registrations and assignments until the ops surface requires it.

### Consumer group keys in etcd

- `/kafscale/consumers/<group>/metadata` holds the group metadata.
- `/kafscale/consumers/<group>/offsets/<topic>/<partition>` holds each committed offset.
- `/kafscale/consumer-groups/<group>` is an empty index key. Brokers write it in the same etcd transaction as a group's metadata when they create the group, when a member joins a group that has no index key, and when the retention sweep sets or refreshes a group's `EmptySince` time. They delete it with the group.

### Partition leases

Each in-use partition is owned by one broker through an etcd lease. The key is
`/kafscale/partition-leases/<topic>/<partition>` and the value is the broker
id, as a decimal string (the broker NodeID). The keys share one etcd session
on the owning broker. The default lease TTL is 10 seconds. The broker refreshes
that session lease while it runs, so its keys, including keys for a deleted
topic, stay until it shuts down, or 10 seconds after it stops refreshing. To
see who owns the partitions of a topic:

```bash
kubectl -n kafscale exec demo-etcd-0 -c etcd -- etcdctl get --prefix /kafscale/partition-leases/orders/
```

The retention sweep runs on the broker that holds the `/kafscale/retention-sweeper` lease.

- Most sweeps list groups from the index keys only, page by page, so a sweep reads one index key and one metadata key per group instead of every offset key name.
- Once an hour, and on the first sweep after a broker takes the lease, the sweep reads all keys under `/kafscale/consumers/` page by page instead.
- That full scan finds groups that have offsets but no metadata, and adds index keys, up to 100 per etcd transaction, for groups that have metadata but no index key, which includes every group created before this change or by a broker running an older version.
- The sweep removes an index key whose group has no metadata and no offsets.
- Kafka `ListGroups` still reads all keys under `/kafscale/consumers/`, page by page, so it also lists groups that have only offsets.

### etcd availability signals (clients)

When etcd is unavailable, the broker rejects producer/admin/consumer-group operations with `REQUEST_TIMED_OUT`. Producers see per-partition errors in the Produce response; admin and group APIs return the same code in their response payloads.

Snapshot job defaults and operator env overrides are listed under [Operator](#operator).

The operator performs an S3 write preflight before enabling snapshots.

- If the check fails, the `EtcdSnapshotAccess` condition is set to `False`, `Ready` is `False` with `SnapshotAccessFailed`, and reconciliation returns an error until access is restored.
- While it fails, the operator still applies the broker and LFS proxy workloads of a cluster whose broker StatefulSet exists, so all broker and LFS proxy spec changes, credential rollouts included, are still applied, and the cluster metadata is still published to etcd.
- For a new cluster, the cluster controller does not create the brokers or LFS proxy or publish the cluster metadata until the check passes, so no brokers start writing etcd state that cannot be snapshotted.
- The topic controller does not run this check, so a KafscaleTopic that references the cluster still publishes its metadata, topics included, to etcd.
- `Ready` becomes `True` only after the check passes.
- Snapshots are uploaded as timestamped files plus a `.sha256` checksum for recovery validation.

### Snapshot restore (KafScale managed etcd)

Snapshot restore refers to **etcd operational data** (cluster metadata/offsets). It is not the broker topic snapshot flow.

When the KafScale operator manages etcd, each cluster pod runs ```restore init containers``` before etcd starts:

- The snapshot download container pulls the latest `.db` snapshot from the configured bucket/prefix.
- The restore container runs `etcdutl snapshot restore --bump-revision 1000000000 --mark-compacted` if the data directory is empty and a snapshot file is present. The bump keeps etcd revisions moving forward after the restore, and marking them compacted ends every watch that started before it.
- If no snapshot is available, etcd starts with a clean data directory.

The schema registry keeps its state in the same etcd.

- Registry pods watch their keys and clear their schema cache when that watch reports a compaction, so a restore run by the operator is handled without restarting them.
- For a manual restore, run `etcdutl snapshot restore` with `--bump-revision` and `--mark-compacted`, or restart every registry pod afterwards.
- Without those flags a pod can't always tell that etcd was restored: the restore keeps the cluster id, and the revision soon climbs past what the pod saw, so each pod keeps serving the schemas it had cached.
- When the registry has `KAFSCALE_S3_BUCKET` set, it records every id it gives out in S3 with a create-only write and skips ids recorded there, so schemas registered after the restore get ids that data written before the restore does not carry. Without S3 it gives out those ids again.
- See [schema-registry.md](schema-registry.md#storage).

The restore image must include `/bin/sh` and an `etcdutl` that supports `--bump-revision` and `--mark-compacted`. The default image ships etcdutl 3.6.14. Override with `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_ETCDCTL_IMAGE` if you use a custom image.

### Installing kafscale-cli

`kafscale-cli` is built from this repository and needs Go 1.26 or later. There is no container image or
released binary for it. From the repository root:

```bash
go build -o bin/kafscale-cli ./cmd/kafscale-cli
```

`kafscale-cli --help` and `kafscale-cli <command> --help` print the commands,
flags and environment variables.

### Topic recovery on the DR spine

For broker topic data, KafScale now supports **recovery into a new topic** on the DR side. This is intentionally not an in-place rollback on the primary cluster.

Use `kafscale-cli restore` to create a fresh target topic, copy `.kfs` segment/index pairs up to a cutoff timestamp, and set the recovered topic's next offsets:

```bash
kafscale-cli restore \
  --topic orders \
  --target-topic orders-restore-20260513 \
  --namespace kafscale \
  --to 2026-05-13T14:23:00Z
```

`--namespace` is the S3 prefix namespace for the source topic, which on an operator cluster is the `KafscaleCluster` namespace. It defaults to `KAFSCALE_NAMESPACE`, then `KAFSCALE_S3_NAMESPACE`, then `default`. `--target-namespace` defaults to the source namespace. `--partitions` is an optional comma-separated list of partitions to restore.

Every environment variable `restore` reads is listed in [kafscale-cli](#kafscale-cli).

Operational semantics:

- Recovery runs against the existing KafScale S3 + etcd control plane, including `KAFSCALE_S3_BUCKET`, `KAFSCALE_S3_REGION`, `KAFSCALE_S3_ENDPOINT`, `KAFSCALE_S3_PATH_STYLE`, and `KAFSCALE_ETCD_ENDPOINTS`.
- S3 credentials are `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY`, optionally `KAFSCALE_S3_SESSION_TOKEN`. If either is unset, the AWS SDK default credential chain is used: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`; the shared config and credentials files with `AWS_PROFILE`; web identity with `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` (EKS IRSA); and ECS or EC2 instance role credentials. Without credentials, restore fails at its first S3 call, listing the source segments, with an error that starts `list objects <prefix>:` and includes `get credentials`, and creates no target.
- Restore fails when there are no segments for the selected partitions under `<namespace>/<topic>/`, or `<namespace>/<topic>@<generation>/` for a re-created topic, or when no record in those segments is at or before `--to`. The command exits with a non-zero status and names the prefix it searched. If the first listing already shows no segments, the target topic is not created; otherwise the target is rolled back. Set `--namespace`, `KAFSCALE_NAMESPACE` or `KAFSCALE_S3_NAMESPACE` to the cluster namespace (`KAFSCALE_NAMESPACE` wins over `KAFSCALE_S3_NAMESPACE`).
- The target topic must be new. KafScale refuses to restore over an existing persisted topic.
- Recovery copies every fully eligible segment/index pair, then inspects the first intersecting segment and truncates it at the first record whose timestamp exceeds the cutoff.
- If the intersecting batch is compressed, exact truncation is not safe, so the restore fails and the operator should choose an earlier cutoff or a segment boundary instead.
- Offsets are preserved inside the recovered topic so replay, validation, and downstream cutover can happen without rewriting the source topic.
- The safer pattern is restore, validate, then cut consumers or downstream jobs over deliberately.

### Pruning deleted topic data

DeleteTopics keeps a deleted topic's S3 data and records it under `/kafscale/deleted-topics/<topic>/<generation>` in etcd. Use `kafscale-cli prune` to list that data and remove it. It runs as a dry run unless you pass `--force`. Every environment variable `prune` reads is listed in [kafscale-cli](#kafscale-cli):

```bash
kafscale-cli prune
kafscale-cli prune --topic orders --older-than 7d
kafscale-cli prune --topic orders --older-than 7d --force
```

- `--topic` limits the run to deleted incarnations of one topic. `--older-than` limits it to incarnations deleted longer ago than the given age, such as `7d` or `24h`.
- The output has one line per deleted incarnation with the topic, generation, S3 prefix, object count, bytes and `deleted_at`, then the totals. A dry run says that nothing was deleted.
- `kafscale-cli prune` needs the same S3 and etcd environment as restore. A dry run needs them too, because it calls CheckBucket, lists etcd and lists S3. It talks to etcd and S3 directly and does not go through the brokers, so access to prune is controlled by the etcd and S3 credentials. `KAFSCALE_ALLOW_ADMIN_APIS` and Kafka ACLs do not apply.
- Each incarnation is matched by its exact prefix with a trailing slash, such as `default/orders@1/`, so it never matches `orders@10` or `orders-x`.
- Prune only counts and deletes partition segment and index files, `<prefix><partition>/segment-<offset>.kfs` and `.index`. It skips every other key under the prefix, including LFS objects that older proxies wrote under `<namespace>/<topic>/lfs/`. LFS objects written now live under `<namespace>/@lfs/`, outside every topic prefix.
- Prune checks that the bucket exists before it lists anything, and fails instead of creating it.
- Before deleting a prefix, prune reads the topic's current generation and the topic snapshot from etcd again. It refuses a prefix that belongs to the current generation or to a live topic.
- Prune deletes the S3 objects first and removes the etcd record only after that succeeds. If listing or deleting fails for an incarnation, prune keeps its record, carries on with the others, and exits with an error. Running prune again finishes the job.
- Prune is safe while brokers run, because brokers only write to the current generation's prefix.
- On a bucket with versioning enabled, prune only adds delete markers. Add a lifecycle rule that expires noncurrent versions to free the space.
- Prune does not delete LFS objects. Topics deleted before the upgrade that added generations have no record, so prune does not cover their data.
- If `kafscale-cli restore` creates the target topic and a later step fails (`UpdateTopicConfig`, `TopicGeneration` or the copy), restore rolls back by retiring that target. That writes a `/kafscale/deleted-topics` record with no objects, or only those copied before the failure, which `kafscale-cli prune` lists. If the rollback itself fails, restore prints `warning: failed to roll back target topic <name>: ...` to stderr, the target stays, and no deleted-topics record is written. `prune --topic <target> --force` removes the record, and every other deleted incarnation of that name.

### Consumer offsets after restore

Etcd restores recover committed consumer offsets. If a consumer has **no committed offsets**, it may start at the end and see zero records even though data exists in S3. In production:

- Ensure consumers commit offsets (default for most Kafka clients).
- Set `auto.offset.reset=earliest` as a safety net for new or uncommitted consumers.

Minimal env + spec checklist for a smooth run:
- Operator env: `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET` (optional override), `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT` (if non-AWS), optionally `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET=1`.
- Cluster spec: `spec.s3.bucket`, `spec.s3.region`, `spec.s3.credentialsSecretRef`, `spec.s3.endpoint` (if non-AWS). Optional read replica: `spec.s3.readBucket`, `spec.s3.readRegion`, `spec.s3.readEndpoint`.
- Secret keys: `KAFSCALE_S3_ACCESS_KEY`, `KAFSCALE_S3_SECRET_KEY`.
- Console auth env: `KAFSCALE_UI_USERNAME`, `KAFSCALE_UI_PASSWORD`. The Helm chart sets them from `console.auth.existingSecret` or from `console.auth.username` and `console.auth.password`.

Recommended operator alerting (when using Prometheus Operator):
- `KafscaleSnapshotAccessFailed`: S3 snapshot writes failing.
- `KafscaleSnapshotStale`: last successful snapshot older than the staleness threshold.
- `KafscaleSnapshotNeverSucceeded`: no successful snapshots recorded.

## Proxy readiness

The proxy exposes `/readyz` and `/livez` on `KAFSCALE_PROXY_HEALTH_ADDR`
(default `:9094`).

`/livez` returns 200 as soon as the process starts. `/readyz` returns 503 with
`starting` until startup finishes: the etcd metadata read, the partition and
group route load, and the Kafka listener bind. While etcd does not answer,
startup waits, and the pod stays up and NotReady.

`/readyz` answers two questions, not one:

1. **Is a backend known?** Cached backend state when fresh, otherwise a live
   metadata fetch from etcd.
2. **Does that backend actually serve Fetch?** A Fetch round-trip on a fresh
   connection that must decode cleanly. Skipped during an etcd outage within
   the TTL, see
   [Proxy behavior when etcd is down](#proxy-behavior-when-etcd-is-down).

The second question exists because the proxy is a full Fetch codec: it decodes,
merges and re-encodes broker Fetch responses. A proxy-to-broker Fetch
serialization or connection-state mismatch therefore breaks consume while
Metadata and ListOffsets keep answering normally. Readiness based on the first
question alone reports `ready` while every consumer silently receives zero
records, and Kubernetes keeps the pod in the Service.

The probe asks for one partition of a deliberately unknown topic ID, so it needs
no real topic and reads no data; a correct broker answers `UNKNOWN_TOPIC_ID`.
The fresh connection is deliberate too: it detects a stale or half-open pooled
connection, which a cached-state check cannot.

Set `KAFSCALE_PROXY_READYZ_FETCH_PROBE=false` to fall back to the shallow
"a backend exists" check. That is an escape hatch for a probe that reports a
false negative in the field, not a recommended steady state: it removes the only
readiness signal that distinguishes "serving" from "running".

**Startup ordering.** With the probe enabled the proxy stays NotReady until a
broker serves Fetch. An installer that waits for proxy readiness before creating
the cluster resource that produces the broker will therefore deadlock. Create
the broker first, or do not gate the install on proxy readiness.

## Proxy behavior when etcd is down

The proxy reads broker and topic metadata from etcd every 10 seconds. When etcd
cannot be read, `KAFSCALE_PROXY_BACKEND_CACHE_TTL_SEC` (default `60`) decides how
long it keeps going on its last good read. The TTL counts from the last
successful read.

### Within the TTL

- The Kafka listener keeps using the brokers, routes and metadata it last read.
- Clients still get metadata and reach a broker, but their produces wait. Brokers
  reject produces with `REQUEST_TIMED_OUT` while etcd is down, and clients keep
  retrying, so the TTL does not keep produces working.
- `/readyz` stays Ready while etcd does not answer within 200 ms, even if the
  Fetch probe cannot reach a broker. The probe still has to succeed when etcd
  answers, and always when `KAFSCALE_PROXY_BACKENDS` lists static brokers. This
  applies to the chart proxy and the LFS proxy.
- The LFS HTTP API already fails. `/lfs/produce` and `/lfs/uploads` read the
  topic generation from etcd on every request, so they return 503
  `metadata_unavailable` as soon as etcd cannot be read.

### Past the TTL

- `/readyz` reports NotReady.
- The Kafka listener answers no Metadata or FindCoordinator request and routes
  no request to a broker, not even over a broker connection that is already open.
- Each rejected request gets `REQUEST_TIMED_OUT`, which clients retry. It is set
  on the response, or on each topic, partition, group or resource the request
  names.
- A Metadata request for all topics names nothing, so it gets an empty reply with
  no brokers, no topics and no error code. `kcat -L` and most clients send this at
  startup. Clients treat it as failed metadata and retry.
- A request type that has no not-ready reply gets no answer.
- After the reply, the proxy closes the client connection.
- ApiVersions is still answered.
- The LFS HTTP API sends nothing to a broker. `/lfs/produce` checks the TTL again
  after the upload, right before the record is sent. An upload that outlasts the
  TTL is deleted and gets 503.

### Recovery

Before it rejects a request, the proxy tries to read etcd again. etcd clients
reconnect with a one-second maximum gRPC backoff, and each connection attempt
is given at least five seconds, so the proxy serves again within a few seconds
after etcd answers, and `/readyz` turns Ready on the next check.

### Notes

- Because reads happen every 10 seconds, an outage gets between the TTL minus 10
  seconds and the full TTL before the proxy stops. With a TTL of 10 seconds or
  less it stops almost at once.
- The limit does not apply when `KAFSCALE_PROXY_BACKENDS` lists static brokers.
- Kubernetes marks the pod NotReady about 30 seconds after `/readyz` starts
  failing (probe every 5 seconds, failureThreshold 6).
- This is on purpose. When etcd is down the cluster has to stop writing to avoid
  data loss, so past the TTL the proxy stops instead of letting clients write
  through stale routing.

### How the LFS proxy finds brokers

The LFS proxy, including its HTTP API, finds brokers through etcd at their
advertised address.

- Brokers in `spec.lfsProxy.backends` take priority over the ones found in etcd.
- When etcd lists no brokers, the HTTP API has no broker.
- When etcd cannot be read, it uses the brokers it last found for as long as
  `spec.lfsProxy.backendCacheTTLSeconds` (default 60 seconds), as the Kafka
  listener does.
- The broker list is cached, but `/lfs/produce` and `/lfs/uploads` still return
  503 at once because they read the topic generation from etcd.

**Before and after the upload**

- Before the HTTP API uploads an object to S3 or starts a multipart upload, it
  connects to a broker. If it cannot reach one, it returns 503
  `backend_unavailable` and uploads nothing.
- If a broker goes away after the upload and the proxy cannot connect to any
  broker, it deletes the object again.

**Routing to the partition owner**

- `/lfs/produce` and multipart completion send the produce to the broker that
  owns the partition, with brokers from etcd and with static backends. They use the same partition
  routes as the Kafka listener, and reuse an idle broker connection only when it
  goes to that broker.
- When the owner is not known yet, they pick one of the listed brokers.
- When a broker answers that it does not lead the partition
  (`NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE`, `FENCED_LEADER_EPOCH` or
  `UNKNOWN_LEADER_EPOCH`), the proxy sends the produce to the current owner, to
  at most 3 brokers in total. It checks the backend cache TTL before each new
  attempt.
- If no broker accepts it, the object is deleted and the request gets 503
  `backend_unavailable`.

**Static backends**

- With static backends the proxy still sends the produce to the partition
  owner from the etcd routes when the owner is known. The listed brokers are
  used when the owner is not known yet, or when the owner address cannot be
  dialed.
- LFS produce goes to the broker's advertised address when it is reachable, and
  only falls back to the list otherwise. An advertised address that fails to
  dial is skipped for 30 seconds, so the proxy does not wait for the dial
  timeout on every produce.
- When a listed broker answers that it does not lead the partition, the proxy
  tries the current owner, or another listed broker if the owner is not known.
- With only one address, for example a broker Service, and no reachable owner,
  there is no other broker to try. A non-owner answer gives 503
  `backend_unavailable` and the object is deleted.
- When the advertised address is not reachable from inside the cluster, for
  example when brokers advertise a host port forward, set
  `spec.lfsProxy.backends` to the broker service, such as
  `kafscale-broker.<namespace>.svc.cluster.local:9092`. Otherwise the proxy stays
  NotReady.

**LFS tracker**

- The tracker uses `backends` only as seed brokers. It then produces to the
  advertised address of the partition leader, so it needs brokers that advertise
  an address pods can reach.
- Without that, `__lfs_ops_state` gets no events and the proxy logs
  `lfs ops tracker: ensure topic failed`.
- To reach the cluster from a host, advertise the in-cluster broker address and
  port forward to the LFS proxy, with `spec.lfsProxy.advertisedHost` and
  `advertisedPort` set to the forwarded address. `make lfs-demo` does this.

## Environment variable index

### kafscale-cli

- `KAFSCALE_NAMESPACE`: First default for `restore --namespace`. If unset or blank, `KAFSCALE_S3_NAMESPACE` is used, then `default`. Optional. Used by `restore`.
- `KAFSCALE_S3_NAMESPACE`: Second default for `restore --namespace` when `KAFSCALE_NAMESPACE` is unset or blank. Optional. Used by `restore`.
- `KAFSCALE_S3_BUCKET`: S3 bucket. Required for `restore` and `prune` unless `KAFSCALE_USE_MEMORY_S3` is set. No default; the CLI fails with `s3 bucket required` if it is unset.
- `KAFSCALE_S3_REGION`: S3 region. Required for `restore` and `prune` unless `KAFSCALE_USE_MEMORY_S3` is set. No default; the CLI fails with `s3 region required` if it is unset.
- `KAFSCALE_S3_ENDPOINT`: Optional S3 endpoint override for `restore` and `prune`. No default.
- `KAFSCALE_S3_PATH_STYLE`: Optional path-style addressing for `restore` and `prune`. Defaults to true when `KAFSCALE_S3_ENDPOINT` is set, and to false when it is not.
- `KAFSCALE_S3_KMS_ARN`: Optional KMS key ARN for SSE-KMS. Used by `restore` and `prune`. No default.
- `KAFSCALE_S3_CONCURRENCY`: Optional cap on concurrent S3 operations for `restore` and `prune` (default `16`).
- `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY`: Optional static S3 credentials for `restore` and `prune`. They are used only when both are set. If either is unset, the AWS SDK default credential chain is used: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`; the shared config and credentials files with `AWS_PROFILE`; web identity with `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` (EKS IRSA); and ECS or EC2 instance role credentials.
- `KAFSCALE_S3_SESSION_TOKEN`: Optional session token sent with those static credentials. Used by `restore` and `prune`. No default.
- `KAFSCALE_ETCD_ENDPOINTS`: Comma-separated etcd endpoints. Required for `restore` and `prune`. No default; the CLI fails with `etcd endpoints required` if it is unset.
- `KAFSCALE_ETCD_USERNAME` and `KAFSCALE_ETCD_PASSWORD`: Optional etcd basic auth for `restore` and `prune`.
- `KAFSCALE_SCHEMA_REGISTRY_URL`: Optional target registry URL for `schema import` (default `http://localhost:8081`), used when `--to` is omitted.
- `KAFSCALE_SCHEMA_IMPORT_AUTH` and `KAFSCALE_SCHEMA_REGISTRY_AUTH`: Optional source and target registry credentials for `schema import`, used when `--auth` or `--to-auth` is empty. This is the recommended way to pass credentials.
- `KAFSCALE_USE_MEMORY_S3`: Optional in-memory S3 client for `restore` and `prune` (default false). Dev and test only.

### MCP server

- `KAFSCALE_MCP_HTTP_ADDR`: Listen address (default `:8090`). The Helm chart sets `:8090`.
- `KAFSCALE_MCP_ETCD_ENDPOINTS`: Comma-separated etcd endpoints. Optional. If unset, the metadata tools return `metadata store not configured`. Helm value `mcp.etcdEndpoints`.
- `KAFSCALE_MCP_ETCD_USERNAME` and `KAFSCALE_MCP_ETCD_PASSWORD`: Optional etcd basic auth. Helm values `mcp.etcd.existingSecret` (keys `ETCD_USERNAME` and `ETCD_PASSWORD`), or `mcp.etcd.username` and `mcp.etcd.password`.
- `KAFSCALE_MCP_BROKER_METRICS_URL`: Broker Prometheus metrics URL. Optional. If unset, `cluster_metrics` returns `metrics provider not configured`. With `KAFSCALE_MCP_ETCD_ENDPOINTS` set, every broker registered in etcd is read with this URL's scheme, port and path and the results are summed, and the URL itself is the fallback when no broker answers. Without etcd only this URL is read. Helm value `mcp.metrics.brokerMetricsURL`.
- `KAFSCALE_MCP_SESSION_TIMEOUT`: Idle session timeout as a Go duration such as `10m`. Optional. If unset, idle sessions are never closed. An invalid value stops the server at startup. Helm value `mcp.sessionTimeout`.
- `KAFSCALE_MCP_AUTH_TOKEN`: Static bearer token for `/mcp`. Optional. If unset, the server is unauthenticated and logs a warning. Helm value `mcp.auth.token`.

### Operator

- `KAFSCALE_OPERATOR_ETCD_ENDPOINTS`: Comma-separated etcd endpoints to use instead of managed etcd.
- `KAFSCALE_OPERATOR_ETCD_IMAGE`: Managed etcd image (default `gcr.io/etcd-development/etcd:v3.6.14`, pinned by digest).
- `KAFSCALE_OPERATOR_ETCD_REPLICAS`: Managed etcd replica count (default `3`).
- `KAFSCALE_OPERATOR_ETCD_STORAGE_SIZE`: PVC size for managed etcd (default `10Gi`).
- `KAFSCALE_OPERATOR_ETCD_STORAGE_CLASS`: StorageClass for managed etcd PVCs.
- `KAFSCALE_OPERATOR_ETCD_STORAGE_MEMORY`: Use in-memory `emptyDir` for etcd data (`1` to enable, test/dev only).
- `KAFSCALE_OPERATOR_ETCD_AUTO_COMPACTION_MODE`: Managed etcd compaction mode (`periodic` or `revision`; default `periodic`).
- `KAFSCALE_OPERATOR_ETCD_AUTO_COMPACTION_RETENTION`: Compaction retention (`5m` for periodic mode; default `5m`).
- `KAFSCALE_OPERATOR_ETCD_QUOTA_BACKEND_BYTES`: Managed etcd backend quota in bytes (default `4294967296`, 4 GiB).
- `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_SCHEDULE`: Baseline maintenance CronJob schedule (default `0 */6 * * *`).
- `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_CHECK_SCHEDULE`: Reactive size-check CronJob schedule (default `*/15 * * * *`).
- `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_ENABLED`: Enable maintenance CronJobs (`0`/`false` to disable; enabled by default).
- `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_SIZE_THRESHOLD_PCT`: Backend size threshold percent of quota before reactive maintenance runs (default `75`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET`: Override snapshot bucket (default: `kafscale-etcd-<namespace>-<cluster>`, separate from broker segment storage).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PREFIX`: Snapshot prefix (default `etcd-snapshots`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SCHEDULE`: Cron schedule for snapshots (default `0 * * * *`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_ETCDCTL_IMAGE`: Etcdctl image for snapshots, restore and maintenance jobs (code default `ghcr.io/kafscale/kafscale-etcd-tools:dev`; the Helm chart sets the chart appVersion tag).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_IMAGE`: AWS CLI image for uploads (default `amazon/aws-cli:2.15.0`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT`: S3 endpoint override (MinIO/custom).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_STALE_AFTER_SEC`: Staleness threshold seconds (default `7200`).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET`: Auto-create the snapshot bucket (`1` to enable).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PROTECT_BUCKET`: Enable versioning + public access block (`1` to enable).
- `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SKIP_PREFLIGHT`: Skip the S3 write preflight (`1` to enable).
- `KAFSCALE_OPERATOR_ETCD_DEFRAG_SCHEDULE`: Deprecated alias for `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_SCHEDULE`. Used only when the new variable is unset.
- `KAFSCALE_OPERATOR_ETCD_DEFRAG_ENABLED`: Deprecated alias for `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_ENABLED`. Setting either one to `0`/`false`/`no`/`off` disables maintenance.
- `KAFSCALE_OPERATOR_ETCD_SILENCE_LOGS`: Suppress etcd client logs from the operator snapshot publisher (`true` to enable).
- `KAFSCALE_OPERATOR_LEADER_KEY`: Override the operator leader election ID (default `kafscale-operator`).
- `KAFSCALE_S3_NAMESPACE`: Prefix used for broker S3 object keys (defaults to the cluster namespace).
- `KAFSCALE_SEGMENT_BYTES`: Broker segment flush threshold in bytes (default `4194304`).
- `KAFSCALE_FLUSH_INTERVAL_MS`: Broker flush interval in milliseconds (default `500`).

### Broker

- `KAFSCALE_BROKER_ADDR`: Kafka listener address (host:port).
- `KAFSCALE_RESPONSE_WRITE_TIMEOUT_MS`: How long a write of a Kafka response may wait for a slow client on the broker and proxy (default `30000` milliseconds; nonpositive uses the default).
  - The response is written in 256 KiB pieces and the idle timeout restarts before each piece.
  - The complete frame has a five-minute deadline, including clients that keep reading slowly.
  - A piece only waits when the socket send buffer is full, and the kernel lets it continue once a large part of that buffer has drained, roughly a third to a half of it.
  - So a client must read that much of the send buffer within each timeout, which can be several MB on a fast link, not 256 KiB.
  - A large response to a client that keeps reading at that pace can continue until the overall five-minute limit.
  - On broker pods, configure `KafscaleCluster.spec.config.responseWriteTimeoutMs` with a positive value; for the proxy chart, set `proxy.responseWriteTimeoutMs`.
  - The proxy logs a warning and uses the default when an integer setting is not a number.
  - A write timeout closes the socket and drains complete requests already read, preserving acks=0 produces.
- `KAFSCALE_BROKER_HOST`: Advertised host (used with `KAFSCALE_BROKER_PORT`).
- `KAFSCALE_BROKER_PORT`: Advertised port. The broker binary defaults to `19092` when run directly. The operator always sets it from the cluster spec (`9092` by default), and a port in `KAFSCALE_BROKER_ADDR` takes precedence.
- `KAFSCALE_BROKER_ID`: Broker node ID.
- `KAFSCALE_METRICS_ADDR`: Metrics listen address.
- `KAFSCALE_CONTROL_ADDR`: Control-plane listen address.
- `KAFSCALE_ETCD_ENDPOINTS`: Etcd endpoints for metadata/offsets.
- `KAFSCALE_ETCD_USERNAME`, `KAFSCALE_ETCD_PASSWORD`: Etcd basic auth.
- `KAFSCALE_S3_BUCKET`: S3 bucket for segments/snapshots.
- `KAFSCALE_S3_REGION`: S3 region.
- `KAFSCALE_S3_ENDPOINT`: S3 endpoint override.
- `KAFSCALE_S3_READ_BUCKET`: Optional read replica bucket (CRR/MRAP).
- `KAFSCALE_S3_READ_REGION`: Optional read replica region.
- `KAFSCALE_S3_READ_ENDPOINT`: Optional read replica endpoint override.
- `KAFSCALE_S3_PATH_STYLE`: Force path-style addressing (`true/false`).
- `KAFSCALE_S3_KMS_ARN`: KMS key ARN for SSE-KMS.
- `KAFSCALE_S3_ACCESS_KEY`, `KAFSCALE_S3_SECRET_KEY`, `KAFSCALE_S3_SESSION_TOKEN`: S3 credentials.
- `KAFSCALE_S3_CONCURRENCY`: Broker-wide cap on concurrent S3 operations (default `64`, `0` to disable). Lower for slower S3-compatible backends. The `kafscale-cli` tool reads the same variable with a default of `16`.

Read replica example (multi-region reads):

```bash
export KAFSCALE_S3_BUCKET=prod-segments
export KAFSCALE_S3_REGION=us-east-1
export KAFSCALE_S3_READ_BUCKET=prod-segments-replica
export KAFSCALE_S3_READ_REGION=eu-west-1
```
- `KAFSCALE_CACHE_BYTES`: Broker cache size in bytes. `KAFSCALE_CACHE_SIZE` is accepted as a legacy alias when `KAFSCALE_CACHE_BYTES` is unset or not positive.
- `KAFSCALE_READAHEAD_SEGMENTS`: Segment readahead count.
- `KAFSCALE_AUTO_CREATE_TOPICS`: Auto-create topics (`true/false`).
- `KAFSCALE_AUTO_CREATE_PARTITIONS`: Partition count for auto-created topics (default `1`). Produce requests cannot increase this count by naming a higher partition. An out-of-range partition on a missing topic is rejected without creating the topic. Existing topics retain their own partition counts.
- `KAFSCALE_USE_MEMORY_S3`: Use in-memory S3 client (dev only).
- `KAFSCALE_OFFSETS_RETENTION_MINUTES`: Retention for Empty consumer groups and their committed offsets (default `10080`, seven days). Zero or negative disables automatic expiry.
  - In Kubernetes, set `KafscaleCluster.spec.config.offsetsRetentionMinutes` to zero or more. Zero disables expiry, and omission uses the broker default.
  - Retention is checked once per minute under the group coordination lease.
  - Groups are listed for the check from the group index, with a full scan of the group and offset keys once an hour. See [Consumer group keys in etcd](#consumer-group-keys-in-etcd).
- `KAFSCALE_PRODUCE_SYNC_FLUSH`: Flush to S3 on Produce when `acks != 0` (default `true`).
- `KAFSCALE_PARTITION_REBALANCE_INTERVAL_MS`: How often a broker hands partitions it holds to their preferred leader (default `10000`; `0` or less turns it off). Only runs with `KAFSCALE_PRODUCE_SYNC_FLUSH=true`. See [Scaling brokers](#scaling-brokers).
- `KAFSCALE_LOG_LEVEL`: Log level (`debug`, `info`, `warn`, `error`).
- `KAFSCALE_TRACE_KAFKA`: Enable protocol tracing (`true/false`).
- `KAFSCALE_THROUGHPUT_WINDOW_SEC`: Throughput window size seconds.
- `KAFSCALE_S3_HEALTH_WINDOW_SEC`: S3 health sampling window seconds.
- `KAFSCALE_S3_LATENCY_WARN_MS`, `KAFSCALE_S3_LATENCY_CRIT_MS`: Latency thresholds.
- `KAFSCALE_S3_ERROR_RATE_WARN`, `KAFSCALE_S3_ERROR_RATE_CRIT`: Error-rate thresholds.
- `KAFSCALE_STARTUP_TIMEOUT_SEC`: Broker startup timeout.
- `KAFSCALE_LAG_SAMPLE_INTERVAL_SEC`: Consumer lag sampling interval in seconds (default `5`).
- `KAFSCALE_ENV`: Set to `prod` to log a warning whenever a broker setting falls back to its default because the variable is unset or invalid.

### Produce flush policy (cost vs durability)

By default, the broker flushes to S3 for every Produce request with `acks != 0`
to minimize the loss window after a broker crash. You can relax this for lower
S3 write costs by disabling sync flush and relying on the background flush
interval/segment size.

Durability-optimized (default):
- `KAFSCALE_PRODUCE_SYNC_FLUSH=true`
- `KAFSCALE_FLUSH_INTERVAL_MS=500`
- `KAFSCALE_SEGMENT_BYTES=4194304`

Cost-optimized (accepts a larger loss window after crash):
- `KAFSCALE_PRODUCE_SYNC_FLUSH=false`
- `KAFSCALE_FLUSH_INTERVAL_MS=2000` (or higher)
- `KAFSCALE_SEGMENT_BYTES=33554432` (or higher)

### Proxy

- `KAFSCALE_PROXY_ADDR`: Proxy listen address (host:port).
- `KAFSCALE_PROXY_HEALTH_ADDR`: Health/readiness listen address for `/readyz` and `/livez`. The proxy binary has no default: if unset, the health server does not start. The Helm chart sets `:9094` when `proxy.health.enabled` is true.
- `KAFSCALE_PROXY_ADVERTISED_HOST`: Address Kafka clients should connect to.
- `KAFSCALE_PROXY_ADVERTISED_PORT`: Advertised port (default `9092`).
- `KAFSCALE_PROXY_ETCD_ENDPOINTS`: Etcd endpoints for metadata snapshots.
- `KAFSCALE_PROXY_ETCD_USERNAME`, `KAFSCALE_PROXY_ETCD_PASSWORD`: Etcd auth for proxy.
- `KAFSCALE_PROXY_BACKENDS`: Optional comma-separated broker list (`host:port`) for backend routing.
- `KAFSCALE_PROXY_BACKEND_CACHE_TTL_SEC`: How long the proxy keeps serving from its last etcd read when etcd cannot be read (default `60` seconds). Past this time the proxy goes NotReady and stops routing. See [Proxy behavior when etcd is down](#proxy-behavior-when-etcd-is-down). The operator sets it from `spec.lfsProxy.backendCacheTTLSeconds`. The Helm chart proxy sets it from `proxy.backendCacheTTLSeconds` (default `60`).
- `KAFSCALE_PROXY_BACKEND_BACKOFF_MS`: Backoff between backend connection retries (default `500`).
- `KAFSCALE_PROXY_BACKEND_RETRIES`: Backend connection retry count (default `6`).
- `KAFSCALE_PROXY_READYZ_FETCH_PROBE`: Gate `/readyz` on a real Fetch round-trip (default `true`). See [Proxy readiness](#proxy-readiness).
- `KAFSCALE_PROXY_LFS_ENABLED`: Enable the LFS module on the unified proxy (default `false`). See [LFS proxy](#lfs-proxy) for its settings.

### Schema registry

See [Schema Registry](schema-registry.md) for what the registry does.

- `KAFSCALE_SCHEMA_REGISTRY_ADDR`: HTTP listen address (default `:8081`).
- `KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS`: Comma-separated etcd endpoints. Required.
- `KAFSCALE_SCHEMA_REGISTRY_ETCD_USERNAME`, `KAFSCALE_SCHEMA_REGISTRY_ETCD_PASSWORD`: Etcd auth for the registry.
- `KAFSCALE_SCHEMA_REGISTRY_REQUEST_TIMEOUT`: Time limit for one request as a Go duration (default `10s`). A write that is still retrying when it runs out fails with error code 50002.
- `KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_DRAIN`: How long the registry keeps serving after SIGTERM or SIGINT, with `/readyz` failing, before it stops accepting connections, as a Go duration (default `5s`, `0` or `0s` turns it off). See [Shutdown](schema-registry.md#shutdown).
- `KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_TIMEOUT`: How long in-flight requests get to finish after the drain period, as a Go duration (default `10s`). Requests still running then are cut off and the registry exits with an error.
- `KAFSCALE_SCHEMA_REGISTRY_INLINE_LIMIT_BYTES`: Schema text above this size is stored in S3 instead of etcd (default `65536`).
- `KAFSCALE_S3_BUCKET`, `KAFSCALE_S3_REGION`, `KAFSCALE_S3_ENDPOINT`, `KAFSCALE_S3_PATH_STYLE`, `KAFSCALE_S3_ACCESS_KEY`, `KAFSCALE_S3_SECRET_KEY`, `KAFSCALE_S3_SESSION_TOKEN`, `KAFSCALE_S3_KMS_ARN`, `KAFSCALE_S3_NAMESPACE`: Same meaning as for the broker.
  - Used for schema text above the inline limit and for the schema id records described in [schema-registry.md](schema-registry.md#storage).
  - Without a bucket, schemas above the inline limit are rejected.
  - With a bucket, the backend must support `PutObject` with `If-None-Match: *`, otherwise the registry exits at startup.
  - When `KAFSCALE_S3_PATH_STYLE` is not set, path style addressing is used when `KAFSCALE_S3_ENDPOINT` is set.
  - As in the broker, an unset or blank `KAFSCALE_S3_NAMESPACE` means `default`.
  - With a bucket set and the namespace `default`, the registry logs a warning at startup, because registries on different etcd clusters that use the same bucket and namespace share one schema id space.
  - Set it per cluster, for example to the namespace the operator gives the brokers.
  - The registry needs `s3:PutObject`, `s3:GetObject` and `s3:DeleteObject` on `<namespace>/@schemas/`.
- After restoring etcd from a backup, restart the registry pods so that no instance serves cached schemas from before the restore.

### Console

- `KAFSCALE_CONSOLE_HTTP_ADDR`: Console listen address.
- `KAFSCALE_CONSOLE_ETCD_ENDPOINTS`: Etcd endpoints for cluster metadata. The console reads status from etcd and writes to it when it creates or deletes a topic.
- `KAFSCALE_CONSOLE_ETCD_USERNAME`, `KAFSCALE_CONSOLE_ETCD_PASSWORD`: Etcd auth for console.
- `KAFSCALE_S3_NAMESPACE`: Same meaning as for the broker. Set it to the `KafscaleCluster` namespace, which the operator gives the brokers. The console writes it into the deleted topic record that `kafscale-cli prune` uses. When it is unset or blank, the console refuses topic deletes with `503`. The Helm chart sets it from `console.s3Namespace` and prints a warning in the install notes when `console.etcdEndpoints` is set and `console.s3Namespace` is empty.
- `KAFSCALE_CONSOLE_BROKER_METRICS_URL`: Broker Prometheus endpoint.
- `KAFSCALE_UI_USERNAME`, `KAFSCALE_UI_PASSWORD`: Console login credentials. The Helm chart sets them from the Secret named by `console.auth.existingSecret` (keys `username` and `password`), or from `console.auth.username` and `console.auth.password`.
- `KAFSCALE_CONSOLE_LFS_ENABLED`: Enable the LFS views and API (`true` to enable; any other value disables them).
- `KAFSCALE_CONSOLE_KAFKA_BROKERS`: Comma-separated brokers used to consume the LFS tracker topic.
- `KAFSCALE_LFS_TRACKER_TOPIC`: Tracker topic to read (default `__lfs_ops_state`).
- `KAFSCALE_CONSOLE_LFS_S3_BUCKET`, `KAFSCALE_CONSOLE_LFS_S3_REGION`, `KAFSCALE_CONSOLE_LFS_S3_ENDPOINT`: S3 location for browsing LFS objects. Path-style addressing is used when an endpoint is set.
- `KAFSCALE_CONSOLE_LFS_S3_ACCESS_KEY`, `KAFSCALE_CONSOLE_LFS_S3_SECRET_KEY`: S3 credentials for the console.
- `KAFSCALE_CONSOLE_LFS_S3_PRESIGN_TTL`: Presigned URL lifetime in seconds (default `300`).

These topic endpoints require a console session cookie, not HTTP basic auth. A request with basic auth, or with no session, returns `401` `unauthorized`. The Helm release name `kafscale` creates the console Service `kafscale-console` on port 80. In a second terminal, forward that Service, then sign in with `POST /ui/api/auth/login`:

```bash
kubectl -n kafscale port-forward svc/kafscale-console 8080:80
```

In Bash, enter the configured credentials when prompted. This example requires `jq`; the password is read silently and the JSON body is sent through stdin.

```bash
IFS= read -rp 'Console username: ' CONSOLE_USERNAME
IFS= read -rsp 'Console password: ' CONSOLE_PASSWORD
printf '\n'
printf '%s' "$CONSOLE_PASSWORD" | jq -cRs --arg username "$CONSOLE_USERNAME" '{username:$username,password:.}' | \
  curl -c cookies.txt -X POST http://127.0.0.1:8080/ui/api/auth/login \
    -H 'Content-Type: application/json' --data-binary @-
unset CONSOLE_PASSWORD CONSOLE_USERNAME
```

Use the same credentials as `KAFSCALE_UI_USERNAME` and `KAFSCALE_UI_PASSWORD`.
A successful login sets an HttpOnly cookie named `kafscale_ui_session` (12 hour
lifetime) and returns `{"authenticated":true}`. Pass that cookie on later
requests, for example `curl -b cookies.txt ...`. `POST /ui/api/auth/logout`
clears it. Login is `POST` only.

If `KAFSCALE_UI_USERNAME` or `KAFSCALE_UI_PASSWORD` is unset, login and the
topic endpoints return `503`. A wrong password returns `401` with `invalid credentials`. More than 20 logins per minute from one IP return `429`.
Sessions are in memory on each console pod, so if `console.replicaCount` is
above 1 a cookie issued by one pod can get `401` from another.

The console serves these authenticated topic endpoints when `KAFSCALE_CONSOLE_ETCD_ENDPOINTS` is set. They use the same etcd metadata path as the broker Kafka `CreateTopics` and `DeleteTopics` APIs.

- `POST /ui/api/status/topics` creates a topic. The JSON body is `{"name":"<topic>","partitions":<count>,"replication_factor":<count>}`. `partitions` must be between 1 and 10000. `replication_factor` is optional and defaults to 1. It returns `201` with `{"name":"<topic>","partitions":<count>,"state":"ready"}`.
- `DELETE /ui/api/status/topics/<topic>` deletes a topic the same way the broker does. It needs `KAFSCALE_S3_NAMESPACE`. It returns `200` with `{"name":"<topic>"}`.
- Errors: `400` for a malformed body or data after the JSON object, an invalid topic name, a partition count outside 1 to 10000 or a replication factor above the broker count, `401` for a request without a valid console session, `404` for an unknown topic, `405` for a method other than `POST` on `/ui/api/status/topics` or `DELETE` on `/ui/api/status/topics/<topic>`, `409` for a topic that already exists or changed during the delete, and `503` when console login is not configured, no etcd endpoints are configured, etcd is unavailable or times out, or a delete runs without `KAFSCALE_S3_NAMESPACE`. Other failures return `500`.

When LFS is enabled, the console serves these authenticated endpoints: `/ui/api/lfs/status`, `/ui/api/lfs/objects`, `/ui/api/lfs/topics`, `/ui/api/lfs/topics/{topic}`, `/ui/api/lfs/events`, `/ui/api/lfs/orphans`, `/ui/api/lfs/s3/browse` and `/ui/api/lfs/s3/presign`.

### LFS proxy

These settings apply when `KAFSCALE_PROXY_LFS_ENABLED=true`. Defaults come from `cmd/proxy/lfs.go` and `cmd/proxy/lfs_tracker.go`.

S3 storage:

- `KAFSCALE_LFS_PROXY_S3_BUCKET`: Required. The proxy refuses known example bucket names.
- `KAFSCALE_LFS_PROXY_S3_REGION`: Required.
- `KAFSCALE_LFS_PROXY_S3_ENDPOINT`: Endpoint override (MinIO or other S3-compatible stores).
- `KAFSCALE_LFS_PROXY_S3_PUBLIC_ENDPOINT`: Endpoint used in URLs handed to clients.
- `KAFSCALE_LFS_PROXY_S3_ACCESS_KEY`, `KAFSCALE_LFS_PROXY_S3_SECRET_KEY`, `KAFSCALE_LFS_PROXY_S3_SESSION_TOKEN`: S3 credentials.
- `KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE`: Path-style addressing (default `true` when an endpoint is set, otherwise `false`).
- `KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET`: Create the bucket at startup (default `false`).
- `KAFSCALE_LFS_PROXY_S3_HEALTH_INTERVAL_SEC`: S3 health check interval (default `30`).
- `KAFSCALE_S3_NAMESPACE`: Object key prefix (default `default` for the LFS module).

Object keys:

- New LFS objects are written to `<namespace>/@lfs/<topic>/<yyyy>/<mm>/<dd>/obj-<uuid>`.
  - After the Nth delete of a topic, the topic part is `<topic>@<N>`, the same name its segments use, so LFS objects of a deleted incarnation stay apart from the current one.
  - When etcd is configured, the proxy reads the generation from etcd before it uploads anything, once per topic per request, so etcd must be reachable for LFS produce.
  - The lookup times out after `KAFSCALE_LFS_PROXY_METADATA_TIMEOUT_MS` (default `3000`).
  - If it fails or times out, `/lfs/produce` and `/lfs/uploads` return 503 `metadata_unavailable` and a Kafka produce with LFS records fails, and nothing is uploaded. Without etcd the proxy uses generation 0.
  - After the upload finishes and right before the produce, the proxy looks up the generation again with the same timeout.
  - If the topic was deleted and recreated in the meantime, or the lookup fails, the proxy deletes the uploaded objects, does not produce, and returns the same 503 `metadata_unavailable` or produce error.
  - Objects it cannot delete are reported as orphans.
  - A delete and recreate in the short gap between this second lookup and the produce is not caught, so in that rare case a record can still point at an object under the previous generation's prefix.
- `@` is not allowed in Kafka topic names, so `@lfs` never clashes with a topic prefix.
- Proxies before this change wrote to `<namespace>/<topic>/lfs/<yyyy>/<mm>/<dd>/obj-<uuid>`. Those objects are not moved. The envelope stores the full key, so consumers, SDKs and `/lfs/download` still read them. `/lfs/download` only accepts keys that start with `<namespace>/@lfs/` or have the old `<namespace>/<topic>/lfs/` shape.

Blobs and checksums:

- `KAFSCALE_LFS_PROXY_MAX_BLOB_SIZE`: Maximum blob size in bytes (default `5368709120`, 5 GiB).
- `KAFSCALE_LFS_PROXY_CHUNK_SIZE`: Multipart chunk size in bytes (default `5242880`, 5 MiB).
- `KAFSCALE_LFS_PROXY_CHECKSUM_ALGO`: Checksum algorithm (default `sha256`).
- `KAFSCALE_LFS_PROXY_ID`: Proxy identifier recorded in tracker events (no default).
- `KAFSCALE_LFS_PROXY_TOPIC_MAX_LENGTH`: Maximum topic name length (default `249`).
- `KAFSCALE_LFS_PROXY_METADATA_TIMEOUT_MS`: Timeout for the etcd topic generation lookup before an LFS upload (default `3000`). In the Helm chart, set `proxy.lfsMetadataTimeoutMs`.

HTTP API:

- `KAFSCALE_LFS_PROXY_HTTP_ADDR`: Listen address for the LFS HTTP API. No default: the API is off unless set.
- `KAFSCALE_LFS_PROXY_METRICS_ADDR`: Listen address for LFS metrics (`/metrics`). No default.
- `KAFSCALE_LFS_PROXY_HTTP_API_KEY`: API key required on HTTP requests (unset means no key check).
- `KAFSCALE_LFS_PROXY_HTTP_READ_TIMEOUT_SEC` (default `30`), `KAFSCALE_LFS_PROXY_HTTP_WRITE_TIMEOUT_SEC` (default `300`), `KAFSCALE_LFS_PROXY_HTTP_IDLE_TIMEOUT_SEC` (default `60`), `KAFSCALE_LFS_PROXY_HTTP_HEADER_TIMEOUT_SEC` (default `10`), `KAFSCALE_LFS_PROXY_HTTP_SHUTDOWN_TIMEOUT_SEC` (default `10`).
- `KAFSCALE_LFS_PROXY_HTTP_MAX_HEADER_BYTES`: Default `1048576` (1 MiB).
- `KAFSCALE_LFS_PROXY_UPLOAD_SESSION_TTL_SEC`: Multipart upload session lifetime (default `3600`).
- `KAFSCALE_LFS_PROXY_DOWNLOAD_TTL_SEC`: Maximum download URL lifetime (default `120`).
- `KAFSCALE_LFS_PROXY_PRESIGN_ENABLED`: Enable presigned URLs (default `false`).
- `KAFSCALE_LFS_PROXY_HTTP_TLS_ENABLED`: Serve the HTTP API over TLS (default `false`). Requires `KAFSCALE_LFS_PROXY_HTTP_TLS_CERT_FILE` and `KAFSCALE_LFS_PROXY_HTTP_TLS_KEY_FILE`. Minimum TLS 1.2.
- `KAFSCALE_LFS_PROXY_HTTP_TLS_CLIENT_CA_FILE`: CA for client certificates. Client certificates are verified if given.
- `KAFSCALE_LFS_PROXY_HTTP_TLS_REQUIRE_CLIENT_CERT`: Require client certificates when a client CA is set (default `false`).

HTTP endpoints (served on `KAFSCALE_LFS_PROXY_HTTP_ADDR`):

- `POST /lfs/produce`: Upload a blob and produce its envelope to Kafka.
- `POST /lfs/download`: Get a download for a stored blob.
- `POST /lfs/uploads`: Start a multipart upload session.
- `PUT /lfs/uploads/{id}/parts/{n}`: Upload a part.
- `POST /lfs/uploads/{id}/complete`: Complete the upload.
- `DELETE /lfs/uploads/{id}`: Abort the upload.
- `GET /swagger` and `GET /api/openapi.yaml`: API docs and OpenAPI spec.

Multipart upload cleanup:

Multipart uploads require `KAFSCALE_PROXY_ETCD_ENDPOINTS`, including when
`KAFSCALE_PROXY_BACKENDS` lists static brokers. The operator and Docker Compose
already provide this connection. The proxy uses its existing etcd credentials;
they must allow reads and writes under `/kafscale/lfs-upload-sessions/`,
`/kafscale/lfs-upload-index/`, and `/kafscale/lfs-upload-locks/`, plus lease
operations. Without a session store, multipart initialization returns 503
`session_store_unavailable` before starting an S3 multipart upload.

Authoritative sessions and checksum state live in etcd under
`/kafscale/lfs-upload-sessions/<bucket>/<namespace>/<id>` (bucket and namespace
components are URL-escaped). S3 remains untrusted blob storage. The proxy does
not load the unsigned session JSON used by the earlier pre-release branch;
restart uploads begun by that version after upgrading. Sessions from the earlier
development format without durable part hashes also require a restart; they
return 410 `upload_restart_required` and are aborted rather than resumed.

Part, completion, and abort calls can reach any replica after a restart.

- An etcd mutex serializes requests for each upload, and every session update or delete checks both the record revision and lock ownership.
- A timed-out old request cannot overwrite newer acknowledged progress. Lost lock leases cancel the active request.
- Lock acquisition uses cancellable attempts, and lock-key and lease cleanup share a bounded timeout, including after cancellation.
- Before issuing an S3 part upload, the proxy persists that part's SHA-256 identity in etcd.
- All retries for the same part number must contain exactly the same bytes, including retries after a timeout or a successful acknowledgement.
- Conflicting retry bytes return 409 `part_conflict` without reaching S3.
- This prevents an old in-flight request from replacing different, acknowledged bytes.
- Failed S3 attempts do not advance the aggregate checksum.
- Completion requires every uploaded part exactly once in order.
- The expected checksum is verified before S3 finalization, and a mismatch aborts the multipart upload.
- If S3 already completed the upload but its response was lost, a retry checks the unique object key and declared size with `s3:GetObject` and resumes publishing the envelope. The expected checksum still comes from etcd.
- Durable publication and discard markers fence cleanup across ownership changes.
- Cleanup does not delete a completed blob once a Kafka request may have referenced it, including after an ambiguous acknowledgement; explicit abort can still remove the session record.
- As with ordinary produces, retrying after an ambiguous Kafka acknowledgement can produce duplicate envelopes.

Send parts sequentially as before; an out-of-order part
returns 409. State-store failures return 503 `session_store_unavailable` and a
rejected stale update returns 409 `upload_conflict`; retry using the same upload
ID and part number. Session records are removed on completion or abort.

A part or complete call on an expired session aborts the multipart upload and returns 410 `upload_expired`.

- Every replica with the HTTP API sweeps every half session TTL (`KAFSCALE_LFS_PROXY_UPLOAD_SESSION_TTL_SEC`), bounded to between 30 seconds and 10 minutes.
- Expiry uses each session's persisted `ExpiresAt`, so a replica with a shorter configured TTL cannot abort an unexpired session during a rollout.
- Session records and an index by S3 key/upload ID are written and deleted atomically.
- The orphan sweep checks this trusted index before aborting uploads under `<namespace>/@lfs/` older than the configured TTL plus one hour.
- It skips tracked sessions, and performs no orphan abort if etcd cannot confirm ownership.
- This also reclaims uploads left by older versions that stored sessions only in pod memory.

The orphan sweep needs `s3:ListBucketMultipartUploads` and `s3:AbortMultipartUpload`.

- If listing is denied, the proxy logs a warning and uploads keep working.
- As a second safety net, configure an S3 bucket lifecycle rule with `AbortIncompleteMultipartUpload` for the LFS object prefix, or the whole bucket if it is dedicated to LFS.
- For example, set `DaysAfterInitiation` to `7`, choosing an interval longer than the longest intended upload. This action leaves completed objects intact.
- Merge the rule into the bucket's existing lifecycle configuration.
- For S3-compatible storage, verify that the provider supports and applies this action.
- See the [Amazon S3 lifecycle configuration guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html).

Kafka backends:

- `KAFSCALE_LFS_PROXY_BACKENDS`: Comma-separated broker list used by the LFS module and tracker. When it is unset, the LFS HTTP API uses the brokers registered in etcd, as the Kafka listener does, and the tracker is off.
- `KAFSCALE_LFS_PROXY_DIAL_TIMEOUT_MS` (default `5000`), `KAFSCALE_LFS_PROXY_BACKEND_RETRIES` (default `6`, minimum `1`), `KAFSCALE_LFS_PROXY_BACKEND_BACKOFF_MS` (default `500`).
  - The dial timeout also bounds the LFS HTTP Kafka request write.
  - After the write, the response gets a separate budget of 15 seconds (the broker Produce timeout) plus the dial timeout, 20 seconds by default.
  - An earlier HTTP request deadline or cancellation stops the exchange.
- `KAFSCALE_LFS_PROXY_BACKEND_SASL_MECHANISM`, `KAFSCALE_LFS_PROXY_BACKEND_SASL_USERNAME`, `KAFSCALE_LFS_PROXY_BACKEND_SASL_PASSWORD`: SASL for backend connections.
- `KAFSCALE_LFS_PROXY_BACKEND_TLS_ENABLED` (default `false`), `KAFSCALE_LFS_PROXY_BACKEND_TLS_CA_FILE`, `KAFSCALE_LFS_PROXY_BACKEND_TLS_CERT_FILE`, `KAFSCALE_LFS_PROXY_BACKEND_TLS_KEY_FILE` (cert and key must be set together), `KAFSCALE_LFS_PROXY_BACKEND_TLS_SERVER_NAME`, `KAFSCALE_LFS_PROXY_BACKEND_TLS_INSECURE_SKIP_VERIFY` (default `false`).

LFS tracker (writes LFS operation events to Kafka):

- `KAFSCALE_LFS_TRACKER_ENABLED` (default `true`), `KAFSCALE_LFS_TRACKER_TOPIC` (default `__lfs_ops_state`), `KAFSCALE_LFS_TRACKER_BATCH_SIZE` (default `100`), `KAFSCALE_LFS_TRACKER_FLUSH_MS` (default `100`), `KAFSCALE_LFS_TRACKER_ENSURE_TOPIC` (default `true`), `KAFSCALE_LFS_TRACKER_PARTITIONS` (default `3`), `KAFSCALE_LFS_TRACKER_REPLICATION_FACTOR` (default `1`).

Operator note: for an LFS proxy deployment (`spec.lfsProxy`) the operator sets these variables (pkg/operator/lfs_proxy_resources.go):

- `KAFSCALE_LFS_PROXY_ADDR`, `KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS`, `KAFSCALE_LFS_PROXY_ADVERTISED_HOST`, `KAFSCALE_LFS_PROXY_ADVERTISED_PORT`, `KAFSCALE_LFS_PROXY_BACKEND_CACHE_TTL_SEC`, `KAFSCALE_LFS_PROXY_BACKENDS` and `KAFSCALE_LFS_PROXY_HEALTH_ADDR`.
- The same values under the names that the proxy binary reads, such as `KAFSCALE_PROXY_ADDR` and `KAFSCALE_PROXY_ETCD_ENDPOINTS`.
- `KAFSCALE_PROXY_LFS_ENABLED=true`.

### Iceberg processor LFS overrides

The Iceberg processor addon has its own LFS environment overrides. See `addons/processors/iceberg-processor/README.md`.

### idoc-explode

`idoc-explode` (cmd/idoc-explode) reads SAP IDoc XML or JSONL envelopes and writes one file per topic. Flags: `-input` (file path; empty reads stdin) and `-out` (output directory).

- `KAFSCALE_IDOC_OUTPUT_DIR`: Default for `-out` (default `idoc-output`).
- `KAFSCALE_IDOC_ITEM_SEGMENTS` (default `E1EDP01,E1EDP19`), `KAFSCALE_IDOC_PARTNER_SEGMENTS` (default `E1EDKA1`), `KAFSCALE_IDOC_STATUS_SEGMENTS` (default `E1STATS`), `KAFSCALE_IDOC_DATE_SEGMENTS` (default `E1EDK03`).
- `KAFSCALE_IDOC_TOPIC_HEADER` (default `idoc-headers`), `KAFSCALE_IDOC_TOPIC_SEGMENTS` (default `idoc-segments`), `KAFSCALE_IDOC_TOPIC_ITEMS` (default `idoc-items`), `KAFSCALE_IDOC_TOPIC_PARTNERS` (default `idoc-partners`), `KAFSCALE_IDOC_TOPIC_STATUS` (default `idoc-status`), `KAFSCALE_IDOC_TOPIC_DATES` (default `idoc-dates`).
- `KAFSCALE_IDOC_MAX_BLOB_SIZE`: Maximum LFS blob size to resolve (default `0`, no limit).
- `KAFSCALE_IDOC_VALIDATE_CHECKSUM`: Validate LFS checksums (default `true`).
- LFS blobs are resolved from S3 using `KAFSCALE_LFS_PROXY_S3_BUCKET`, `KAFSCALE_LFS_PROXY_S3_REGION`, `KAFSCALE_LFS_PROXY_S3_ENDPOINT`, `KAFSCALE_LFS_PROXY_S3_ACCESS_KEY`, `KAFSCALE_LFS_PROXY_S3_SECRET_KEY`, `KAFSCALE_LFS_PROXY_S3_SESSION_TOKEN` and `KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE`. Without bucket and region, blobs are not fetched from S3.

### Topic auto-creation through the proxy

In plain mode, a Metadata request for a missing named topic is forwarded to a
broker before the proxy rewrites the advertised addresses. The broker decides
whether to create the topic using `KAFSCALE_AUTO_CREATE_TOPICS` and its configured
partition count. This preserves direct-broker behavior for older request versions
and clients that leave `AllowAutoTopicCreation` unset. If broker auto-creation is
disabled, the response contains `UNKNOWN_TOPIC_OR_PARTITION`. Known-topic and
ID-based lookups continue to use the metadata store. Authenticated proxies always
forward Metadata to enforce broker ACLs.

## External Kafka access

By default, brokers advertise the in-cluster service DNS name. That works for
clients running inside Kubernetes. External clients need a reachable address in
Metadata responses.

**Recommended:** deploy the Kafka-aware proxy (`proxy.enabled=true`). When the
proxy is enabled, clients connect to the **proxy Service**, not individual broker
pods. The proxy answers Metadata/FindCoordinator with a single stable endpoint,
then forwards all other Kafka requests to brokers. This keeps clients connected as
broker pods scale or rotate.

**Optional:** expose brokers directly via `KafscaleCluster` `spec.brokers` Service
settings (LoadBalancer or NodePort). Use this only when you intentionally bypass
the proxy (traffic isolation, pinned producers, or legacy integrations). Broker
NodePort does not replace the proxy entrypoint in scaled deployments.

The Helm chart does **not** ship a Kafka Ingress resource. External TLS uses
LoadBalancer annotations or an external TCP gateway (see
[Proxy TLS via LoadBalancer](#proxy-tls-via-loadbalancer)).

### Kafka proxy (recommended)

Recommended settings:
- Run 2+ proxy replicas behind a LoadBalancer service.
- Point the proxy at etcd via `KAFSCALE_PROXY_ETCD_ENDPOINTS` so it can read the cluster snapshot.
- After the proxy forwards CreateTopics, CreatePartitions or DeleteTopics, it reads the snapshot again, so the next topic list shows the change. Only the proxy replica that forwarded the request does this. Other replicas pick up the change from the etcd watch, usually within milliseconds but not instantly, so a topic list through another replica right after a create or delete can briefly be stale.
- Set `KAFSCALE_PROXY_ADVERTISED_HOST`/`KAFSCALE_PROXY_ADVERTISED_PORT` to the public DNS + port clients should use.

Helm values to enable:
- `proxy.enabled=true`
- `proxy.etcdEndpoints[0]=http://<cluster>-etcd-client.<namespace>.svc.cluster.local:2379`
- `proxy.advertisedHost=<public DNS>`

The examples below use the quickstart cluster `demo` in namespace `kafscale`.
Replace `demo-etcd-client` when your `KafscaleCluster` has a different name.

Example (HA proxy + external access):

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set proxy.enabled=true \
  --set proxy.replicaCount=2 \
  --set proxy.service.type=LoadBalancer \
  --set proxy.service.port=9092 \
  --set proxy.advertisedHost=kafka.example.com \
  --set proxy.advertisedPort=9092 \
  --set 'proxy.etcdEndpoints[0]=http://demo-etcd-client.kafscale.svc.cluster.local:2379'
```

For local clusters (kind, minikube), pin the proxy NodePort so host port mappings
stay stable. Set `proxy.service.type=NodePort` and `proxy.service.nodePort` to a
value in `30000–32767`, then set `proxy.advertisedHost` to the node IP (or the
mapped host address). See `deploy/helm/kafscale/README.md` for details.

Example (kind / local dev):

```bash
helm upgrade --install kafscale deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set proxy.enabled=true \
  --set proxy.service.type=NodePort \
  --set proxy.service.nodePort=30092 \
  --set proxy.advertisedHost=127.0.0.1 \
  --set proxy.advertisedPort=30092 \
  --set 'proxy.etcdEndpoints[0]=http://demo-etcd-client.kafscale.svc.cluster.local:2379'
```

Helm chart docs: `deploy/helm/kafscale/README.md`.

### Direct broker exposure (optional)

Broker exposure settings (KafscaleCluster `spec.brokers`):
- `advertisedHost` / `advertisedPort`: Address Kafka clients should connect to.
- `service.type`: `ClusterIP`, `LoadBalancer`, or `NodePort`.
- `service.annotations`: Cloud provider LB annotations.
- `service.loadBalancerIP` / `service.loadBalancerSourceRanges`: Static IP + CIDR allowlist.
- `service.externalTrafficPolicy`: `Cluster` or `Local`.
- `service.kafkaNodePort` / `service.metricsNodePort`: Optional NodePort overrides.

Example (GKE/AWS/Azure load balancer):

```yaml
apiVersion: kafscale.io/v1
kind: KafscaleCluster
metadata:
  name: kafscale
  namespace: kafscale
spec:
  brokers:
    advertisedHost: kafka.example.com
    advertisedPort: 9092
    service:
      type: LoadBalancer
      annotations:
        networking.gke.io/load-balancer-type: "External"
      loadBalancerSourceRanges:
        - 203.0.113.0/24
  s3:
    bucket: kafscale
    region: us-east-1
    credentialsSecretRef: kafscale-s3-credentials
  etcd:
    endpoints: []
```

TLS note: brokers speak plaintext today. If you need TLS for Kafka traffic,
terminate TLS at your load balancer, ingress TCP proxy, or service mesh and
advertise that endpoint in `advertisedHost`/`advertisedPort`. See `docs/security.md`
for the current transport security posture.

Example certificate (cert-manager) for a TCP proxy or load balancer that uses a
Kubernetes TLS secret:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: kafscale-kafka-cert
  namespace: kafscale
spec:
  secretName: kafscale-kafka-tls
  dnsNames:
    - kafka.example.com
  issuerRef:
    name: letsencrypt-prod
    kind: ClusterIssuer
```

## Upgrades & rollbacks

- Use `helm upgrade --install` with the desired image tags. Each restarted broker releases its partition and group leases on shutdown and the other brokers take them over. The BrokerControl `DrainPartitions` RPC is not implemented.
- Helm installs CRDs on the first installation but does not upgrade CRDs from
  `crds/`. Before upgrading the chart or applying a cluster with `spec.security`,
  apply the new schema explicitly:

  ```sh
  kubectl apply --server-side --force-conflicts --field-manager=kafscale-crd-upgrade -f deploy/helm/kafscale/crds/
  kubectl wait --for=condition=Established crd/kafscaleclusters.kafscale.io
  kubectl explain kafscalecluster.spec.security --api-version=kafscale.io/v1
  ```

  Then run `helm upgrade --install` and reapply the cluster manifest with
  `kubectl apply --validate=strict -f cluster.yaml`. Fields submitted against an
  old schema may already have been pruned; upgrading the CRD does not restore
  them. See [Kafka proxy authentication](kafka-authentication.md) for the full
  configuration and rotation procedure.
- Upgrading to a version with the consumer group index needs no manual step. Brokers on the old and new versions can run side by side: a group created or deleted by an old broker is fixed up by the next full scan, at most an hour later, and until then it is only swept late. Rolling back leaves the `/kafscale/consumer-groups/` keys in place, and older brokers ignore them.
- Rollbacks can be performed with `helm rollback kafscale <REVISION>` which restores the previous deployment and service versions.  Brokers are stateless so the recovery window is short.

## S3 cost estimation (example)

Assumptions:

- 100 GB/day ingestion
- 7-day retention
- 4 MB average segment size
- 3 broker pods

Monthly costs (US-East-1):

| Item | Calculation | Cost |
|------|-------------|------|
| Storage | 700 GB x $0.023/GB | $16.10 |
| PUT requests | 25,000/day x 30 x $0.005/1000 | $3.75 |
| GET requests | 100,000/day x 30 x $0.0004/1000 | $1.20 |
| Data transfer (in-region) | Free | $0 |
| **Total S3** | | **~$21/month** |

## Multi-region S3 (CRR) for global reads

Kafscale writes to a primary bucket and can optionally read from a replica bucket in the broker region. With S3 Cross-Region Replication (CRR), objects written to the primary are asynchronously copied to replica buckets in other regions. Brokers attempt reads from their local replica and fall back to the primary if the replica is missing an object (for example, due to CRR lag).

### CRR setup (AWS)

1. **Create buckets in each region**:
   - Primary: `kafscale-prod-us-east-1`
   - Replica: `kafscale-prod-eu-west-1`
   - Replica: `kafscale-prod-ap-southeast-1`

2. **Enable versioning on all buckets** (required for CRR).

3. **Create a replication rule** on the primary bucket:
   - Replicate all objects (or the Kafscale prefix) to both replica buckets.
   - Use an IAM role that can write to the replica buckets.

4. **Optional**: Enable encryption (SSE-KMS) on all buckets with compatible keys.

### KafScale configuration (per region)

Primary region (writers and primary readers):
```yaml
spec:
  s3:
    bucket: kafscale-prod-us-east-1
    region: us-east-1
    credentialsSecretRef: kafscale-s3-creds
```

EU read replica:
```yaml
spec:
  s3:
    bucket: kafscale-prod-us-east-1
    region: us-east-1
    readBucket: kafscale-prod-eu-west-1
    readRegion: eu-west-1
    credentialsSecretRef: kafscale-s3-creds
```

Asia read replica:
```yaml
spec:
  s3:
    bucket: kafscale-prod-us-east-1
    region: us-east-1
    readBucket: kafscale-prod-ap-southeast-1
    readRegion: ap-southeast-1
    credentialsSecretRef: kafscale-s3-creds
```

### How reads and writes work

- **Writes** always go to the primary bucket.
- **Reads** go to the local `readBucket` first; on miss or error, the broker retries against the primary bucket.
- **List/restore** operations use the primary bucket to avoid missing newly written segments.

### Latency and consistency impact

- **Lower read latency** for consumers in EU/Asia when objects are already replicated.
- **CRR lag** means the newest segments may not appear immediately in the replica; the broker will fall back to the primary for those reads.
- **Extra cross-region traffic** occurs only on replica misses; steady-state reads stay in-region once replication catches up.

## Broker and managed etcd affinity overrides

`spec.brokers.affinity` and `spec.etcd.affinity` accept Kubernetes `Affinity`
objects independently. Omit a field (or set it to `null`) to retain the default
soft pod anti-affinity on `kubernetes.io/hostname`. An explicit `affinity: {}`
disables that default. A non-empty object replaces the entire affinity; the
operator does not merge the default into it. Removing an override restores the
default on the next reconciliation. Changes update the StatefulSet pod template
and can trigger a rolling update. Etcd placement applies only to managed etcd;
it does not affect an externally configured etcd cluster.

For example, require brokers on a labeled node pool and disable etcd affinity
in a single-node development cluster (merge these fields into your existing CR):

```yaml
spec:
  brokers:
    affinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
            - matchExpressions:
                - key: kafscale.io/node-pool
                  operator: In
                  values: [brokers]
  etcd:
    affinity: {}
```

To extend the default, include its soft anti-affinity term alongside your custom
rules. For a cluster named `example`, the broker selector is
`{app: kafscale-broker, cluster: example}`; etcd uses
`{app: kafscale-etcd, cluster: example}`. The default term has weight `100` and
topology key `kubernetes.io/hostname`. Required affinity/anti-affinity rules can
leave pods Pending when matching nodes or topology domains are unavailable.

The affinity CRD fields use a generated structural schema. After changing the
API types or upgrading Kubernetes API dependencies, regenerate those fields
with `python3 hack/update_affinity_schema.py` (requires Go and PyYAML).
`python3 hack/update_affinity_schema.py --check` checks for drift while preserving
the other manually maintained CRD fields.

## Conditional segment conflicts

A failed conditional segment PUT or a failed index upload after a successful segment PUT fences the old log handle. The broker evicts only that handle; the next request restores the immutable winning segment and continues after its last offset under the current lease. Restore rebuilds every read index in memory from validated segment bytes, checking the checksum, batch framing and offset bounds. Sidecar indexes are not trusted or required for partition restore: even an ordered, in-bounds sidecar can point at the wrong batch. This requires full-segment reads during restore, with the existing bounded worker pool and S3 semaphore. Failed append cleanup never deletes an immutable segment or its sidecar, including after a delayed old-writer failure. Recovery does not overwrite either object. Malformed segments and overlapping ranges fail restore rather than silently selecting or skipping data.

This recovery prevents reuse of the winning segment’s occupied offsets. It does not deduplicate producer retries: a timed-out write may already exist in S3, and KafScale does not implement idempotent producers or `InitProducerId`. Retrying the same records can append them again at later offsets.

## Produce deadlines and topic generations

A positive Produce `TimeoutMillis` bounds broker processing, including partition
lease acquisition, log restoration and the acknowledged S3 flush. An expired
request returns `REQUEST_TIMED_OUT`; a timeout does not prove that no data was
written, so normal producer retry and idempotence rules still apply. A
nonpositive value adds no request deadline; the transport and storage timeouts
still apply. With `acks=0` the broker sends no response.

Persisted Fetch responses return complete record batches. The first batch can
exceed `max.partition.fetch.bytes`, allowing consumers to make progress.

Topic deletion advances the etcd generation and retires the old storage prefix.
Topics deleted before upgrading to generation-aware deletion have no retirement
marker. Their old objects remain under `<namespace>/<topic>/`; recreating the
same name can recover those objects as generation 0. Before recreating such a
topic, archive or remove its exact old segment and LFS prefixes, or use a new
topic name. The upgrade cannot infer earlier deletions and does not remove old
objects automatically.

Configure etcd for processors that must exclude deleted topics. Iceberg's `auto`
or `etcd` discovery checks the current generation even without checkpoints and
fails closed when generation metadata cannot be read. S3-only discovery can
select the newest visible generation, but cannot detect a deletion for which no
new segments exist. SQL uses the same generation metadata when etcd endpoints
are configured.
