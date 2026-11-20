<!--
Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# Upgrade and compatibility guide

This is the canonical guide for upgrading from published v1.6.3
(`092291b78b8abf1e5bde6cafd92550e9726b46e0`) to v2.0.0.

## Compatibility has separate boundaries

| Surface | Scope and limits |
| --- | --- |
| Go source | The module path is `github.com/KafScale/platform/v2`. Compare exported root-module symbols against published v1.6.3 after rewriting that checkout onto `/v2`, including interface implementations and comparable types. Unkeyed struct literals are not covered; see [Use keyed struct literals](#use-keyed-struct-literals). This does not compare nested processor modules, binary ABI or all runtime semantics. |
| Kafka wire | Brokers and proxies advertise the API versions they implement. Focused classic consumer-group, Produce/Fetch and native client tests cover those paths; they do not establish every Kafka API or every client/version. Legacy MessageSets are rejected; magic-2 batches are required. |
| Schema Registry REST | The built-in service implements the subset documented in [Schema Registry](schema-registry.md), with Avro, Protobuf and JSON Schema. Its compatibility modes describe schema evolution, separately from Go or Kafka compatibility. The reported matrix used Confluent 8.4.0 Java serializers, confluent-kafka-python 2.16.1 and franz-go `pkg/sr` 1.8.0. It is not a claim of full Confluent product parity. |
| Kubernetes CRDs | `kafscale.io/v1` is the served and stored version. `kafscale.io/v1alpha1` is still served but deprecated; see [CRD API version v1](#crd-api-version-v1). Apply the updated CRD before using new fields. Schema validation and Helm rendering checks do not prove a rolling upgrade on every Kubernetes/operator version. |
| Persisted data | Existing offset/group metadata and LFS envelopes remain readable in the covered tests. Topic recreation introduces generation-specific S3 prefixes; registry hash/descriptor aliases preserve covered schema IDs. Back up etcd and S3 together. Forward readability does not imply that an older binary can interpret new data on rollback. |

## CRD API version v1

`KafscaleCluster` and `KafscaleTopic` now use `apiVersion: kafscale.io/v1`.
The v1 schema is identical to v1alpha1, so no conversion webhook is needed
(conversion strategy `None`).

- Both versions are served. v1 is the storage version.
- v1alpha1 is deprecated. Requests that use it still work, and the API server
  returns a warning such as `Warning: kafscale.io/v1alpha1 KafscaleCluster is deprecated; use kafscale.io/v1 KafscaleCluster. v1alpha1 will be removed in a future release.`
- v1alpha1 will be removed in a future release. No removal date or version is
  set.
- The operator uses v1. The console, MCP server and CLI do not read the CRDs.
- Existing v1alpha1 objects keep working. You can read every object as either
  version: `kubectl get kafscaleclusters.v1.kafscale.io` or
  `kubectl get kafscaleclusters.v1alpha1.kafscale.io`.
- An object stored as v1alpha1 is rewritten as v1 the next time it is written,
  for example by `kubectl apply` or an operator status update.

### Upgrade steps

Helm does not upgrade CRDs from `crds/`, so apply them first, then upgrade the
chart. Run these from a checkout of the release you are upgrading to:

```sh
kubectl apply --server-side --force-conflicts --field-manager=kafscale-crd-upgrade -f deploy/helm/kafscale/crds/
kubectl wait --for=condition=Established crd/kafscaleclusters.kafscale.io crd/kafscaletopics.kafscale.io
helm upgrade --install kafscale deploy/helm/kafscale --namespace kafscale --reset-then-reuse-values
```

`--reset-then-reuse-values` needs Helm 3.14 or later. It keeps the values you
set at install and takes the chart defaults for keys that are new in 2.0, such
as `schemaRegistry`. Do not use `--reuse-values` for this upgrade. It reuses
only the 1.x values, and the upgrade fails with
`nil pointer evaluating interface {}.enabled`. With Helm older than 3.14, save
your values and pass them with `-f`:

```sh
umask 077
helm get values kafscale --namespace kafscale -o yaml > kafscale-values.yaml
helm upgrade --install kafscale deploy/helm/kafscale --namespace kafscale -f kafscale-values.yaml
```

The saved file can hold credentials you set as values. Delete it after the
upgrade.

`--force-conflicts` is required when Helm installed the CRDs, which is the
default. Without it the apply fails with `conflict with "helm": .spec.versions`.

Then change `apiVersion: kafscale.io/v1alpha1` to `apiVersion: kafscale.io/v1`
in your manifests, Helm values and GitOps sources. Nothing else in the manifest
changes.

### Rewrite stored objects

To move every stored object to v1 now instead of on its next write, replace
each object with its current content, then record that only v1 is stored:

```sh
kubectl get kafscaleclusters.kafscale.io --all-namespaces -o json | kubectl replace -f -
kubectl get kafscaletopics.kafscale.io --all-namespaces -o json | kubectl replace -f -
kubectl patch crd kafscaleclusters.kafscale.io --subresource=status --type=merge -p '{"status":{"storedVersions":["v1"]}}'
kubectl patch crd kafscaletopics.kafscale.io --subresource=status --type=merge -p '{"status":{"storedVersions":["v1"]}}'
```

Run the two `kubectl patch` commands only after both `kubectl replace` commands
succeed. A `kubectl replace` that fails with a conflict means the operator
updated the object at the same time; run it again. The future release that
removes v1alpha1 requires this step first.

### Go types

Import `github.com/KafScale/platform/v2/api/v1` in new code:

```go
import kafscalev1 "github.com/KafScale/platform/v2/api/v1"
```

The module path is `github.com/KafScale/platform/v2`.
`go get github.com/KafScale/platform@v2.0.0` fails because that tag's `go.mod`
has no `/v2` suffix. The existing tag also cannot be fetched as
`github.com/KafScale/platform/v2@v2.0.0`. Future 2.x releases must use this path.
v1.6.3 stays
`github.com/KafScale/platform`. Do not tag v1.7.0 on a commit whose module
path is `/v2`.

The Iceberg processor module is
`github.com/KafScale/platform/v2/addons/processors/iceberg-processor`. It
requires `github.com/KafScale/platform/v2` and replaces that module with the
repository root. Standalone image builds require an explicit
`--build-arg PLATFORM_VERSION=<published /v2 version>`. The build validates
that published artifact, drops the local replace, and updates the required
version. There is no default published version: `v2.0.0` is invalid, and a
release containing the corrected module declaration must be published first.
Missing or unavailable versions fail with an actionable error.
`USE_LOCAL_PLATFORM=1` keeps the replace and builds from the checkout without
requiring `PLATFORM_VERSION`. From the repository root:

```sh
docker build -f addons/processors/iceberg-processor/Dockerfile \
  --build-arg USE_LOCAL_PLATFORM=1 \
  --build-arg MODULE_DIR=addons/processors/iceberg-processor \
  -t iceberg-processor:dev .
```

The skeleton processor module is
`github.com/KafScale/platform/v2/addons/processors/skeleton`. The SQL processor
module is `github.com/kafscale/platform/v2/addons/processors/sql-processor`.

The `api/v1` types are aliases of the `api/v1alpha1` types, so they are the
same Go types and code written against
`github.com/KafScale/platform/v2/api/v1alpha1` keeps compiling.
`api/v1alpha1` is deprecated and will be removed together with the v1alpha1
CRD version.

The packages differ only in the version they register. `kafscalev1.AddToScheme`
maps the types to `kafscale.io/v1`, and `v1alpha1.AddToScheme` maps them to
`kafscale.io/v1alpha1`. Add only one of them to a scheme. With both, each type
maps to two kinds and controller-runtime clients return an error instead of
picking one.

### Rollback

Rolling the operator back to a release that only knows v1alpha1 keeps working
while the new CRDs are installed, because v1alpha1 is still served. Once objects
are stored as v1, the old v1alpha1-only CRDs from 1.x cannot be reapplied. This
happens within seconds of the CRD apply, before the chart upgrade, because the
running 1.x operator updates object status. The
API server refuses to remove v1 from the CRD while `status.storedVersions` lists
it. A chart rollback must keep the 2.0 CRDs.

## Go API migration

Published source remains supported. `CoordinatorConfig` retains its exact single
field and comparability, so existing code continues to compile:

```go
cfg := broker.CoordinatorConfig{CleanupInterval: time.Second}
configs := map[broker.CoordinatorConfig]string{cfg: "legacy"}
_ = configs
coordinator := broker.NewGroupCoordinator(store, brokerInfo, &cfg)
defer coordinator.Stop()
```

For retention settings and coordination callbacks, use the new API:

```go
options := broker.CoordinatorOptions{
    CleanupInterval: time.Second,
    OffsetsRetention: 7 * 24 * time.Hour,
    OwnsGroup: leases.Owns,
    AcquireGroup: leases.Acquire,
    ReleaseGroup: leases.Release,
    GroupDeleted: leases.Release,
}
coordinator := broker.NewGroupCoordinatorWithOptions(store, brokerInfo, &options)
defer coordinator.Stop()
```

Code built from intermediate main revisions that put callbacks or retention
settings on `CoordinatorConfig` must move them to `CoordinatorOptions` and use
`NewGroupCoordinatorWithOptions`. `CoordinatorOptions` contains functions and is
not comparable. Keep comparable configuration keys in the legacy type or a
separate application-owned value type.

External implementations of the v1.6.3 `metadata.Store` do not need new methods.
For example, an existing `var _ metadata.Store = (*CustomStore)(nil)` remains
valid. All-group listing is still available:

```go
allOffsets, err := store.ListConsumerOffsets(ctx)
```

For a group-specific read, replace application-side filtering with:

```go
offsets, err := metadata.ListGroupConsumerOffsets(ctx, store, groupID)
```

The helper selects the optional `metadata.GroupConsumerOffsetLister` interface
when implemented. Otherwise it calls the original `ListConsumerOffsets`, filters
by exact group ID, preserves offset metadata and propagates errors/cancellation.
The fallback scans all groups, so unrelated backend read failures can affect it.
Implement the optional method to get efficient group-scoped reads; the built-in
in-memory and etcd stores implement it. Existing concrete-store methods remain
available. Other atomic group/offset extensions remain optional too; custom
stores must implement those capabilities to gain the corresponding fencing
semantics. Source compatibility alone does not supply atomicity.

### Use keyed struct literals

These exported structs gained fields since v1.6.3. Unkeyed (positional)
literals of them written against v1.6.3 no longer compile. Keyed literals
compile against both versions.

| Struct | Added fields |
| --- | --- |
| `metadata.ConsumerOffset` | `Metadata` |
| `metadata.EtcdStoreConfig` | `StorageNamespace` |
| `v1.ClusterConfigSpec`, `v1alpha1.ClusterConfigSpec` | `OffsetsRetentionMinutes` (first field), `ResponseWriteTimeoutMs` |
| `v1.KafscaleClusterSpec`, `v1alpha1.KafscaleClusterSpec` | `Security` (first field) |

```go
offset := metadata.ConsumerOffset{Group: "orders", Topic: "events", Partition: 0, Offset: 42}
```

`broker.Server` gained `WriteTimeout`, and the generated `pkg/gen/metadata`
types `ConsumerGroup` and `GroupMember` gained fields. These structs have
unexported fields, so code outside their packages could not use unkeyed
literals of them against v1.6.3 either. The v1.6.3 unkeyed `CoordinatorConfig`
literal still compiles.

### Deprecation policy

`CoordinatorConfig` and `NewGroupCoordinator` are deprecated in favor of the
options API, but remain supported in this release. Deprecated exported APIs
must have a supported replacement and migration guidance alongside them.
Removal requires an announced future breaking release. No removal version is
scheduled. `Store.ListConsumerOffsets` retains its all-group purpose and is not
deprecated.

## LFS checksum behavior

The Node, browser, Python and Java SDK resolvers validate the SHA-256 of the
downloaded object by default. The envelope `checksum_alg` field selects the
expected value:

| `checksum_alg` | Expected SHA-256 |
| --- | --- |
| missing or `sha256` | `checksum`, or `sha256` when `checksum` is empty |
| `md5`, `crc32` or `none` | `sha256` |
| any other value | Resolution fails with an unsupported checksum algorithm error. |

The proxy writes the SHA-256 of the stored object to `sha256` for every
algorithm, and every SDK envelope decoder rejects an envelope whose `sha256` is
missing or empty before it reads the object. A mismatch
rejects resolution before a record is returned. Objects whose SHA-256 matches
the expected value return their exact bytes. With `none`, the SDKs still
validate SHA-256. The Go resolver in `pkg/lfs` validates `checksum` with the
named algorithm and skips validation for `none`.

Disabling validation returns the downloaded bytes, including corrupt bytes:
`validateChecksum: false` in the Node and browser SDKs, `validate_checksum=False`
in Python, and `validateChecksum` set to `false` in the Java `LfsResolver`
constructor. Plain records pass through, and the envelope payload size limit
still applies. See [issue #297](https://github.com/KafScale/platform/issues/297).

## Deployment and rollback considerations

- Building from source requires Go 1.26 or newer, as required by the maintained Protobuf reflection library.

- Empty groups written by older brokers receive a full retention period from first discovery. Apply the updated KafscaleCluster CRD before setting `spec.config.offsetsRetentionMinutes`; omit it for the seven-day default or set it to `0` to disable automatic expiry.

- During a rolling upgrade, deleting a group through an older broker removes its metadata but leaves its offsets. Upgraded brokers discover those offsets and list the group again as `Empty`. After all brokers are upgraded, delete the inactive group again to remove its retained offsets.

- Members recorded by older brokers without heartbeat timestamps receive one local session timeout to reconnect after coordinator takeover. They are not expired immediately during a rolling upgrade.

- On coordinator takeover, recovered heartbeat times receive a five-second expiry margin for clock skew. Locally received heartbeats return to the configured session timeout.

- Topics that already exist with names outside the allowed characters, such as names with `@` or `/`, keep loading and accept Produce and Fetch as before. Only new creates are rejected. A topic named `<name>@<N>` shares an S3 prefix with generation N of `<name>`, so do not delete and re-create `<name>` while it exists. Move its data to a topic with a valid name and delete it first.

- Schema registry rollback: rolling back to a main build from between #277 and #282 registers duplicate versions for 5 of the 54 edge schemas in our upgrade test.
  - These are the Avro schemas with the `5e-324` and `1e-323` double defaults, registered with and without `normalize=true`, and the schema with an empty nested namespace registered with `normalize=true`.
  - Those builds do not match the hashes that #282 stores, so re-registering one of these schemas creates a new version and id.
  - The #282 review build `035b209` passed that 54-schema regression. This does not establish general rollback safety.

- The proxy no longer keeps serving through an etcd outage without a time limit. Earlier builds kept using the last broker list with no limit.
  - Within `KAFSCALE_PROXY_BACKEND_CACHE_TTL_SEC` (default `60`) of the last successful etcd read, the Kafka listener keeps using its last metadata. Produces still wait, because brokers do not ack while etcd is down.
  - Past the TTL the proxy reports NotReady, answers Kafka requests with `REQUEST_TIMED_OUT` (an all-topics Metadata request gets an empty reply), closes the client connection, and still answers ApiVersions.
  - The LFS HTTP API returns 503 `metadata_unavailable` as soon as etcd cannot be read, even within the TTL.
  - The proxy serves again within a few seconds after etcd answers. etcd clients use a one-second maximum gRPC reconnect backoff, and each connection attempt is given at least five seconds, so recovery is no longer delayed by the default two-minute backoff. The limit does not apply when `KAFSCALE_PROXY_BACKENDS` lists static brokers.
  - Full details: [Proxy behavior when etcd is down](operations.md#proxy-behavior-when-etcd-is-down).

- Brokers now upload segments with `PutObject` and `If-None-Match: *` (#324, #327).
  - A broker that lost its partition lease can no longer overwrite segments written by the new owner.
  - The S3 backend must honor the condition. AWS S3 and MinIO do. Backblaze B2 and the Google Cloud Storage XML API do not.
  - Brokers check this at startup and refuse to start if a second write to the same probe key does not return `412 Precondition Failed` (#336). A backend that rejects the first conditional write with `501` or the error code `NotImplemented` fails startup at once with `backend rejects If-None-Match: *`. A first write that fails with the error code `InvalidArgument` or `InvalidRequest` fails startup at once with `backend rejected the conditional PutObject (If-None-Match: *)`. Allow `s3:DeleteObject` on `__health/` for probe cleanup. See [Conditional writes](supported.md#conditional-writes).
  - Code that implements `storage.S3Client` itself must make `UploadSegment` fail with `storage.ErrPreconditionFailed` when the key already exists.

- With brokers found in etcd, the LFS HTTP API now sends `/lfs/produce` and multipart completion to the broker that owns the partition.
  - A broker that does not lead the partition used to make these requests fail with HTTP 502 and leave the uploaded object in S3 as an orphan.
  - The proxy now tries the current owner, and when no broker accepts the produce the request gets 503 `backend_unavailable` and the object is deleted.
  - This also applies to static backends (`spec.lfsProxy.backends`): the produce goes to the owner from the etcd routes when it is known, and the listed brokers are used when it is not known or cannot be dialed. A non-owner answer gives 503 with the object deleted instead of 502 with an orphan, after the owner or another listed broker was tried.
  - With only one address, for example a broker Service, and no reachable owner, there is no other broker to try.
  - Clients that treat 502 from these endpoints as final should also retry on 503 (#318).

## LFS proxy S3 credential keys

Before this release, an operator managed LFS proxy (`spec.lfsProxy`) read only the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` keys of the `spec.s3.credentialsSecretRef` secret.

- A secret with only the `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` keys, the format the brokers and snapshot jobs use, kept the LFS proxy pods in `CreateContainerConfigError`. The proxy now accepts either pair.
- No migration is needed: secrets with the `AWS_` keys work as before and keep priority when both pairs are present.
- A secret that has neither pair, or does not exist, no longer blocks the pod; the proxy then uses the default AWS credential chain if one is available.
- The operator flags such a secret, and one it cannot read, with the `S3Credentials` condition set to `False` and a warning in its log.
- When the secret holds only part of a pair, the condition message names the keys found, because the etcd snapshot jobs can fail on such a secret instead of falling back.
- The proxy logs which credential source it uses at startup, so check both the condition and that log if LFS uploads fail with S3 access errors after the upgrade.

The LFS proxy forwards `AWS_SESSION_TOKEN` alongside the AWS key pair. When both
pairs are present, it uses the AWS pair and its token; the KafScale fallback
retains its own token. A permanent AWS pair does not inherit a KafScale token.
The startup credential log includes `session_token=true/false` for static keys,
without exposing the token. The flag is omitted for default-chain credentials,
which may resolve later through a provider.

When `spec.s3.credentialsSecretRef` is set, the broker and LFS proxy pods roll once after the upgrade, because their pod templates gain the `kafscale.io/s3-credentials-checksum` annotation, and roll again whenever a credential key in the secret changes.

- This also happens while the managed etcd snapshot check fails, for example with a 403 on the snapshot bucket: `Ready` then stays `False` with `SnapshotAccessFailed`, but the broker and LFS proxy workloads are still applied, with every spec change (for example image, replicas and resources), not only credential changes.
- The cluster metadata in etcd is still published to match, so a scale-down does not leave removed brokers as partition leaders.
- Earlier releases held back all broker and LFS proxy workload updates while the check failed.
- If you relied on a failed snapshot check to freeze broker updates, that no longer happens.
- Only a new cluster waits for that check before its brokers and LFS proxy are created.

S3 key values in the secret that are empty or only whitespace now count as unset everywhere, and other values are trimmed before use.

- Before, the operator condition, the operator snapshot check and the LFS proxy static keys trimmed them, but the brokers, the LFS proxy fallback keys and the AWS CLI in the etcd snapshot containers used the raw values, so a blank `KAFSCALE_S3_ACCESS_KEY` or `KAFSCALE_S3_SECRET_KEY` next to a valid `AWS_*` pair gave the snapshot containers an empty or blank key, and a padded value failed to sign.
- The snapshot containers now select the pair in their script instead of through `secretKeyRef` environment variables: the `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` pair when both keys are set, otherwise the `AWS_*` pair with `AWS_SESSION_TOKEN`.
- Secrets with valid keys keep working with the same pair and token as before.
- A secret with the `AWS_*` pair and blank `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` keys now reports `S3Credentials` as `True` with `SecretKeysFound`.
- This changes the managed etcd pod template, which the earlier change in this release already does, so an upgrade from the last release still restarts the managed etcd pods once.

Whitespace here means space, tab, newline, carriage return, vertical tab and
form feed, in the Go code and in the snapshot container scripts alike. Unicode
spaces such as U+00A0 are kept as part of the value. In the last release the
operator snapshot check, the operator checks of the `spec.s3` endpoint fields
and `spec.s3.credentialsSecretRef`, and the LFS proxy S3 key, region and
endpoint settings also trimmed Unicode spaces. They now keep them, so a value
padded with U+00A0 is used as written, as the etcd snapshot containers already
did. Remove such characters from the secret or the settings before upgrading.

## Validation scope and rerun commands

The QA reported on `bd1813ad2644c56736296f1e5dfff99901707ca8` in #298 passed
required release gates, a 30-minute soak (35,981 exact-byte-validated records),
three five-minute fuzz campaigns (28,794,235 executions), native Go/Python/Java/JS
LFS round trips, and Java Protobuf old-record decoding after registry restart.
Those are historical results on that revision, not a fresh certification of
the candidate head. The referenced `/workspace/qa` files are external evidence,
not repository files.

For this change, the module API comparison reproduced exactly the two reported
breaks on the original QA revision and found no incompatible exported changes
against v1.6.3 after the repair. The resolver unit tests and the real MinIO S3
overwrite probe failed against the original resolver and passed after the repair,
including valid-object and validation-disabled checks. The md5 and crc32
envelope cases in the Node, browser, Python and Java resolver tests failed
before the `checksum_alg` repair and pass after it. Focused broker, metadata
and proxy suites passed with the Go race detector. These results do not replace
the historical soak or establish general upgrade/rollback safety.

The focused regressions for this change are:

```sh
bash scripts/check_go_api_compatibility.sh
npm --prefix lfs-client-sdk/js test
npm --prefix lfs-client-sdk/js-browser test
(cd lfs-client-sdk/python && python3 -m pytest)
mvn -f lfs-client-sdk/java/pom.xml -Dtest=LfsResolverTest test
go test -race ./pkg/broker ./pkg/metadata ./cmd/broker ./cmd/proxy
```

The API script uses pinned `golang.org/x/exp/cmd/apidiff`, checks all exported
root-module packages against v1.6.3 after rewriting that checkout onto
`github.com/KafScale/platform/v2`, and compiles an external fixture with the
exact published Store method set plus comparable/map-key/unkeyed
`CoordinatorConfig` usage.
Runtime tests exercise the legacy constructor, group-offset fallback, optional
capability dispatch, error propagation and broker OffsetFetch through an old
Store implementation. Lease and retention tests exercise the new constructor.

Run the real S3 overwrite probe against a disposable test S3 service with
standard AWS credentials configured:

```sh
LFS_S3_ENDPOINT=http://127.0.0.1:9000 npm --prefix lfs-client-sdk/js run test:s3
```

It creates a unique bucket and runs one pass each for `sha256`, `md5`, `crc32` and `none` envelopes.

- Each pass uploads 262,144 bytes, verifies exact valid bytes, overwrites the same key with a same-length first-byte mutation, requires default and explicit validation to reject it, and verifies disabled validation returns the altered bytes. It deletes its object and bucket afterward.
- This is a real S3 API test; it does not rerun the historical native Kafka round trip.
- Unit tests must fail against the old resolver and pass against the candidate.
- Before a merge recommendation, inspect every PR review/comment against final head and repeat focused corruption and compatibility regressions.
- Publishing and release version selection are separate decisions.

## CodeQL port-conversion triage

The two retained `go/incorrect-integer-conversion` warnings follow
`cmd/proxy/main.go:portFromAddr` (`strconv.Atoi`) into the two `envPort` fallback
casts. Local SARIF has matching source/sink flows; GitHub alerts #62 and #66
are dismissed, which alone does not establish safety.

The fallback previously accepted values outside both the TCP range and, on a
64-bit machine, the `int32` range. A value such as `4294967297` could narrow to
`1` if it reached the cast. This is a real configuration-validation defect;
the evidence does not establish an exploitable vulnerability. Parsing address
ports as unsigned 16-bit values and bounding both configured and fallback ports
to `1..65535` makes the narrowing safe. Invalid fallback values use `9092`.
Boundary tests cover `1`, `65535`, zero, negative, `65536`, signed-32-bit edges,
wraparound candidates, malformed input and overflow. No alert was dismissed or
reclassified by this change.

### Security dependency updates

Builds use Go 1.26.11. The operator's default managed etcd image is
`gcr.io/etcd-development/etcd:v3.6.14`, pinned by digest, and snapshot tools
include etcdctl/etcdutl 3.6.14. Allow pulls from `gcr.io` when using the default.
An explicit `KAFSCALE_OPERATOR_ETCD_IMAGE` selects the supplied image instead;
update custom images to a patched etcd release as well. Docker Compose uses
etcd 3.5.33 to preserve its existing 3.5 data-format release line.
