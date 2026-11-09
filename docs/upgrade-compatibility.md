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

This is the canonical guide for the unreleased changes after published v1.6.3
(`092291b78b8abf1e5bde6cafd92550e9726b46e0`). No release version is assigned here.
The release recommendation remains withdrawn pending the gates in
[issue #298](https://github.com/KafScale/platform/issues/298).

## Compatibility has separate boundaries

| Surface | Scope and limits |
| --- | --- |
| Go source | Compare exported root-module APIs against published v1.6.3, including interface implementations and comparable types. Unkeyed struct literals are not covered; see [Use keyed struct literals](#use-keyed-struct-literals). This does not compare nested processor modules, binary ABI or all runtime semantics. |
| Kafka wire | Brokers and proxies advertise the API versions they implement. Focused classic consumer-group, Produce/Fetch and native client tests cover those paths; they do not establish every Kafka API or every client/version. Legacy MessageSets are rejected; magic-2 batches are required. |
| Schema Registry REST | The built-in service implements the subset documented in [Schema Registry](schema-registry.md), with Avro, Protobuf and JSON Schema. Its compatibility modes describe schema evolution, separately from Go or Kafka compatibility. The reported matrix used Confluent 8.4.0 Java serializers, confluent-kafka-python 2.16.1 and franz-go `pkg/sr` 1.8.0. It is not a claim of full Confluent product parity. |
| Kubernetes CRDs | Apply the updated CRD before using new fields. Schema validation and Helm rendering checks do not prove a rolling upgrade on every Kubernetes/operator version. |
| Persisted data | Existing offset/group metadata and LFS envelopes remain readable in the covered tests. Topic recreation introduces generation-specific S3 prefixes; registry hash/descriptor aliases preserve covered schema IDs. Back up etcd and S3 together. Forward readability does not imply that an older binary can interpret new data on rollback. |

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
| `v1alpha1.ClusterConfigSpec` | `OffsetsRetentionMinutes` (first field), `ResponseWriteTimeoutMs` |
| `v1alpha1.KafscaleClusterSpec` | `Security` (first field) |

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

- Schema registry rollback: rolling back to a main build from between #277 and #282 registers duplicate versions for 5 of the 54 edge schemas in our upgrade test. These are the Avro schemas with the `5e-324` and `1e-323` double defaults, registered with and without `normalize=true`, and the schema with an empty nested namespace registered with `normalize=true`. Those builds do not match the hashes that #282 stores, so re-registering one of these schemas creates a new version and id. The #282 review build `035b209` passed that 54-schema regression; this does not establish general rollback safety.

## LFS proxy S3 credential keys

Before this release, an operator managed LFS proxy (`spec.lfsProxy`) read only
the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` keys of the
`spec.s3.credentialsSecretRef` secret. A secret with only the
`KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` keys, the format the
brokers and snapshot jobs use, kept the LFS proxy pods in
`CreateContainerConfigError`. The proxy now accepts either pair. No migration is
needed: secrets with the `AWS_` keys work as before and keep priority when both
pairs are present. A secret that has neither pair, or does not exist, no longer
blocks the pod; the proxy then uses the default AWS credential chain if one is
available. The operator flags such a secret, and one it cannot read, with the
`S3Credentials` condition set to `False` and a warning in its log. When the
secret holds only part of a pair, the condition message names the keys found,
because the etcd snapshot jobs can fail on such a secret instead of falling
back. The proxy logs which credential source it uses at startup, so check both
the condition and that log if LFS uploads fail with S3 access errors after the
upgrade.

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
root-module packages, and compiles an external fixture with the exact published
Store method set plus comparable/map-key/unkeyed `CoordinatorConfig` usage.
Runtime tests exercise the legacy constructor, group-offset fallback, optional
capability dispatch, error propagation and broker OffsetFetch through an old
Store implementation. Lease and retention tests exercise the new constructor.

Run the real S3 overwrite probe against a disposable test S3 service with
standard AWS credentials configured:

```sh
LFS_S3_ENDPOINT=http://127.0.0.1:9000 npm --prefix lfs-client-sdk/js run test:s3
```

It creates a unique bucket and runs one pass each for `sha256`, `md5`, `crc32`
and `none` envelopes. Each pass uploads 262,144 bytes, verifies exact valid
bytes, overwrites the same key with a same-length first-byte mutation, requires
default and explicit validation to reject it, and verifies disabled validation
returns the altered bytes. It deletes its object and bucket afterward. This is a real
S3 API test; it does not rerun the historical native Kafka round trip. Unit tests
must fail against the old resolver and pass against the candidate. Before a merge
recommendation, inspect every PR review/comment against final head and repeat
focused corruption and compatibility regressions. Publishing and release version
selection are separate decisions.

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
