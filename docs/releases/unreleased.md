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

# Unreleased

## New features

- New `kafscale-schema-registry` service, deployed with `schemaRegistry.enabled` in the Helm chart.
  - It serves the Confluent Schema Registry REST API for Avro, Protobuf and JSON Schema, keeps its state in etcd, and stores schema text above 64 KiB in S3.
  - Several replicas can serve writes at the same time: ids are unique and increase, and the same schema always gets the same id.
  - Schemas are stored and deduplicated in the form Confluent Schema Registry 8.4.0 stores them, and `normalize=true` is supported.
  - It supports all seven Avro compatibility levels, `READWRITE`, `READONLY` and `IMPORT` modes, references, and soft and hard deletes. A hard delete keeps the schema id.
  - Responses carry the Confluent schema `guid`, and `GET /schemas/guids/{guid}` serves clients that send it in a record header.
  - With S3 configured, every issued id is recorded in S3 and never given out again, also after an etcd restore. It has no authentication.
  - Tested with the Java `KafkaAvroSerializer` and `KafkaAvroDeserializer` 8.4.0, confluent-kafka-python 2.16.1 and franz-go `pkg/sr` 1.8.0, and with data written through Confluent Schema Registry 8.4.0 and imported with its ids.
  - See `docs/schema-registry.md`, which also covers using an existing registry instead (#154).

- A registry run from a main build between #277 and this release stored two kinds of schema in an older form: double defaults below about 2.2e-308, such as `5e-324`, and, with `normalize=true`, a nested type with `"namespace":""`.
  - Lookups and registers in the same subject still find them and keep their ids and versions.
  - Registering such a schema in another subject gives it a new id, and its `guid` stays the one computed from the stored text.
  - Schemas written by such a build are compared through the slower path on every lookup or register in their subject that does not match the stored hash. Schemas written by this release record the form they were stored in and skip it.

- The built in schema registry supports JSON Schema with references, all seven compatibility levels, normalization and CLI import. JSON schemas share the existing etcd and S3 storage across replicas. Cross-type registrations can no longer overwrite another schema's GUID (#288).
- Equivalent Protobuf text and base64 descriptors with references reuse the same schema id across subjects. Descriptor aliases preserve existing text hashes, imported ids and S3 markers (#287).
- The built in schema registry supports Protobuf schemas, with references and imports, the well known types, all seven compatibility levels with the Confluent Schema Registry 8.4.0 rules, `normalize=true`, and `format=serialized` for clients that fetch descriptors, and base64 `FileDescriptorProto` input on register and lookup, which the confluent-kafka-python `ProtobufSerializer` sends.
  - `kafscale-cli schema import` copies Protobuf subjects with their ids.
  - Tested with the Java `KafkaProtobufDeserializer` 8.4.0, confluent-kafka-python 2.16.1 and franz-go `pkg/sr` 1.8.0 on records written through Confluent Schema Registry 8.4.0 and imported.
  - A proto3 enum whose first value is not zero is rejected; see `docs/schema-registry.md` for the other differences.

- `schemaregistry.Error` has an unexported field for error details and is not comparable. This package was not present in published v1.6.3; code using intermediate main builds should compare `Code` or use `errors.As`.

- `kafscale-cli schema import --from <url>` copies subjects, versions, ids, references, soft deletes, compatibility levels and subject modes from Confluent Schema Registry or another registry with its REST API into the built in registry.
  - It keeps the ids, can be run again, and fails on any id or version that the target holds with a different schema.
  - Subjects in a non-default Confluent context are skipped with a message and make the run exit with an error.
  - A failed run still prints its counts and whether the target is in `IMPORT` mode.
  - With S3 configured, an import may take over an id that S3 recorded for a failed write when etcd has no schema with that id; it raises the id counter first so the failed write cannot finish later (#154).

- `kafscale-cli prune` lists the S3 data that deleted topics keep and deletes it with `--force`. It filters by `--topic` and `--older-than`, deletes only partition segment and index files, refuses live topics and the current generation, fails when the bucket does not exist, and is safe while brokers run. Access is controlled by the etcd and S3 credentials. It does not cover LFS objects or topics deleted before the upgrade (#262).

- The LFS proxy writes new objects under their own prefix, `<namespace>/@lfs/<topic>/` or `<namespace>/@lfs/<topic>@<N>/` after the Nth delete, instead of inside the topic's segment folder.
  - Objects written before stay where they are and remain readable, because the envelope stores the full key.
  - When etcd is configured, LFS produce needs it reachable: the proxy looks up the topic generation once per topic before any upload, with a timeout set by `KAFSCALE_LFS_PROXY_METADATA_TIMEOUT_MS` (default 3000 ms), and returns 503 `metadata_unavailable` or a produce error if the lookup fails.
  - It checks the generation again after the upload and right before the produce, and if the topic was recreated in between it deletes the uploaded objects and fails the same way instead of producing.
  - `/lfs/download` only accepts keys under `<namespace>/@lfs/` or the old `<namespace>/<topic>/lfs/` shape.

## Bug fixes

### Security and dependencies

- The SQL proxy contains malformed upstream PostgreSQL decoder panics (GO-2026-4518), closes the failed exchange and continues serving other connections. The archived codec has no patched v2 release.
- Update vulnerable Go and JavaScript dependencies: OpenTelemetry SDK/trace 1.47.0, JOSE 4.1.6, Brotli 1.2.7, compress 1.20.2, x/net 0.62.0, x/crypto 0.59.0 and fast-xml-parser 5.12.0. Processor modules use the same patched dependency versions. Build KafScale binaries with Go 1.26.11 to include the HTTP server security fix.
- Managed etcd and snapshot tools use etcd 3.6.14. The managed server image comes from `gcr.io/etcd-development/etcd` and is pinned by digest; Docker Compose keeps the 3.5 line and moves to 3.5.33. These releases fix watch/transaction authorization bypasses and unbounded TLS handshakes. Existing image overrides take precedence. Demo probes use the shell-equipped snapshot tools image.
- CI installs Python dependency closures pinned by version and SHA-256, and processor base images are pinned by digest.
- Group-routing failures log fixed reason codes instead of raw errors containing request data. Group and generic backend failures build their Kafka error responses from the request body, so a failed broker connection returns `REQUEST_TIMED_OUT` instead of closing without a response.
- Bound proxy advertised ports and address-derived fallbacks to `1..65535` before narrowing to Kafka's `int32` port field. The two CodeQL conversion warnings exposed a configuration-validation defect, not a confirmed vulnerability (#298).
- Protobuf descriptor printing uses `jhump/protoreflect/v2`, removing the v1 dependency. Map-entry repair rejects references to entries in the wrong scope instead of silently rewriting them (#284).

### Broker produce, storage and topics

- Conditional segment conflicts evict only the failed cached log, allowing the next request to adopt the immutable winner and continue under the same lease. Retained handles remain fenced for reads and writes. Restore rebuilds all read indexes from validated segment bytes instead of trusting sidecar offset/position pairs, and rejects overlapping segment ranges. This adds full-segment reads during restore within the existing concurrency limits. Failed index uploads also fence the handle without deleting an immutable claim another owner may have adopted. Winning S3 objects remain unchanged; producer retries can still duplicate records because idempotent producers are unsupported (#343).

- Fetch returns `REQUEST_TIMED_OUT` when the metadata offset read times out, is canceled, or fails while etcd is unavailable, instead of interpreting a failed read as watermark zero and triggering `OFFSET_OUT_OF_RANGE`. Unknown watermarks are reported as -1. Other offset-read failures return `UNKNOWN_SERVER_ERROR`; unknown-topic and ownership fencing responses retain their existing meanings (#342).

- Missing topic configs derive the replication factor from partition replicas, defaulting to 1 when replica metadata is absent. They no longer use the partition count, which made CLI restores fail when partitions exceeded brokers and made MCP `describe_configs` report an incorrect factor. Partition expansion uses the same fallback and atomically updates existing stored configs to keep their partition counts current without overwriting concurrent config edits (#338).
- Topic config updates participate in expansion's snapshot revision checks and derive partition counts from the current topology. Delayed config creation or updates retry after expansion instead of restoring an old count, reject a topic recreated during the operation, and publish local config state only after a successful commit. CLI restore uses the same guarded update path (#338).

- Brokers check S3 conditional PUT support before opening listeners. A second write to a unique probe key must return `412 Precondition Failed`; backends that ignore or reject the condition fail startup with a clear error. Startup probes are cleaned up after successful and failed attempts (#336).
- A broker whose S3 backend rejects the first `PutObject` with `If-None-Match: *` now exits at once. HTTP `501` or the error code `NotImplemented` fails with `backend rejects If-None-Match: * ... partition segments cannot be fenced`. The error codes `InvalidArgument` and `InvalidRequest` fail with `backend rejected the conditional PutObject (If-None-Match: *) ... partition segments cannot be fenced`. Before, it retried the probe until `KAFSCALE_STARTUP_TIMEOUT_SEC` expired, and the pod restarted and did the same again. Other startup S3 errors still retry until the timeout (#360).

- Conditional S3 write conflicts (412 precondition failed and 409 conditional conflict) no longer count as S3 health errors and block produces across the broker. The conflicting partition remains fenced; S3 latency and other failures still drive backpressure (#335).

- Produce requests targeting a partition that does not exist now finish promptly.
  - Auto-creation makes at most one attempt per partition-log lookup instead of spinning when the topic already exists.
  - Requests requiring a response receive `UNKNOWN_TOPIC_OR_PARTITION`, including when another broker holds a lease for that nonexistent partition; valid partitions still receive normal leader redirection.
  - Topic-level metadata errors, including invalid topic names, are preserved across lease owners.
  - Requests with `acks=0` still send no response.
  - Requests with no timeout no longer occupy a broker worker indefinitely, and other valid partitions in the same produce request continue to be processed.
  - Automatic creation uses `KAFSCALE_AUTO_CREATE_PARTITIONS` (default 1); a client partition ID cannot increase that count.
  - Out-of-range IDs for missing topics are rejected without creating a topic, including integer boundary values.
  - Existing topics may have more partitions than this automatic-creation count (#326).
- Broker topic creation reads the authoritative etcd metadata snapshot and uses compare-and-swap to preserve concurrent changes.
  - Racing creates no longer lose a topic, and a stale broker cannot replace the cluster's brokers or existing partition counts with its startup view.
  - Startup demo topics are never copied into etcd.
  - Partition expansion and topic deletion use the same snapshot fencing; expansion publishes missing partition state atomically in batches without resetting existing state, and a stale operation cannot modify a deleted and recreated topic.
  - Snapshot watches retry failed initial reads and replay updates committed between the full read and watch registration.
  - Operator publication rebuilds each CAS retry from its original desired state and the latest snapshot, preventing deleted auto-created topics from reappearing; unreadable snapshots are preserved and reported as errors (#325).
- Produce and Fetch return `KAFKA_STORAGE_ERROR` (56) instead of `UNKNOWN_SERVER_ERROR` (-1) when S3 is unavailable, when a write or read to S3 fails, and when a partition cannot be restored from S3.
  - Clients such as librdkafka now retry instead of failing the message.
  - Degraded S3 still returns `REQUEST_TIMED_OUT` (7).
  - Produce v8 and later include a message such as "object storage unavailable, retry later" (#255).
  - Produce below v4 and Fetch v5 or below receive `NOT_LEADER_OR_FOLLOWER` (6) instead, as Kafka does for versions that predate the storage error.
  - ListOffsets returns `KAFKA_STORAGE_ERROR` when an earliest lookup cannot load the partition from S3.
  - Produce append errors that do not come from S3 still return `UNKNOWN_SERVER_ERROR`.
- Store every batch in a partition's Produce request together. Failed writes leave buffered data and offsets unchanged, so retrying a multi-batch request cannot duplicate a stored prefix (#250).
- DeleteTopics keeps the deleted topic's S3 data and records it under `/kafscale/deleted-topics/<topic>/<generation>` in etcd. A topic created again with the same name starts empty at offset 0 under `<namespace>/<topic>@<generation>/`. The snapshot update, offset deletes, record and generation bump commit in one etcd transaction (#262).
- CreateTopics, auto-create on Metadata or Produce, and `kafscale-cli restore` reject topic names that Kafka does not allow. Names may use only `a-z`, `A-Z`, `0-9`, `.`, `_` and `-`, are at most 249 characters, and cannot be `.` or `..`. Rejected creates return `INVALID_TOPIC_EXCEPTION` (17). Auto-create skips invalid names (#262).
- Produce honors positive request timeouts. Kafka response writes have a five-minute overall limit, and persisted Fetch responses return a whole first batch even when it exceeds the requested byte limit (#286).
- Produce returns `REQUEST_TIMED_OUT` (7) instead of `UNKNOWN_SERVER_ERROR` when the request context expires before the append finishes (#263).
- Offset callbacks cannot restore deleted-topic offsets or overwrite a recreated generation. Generation watches resume at their last revision and reload after compaction (#286).

### Consumer groups

- MCP `describe_group` recognizes groups that have committed offsets without joining, matching `list_groups` instead of reporting them as missing. Group descriptions include committed offsets and their metadata, sorted by topic and partition (#337).

- Broker shutdown closes idle Kafka connections and closes active connections after their in-flight response. Clients rediscover their consumer group coordinator instead of continuing to use sockets whose broker has released its coordination leases (#285).
- Concurrent heartbeats on one broker no longer starve offset commits for the same consumer group. Group writes queue independently, and waiting requests respect their deadlines; etcd comparisons still protect against membership changes from other brokers.
- The consumer group retention sweep no longer reads every offset key name to list groups.
  - Brokers now keep an index key per group under `/kafscale/consumer-groups/`, and most sweeps read only the index, page by page.
  - At 1,000 groups with 10 partitions each, a sweep listing reads 2,000 keys instead of 12,000, and no single etcd response holds more than 1,000 keys instead of 11,000.
  - A full scan of the group and offset keys still runs once an hour, and on the first sweep after a broker takes the sweeper lease, to find groups that have only offsets and to add index keys for existing groups.
  - It needs no manual migration, and old and new brokers can run together during a rolling upgrade.
  - `ListGroups` now reads its keys page by page as well (#279).
- The consumer group retention sweep no longer holds the coordinator lock while it deletes a group in etcd, so joins, heartbeats and commits for other groups are not blocked behind it.
  - A group lease the sweep acquired is now released when member expiry fails or live members remain.
  - Every broker expires the Empty groups it already coordinates, without listing groups.
  - Only one broker lists all groups, under the `/kafscale/retention-sweeper` etcd lease, to expire groups no broker coordinates, so the cost of listing all groups is paid once per check interval for the cluster instead of once per broker.
  - The group lease is released after the coordinator lock is dropped, for both retention and DeleteGroups, so a slow etcd release does not block other groups (#263).
- OffsetCommit returns `COORDINATOR_LOAD_IN_PROGRESS` (14) instead of `COORDINATOR_NOT_AVAILABLE` (15) when a commit keeps losing races with other writes to the group, so clients retry without looking up the coordinator again.
  - A member commit that races with the member leaving or a rebalance now returns `UNKNOWN_MEMBER_ID` or `ILLEGAL_GENERATION` instead of storing the offset.
  - A commit with any negative generation is accepted on an Empty group whatever the member ID, as Kafka does.
  - The coordinator commits through the new `metadata.CommitMemberConsumerOffsetIfGroupExists`, which runs the member checks on stores that implement the new `ConsumerGroupMemberOffsetCommitter` interface.
  - Other stores fall back to `CommitConsumerOffsetIfGroupExists`, which keeps its signature and behaviour: `ConsumerGroupOffsetCommitter` if implemented, else `CommitConsumerOffset`, without the member check (#263).
- A member commit that runs out of time or cannot reach etcd (deadline exceeded, canceled, store unavailable, or an etcd error with gRPC code Unavailable or DeadlineExceeded, such as no leader, leader changed or request timed out) now returns `COORDINATOR_LOAD_IN_PROGRESS` (14) instead of `UNKNOWN_SERVER_ERROR` (-1), which Java's commitSync treats as fatal.
  - Permanent etcd errors such as request too large, permission denied and no space still return -1.
  - A commit that loses an etcd race to a membership or generation change waits a short random 1 to 5 ms before retrying.
  - A race with a heartbeat-only write retries after a random 100 to 500 µs, so a commit that keeps losing to heartbeats does not hammer etcd (#263).
- On coordinator takeover, recovered members get the five-second margin plus a third of their session timeout, which covers how stale the throttled stored heartbeat time can be. The retention sweep uses the same margin before it treats stored members as dead (#263).
- Heartbeats no longer rewrite the consumer group in etcd on every call. The stored heartbeat time is refreshed at most once per third of the session timeout, which keeps coordinator takeover and retention correct. A member commit is only retried against its limit when the group generation or member set changes, so heartbeats from other members no longer make commits fail with `COORDINATOR_LOAD_IN_PROGRESS` (#263).
- The broker and proxy advertise DescribeGroups versions 0 to 5, and the proxy advertises ListGroups versions 0 to 5. librdkafka before 2.3, used by older confluent-kafka-python releases, asks for DescribeGroups up to version 4 and failed `describe_consumer_groups` with `_UNSUPPORTED_FEATURE` (#263).
- Expire Empty consumer groups and their committed offsets after seven days by default. Configure `KAFSCALE_OFFSETS_RETENTION_MINUTES` or `spec.config.offsetsRetentionMinutes` on KafscaleCluster. Zero disables expiry in both; the environment variable also treats a negative value as disabled, and the CRD rejects negative values. Joins and commits reset the retention clock, and leased, revision-fenced deletion protects active groups and concurrent writes (#238).
- Remove members that disconnect during a pending JoinGroup through the broker or proxy from the rebalance and persisted group state, so survivors can receive their partitions without waiting for the dead member's session timeout (#251).
- Consumer groups remain visible as `Empty` after their last member leaves or expires, with committed offsets preserved. Deleting a group with active members returns `NON_EMPTY_GROUP` (68) without changing its members or offsets (#230, #236).
- DeleteGroups acquires the group coordinator lease before expiring stale stored members after a coordinator crash. Other brokers return `NOT_COORDINATOR` without caching or changing the group. Groups with a recent heartbeat remain protected by `NON_EMPTY_GROUP` (#237).
- The proxy routes ListOffsets and OffsetForLeaderEpoch to the broker that owns each partition, like Produce and Fetch. Before, it sent them to one broker, so consumers through the proxy that start from the earliest offset stalled on partitions that broker did not own. When a broker answers `NOT_LEADER_OR_FOLLOWER`, the proxy reads the partition owner from etcd again and retries, up to 3 attempts in total. When no partition in the request has a known owner, the proxy sends the request to one broker as before (#355).
- The proxy starts its `/livez` and `/readyz` server before it connects to etcd. `/livez` returns 200 from the start, and `/readyz` returns 503 `starting` until the proxy has loaded its routes and opened the Kafka listener. Before, the health server started only after etcd answered, so a first Helm install with the proxy enabled before etcd was up got the pod restarted by its liveness probe (#359).
- The proxy routes DeleteGroups to each group's owner, including multi-group requests. Brokers return the lease owner from FindCoordinator and release group leases after successful deletion, allowing immediate recreation on another broker (#237).
- Advertise Fetch v4-v13 and older supported classic group API versions on the broker and proxy so librdkafka clients negotiate magic-2 records and consumer groups correctly. Hold JoinGroup replies until the known members rejoin or the rebalance deadline expires, and encode empty Fetch partitions with zero-length records. Validate every Produce batch before storage; reject legacy MessageSets with `UNSUPPORTED_VERSION` and incomplete frames with `CORRUPT_MESSAGE` (#240).

### Proxy and etcd outages

- The Kafka listener rejects Produce responses with a mismatched correlation ID instead of accepting an unrelated acknowledgement.
- During an etcd outage the proxy kept using the last broker list from etcd without a time limit, because a failed etcd read was only logged and still counted as a successful refresh.
  - `KAFSCALE_PROXY_BACKEND_CACHE_TTL_SEC` (default 60 seconds) now applies: after a failed read the proxy, including its Kafka listener, readiness check and LFS HTTP API, uses the brokers, routes and metadata it last read until that time has passed since the last successful read.
  - After that it reports NotReady, and its Kafka listener answers no Metadata or FindCoordinator request and routes no request to a broker, not even over a broker connection that is already open.
  - Each such request gets the reply the proxy already sends while it is not ready: `REQUEST_TIMED_OUT`, which clients retry, on the response or on each topic, partition, group or resource the request names.
  - A Metadata request for all topics, which `kcat -L` and most clients send at startup, names none, so it gets an empty reply with no brokers, no topics and no error code, and clients treat it as failed metadata and retry.
  - A request type with no not-ready reply gets no answer. The client connection is then closed. ApiVersions is still answered.
  - Before rejecting a request the proxy tries to read etcd again, so it serves again as soon as etcd answers.
  - The LFS HTTP API needs etcd for every request: `/lfs/produce` and `/lfs/uploads` read the topic generation from etcd each time, so they return 503 `metadata_unavailable` as soon as etcd cannot be read, even within the TTL.
  - Past the TTL they send nothing to a broker, and `/lfs/produce` checks the TTL again after the upload, right before the record is sent, so an upload that outlasts it is deleted and gets 503.
  - The limit does not apply when `KAFSCALE_PROXY_BACKENDS` lists static brokers.
  - This is on purpose: when etcd is down, the cluster has to stop to avoid data loss, so past the TTL the proxy stops instead of letting clients write through stale routing (#318).
- Stop Fetch long-polls when a client disconnects, including through the proxy. Bound Kafka response writes to 30 seconds by default with `KAFSCALE_RESPONSE_WRITE_TIMEOUT_MS`; timed-out writes close the socket and preserve the already-read request drain (#257).
- Kafka response writes set the write deadline for each 256 KiB chunk instead of once for the whole frame, so a large response to a slow but live client is not cut off. After a write fails, the broker and the proxy stop writing to that connection instead of logging one error for each queued response (#263).
- The proxy chart renders `proxy.responseWriteTimeoutMs`, `proxy.lfsMetadataTimeoutMs` and `proxy.security.handshakeTimeoutMs` as plain integers. Values of 1000000 or more were rendered as `1.2e+06`, so the proxy logged a warning and used the default (#263).
- Helm rejects handshake deadlines outside 1..60000; 0 or an empty value selects the default (#286).
- The proxy chart omits `KAFSCALE_PROXY_TLS_HANDSHAKE_TIMEOUT_MS` when `proxy.security.handshakeTimeoutMs` is empty or null. It rendered `"0"`, which the proxy rejects at startup, so TLS proxies with an empty value failed to start (#278).
- The Helm chart can set `proxy.responseWriteTimeoutMs` and `proxy.lfsMetadataTimeoutMs`. The proxy logs a warning when an integer environment setting is not a number. The KafscaleCluster CRD rejects a negative `offsetsRetentionMinutes` (#263).

### LFS proxy and SDKs

- The proxy tracks LFS uploads whose Kafka publication fails, including broker connection failures returned as per-partition errors. Tracking follows the final result for each topic and partition, so successful records in a mixed request are excluded. Objects with uncertain acknowledgements are retained because Kafka may already reference them (#322).
- Kafka LFS produces with `acks=0` forward the rewritten envelope instead of an empty frame and track uploads after connection or write failures.
- The LFS HTTP API waits for the broker's full 15-second Produce timeout plus the configured I/O budget (5 seconds by default) after sending the request. The previous shared 5-second write/read deadline could fail a request before the broker's timeout. Earlier caller deadlines and cancellation still stop the exchange.
- The Node JavaScript LFS resolver now validates SHA-256 instead of silently returning corrupted S3 objects. Default and explicit validation reject mismatches; valid objects and validation-disabled behavior are preserved (#297).
- The Java and browser LFS envelope decoders now reject an envelope with an empty `sha256`, like the Node, Python and Go decoders, before any S3 read. The proxy never writes an empty `sha256`. A hand written envelope with an empty `sha256` failed before as a checksum mismatch, except when `checksum` held the correct SHA-256 and `checksum_alg` was missing or `sha256`; such an envelope resolved in Java and the browser and now fails with an invalid envelope error. Write the SHA-256 to `sha256` as well (#302).
- The operator managed LFS proxy (`spec.lfsProxy`) now starts with the S3 secret format the brokers use.
  - It read only the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` keys of `spec.s3.credentialsSecretRef`, so a secret with `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` kept its pods in `CreateContainerConfigError`.
  - It now accepts either pair, and the `AWS_` pair keeps priority when both are present, so existing secrets work unchanged.
  - A secret without either pair no longer blocks the pod; the proxy then uses the default AWS credential chain.
  - The operator already set the env names the proxy reads, such as `KAFSCALE_PROXY_ADDR`; the note in `docs/operations.md` that said otherwise is corrected.
  - `make lfs-demo` now uses `spec.lfsProxy` instead of its own proxy deployment, with `spec.lfsProxy.backends` set to the broker service because the demo brokers advertise a host port forward address.
  - The proxy is `kafscale-lfs-proxy`, its objects go to the `kafscale-snapshots` bucket, and `lfs-demo-run` is an alias for `lfs-demo-up`.
  - `lfs-demo-up` no longer reads the kubeconfig before the cluster exists (#304).
- The operator-managed LFS proxy now forwards `AWS_SESSION_TOKEN` with the AWS key pair, so temporary AWS credentials work when that pair takes priority over `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY`. The KafScale fallback keeps its own session token, and permanent AWS keys do not borrow it. Startup logs show a boolean `session_token` flag for static keys without logging the token; the default chain resolves credentials later and omits the flag (#307).
- The LFS proxy HTTP API (`/lfs/produce` and `/lfs/uploads`) now works without `spec.lfsProxy.backends`.
  - It only used that static list, so without it every produce returned 503 "no backends available".
  - It now uses the brokers registered in etcd, as the Kafka listener of the proxy already did.
  - When etcd lists no brokers, there is no broker; only when etcd cannot be read does it keep using the brokers it last found, for as long as `spec.lfsProxy.backendCacheTTLSeconds` (default 60 seconds).
  - Brokers in `spec.lfsProxy.backends` or `KAFSCALE_PROXY_BACKENDS` still take priority.
  - It also left the uploaded object in S3 on that failure.
  - It now connects to a broker before it uploads and returns 503 `backend_unavailable` without uploading when it cannot reach one, for example when etcd still lists brokers that were scaled down.
  - An object uploaded before a broker went away is deleted.
  - With brokers from etcd, `/lfs/produce` and multipart completion send the produce to the broker that owns the partition, using the same partition routes as the Kafka listener, and reuse an idle broker connection only when it goes to that broker.
  - When the owner is not known yet, they pick one of the listed brokers.
  - When a broker answers that it does not lead the partition (`NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE`, `FENCED_LEADER_EPOCH` or `UNKNOWN_LEADER_EPOCH`), the proxy sends the produce to the current owner, to at most 3 brokers in total, and checks the backend cache TTL before each new attempt.
  - If no broker accepts it, the object is deleted and the request gets 503 `backend_unavailable`; before, the first broker's rejection gave 502 and left the object as an orphan.
  - With static backends (`spec.lfsProxy.backends` or `KAFSCALE_PROXY_BACKENDS`) the produce also goes to the partition owner from the etcd routes when it is known. The listed brokers are used when the owner is not known yet or its address cannot be dialed, for example when brokers advertise a host port forward. An owner address that fails to dial is skipped for 30 seconds. Before, setting static backends turned owner routing off, so with a broker Service as the only address about half of the produces got 503 (#356).
  - With only one address, for example a broker Service, and no reachable owner, there is no other broker to try, so a non-owner answer gives 503 `backend_unavailable` and the object is deleted.
  - When the produce request was already sent and then fails for another reason, the object is still kept and counted as an orphan, because the broker may have written the record.
  - A multipart upload (`/lfs/uploads`) without a reachable broker now fails at the start instead of at completion, so no multipart upload is left open in S3 (#318).
- The Node, browser, Python and Java LFS resolvers honor `checksum_alg`. Valid md5 and crc32 objects resolve instead of failing with a checksum mismatch; they are validated against the envelope `sha256` field, so corrupt objects are still rejected. An unknown `checksum_alg` fails resolution when validation is enabled (#297).
- When the LFS proxy cannot delete uploads it discards after a failed produce rewrite, the orphan reason now names the failure, such as `checksum_mismatch_delete_failed` or `blob_too_large_delete_failed`, instead of always `generation_changed_delete_failed` (#263).
- When one LFS record in a produce request fails, the proxy deletes the objects it already uploaded for earlier records in that request (#263).
- LFS proxy multipart uploads (`/lfs/uploads`) work across replicas and pod restarts.
  - Sessions, progress, and authoritative checksum state live in etcd; S3 remains untrusted blob storage.
  - Multipart uploads require `KAFSCALE_PROXY_ETCD_ENDPOINTS`, even with static brokers; operator and Docker Compose deployments already configure it.
  - State-store failures return 503 `session_store_unavailable`, and stale writes return 409 `upload_conflict`.
  - Restart uploads from unsigned S3 sessions or the earlier development etcd format without durable part hashes (410 `upload_restart_required`).
  - Expiry cleanup uses persisted session deadlines, preserves live uploads through TTL changes, and performs no orphan abort when etcd cannot confirm ownership.
  - Cleanup needs `s3:ListBucketMultipartUploads` and `s3:AbortMultipartUpload` (#321).
- Multipart retries bind each part's SHA-256 identity before sending it to S3. Retries, including those after timeouts or acknowledgements, must use identical bytes; conflicting bytes return 409 `part_conflict`. Failed S3 attempts do not advance the aggregate checksum. Lock waits and lease cleanup remain bounded after cancellation (#321).
- Multipart completion validates every uploaded part exactly once in order and checks the expected checksum before S3 finalization. A lost successful S3 completion response can be recovered by checking the existing object at its unique key (requires `s3:GetObject`). Durable publication/discard markers prevent stale cleanup from deleting a possibly Kafka-referenced blob. Envelope publication retains ordinary at-least-once retry semantics; ambiguous Kafka acknowledgements may produce duplicate envelopes (#321, #323).

### Operator and S3 credentials

- The operator sets an `S3Credentials` condition on each KafscaleCluster.
  - It is `False`, with a warning in the operator log once per change, when `spec.s3.credentialsSecretRef` names a secret that is missing, cannot be read or holds neither the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` pair nor the `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` pair.
  - The message says what that means: no static keys from the secret and the default AWS credential chain if one is available; for a secret with only part of a pair, the keys found and that the etcd snapshot jobs can fail instead of falling back; for a secret the operator cannot read, only that it could not be verified.
  - It is also `False`, with reason `SecretKeysMixed`, when the secret holds the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` pair plus `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` values that do not form a complete pair, which every workload ignores (#312), and when it holds the `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` pair and `AWS_SESSION_TOKEN` but no `KAFSCALE_S3_SESSION_TOKEN`, because the etcd snapshot jobs then use a token from the other pair.
  - Nothing else changes: pods still start, the fallback stays, and a missing secret still fails the managed etcd snapshot check as before.
  - A change to that secret now triggers a reconcile.
  - When reconciling the Kafka NetworkPolicy, the managed etcd, or the broker, service, LFS proxy or HPA objects fails, the operator now still writes the cluster status, so the `S3Credentials` condition and any etcd snapshot conditions from that reconcile are kept.
  - The `Ready` condition is not changed on that path.
  - Each retry of that status write checks the KafscaleCluster UID and spec generation, so an old reconcile cannot write stale conditions onto a recreated cluster or a newer spec, while unrelated concurrent conditions are kept.
  - The `snapshot-download` init container of the managed etcd now reads the whole secret, as the snapshot upload job already did, so it accepts the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` pair as well; `KAFSCALE_S3_ACCESS_KEY`, `KAFSCALE_S3_SECRET_KEY` and `KAFSCALE_S3_SESSION_TOKEN` keep priority.
  - Before, a secret with only the `AWS_` pair gave it no credentials, so restoring a snapshot failed.
  - When `spec.s3.credentialsSecretRef` is set, this changes the managed etcd pod template, so the operator restarts the managed etcd pods once after the upgrade.
  - The broker StatefulSet and the LFS proxy Deployment pod templates now carry a `kafscale.io/s3-credentials-checksum` annotation computed over the credential keys present in the secret (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `KAFSCALE_S3_ACCESS_KEY`, `KAFSCALE_S3_SECRET_KEY`, `KAFSCALE_S3_SESSION_TOKEN`).
  - When `spec.s3.credentialsSecretRef` is set, brokers and the LFS proxy roll once after the upgrade and roll again whenever those keys change, so running pods pick up rotated or fixed keys.
  - Other keys in the secret do not roll them, and clusters without a `credentialsSecretRef` see no template change.
  - The schema registry from the Helm chart does not roll when the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` keys in the Secret named by `schemaRegistry.s3.credentialsSecret` change, and it keeps using the old keys until it restarts. Once the old keys are revoked or expire, every request that has to read or write S3 fails with HTTP 500 and error code 50001. Update the Secret, restart the registry, check the rollout, and only then revoke the old keys. The chart passes no session token to the registry, so temporary credentials from that Secret are not supported. Pointing `schemaRegistry.s3.credentialsSecret` at a different Secret changes the pod template and rolls it. docs/operations.md and docs/schema-registry.md show how to find the Deployment by its labels and restart it (#361).
  - This rollout also runs when the managed etcd snapshot check fails: once the broker StatefulSet exists, the operator still applies the broker and LFS proxy workloads, with every broker and LFS proxy spec change (for example image, replicas and resources), not only credential changes, and publishes the matching cluster metadata to etcd, so a scale-down does not leave removed brokers as partition leaders.
  - It then sets `Ready` to `False` with `SnapshotAccessFailed` and retries.
  - This is a behavior change: in earlier releases a failed snapshot check held back all broker and LFS proxy workload updates, so pods also kept old, possibly revoked keys.
  - For a new cluster, the cluster controller still creates no brokers or LFS proxy and publishes no metadata until the check passes, but the topic controller still publishes the cluster metadata, topics included, for each KafscaleTopic that references it.
  - `Ready` becomes `True` only after the check passes.
  - The operator's managed etcd snapshot check now uses `KAFSCALE_S3_SESSION_TOKEN` with the `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` pair whenever the secret holds that key, even with an empty value, as the snapshot jobs do.
  - Before, it fell back to `AWS_SESSION_TOKEN` and could sign the `KAFSCALE_S3_ACCESS_KEY` pair with the other pair's token.
  - The LFS proxy logs at startup whether it uses static keys or the default AWS credential chain, without the values (#307).
- S3 key values that are empty or only whitespace now count as unset in every component, and other values are trimmed.
  - Whitespace here means space, tab, newline, carriage return, vertical tab and form feed, the same set in the Go code and in the snapshot container scripts.
  - Unicode spaces such as U+00A0 are kept as part of the value.
  - This applies to: the operator `S3Credentials` condition and snapshot check, the brokers, the LFS proxy, the schema registry and the etcd snapshot upload and restore containers.
  - Before, the brokers, the LFS proxy fallback keys, the schema registry and the snapshot containers used the raw values.
  - The schema registry and the brokers then signed with a blank or padded key instead of using the default AWS credential chain, and a blank `KAFSCALE_S3_ACCESS_KEY` or `KAFSCALE_S3_SECRET_KEY` next to a valid `AWS_*` pair gave the snapshot containers an empty key while the condition said they used the `AWS_*` pair.
  - The snapshot containers now pick the pair in their script: the `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY` pair when both keys are set, with `KAFSCALE_S3_SESSION_TOKEN` when the secret holds it (a blank value means no token) and `AWS_SESSION_TOKEN` otherwise, or else the `AWS_*` pair with `AWS_SESSION_TOKEN`.
  - A `KAFSCALE_S3_ACCESS_KEY` or `KAFSCALE_S3_SECRET_KEY` that does not complete the pair next to an `AWS_*` pair is now ignored by every workload instead of giving the snapshot containers a mismatched pair; the condition still reports it as `SecretKeysMixed`.
  - A `spec.s3.credentialsSecretRef` that is only whitespace no longer gives the broker and LFS proxy pods a secret reference.
  - The `S3Credentials` messages quote the secret name and key names.
  - Secrets with valid keys keep working as before.
  - In earlier releases the operator snapshot check, the operator checks of the `spec.s3` endpoint fields and `spec.s3.credentialsSecretRef`, and the LFS proxy S3 key, region and endpoint settings trimmed Unicode spaces too.
  - They now keep them, so a value padded with U+00A0 is used as written, as the snapshot containers already did.
  - This changes the managed etcd pod template, which the earlier change in this release already does (#312).
- The managed etcd snapshot-download init container now passes `--cli-connect-timeout 2` and `--cli-read-timeout 5` on AWS CLI calls, sets `AWS_MAX_ATTEMPTS=1`, and bounds each complete `list-objects-v2` lookup to 120 seconds.
  - JSON output selects one newest `.db` snapshot across all pages, including prefixes with accumulated health objects.
  - Failed or timed-out listings are discarded and retried.
  - Exhausting retries without a successful listing fails the init container instead of starting empty.
  - A missing Python decoder or persistent invalid JSON fails the init container instead of treating a decoder failure as an empty bucket.
  - Snapshot copies keep those inactivity deadlines and are not given an absolute transfer deadline.
  - Lookup errors are not treated as snapshot keys.
  - When the bucket has no snapshot the 15-attempt loop still exits 0 (#295).

### Schema registry

- Schema registry pods now notice an etcd snapshot restore and clear their schema cache without a restart. Each pod watches its etcd keys and resets when the watch reports a compaction. The operator now restores etcd snapshots with `etcdutl snapshot restore --bump-revision 1000000000 --mark-compacted`, which triggers this. Manual restores should use the same flags or restart the registry pods. See `docs/schema-registry.md` and `docs/operations.md`.

- The schema registry now shuts down gracefully, so that the rollout restart S3 key rotation needs does not drop requests. Before, it stopped accepting connections at once on SIGTERM while the Service could still send it traffic, which gave refused connections and client timeouts during a restart.
  - On SIGTERM or SIGINT `/readyz` returns 503 at once, the registry keeps serving for `KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_DRAIN` (default `5s`), then waits up to `KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_TIMEOUT` (default `10s`, before it was a fixed 5s) for in-flight requests.
  - New chart values: `schemaRegistry.shutdownDrain`, `schemaRegistry.shutdownTimeout`, `schemaRegistry.preStopSleepSeconds` (default `0`, no hook) and `schemaRegistry.terminationGracePeriodSeconds` (default `30`, the Kubernetes default). Upgrading the chart changes the pod template, so the registry rolls once.
  - See `docs/schema-registry.md#shutdown`.

### Processors

- The Iceberg and SQL processors map segments under `<topic>@<generation>/` to the topic `<topic>` and its table, and keep a separate checkpoint for each generation, so a re-created topic is processed from offset 0 (#262).
- SQL retries generation-client initialization, supports etcd credentials, and reads current broker snapshots alongside the legacy format. Iceberg filters retired generations without checkpoints and releases leases when the discovered generation changes (#286).

### Go API compatibility

- Restore the exact published v1.6.3 `broker.CoordinatorConfig` shape and comparability. The legacy constructor remains supported and deprecated; use `CoordinatorOptions` and `NewGroupCoordinatorWithOptions` for retention and lease hooks. `metadata.Store` retains its original method set; group offset listing is an optional `GroupConsumerOffsetLister` capability with a filtering adapter (#298).

### Console

- Console and MCP readiness now checks configured etcd with a live read and a one-second deadline. Helm probes use `/readyz`, so an unreachable endpoint produces HTTP 503 even with cached metadata; `/healthz` remains a process liveness check (#340).

- Transient cluster-status and LFS-status failures no longer return the authenticated console to the login screen. Only HTTP 401 ends the UI session; HTTP 503 displays the service failure and preserves existing streams (#339).

- The LFS status endpoint remains available when LFS is disabled. The console shows the disabled state without starting LFS data requests or event streams, and closes streams on tab changes and logout. A bundled favicon is served at both `/favicon.ico` and `/ui/favicon.ico` (#339).

- The Helm chart renders `KAFSCALE_CONSOLE_OPERATOR_METRICS_URL` with a numeric port, such as `http://<release>-kafscale-operator-metrics.<namespace>.svc.cluster.local:8080/metrics`. It rendered the port as `%!d(float64=8080)`, so the console etcd snapshot panel showed n/a (#358).

- The operator `EtcdSnapshot` condition and the `kafscale_operator_etcd_snapshot_*` gauges stay current between CronJob changes. The operator watches the snapshot CronJob and reconciles when its status changes, at least once a minute while a managed snapshot CronJob exists, including when snapshots are stale or have never succeeded, and again when the last successful snapshot passes `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_STALE_AFTER_SEC`, so `kafscale_operator_etcd_snapshot_age_seconds` stays current and `kafscale_operator_etcd_snapshot_stale` and the `KafscaleSnapshotStale` alert fire when snapshots stop (#357).

- The console `POST /ui/api/status/topics` and `DELETE /ui/api/status/topics/<topic>` endpoints create and delete topics in etcd through the same metadata path as the broker. They returned `202` and changed nothing. Create takes `{"name","partitions","replication_factor"}` and returns `201`, delete returns `200`, and errors return `400`, `404`, `409` or `503`. The console needs write access to etcd for these calls. Delete also needs `KAFSCALE_S3_NAMESPACE`, or `console.s3Namespace` in the Helm chart, set to the `KafscaleCluster` namespace, so that the deleted topic record points at the brokers' S3 prefix. Without it, delete returns `503` and changes nothing (#362).

- Metrics streams send their first snapshot immediately, then refresh periodically. Previously clients waited for the first refresh interval before receiving a response, which could cause connection timeouts under load.

### CI, tests and demos

- Operator-managed brokers now have an HTTP readiness probe, and broker `/readyz` checks live etcd access as well as S3 health. The operator kind snapshot suite asserts NotReady during an etcd outage, verifies recovery after restore, waits for all expected etcd members and broker readiness before producing, retries its startup smoke test once, and checks backend health after restore. CI now runs this suite with nftables alongside the existing operator envtests. Separate `test-operator-envtest` and `test-operator-kind` Make targets support both paths; envtest setup is pinned to Kubernetes 1.35.0 and fails when assets are unavailable (#341).

- Broker and console tests use isolated random-port etcd fixtures. Test setup no longer terminates arbitrary processes listening on fixed ports.
- `make lfs-demo` no longer loses LFS tracker events (#308).
  - The demo brokers advertised the host port forward address `127.0.0.1:39092`, which the LFS proxy pod cannot reach. The tracker could not create `__lfs_ops_state` or deliver events, and the proxy logged a warning.
  - The brokers now advertise `kafscale-broker.kafscale-demo.svc.cluster.local:9092`.
  - The LFS proxy advertises `127.0.0.1:39092`, and `lfs-client-sdk` `pf-start` forwards that port to the proxy instead of the broker, so host clients connect through the proxy. The proxy no longer logs warnings in this stack.
- CI now runs the browser, Python and Java LFS SDK unit tests as well as the Node ones, and fails when they fail. The Java `LfsProducerIntegrationTest` is excluded in CI because it needs Docker images that CI does not build (#306).
- The CI librdkafka compatibility test retries the apt install of `kcat` and, if apt still fails, runs `kcat` from a pinned container image instead, so an unreachable Ubuntu mirror no longer fails the job (#307).
- The CI librdkafka compatibility test gives its two apt calls one shared 90 second limit (#313, #314, #317).
  - The `kcat` fallback starts in about a minute and a half instead of about ten minutes when the Ubuntu mirror is down.
  - It uses the apt `kcat` only when `kcat -V` runs. A missing or half-installed `kcat`, for example after the limit stops an install, also starts the fallback instead of failing the job.
  - The fallback image is now `ueisele/kcat:1.7.1-librdkafka2.11.1`, pinned by digest, so a fallback run still tests a librdkafka 2.x client (2.11.1, where the apt package has 2.3.0). The step logs the kcat and librdkafka versions and where kcat came from.
- `make lfs-demo` now exists (#302).
  - The LFS SDK kind helpers in `lfs-client-sdk/Makefile` (`run-all`, `lfs-demo-up`) called it, but the root Makefile had no such target.
  - It starts the kind demo stack with one broker, MinIO and the operator managed LFS proxy described in the #304 entry below.
  - `KAFSCALE_KIND_KUBE_PROXY_MODE=nftables` creates the kind cluster with the nftables kube-proxy mode, for hosts without iptables support. Leave it unset for the kind default.
  - `lfs-client-sdk/Makefile` now creates one kubeconfig file per run and passes it to the make calls it starts. Before, every use made a new empty file, so `pf-start` and `wait-http` could not reach the cluster.

## Upgrade notes

The full guide is [upgrade and compatibility guide](../upgrade-compatibility.md).
It covers Go API migration, the deprecation policy, CRD rollout, persisted data,
rollback limits and the tested compatibility scope. The points below are the
ones most likely to affect you.

### Behavior changes

- **Proxy stops past the etcd TTL.** When etcd cannot be read for longer than
  `KAFSCALE_PROXY_BACKEND_CACHE_TTL_SEC` (default `60`, set with
  `spec.lfsProxy.backendCacheTTLSeconds` or the chart value
  `proxy.backendCacheTTLSeconds`), the proxy goes NotReady, answers Kafka
  requests with `REQUEST_TIMED_OUT`, closes client connections and keeps
  answering ApiVersions. The LFS HTTP API returns 503 `metadata_unavailable` as
  soon as etcd cannot be read. Before this release the proxy kept routing on
  stale metadata with no time limit. See
  [Proxy behavior when etcd is down](../operations.md#proxy-behavior-when-etcd-is-down).
- **LFS produce returns 503 instead of 502.** `/lfs/produce` and multipart
  completion return 503 `backend_unavailable` and delete the uploaded object when
  no broker can be reached, when the etcd TTL has passed, or when the brokers it
  tries, at most 3 (`lfsProduceAttempts`), all answer that they do not lead the
  partition.
  - Any other broker rejection still returns 502 (403 for authorization errors),
    and the object is kept as an orphan.
  - Clients that treat 502 as final should also retry on 503 (#318).
- **Segment uploads are create-only.** Brokers write segments with `PutObject`
  and `If-None-Match: *`, so a broker that lost its partition lease cannot
  overwrite data written by the new owner (#324). The S3 backend must support
  conditional writes. See [Supported storage](../supported.md#conditional-writes).
- **LFS multipart sessions live in etcd.** Uploads work across proxy replicas
  and restarts. The orphan sweep needs `s3:ListBucketMultipartUploads` and
  `s3:AbortMultipartUpload` (#321).

- **Schema registry takes longer to stop.** On SIGTERM it now keeps serving
  for 5 seconds with `/readyz` failing before it stops, and gives in-flight
  requests up to 10 seconds instead of 5. Set
  `KAFSCALE_SCHEMA_REGISTRY_SHUTDOWN_DRAIN=0s` for the old immediate stop. See
  [Shutdown](../schema-registry.md#shutdown).

### Deprecations

- `broker.CoordinatorConfig` and `broker.NewGroupCoordinator` are deprecated.
  Use `CoordinatorOptions` with `NewGroupCoordinatorWithOptions`. Both old APIs
  still work in this release and no removal version is set. See
  [Go API migration](../upgrade-compatibility.md#go-api-migration).
- `KAFSCALE_OPERATOR_ETCD_DEFRAG_SCHEDULE` and
  `KAFSCALE_OPERATOR_ETCD_DEFRAG_ENABLED` remain deprecated aliases of
  `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_SCHEDULE` and
  `KAFSCALE_OPERATOR_ETCD_MAINTENANCE_ENABLED`. They are not new in this
  release.

No exported Go API was removed or changed incompatibly since v1.6.3.
No release version is selected yet.
