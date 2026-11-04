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

- New `kafscale-schema-registry` service, deployed with `schemaRegistry.enabled` in the Helm chart. It serves the Confluent Schema Registry REST API for Avro schemas, keeps its state in etcd, and stores schema text above 64 KiB in S3. Several replicas can serve writes at the same time: ids are unique and increase, and the same schema always gets the same id. Schemas are stored and deduplicated in the form Confluent Schema Registry 8.4.0 stores them, and `normalize=true` is supported. It supports all seven Avro compatibility levels, `READWRITE`, `READONLY` and `IMPORT` modes, references, and soft and hard deletes. A hard delete keeps the schema id. Responses carry the Confluent schema `guid`, and `GET /schemas/guids/{guid}` serves clients that send it in a record header. With S3 configured, every issued id is recorded in S3 and never given out again, also after an etcd restore. It has no authentication. Tested with the Java `KafkaAvroSerializer` and `KafkaAvroDeserializer` 8.4.0, confluent-kafka-python 2.16.1 and franz-go `pkg/sr` 1.8.0, and with data written through Confluent Schema Registry 8.4.0 and imported with its ids. See `docs/schema-registry.md`, which also covers using an existing registry instead (#154).

- A registry run from a main build between #277 and this release stored two kinds of schema in an older form: double defaults below about 2.2e-308, such as `5e-324`, and, with `normalize=true`, a nested type with `"namespace":""`. Lookups and registers in the same subject still find them and keep their ids and versions. Registering such a schema in another subject gives it a new id, and its `guid` stays the one computed from the stored text. Schemas written by such a build are compared through the slower path on every lookup or register in their subject that does not match the stored hash; schemas written by this release record the form they were stored in and skip it.

- The built in schema registry supports Protobuf schemas, with references and imports, the well known types, all seven compatibility levels with the Confluent Schema Registry 8.4.0 rules, `normalize=true`, and `format=serialized` for clients that fetch descriptors, and base64 `FileDescriptorProto` input on register and lookup, which the confluent-kafka-python `ProtobufSerializer` sends. `kafscale-cli schema import` copies Protobuf subjects with their ids. Tested with the Java `KafkaProtobufDeserializer` 8.4.0, confluent-kafka-python 2.16.1 and franz-go `pkg/sr` 1.8.0 on records written through Confluent Schema Registry 8.4.0 and imported. A proto3 enum whose first value is not zero is rejected; see `docs/schema-registry.md` for the other differences.

- `schemaregistry.Error` in the Go package has an unexported field for error details, so two `Error` values can no longer be compared with `==`. Compare the `Code` field or use `errors.As` instead.

- `kafscale-cli schema import --from <url>` copies subjects, versions, ids, references, soft deletes, compatibility levels and subject modes from Confluent Schema Registry or another registry with its REST API into the built in registry. It keeps the ids, can be run again, and fails on any id or version that the target holds with a different schema. Subjects in a non-default Confluent context are skipped with a message and make the run exit with an error. A failed run still prints its counts and whether the target is in `IMPORT` mode. With S3 configured, an import may take over an id that S3 recorded for a failed write when etcd has no schema with that id; it raises the id counter first so the failed write cannot finish later (#154).

- `kafscale-cli prune` lists the S3 data that deleted topics keep and deletes it with `--force`. It filters by `--topic` and `--older-than`, deletes only partition segment and index files, refuses live topics and the current generation, fails when the bucket does not exist, and is safe while brokers run. Access is controlled by the etcd and S3 credentials. It does not cover LFS objects or topics deleted before the upgrade (#262).

- The LFS proxy writes new objects under their own prefix, `<namespace>/@lfs/<topic>/` or `<namespace>/@lfs/<topic>@<N>/` after the Nth delete, instead of inside the topic's segment folder. Objects written before stay where they are and remain readable, because the envelope stores the full key. When etcd is configured, LFS produce needs it reachable: the proxy looks up the topic generation once per topic before any upload, with a timeout set by `KAFSCALE_LFS_PROXY_METADATA_TIMEOUT_MS` (default 3000 ms), and returns 503 `metadata_unavailable` or a produce error if the lookup fails. It checks the generation again after the upload and right before the produce, and if the topic was recreated in between it deletes the uploaded objects and fails the same way instead of producing. `/lfs/download` only accepts keys under `<namespace>/@lfs/` or the old `<namespace>/<topic>/lfs/` shape.

## Bug fixes

- Concurrent heartbeats on one broker no longer starve offset commits for the same consumer group. Group writes queue independently, and waiting requests respect their deadlines; etcd comparisons still protect against membership changes from other brokers.

- Protobuf descriptor printing uses `jhump/protoreflect/v2`, removing the v1 dependency. Map-entry repair rejects references to entries in the wrong scope instead of silently rewriting them (#284).

- The consumer group retention sweep no longer reads every offset key name to list groups. Brokers now keep an index key per group under `/kafscale/consumer-groups/`, and most sweeps read only the index, page by page. At 1,000 groups with 10 partitions each, a sweep listing reads 2,000 keys instead of 12,000, and no single etcd response holds more than 1,000 keys instead of 11,000. A full scan of the group and offset keys still runs once an hour, and on the first sweep after a broker takes the sweeper lease, to find groups that have only offsets and to add index keys for existing groups. It needs no manual migration, and old and new brokers can run together during a rolling upgrade. `ListGroups` now reads its keys page by page as well (#279).

- Schema registry pods now notice an etcd snapshot restore and clear their schema cache without a restart. Each pod watches its etcd keys and resets when the watch reports a compaction. The operator now restores etcd snapshots with `etcdutl snapshot restore --bump-revision 1000000000 --mark-compacted`, which triggers this. Manual restores should use the same flags or restart the registry pods. See `docs/schema-registry.md` and `docs/operations.md`.

- The consumer group retention sweep no longer holds the coordinator lock while it deletes a group in etcd, so joins, heartbeats and commits for other groups are not blocked behind it. A group lease the sweep acquired is now released when member expiry fails or live members remain. Every broker expires the Empty groups it already coordinates, without listing groups. Only one broker lists all groups, under the `/kafscale/retention-sweeper` etcd lease, to expire groups no broker coordinates, so the cost of listing all groups is paid once per check interval for the cluster instead of once per broker. The group lease is released after the coordinator lock is dropped, for both retention and DeleteGroups, so a slow etcd release does not block other groups (#263).

- OffsetCommit returns `COORDINATOR_LOAD_IN_PROGRESS` (14) instead of `COORDINATOR_NOT_AVAILABLE` (15) when a commit keeps losing races with other writes to the group, so clients retry without looking up the coordinator again. A member commit that races with the member leaving or a rebalance now returns `UNKNOWN_MEMBER_ID` or `ILLEGAL_GENERATION` instead of storing the offset. A commit with any negative generation is accepted on an Empty group whatever the member ID, as Kafka does. The coordinator commits through the new `metadata.CommitMemberConsumerOffsetIfGroupExists`, which runs the member checks on stores that implement the new `ConsumerGroupMemberOffsetCommitter` interface. Other stores fall back to `CommitConsumerOffsetIfGroupExists`, which keeps its signature and behaviour: `ConsumerGroupOffsetCommitter` if implemented, else `CommitConsumerOffset`, without the member check (#263).
- A member commit that runs out of time or cannot reach etcd (deadline exceeded, canceled, store unavailable, or an etcd error with gRPC code Unavailable or DeadlineExceeded, such as no leader, leader changed or request timed out) now returns `COORDINATOR_LOAD_IN_PROGRESS` (14) instead of `UNKNOWN_SERVER_ERROR` (-1), which Java's commitSync treats as fatal. Permanent etcd errors such as request too large, permission denied and no space still return -1. A commit that loses an etcd race to a membership or generation change waits a short random 1 to 5 ms before retrying. A race with a heartbeat-only write retries after a random 100 to 500 µs, so a commit that keeps losing to heartbeats does not hammer etcd (#263).
- On coordinator takeover, recovered members get the five-second margin plus a third of their session timeout, which covers how stale the throttled stored heartbeat time can be. The retention sweep uses the same margin before it treats stored members as dead (#263).
- Heartbeats no longer rewrite the consumer group in etcd on every call. The stored heartbeat time is refreshed at most once per third of the session timeout, which keeps coordinator takeover and retention correct. A member commit is only retried against its limit when the group generation or member set changes, so heartbeats from other members no longer make commits fail with `COORDINATOR_LOAD_IN_PROGRESS` (#263).

- The broker and proxy advertise DescribeGroups versions 0 to 5, and the proxy advertises ListGroups versions 0 to 5. librdkafka before 2.3, used by older confluent-kafka-python releases, asks for DescribeGroups up to version 4 and failed `describe_consumer_groups` with `_UNSUPPORTED_FEATURE` (#263).

- Produce and Fetch return `KAFKA_STORAGE_ERROR` (56) instead of `UNKNOWN_SERVER_ERROR` (-1) when S3 is unavailable, when a write or read to S3 fails, and when a partition cannot be restored from S3. Clients such as librdkafka now retry instead of failing the message. Degraded S3 still returns `REQUEST_TIMED_OUT` (7). Produce v8 and later include a message such as "object storage unavailable, retry later" (#255). Produce below v4 and Fetch v5 or below receive `NOT_LEADER_OR_FOLLOWER` (6) instead, as Kafka does for versions that predate the storage error. ListOffsets returns `KAFKA_STORAGE_ERROR` when an earliest lookup cannot load the partition from S3. Produce append errors that do not come from S3 still return `UNKNOWN_SERVER_ERROR`.

- Expire Empty consumer groups and their committed offsets after seven days by default. Configure `KAFSCALE_OFFSETS_RETENTION_MINUTES` or `spec.config.offsetsRetentionMinutes` on KafscaleCluster. Zero disables expiry in both; the environment variable also treats a negative value as disabled, and the CRD rejects negative values. Joins and commits reset the retention clock, and leased, revision-fenced deletion protects active groups and concurrent writes (#238).

- Remove members that disconnect during a pending JoinGroup through the broker or proxy from the rebalance and persisted group state, so survivors can receive their partitions without waiting for the dead member's session timeout (#251).

- Store every batch in a partition's Produce request together. Failed writes leave buffered data and offsets unchanged, so retrying a multi-batch request cannot duplicate a stored prefix (#250).

- Consumer groups remain visible as `Empty` after their last member leaves or expires, with committed offsets preserved. Deleting a group with active members returns `NON_EMPTY_GROUP` (68) without changing its members or offsets (#230, #236).

- DeleteGroups acquires the group coordinator lease before expiring stale stored members after a coordinator crash. Other brokers return `NOT_COORDINATOR` without caching or changing the group. Groups with a recent heartbeat remain protected by `NON_EMPTY_GROUP` (#237).

- The proxy routes DeleteGroups to each group's owner, including multi-group requests. Brokers return the lease owner from FindCoordinator and release group leases after successful deletion, allowing immediate recreation on another broker (#237).

- Advertise Fetch v4-v13 and older supported classic group API versions on the broker and proxy so librdkafka clients negotiate magic-2 records and consumer groups correctly. Hold JoinGroup replies until the known members rejoin or the rebalance deadline expires, and encode empty Fetch partitions with zero-length records. Validate every Produce batch before storage; reject legacy MessageSets with `UNSUPPORTED_VERSION` and incomplete frames with `CORRUPT_MESSAGE` (#240).

- Stop Fetch long-polls when a client disconnects, including through the proxy. Bound Kafka response writes to 30 seconds by default with `KAFSCALE_RESPONSE_WRITE_TIMEOUT_MS`; timed-out writes close the socket and preserve the already-read request drain (#257).

- DeleteTopics keeps the deleted topic's S3 data and records it under `/kafscale/deleted-topics/<topic>/<generation>` in etcd. A topic created again with the same name starts empty at offset 0 under `<namespace>/<topic>@<generation>/`. The snapshot update, offset deletes, record and generation bump commit in one etcd transaction (#262).

- CreateTopics, auto-create on Metadata or Produce, and `kafscale-cli restore` reject topic names that Kafka does not allow. Names may use only `a-z`, `A-Z`, `0-9`, `.`, `_` and `-`, are at most 249 characters, and cannot be `.` or `..`. Rejected creates return `INVALID_TOPIC_EXCEPTION` (17). Auto-create skips invalid names (#262).

- The Iceberg and SQL processors map segments under `<topic>@<generation>/` to the topic `<topic>` and its table, and keep a separate checkpoint for each generation, so a re-created topic is processed from offset 0 (#262).

- Kafka response writes set the write deadline for each 256 KiB chunk instead of once for the whole frame, so a large response to a slow but live client is not cut off. After a write fails, the broker and the proxy stop writing to that connection instead of logging one error for each queued response (#263).
- The proxy chart renders `proxy.responseWriteTimeoutMs`, `proxy.lfsMetadataTimeoutMs` and `proxy.security.handshakeTimeoutMs` as plain integers. Values of 1000000 or more were rendered as `1.2e+06`, so the proxy logged a warning and used the default (#263).
- The proxy chart omits `KAFSCALE_PROXY_TLS_HANDSHAKE_TIMEOUT_MS` when `proxy.security.handshakeTimeoutMs` is empty or null. It rendered `"0"`, which the proxy rejects at startup, so TLS proxies with an empty value failed to start.
- When the LFS proxy cannot delete uploads it discards after a failed produce rewrite, the orphan reason now names the failure, such as `checksum_mismatch_delete_failed` or `blob_too_large_delete_failed`, instead of always `generation_changed_delete_failed` (#263).

- The Helm chart can set `proxy.responseWriteTimeoutMs` and `proxy.lfsMetadataTimeoutMs`. The proxy logs a warning when an integer environment setting is not a number. The KafscaleCluster CRD rejects a negative `offsetsRetentionMinutes` (#263).

- Produce returns `REQUEST_TIMED_OUT` (7) instead of `UNKNOWN_SERVER_ERROR` when the request context expires before the append finishes (#263).

- When one LFS record in a produce request fails, the proxy deletes the objects it already uploaded for earlier records in that request (#263).

## Upgrade notes

- Building from source requires Go 1.26 or newer, as required by the maintained Protobuf reflection library.

- Empty groups written by older brokers receive a full retention period from first discovery. Apply the updated KafscaleCluster CRD before setting `spec.config.offsetsRetentionMinutes`; omit it for the seven-day default or set it to `0` to disable automatic expiry.

- During a rolling upgrade, deleting a group through an older broker removes its metadata but leaves its offsets. Upgraded brokers discover those offsets and list the group again as `Empty`. After all brokers are upgraded, delete the inactive group again to remove its retained offsets.

- Members recorded by older brokers without heartbeat timestamps receive one local session timeout to reconnect after coordinator takeover. They are not expired immediately during a rolling upgrade.

- On coordinator takeover, recovered heartbeat times receive a five-second expiry margin for clock skew. Locally received heartbeats return to the configured session timeout.

- Topics that already exist with names outside the allowed characters, such as names with `@` or `/`, keep loading and accept Produce and Fetch as before. Only new creates are rejected. A topic named `<name>@<N>` shares an S3 prefix with generation N of `<name>`, so do not delete and re-create `<name>` while it exists. Move its data to a topic with a valid name and delete it first.

- Schema registry rollback: rolling back to a main build from between #277 and #282 registers duplicate versions for 5 of the 54 edge schemas in our upgrade test. These are the Avro schemas with the `5e-324` and `1e-323` double defaults, registered with and without `normalize=true`, and the schema with an empty nested namespace registered with `normalize=true`. Those builds do not match the hashes that #282 stores, so re-registering one of these schemas creates a new version and id. Rolling back to the #282 review build 035b209 is safe.
