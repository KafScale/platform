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

- `kafscale-cli prune` lists the S3 data that deleted topics keep and deletes it with `--force`. It filters by `--topic` and `--older-than`, deletes only partition segment and index files, refuses live topics and the current generation, fails when the bucket does not exist, and is safe while brokers run. Access is controlled by the etcd and S3 credentials. It does not cover LFS objects or topics deleted before the upgrade (#262).

- The LFS proxy writes new objects under their own prefix, `<namespace>/@lfs/<topic>/` or `<namespace>/@lfs/<topic>@<N>/` after the Nth delete, instead of inside the topic's segment folder. Objects written before stay where they are and remain readable, because the envelope stores the full key. When etcd is configured, LFS produce needs it reachable: the proxy looks up the topic generation once per topic before any upload, with a timeout set by `KAFSCALE_LFS_PROXY_METADATA_TIMEOUT_MS` (default 3000 ms), and returns 503 `metadata_unavailable` or a produce error if the lookup fails. It checks the generation again after the upload and right before the produce, and if the topic was recreated in between it deletes the uploaded objects and fails the same way instead of producing. `/lfs/download` only accepts keys under `<namespace>/@lfs/` or the old `<namespace>/<topic>/lfs/` shape.

## Bug fixes

- The consumer group retention sweep no longer holds the coordinator lock while it deletes a group in etcd, so joins, heartbeats and commits for other groups are not blocked behind it. A group lease the sweep acquired is now released when member expiry fails or live members remain. Every broker expires the Empty groups it already coordinates, without listing groups. Only one broker lists all groups, under the `/kafscale/retention-sweeper` etcd lease, to expire groups no broker coordinates, so the cost of listing all groups is paid once per check interval for the cluster instead of once per broker. The group lease is released after the coordinator lock is dropped, for both retention and DeleteGroups, so a slow etcd release does not block other groups (#263).

- OffsetCommit returns `COORDINATOR_LOAD_IN_PROGRESS` (14) instead of `COORDINATOR_NOT_AVAILABLE` (15) when a commit keeps losing races with other writes to the group, so clients retry without looking up the coordinator again. A member commit that races with the member leaving or a rebalance now returns `UNKNOWN_MEMBER_ID` or `ILLEGAL_GENERATION` instead of storing the offset. A commit with any negative generation is accepted on an Empty group whatever the member ID, as Kafka does. A metadata store that does not implement `ConsumerGroupOffsetCommitter` now gets `ErrConsumerGroupCommitUnsupported` for member commits instead of an unchecked write (#263).

- The broker and proxy advertise DescribeGroups versions 0 to 5, and the proxy advertises ListGroups versions 0 to 5. librdkafka before 2.3, used by older confluent-kafka-python releases, asks for DescribeGroups up to version 4 and failed `describe_consumer_groups` with `_UNSUPPORTED_FEATURE` (#263).

- Produce and Fetch return `KAFKA_STORAGE_ERROR` (56) instead of `UNKNOWN_SERVER_ERROR` (-1) when S3 is unavailable, when a write or read to S3 fails, and when a partition cannot be restored from S3. Clients such as librdkafka now retry instead of failing the message. Degraded S3 still returns `REQUEST_TIMED_OUT` (7). Produce v8 and later include a message such as "object storage unavailable, retry later" (#255). Produce below v4 and Fetch v5 or below receive `NOT_LEADER_OR_FOLLOWER` (6) instead, as Kafka does for versions that predate the storage error. ListOffsets returns `KAFKA_STORAGE_ERROR` when an earliest lookup cannot load the partition from S3. Produce append errors that do not come from S3 still return `UNKNOWN_SERVER_ERROR`.

- Expire Empty consumer groups and their committed offsets after seven days by default. Configure `KAFSCALE_OFFSETS_RETENTION_MINUTES` or `spec.config.offsetsRetentionMinutes` on KafscaleCluster; zero or negative disables expiry. Joins and commits reset the retention clock, and leased, revision-fenced deletion protects active groups and concurrent writes (#238).

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

## Upgrade notes

- Empty groups written by older brokers receive a full retention period from first discovery. Apply the updated KafscaleCluster CRD before setting `spec.config.offsetsRetentionMinutes`; omit it for the seven-day default or set it to `0` to disable automatic expiry.

- During a rolling upgrade, deleting a group through an older broker removes its metadata but leaves its offsets. Upgraded brokers discover those offsets and list the group again as `Empty`. After all brokers are upgraded, delete the inactive group again to remove its retained offsets.

- Members recorded by older brokers without heartbeat timestamps receive one local session timeout to reconnect after coordinator takeover. They are not expired immediately during a rolling upgrade.

- On coordinator takeover, recovered heartbeat times receive a five-second expiry margin for clock skew. Locally received heartbeats return to the configured session timeout.

- Topics that already exist with names outside the allowed characters, such as names with `@` or `/`, keep loading and accept Produce and Fetch as before. Only new creates are rejected. A topic named `<name>@<N>` shares an S3 prefix with generation N of `<name>`, so do not delete and re-create `<name>` while it exists. Move its data to a topic with a valid name and delete it first.
