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

## Bug fixes

- Produce and Fetch return `KAFKA_STORAGE_ERROR` (56) instead of `UNKNOWN_SERVER_ERROR` (-1) when S3 is unavailable, when a write or read to S3 fails, and when a partition cannot be restored from S3. Clients such as librdkafka now retry instead of failing the message. Degraded S3 still returns `REQUEST_TIMED_OUT` (7). Produce v8 and later include a message such as "object storage unavailable, retry later" (#255).

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
