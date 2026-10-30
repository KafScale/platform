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

## Bug fixes

- Remove members that disconnect during a pending JoinGroup through the broker or proxy from the rebalance and persisted group state, so survivors can receive their partitions without waiting for the dead member's session timeout (#251).

- Store every batch in a partition's Produce request together. Failed writes leave buffered data and offsets unchanged, so retrying a multi-batch request cannot duplicate a stored prefix (#250).

- Consumer groups remain visible as `Empty` after their last member leaves or expires, with committed offsets preserved. Deleting a group with active members returns `NON_EMPTY_GROUP` (68) without changing its members or offsets (#230, #236).

- DeleteGroups acquires the group coordinator lease before expiring stale stored members after a coordinator crash. Other brokers return `NOT_COORDINATOR` without caching or changing the group. Groups with a recent heartbeat remain protected by `NON_EMPTY_GROUP` (#237).

- The proxy routes DeleteGroups to each group's owner, including multi-group requests. Brokers return the lease owner from FindCoordinator and release group leases after successful deletion, allowing immediate recreation on another broker (#237).

- Advertise Fetch v4-v13 and older supported classic group API versions on the broker and proxy so librdkafka clients negotiate magic-2 records and consumer groups correctly. Hold JoinGroup replies until the known members rejoin or the rebalance deadline expires, and encode empty Fetch partitions with zero-length records. Validate every Produce batch before storage; reject legacy MessageSets with `UNSUPPORTED_VERSION` and incomplete frames with `CORRUPT_MESSAGE` (#240).

## Upgrade notes

- During a rolling upgrade, deleting a group through an older broker removes its metadata but leaves its offsets. Upgraded brokers discover those offsets and list the group again as `Empty`. After all brokers are upgraded, delete the inactive group again to remove its retained offsets.

- Members recorded by older brokers without heartbeat timestamps receive one local session timeout to reconnect after coordinator takeover. They are not expired immediately during a rolling upgrade.

- On coordinator takeover, recovered heartbeat times receive a five-second expiry margin for clock skew. Locally received heartbeats return to the configured session timeout.
