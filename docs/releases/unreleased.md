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

- Consumer groups remain visible as `Empty` after their last member leaves or expires, with committed offsets preserved. Deleting a group with active members returns `NON_EMPTY_GROUP` (68) without changing its members or offsets (#230, #236).

- DeleteGroups acquires the group coordinator lease before expiring stale stored members after a coordinator crash. Other brokers return `NOT_COORDINATOR` without caching or changing the group. Groups with a recent heartbeat remain protected by `NON_EMPTY_GROUP` (#237).

## Upgrade notes

- During a rolling upgrade, deleting a group through an older broker removes its metadata but leaves its offsets. Upgraded brokers discover those offsets and list the group again as `Empty`. After all brokers are upgraded, delete the inactive group again to remove its retained offsets.

- Members recorded by older brokers without heartbeat timestamps receive one local session timeout to reconnect after coordinator takeover. They are not expired immediately during a rolling upgrade.
