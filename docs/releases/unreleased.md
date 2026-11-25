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

- Support nontransactional idempotent producers through brokers and the Kubernetes proxy (`InitProducerId` v0-v4). Default Java producers can send without disabling idempotence (#28).
- Allocate producer IDs durably through etcd; deduplicate the last five batches per producer and partition under the append lock. Rebuild sequence state from S3 on restart and ownership changes, including after uncertain writes. Idempotent writes require `acks=all` and always flush before success.
- Return explicit protocol errors for transactions, control batches and `read_committed`; omit unsupported APIs from version negotiation. Transactions and cross-session exactly-once delivery remain unsupported.
- Upgrade all brokers and proxies before enabling idempotence. See [protocol guarantees and limits](../protocol.md#idempotent-producers).
