# Unreleased

- Support nontransactional idempotent producers through brokers and the Kubernetes proxy (`InitProducerId` v0-v4). Default Java producers can send without disabling idempotence (#28).
- Allocate producer IDs durably through etcd; deduplicate the last five batches per producer and partition under the append lock. Rebuild sequence state from S3 on restart and ownership changes, including after uncertain writes. Idempotent writes require `acks=all` and always flush before success.
- Return explicit protocol errors for transactions, control batches and `read_committed`; omit unsupported APIs from version negotiation. Transactions and cross-session exactly-once delivery remain unsupported.
- Upgrade all brokers and proxies before enabling idempotence. See [protocol guarantees and limits](../protocol.md#idempotent-producers).
