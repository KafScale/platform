# KafScale Limitations

## KS-LIMIT-001
**Claim:** KafScale does not support Kafka transactions or exactly-once semantics.

**Scope:** Producer semantics, stream processing guarantees

**Status:** Verified  
**Last reviewed:** 2026-01-02

**Evidence:**
- Official documentation
- Design choice: stateless brokers

**Implications:**
- At-least-once delivery only
- Suitable for event streaming, not transactional pipelines