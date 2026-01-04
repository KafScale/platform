# Kafka Compatibility Claims

## KS-COMP-001
**Claim:** KafScale is Kafka protocol compatible for producers and consumers.

**Scope:** Kafka client APIs (produce, consume, metadata)

**Status:** Verified  
**Last reviewed:** 2026-01-02

**Evidence:**
- E10 Java Kafka client demo
- E20 Spring Boot demo
- Official docs

**Limitations:**
- Transactions are not supported
- Exactly-once semantics are not supported