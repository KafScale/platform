# KafScale Architecture Claims

## KS-ARCH-001
**Claim:** KafScale brokers are stateless.

**Scope:** Broker runtime, scaling, failure recovery

**Status:** Verified  
**Last reviewed:** 2026-01-02

**Evidence:**
- KafScale documentation: https://kafscale.io/docs
- Object storage based log persistence
- etcd-based metadata

**Implications:**
- Brokers can be restarted without data loss
- Horizontal scaling does not require partition rebalancing

**Limitations:**
- Object storage latency impacts tail latency