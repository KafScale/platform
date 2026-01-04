# Talk Abstract — 101_kafscale-dev-guide

> Talk Abstract Agent v1 — Generated 2026-01-04

---

## Talk Title

**Stateless Kafka: A Hands-On Exploration of Brokers Without Storage**

---

## Abstract

Traditional Kafka's architecture creates a platform team bottleneck: every new application requires capacity planning, replication setup, and partition rebalancing. This talk explores an alternative architecture that separates compute from storage—stateless brokers backed by S3-compatible object storage—while maintaining full Kafka protocol compatibility.

We'll walk through a hands-on tutorial demonstrating four production patterns: pure Java Kafka clients, Spring Boot integration on Kubernetes, Apache Flink stream processing, and Apache Spark structured streaming. Each example shows how existing Kafka applications migrate with minimal configuration changes (typically just changing the bootstrap server and disabling idempotence).

The architectural insight centers on separation of concerns: durable storage belongs in S3 (11 nines durability), distributed consensus belongs in etcd (cluster metadata), and brokers become stateless compute that can scale from 0→N instances without data movement. This shift removes operational bottlenecks but introduces tradeoffs—S3 adds 10-50ms latency compared to local disk, and transactional features aren't supported.

Attendees will see working code for all four exercises, understand the profile system for different deployment scenarios (local development vs in-cluster vs remote), and learn when this architecture fits their use case versus when traditional Kafka remains the better choice. The talk emphasizes transparency: every technical claim references a verified claims registry, and limitations are foregrounded rather than hidden.

This is not a product pitch—it's an architectural exploration backed by runnable examples and honest tradeoff analysis.

---

## Key Takeaways

1. **How stateless broker architecture removes Kafka operational bottlenecks through storage separation**
   - Attendees will understand the architectural shift from compute-coupled-to-storage (traditional Kafka) to stateless compute + durable object storage (KafScale)

2. **Hands-on migration patterns for Java clients, Spring Boot, Flink, and Spark with Kafka protocol compatibility**
   - Attendees will see working code demonstrating that existing Kafka applications work with minimal configuration changes

3. **Decision framework for evaluating S3 latency tradeoffs vs operational simplicity in production contexts**
   - Attendees will leave with a structured checklist for determining when this architecture fits their use case

---

## Target Audience

- **Primary**: Software engineers, platform engineers, SREs, and architects responsible for streaming infrastructure
- **Secondary**: Engineering managers evaluating Kafka alternatives for cost or operational simplification
- **Background Expected**: Familiarity with Kafka concepts (topics, partitions, consumer groups)

---

## Technical Level

**Intermediate** (assumes Kafka familiarity, no prior KafScale knowledge required)

---

## Session Format

- **Duration**: 40-minute talk + 10-minute Q&A
- **Format**: Presentation with live code walkthrough
- **Demo Requirements**: Screen sharing for code examples (backup: pre-recorded demo clips if live environment fails)

---

## Speaker Notes

**Demo Format Options**:
- **Option A (Preferred)**: Live terminal with pre-built tutorial environment, walking through E10→E20 exercises
- **Option B (Backup)**: Slides + pre-recorded demo videos showing successful deployments
- **Option C (Hybrid)**: Slides for architecture, live code for one exercise (E20), recorded clips for Flink/Spark

**Materials Needed**:
- Laptop with terminal access
- GitHub repository cloned locally
- Docker Desktop running (for local demo)
- Backup: USB drive with pre-recorded videos

---

## CFP Submission Checklist

- [ ] Verify CFP deadline and submission requirements
- [ ] Prepare speaker bio (150 words)
- [ ] Upload headshot photo if required
- [ ] Specify A/V requirements (screen sharing, terminal access)
- [ ] Indicate demo format preference (live vs recorded)
- [ ] Provide GitHub repository link as supporting material
- [ ] Link to published blog post (if available)
- [ ] Indicate if this is a new talk or presented before

---

## Target Conferences/Meetups

**Tier 1** (International conferences):
- Kafka Summit
- QCon
- GOTO Conference
- KubeCon + CloudNativeCon
- Strange Loop

**Tier 2** (Regional conferences):
- Local cloud-native meetups
- Platform Engineering meetups
- Stream Processing user groups
- JVM language conferences (for Java/Spring Boot angle)

**Tier 3** (Company/community events):
- Internal engineering all-hands
- University guest lectures
- Open-source project showcases

---

## Success Metrics

**Acceptance Rate**: Track CFP submissions vs acceptances

**Engagement Indicators**:
- Questions during Q&A (quality and depth)
- Requests for slides/code after talk
- GitHub stars/forks spike post-talk
- Follow-up conversations at conference

**Content Reuse**:
- Record talk for YouTube/conference archive
- Extract slides for blog post illustrations
- Use demo videos for social media clips
