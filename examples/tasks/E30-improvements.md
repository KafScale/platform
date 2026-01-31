# E30_flink-kafscale-demo Improvement Tasks

**Example:** Flink KafScale Word Count Demo
**Current Quality Score:** 8/10
**Target Quality Score:** 9/10

---

## High Priority

### T30-001: Add unit tests for word count logic
**Effort:** Medium
**Impact:** High

Add JUnit tests for:
- [ ] Word parsing from headers
- [ ] Word parsing from keys
- [ ] Word parsing from values
- [ ] Stats counting (no-key, no-header, no-value)
- [ ] Aggregation logic

**Files to create:**
- `src/test/java/com/example/kafscale/flink/WordCountLogicTest.java`
- `src/test/java/com/example/kafscale/flink/MessageParserTest.java`

**Dependencies:**
- Add JUnit 5 to pom.xml
- Add flink-test-utils

---

### T30-002: Add .gitignore
**Effort:** Low
**Impact:** Medium

Create `.gitignore` with:
```
target/
*.class
*.jar
*.log
.idea/
*.iml
flink-checkpoints/
```

---

### T30-003: Improve delivery guarantee defaults
**Effort:** Low
**Impact:** High

Change sink configuration:
- `delivery.guarantee=none` → `delivery.guarantee=at-least-once`
- Enable idempotence when broker supports it
- Document KafScale compatibility notes

---

## Medium Priority

### T30-004: Add Flink integration test
**Effort:** High
**Impact:** Medium

Create integration test using MiniCluster:
- [ ] Set up MiniClusterWithClientResource
- [ ] Test job execution with in-memory source/sink
- [ ] Verify word counts are correct
- [ ] Test checkpoint/restore behavior

**File:** `src/test/java/com/example/kafscale/flink/WordCountJobIT.java`

---

### T30-005: Add windowed aggregation example
**Effort:** Medium
**Impact:** Medium

Extend WordCountJob:
- [ ] Add tumbling window option (1 minute)
- [ ] Add sliding window option (5 min window, 1 min slide)
- [ ] Output window start/end timestamps
- [ ] Configure via environment variable

---

### T30-006: Add watermark strategy
**Effort:** Medium
**Impact:** Medium

Implement event-time processing:
- [ ] Extract timestamp from message
- [ ] Configure watermark strategy
- [ ] Handle late data
- [ ] Add lateness counter

---

### T30-007: Refactor to separate job configuration
**Effort:** Medium
**Impact:** Medium

Extract configuration to separate class:
- `FlinkJobConfig.java` - All env var parsing
- `KafkaSourceBuilder.java` - Source construction
- `KafkaSinkBuilder.java` - Sink construction

---

## Low Priority

### T30-008: Add RocksDB state backend example
**Effort:** Low
**Impact:** Low

- Document when to use RocksDB vs HashMap
- Add profile for RocksDB configuration
- Include checkpoint configuration for large state

---

### T30-009: Add Flink Kubernetes Operator deployment
**Effort:** Medium
**Impact:** Low

- Add FlinkDeployment CRD YAML
- Document operator installation
- Add savepoint management instructions

---

### T30-010: Add parallel execution example
**Effort:** Medium
**Impact:** Low

- Configure parallelism > 1
- Document scaling behavior
- Add metrics for per-task performance

---

## Acceptance Criteria for Score 9/10

- [ ] Unit test coverage > 70%
- [ ] Integration test with MiniCluster passes
- [ ] .gitignore present
- [ ] Improved delivery guarantees
- [ ] CI workflow runs tests
