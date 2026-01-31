# E40_spark-kafscale-demo Improvement Tasks

**Example:** Spark KafScale Word Count Demo
**Current Quality Score:** 7.5/10
**Target Quality Score:** 9/10

---

## High Priority

### T40-001: Add unit tests for word count logic
**Effort:** Medium
**Impact:** High

Add JUnit tests for:
- [ ] Word parsing from headers
- [ ] Word parsing from keys
- [ ] Word parsing from values
- [ ] Stats counting logic
- [ ] Configuration parsing

**Files to create:**
- `src/test/java/com/example/kafscale/spark/WordCountLogicTest.java`
- `src/test/java/com/example/kafscale/spark/ConfigurationTest.java`

**Dependencies:**
- Add JUnit 5 to pom.xml
- Add Spark test dependencies

---

### T40-002: Add .gitignore
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
spark-checkpoints/
metastore_db/
derby.log
```

---

### T40-003: Add durable checkpoint example
**Effort:** Low
**Impact:** High

- Add profile for S3 checkpoints
- Add profile for HDFS checkpoints
- Document checkpoint directory requirements
- Warn about /tmp limitations prominently

---

## Medium Priority

### T40-004: Add Spark streaming integration test
**Effort:** High
**Impact:** Medium

Create integration test:
- [ ] Use SharedSparkSession for testing
- [ ] Create test DataFrame with mock Kafka data
- [ ] Run word count transformation
- [ ] Verify output counts
- [ ] Test checkpoint recovery

**File:** `src/test/java/com/example/kafscale/spark/WordCountSparkJobIT.java`

---

### T40-005: Add windowed aggregation example
**Effort:** Medium
**Impact:** Medium

Extend WordCountSparkJob:
- [ ] Add tumbling window (window function)
- [ ] Add sliding window option
- [ ] Output window timestamps
- [ ] Configure via environment variable

---

### T40-006: Add watermark support
**Effort:** Medium
**Impact:** Medium

Implement event-time processing:
- [ ] Add withWatermark() call
- [ ] Configure late data threshold
- [ ] Document event-time vs processing-time

---

### T40-007: Add more deployment scripts
**Effort:** Medium
**Impact:** Medium

Create scripts matching E30's structure:
- `scripts/run-standalone-local.sh`
- `scripts/run-docker-local.sh`
- `scripts/run-k8s-stack.sh`

---

### T40-008: Add Kafka sink output option
**Effort:** Medium
**Impact:** Medium

Mirror E30's sink capability:
- [ ] Add Kafka sink for word counts
- [ ] Configure output topic
- [ ] Add enable/disable flag

---

## Low Priority

### T40-009: Add Kubernetes deployment example
**Effort:** Medium
**Impact:** Low

- Add Spark on Kubernetes deployment YAML
- Document spark-submit for K8s
- Add resource configuration

---

### T40-010: Refactor to separate concerns
**Effort:** Medium
**Impact:** Low

Extract into:
- `SparkJobConfig.java` - Configuration
- `KafkaReader.java` - Source setup
- `WordCountTransformer.java` - Business logic
- `OutputWriter.java` - Sink logic

---

### T40-011: Add Delta Lake merge example
**Effort:** Medium
**Impact:** Low

Demonstrate idempotent writes:
- [ ] Add merge operation instead of append
- [ ] Handle duplicate processing
- [ ] Document exactly-once pattern

---

## Acceptance Criteria for Score 9/10

- [ ] Unit test coverage > 70%
- [ ] Integration test passes
- [ ] .gitignore present
- [ ] Durable checkpoint documented
- [ ] Deployment scripts match E30
- [ ] CI workflow runs tests
