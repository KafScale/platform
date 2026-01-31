# E10_java-kafka-client-demo Improvement Tasks

**Example:** Java Kafka Client Demo
**Current Quality Score:** 7/10
**Target Quality Score:** 9/10

---

## High Priority

### T10-001: Add unit tests
**Effort:** Medium
**Impact:** High

Add JUnit 5 tests for:
- [ ] Configuration parsing (CLI args, env vars)
- [ ] Topic creation logic
- [ ] Message serialization
- [ ] Producer callback handling
- [ ] Consumer poll logic

**Files to create:**
- `src/test/java/com/example/kafscale/SimpleDemoTest.java`
- `src/test/java/com/example/kafscale/ConfigurationTest.java`

**Dependencies:**
- Add JUnit 5, Mockito to pom.xml
- Add test-scoped Kafka testcontainers

---

### T10-002: Add .gitignore
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
```

---

### T10-003: Improve default delivery guarantees
**Effort:** Low
**Impact:** High

Change defaults in SimpleDemo.java:
- `acks=0` → `acks=all`
- `enable.idempotence=false` → `enable.idempotence=true`
- Add documentation explaining the change

---

## Medium Priority

### T10-004: Add integration test with Testcontainers
**Effort:** Medium
**Impact:** Medium

Create integration test that:
- [ ] Starts Kafka via Testcontainers
- [ ] Runs the full demo flow
- [ ] Verifies messages are produced and consumed

**File:** `src/test/java/com/example/kafscale/IntegrationTest.java`

---

### T10-005: Add fixed group ID option for offset persistence
**Effort:** Low
**Impact:** Medium

- Add `--persist-offsets` flag
- When enabled, use a stable group ID
- Document offset persistence behavior

---

### T10-006: Refactor to separate concerns
**Effort:** Medium
**Impact:** Medium

Split SimpleDemo.java into:
- `ConfigParser.java` - CLI/env configuration
- `DemoProducer.java` - Producer logic
- `DemoConsumer.java` - Consumer logic
- `ClusterInspector.java` - Metadata operations

---

## Low Priority

### T10-007: Add JSON serialization option
**Effort:** Medium
**Impact:** Low

- Add `--format=json` flag
- Implement JsonSerializer/Deserializer
- Add sample JSON message structure

---

### T10-008: Add logging configuration
**Effort:** Low
**Impact:** Low

- Add `logback.xml` for configurable logging
- Reduce verbose Kafka client logs
- Add structured logging option

---

### T10-009: Add picocli for CLI parsing
**Effort:** Medium
**Impact:** Low

Replace manual argument parsing with picocli:
- Better help output
- Type validation
- Subcommands support

---

## Acceptance Criteria for Score 9/10

- [ ] Unit test coverage > 70%
- [ ] Integration test passes
- [ ] .gitignore present
- [ ] Production-safe defaults
- [ ] CI workflow runs tests
