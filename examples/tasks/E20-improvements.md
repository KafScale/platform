# E20_spring-boot-kafscale-demo Improvement Tasks

**Example:** Spring Boot KafScale Demo
**Current Quality Score:** 8.5/10
**Target Quality Score:** 9.5/10

---

## High Priority

### T20-001: Add unit tests for services
**Effort:** Medium
**Impact:** High

Add Spring Boot test slices for:
- [ ] `OrderProducerServiceTest` - Mock KafkaTemplate
- [ ] `OrderConsumerServiceTest` - Verify order processing
- [ ] `OrderControllerTest` - MockMvc for REST endpoints
- [ ] `OrderTest` - Model validation

**Files to create:**
- `src/test/java/com/example/kafscale/service/OrderProducerServiceTest.java`
- `src/test/java/com/example/kafscale/service/OrderConsumerServiceTest.java`
- `src/test/java/com/example/kafscale/controller/OrderControllerTest.java`

**Dependencies:**
- spring-boot-starter-test (already included)
- spring-kafka-test for embedded Kafka

---

### T20-002: Add .gitignore
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
application-local.yml
```

---

### T20-003: Secure diagnostic endpoints
**Effort:** Low
**Impact:** High

Add Spring Security configuration:
- [ ] Create `SecurityConfig.java`
- [ ] Protect `/api/orders/config` endpoint
- [ ] Protect `/api/orders/cluster-info` endpoint
- [ ] Add basic auth or profile-based disabling

---

## Medium Priority

### T20-004: Add integration test with EmbeddedKafka
**Effort:** Medium
**Impact:** Medium

Create integration test that:
- [ ] Uses `@EmbeddedKafka` annotation
- [ ] Produces order via REST API
- [ ] Verifies consumer receives and stores order
- [ ] Tests cluster-info endpoint

**File:** `src/test/java/com/example/kafscale/IntegrationTest.java`

---

### T20-005: Fix consumer group ID for offset persistence
**Effort:** Low
**Impact:** Medium

Change in `application.yml`:
- Remove `${random.uuid}` from group ID
- Use fixed `kafscale-demo-group`
- Add documentation about offset tracking

---

### T20-006: Add Dead Letter Queue support
**Effort:** Medium
**Impact:** Medium

Implement error handling:
- [ ] Add `@RetryableTopic` annotation
- [ ] Configure DLT topic
- [ ] Add DLT consumer for monitoring
- [ ] Expose DLT messages in UI

---

### T20-007: Add database persistence
**Effort:** High
**Impact:** Medium

Replace in-memory list with:
- [ ] Add H2 (dev) / PostgreSQL (prod) profiles
- [ ] Create JPA `OrderEntity`
- [ ] Add `OrderRepository`
- [ ] Add pagination to GET `/api/orders`

---

## Low Priority

### T20-008: Improve delivery guarantees
**Effort:** Low
**Impact:** Medium

Change producer config:
- `acks: 0` → `acks: all`
- Add `enable.idempotence: true`
- Document trade-offs

---

### T20-009: Add Swagger/OpenAPI documentation
**Effort:** Low
**Impact:** Low

- Add springdoc-openapi dependency
- Configure API info
- Add endpoint descriptions

---

### T20-010: Add GitHub Actions workflow
**Effort:** Low
**Impact:** Low

Create `.github/workflows/e20-test.yml`:
- Build with Maven
- Run unit tests
- Run integration tests

---

## Acceptance Criteria for Score 9.5/10

- [ ] Unit test coverage > 80%
- [ ] Integration test passes with EmbeddedKafka
- [ ] .gitignore present
- [ ] Diagnostic endpoints secured
- [ ] Fixed consumer group ID
- [ ] CI workflow runs tests
