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

# LFS Test Specification

## Overview

This document defines the test strategy and test cases for the LFS (Large File Support) feature.

---

## Test Environments

### Local Development

| Component | Implementation |
|-----------|----------------|
| S3 | MinIO (Docker) |
| Broker | KafScale (local) |
| Proxy | LFS Proxy (local) |

### CI/CD

| Component | Implementation |
|-----------|----------------|
| S3 | MinIO (Docker Compose) |
| Broker | KafScale (Docker) |
| Proxy | LFS Proxy (Docker) |

### Integration/Staging

| Component | Implementation |
|-----------|----------------|
| S3 | AWS S3 or MinIO cluster |
| Broker | KafScale cluster |
| Proxy | LFS Proxy (Kubernetes) |

---

## Test Categories

### 1. Unit Tests

#### 1.1 Proxy: Envelope Creation

| Test ID | Description | Input | Expected Output |
|---------|-------------|-------|-----------------|
| UT-ENV-001 | Create valid envelope | bucket, key, size, sha256 | Valid JSON with kfs_lfs=1 |
| UT-ENV-002 | Envelope round-trip | Envelope struct | Encode→Decode matches |
| UT-ENV-003 | Required fields present | Minimal input | All required fields in JSON |
| UT-ENV-004 | Optional fields included | Full input | All fields in JSON |
| UT-ENV-005 | S3 key generation | topic, timestamp | Correct path format |

```go
func TestEnvelopeCreation(t *testing.T) {
    env := NewEnvelope(EnvelopeParams{
        Bucket: "test-bucket",
        Key:    "ns/topic/lfs/2026/01/31/abc123",
        Size:   1024,
        SHA256: "a1b2c3...",
    })

    assert.Equal(t, 1, env.Version)
    assert.Equal(t, "test-bucket", env.Bucket)
    assert.NotEmpty(t, env.CreatedAt)
}
```

#### 1.2 Proxy: Header Detection

| Test ID | Description | Input | Expected Output |
|---------|-------------|-------|-----------------|
| UT-HDR-001 | Detect LFS_BLOB header | Header present | isLfs=true |
| UT-HDR-002 | No header | No LFS_BLOB | isLfs=false |
| UT-HDR-003 | Extract checksum from header | Header with value | checksum extracted |
| UT-HDR-004 | Empty header value | Header with empty | checksum=nil |
| UT-HDR-005 | Case sensitivity | "lfs_blob" lowercase | isLfs=false (case sensitive) |

```go
func TestHeaderDetection(t *testing.T) {
    headers := []kafka.Header{
        {Key: "LFS_BLOB", Value: []byte("abc123")},
    }

    isLfs, checksum := DetectLfsHeader(headers)

    assert.True(t, isLfs)
    assert.Equal(t, "abc123", checksum)
}
```

#### 1.3 Proxy: Checksum Computation

| Test ID | Description | Input | Expected Output |
|---------|-------------|-------|-----------------|
| UT-CHK-001 | SHA256 of known data | "hello world" | Known hash |
| UT-CHK-002 | Incremental computation | Chunked data | Same as full |
| UT-CHK-003 | Empty data | Empty bytes | SHA256 of empty |
| UT-CHK-004 | Large data | 100MB random | Valid hash |

```go
func TestIncrementalChecksum(t *testing.T) {
    data := []byte("hello world")

    // Full computation
    fullHash := sha256.Sum256(data)

    // Incremental computation
    hasher := NewIncrementalHasher()
    hasher.Write(data[:5])
    hasher.Write(data[5:])
    incHash := hasher.Sum()

    assert.Equal(t, fullHash[:], incHash)
}
```

#### 1.4 Consumer Wrapper: Envelope Detection

| Test ID | Description | Input | Expected Output |
|---------|-------------|-------|-----------------|
| UT-DET-001 | Valid LFS envelope | JSON with kfs_lfs | isLfs=true |
| UT-DET-002 | Non-LFS JSON | Regular JSON | isLfs=false |
| UT-DET-003 | Binary data | Random bytes | isLfs=false |
| UT-DET-004 | Empty value | Empty | isLfs=false |
| UT-DET-005 | Large value | >1KB | isLfs=false (optimization) |
| UT-DET-006 | Malformed JSON | Invalid JSON | isLfs=false, no panic |

```go
func TestEnvelopeDetection(t *testing.T) {
    tests := []struct {
        name     string
        value    []byte
        expected bool
    }{
        {"valid envelope", []byte(`{"kfs_lfs":1,"bucket":"b","key":"k"}`), true},
        {"regular json", []byte(`{"foo":"bar"}`), false},
        {"binary data", []byte{0x00, 0x01, 0x02}, false},
        {"empty", []byte{}, false},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            assert.Equal(t, tt.expected, IsLfsEnvelope(tt.value))
        })
    }
}
```

#### 1.5 Consumer Wrapper: S3 Resolution

| Test ID | Description | Input | Expected Output |
|---------|-------------|-------|-----------------|
| UT-RES-001 | Resolve valid pointer | Valid envelope | Blob bytes |
| UT-RES-002 | Checksum validation pass | Matching checksum | Success |
| UT-RES-003 | Checksum validation fail | Mismatched checksum | Error |
| UT-RES-004 | Size validation pass | Matching size | Success |
| UT-RES-005 | Size validation fail | Mismatched size | Error |

---

### 2. Integration Tests

#### 2.1 Proxy + S3 Integration

| Test ID | Description | Setup | Steps | Expected |
|---------|-------------|-------|-------|----------|
| IT-S3-001 | Upload small blob | MinIO running | Upload 1KB | Object in S3 with correct content |
| IT-S3-002 | Upload large blob | MinIO running | Upload 100MB | Object in S3, multipart used |
| IT-S3-003 | Upload failure recovery | MinIO stops mid-upload | Upload 10MB | Error returned, no partial object |
| IT-S3-004 | Checksum stored | MinIO running | Upload with checksum | Checksum in envelope matches |
| IT-HTTP-CHUNK-001 | Initiate chunked upload | Proxy + S3 | POST /lfs/uploads | Upload session created |
| IT-HTTP-CHUNK-002 | Upload parts | Proxy + S3 | PUT parts 1..N | Parts stored, ETags returned |
| IT-HTTP-CHUNK-003 | Complete upload | Proxy + S3 + Kafka | POST complete | Envelope produced, object exists |
| IT-HTTP-CHUNK-004 | Abort upload | Proxy + S3 | DELETE upload | Multipart aborted, no object |

```go
func TestProxyS3Upload(t *testing.T) {
    ctx := context.Background()
    minio := startMinIO(t)
    defer minio.Stop()

    proxy := startProxy(t, minio.Endpoint())
    defer proxy.Stop()

    // Upload via Kafka protocol
    producer := kafka.NewProducer(proxy.Addr())
    record := &kafka.Record{
        Topic:   "test-topic",
        Value:   make([]byte, 1024*1024), // 1MB
        Headers: []kafka.Header{{Key: "LFS_BLOB", Value: nil}},
    }
    err := producer.Produce(ctx, record)
    require.NoError(t, err)

    // Verify in S3
    obj, err := minio.GetObject("kafscale-lfs", /* key from envelope */)
    require.NoError(t, err)
    assert.Equal(t, 1024*1024, len(obj))
}
```

#### 2.2 Proxy + Broker Integration

| Test ID | Description | Setup | Steps | Expected |
|---------|-------------|-------|-------|----------|
| IT-BRK-001 | Pointer record produced | Broker + Proxy | LFS produce | Pointer in topic |
| IT-BRK-002 | Non-LFS passthrough | Broker + Proxy | Normal produce | Record unchanged |
| IT-BRK-003 | Broker unavailable | Proxy only | LFS produce | Error, no orphan |
| IT-BRK-004 | Broker slow | Slow broker | LFS produce | Success with timeout |

```go
func TestPointerRecordProduced(t *testing.T) {
    broker := startBroker(t)
    minio := startMinIO(t)
    proxy := startProxy(t, broker.Addr(), minio.Endpoint())

    producer := kafka.NewProducer(proxy.Addr())
    producer.Produce(ctx, &kafka.Record{
        Topic:   "test-topic",
        Value:   largeBlob,
        Headers: []kafka.Header{{Key: "LFS_BLOB", Value: nil}},
    })

    // Consume from broker directly
    consumer := kafka.NewConsumer(broker.Addr())
    record := consumer.Fetch(ctx)

    // Should be envelope, not original blob
    assert.True(t, IsLfsEnvelope(record.Value))
    assert.Less(t, len(record.Value), 1000)  // Small envelope
}
```

#### 2.3 Consumer Wrapper Integration

| Test ID | Description | Setup | Steps | Expected |
|---------|-------------|-------|-------|----------|
| IT-CON-001 | Resolve LFS record | Full stack | Consume LFS | Original bytes returned |
| IT-CON-002 | Passthrough non-LFS | Full stack | Consume normal | Record unchanged |
| IT-CON-003 | S3 unavailable | No S3 | Consume LFS | Error surfaced |
| IT-CON-004 | Concurrent resolution | Full stack | 10 parallel | All resolved correctly |

```go
func TestConsumerResolution(t *testing.T) {
    // Setup full stack
    broker := startBroker(t)
    minio := startMinIO(t)
    proxy := startProxy(t, broker.Addr(), minio.Endpoint())

    // Produce LFS record
    originalData := randomBytes(10 * 1024 * 1024)  // 10MB
    producer := kafka.NewProducer(proxy.Addr())
    producer.Produce(ctx, &kafka.Record{
        Topic:   "test-topic",
        Value:   originalData,
        Headers: []kafka.Header{{Key: "LFS_BLOB", Value: nil}},
    })

    // Consume with LFS wrapper
    baseConsumer := kafka.NewConsumer(broker.Addr())
    lfsConsumer := lfs.NewConsumer(baseConsumer, lfs.Config{
        S3Endpoint: minio.Endpoint(),
        S3Bucket:   "kafscale-lfs",
    })

    records := lfsConsumer.Poll(ctx)
    resolved, err := records[0].Value()

    require.NoError(t, err)
    assert.Equal(t, originalData, resolved)
}
```

---

### 3. End-to-End Tests

#### 3.1 Happy Path Scenarios

| Test ID | Description | Producer | Consumer | Validation |
|---------|-------------|----------|----------|------------|
| E2E-001 | Small LFS blob | Kafka + header | LFS wrapper | Content matches |
| E2E-002 | Large LFS blob (100MB) | Kafka + header | LFS wrapper | Content matches, checksum valid |
| E2E-003 | Streaming upload | HTTP SDK | LFS wrapper | Content matches |
| E2E-003A | Chunked upload | Browser SDK | Chunked flow | Content matches |
| E2E-003B | Chunked resume | Browser SDK | Interrupt mid-upload | Resume completes |
| E2E-004 | Mixed traffic | LFS + non-LFS | LFS wrapper | Both work correctly |
| E2E-005 | Multiple partitions | LFS to 3 partitions | LFS wrapper | All resolved |
| E2E-006 | Proxy restart during HTTP upload | Restart proxy mid-upload | HTTP SDK | Retries succeed or clear error |

```go
func TestE2EHappyPath(t *testing.T) {
    stack := startFullStack(t)
    defer stack.Stop()

    // Producer: normal Kafka client with header
    producer := kafka.NewProducer(stack.ProxyAddr())

    testData := randomBytes(50 * 1024 * 1024)  // 50MB
    checksum := sha256Hex(testData)

    err := producer.Produce(ctx, &kafka.Record{
        Topic:   "e2e-test",
        Key:     []byte("test-key"),
        Value:   testData,
        Headers: []kafka.Header{
            {Key: "LFS_BLOB", Value: []byte(checksum)},
        },
    })
    require.NoError(t, err)

    // Consumer: LFS wrapper
    consumer := lfs.NewConsumer(
        kafka.NewConsumer(stack.BrokerAddr()),
        stack.LfsConfig(),
    )

    records := consumer.Poll(ctx)
    require.Len(t, records, 1)

    resolved, err := records[0].Value()
    require.NoError(t, err)
    assert.Equal(t, testData, resolved)
    assert.Equal(t, checksum, sha256Hex(resolved))
}
```

#### 3.2 Failure Scenarios

| Test ID | Description | Failure Injected | Expected Behavior |
|---------|-------------|------------------|-------------------|
| E2E-F01 | S3 down during upload | Stop MinIO mid-upload | Producer gets error |
| E2E-F02 | Broker down after S3 | Stop broker after S3 upload | Orphan logged |
| E2E-F03 | S3 down during consume | Stop MinIO during fetch | Consumer gets error |
| E2E-F04 | Checksum mismatch | Corrupt S3 object | Consumer gets error |
| E2E-F05 | Proxy crash recovery | Kill/restart proxy | Next request succeeds |
| E2E-F06 | HTTP backend unavailable | Stop broker or S3 | HTTP SDK sees 502/503 |

```go
func TestE2EChecksumMismatch(t *testing.T) {
    stack := startFullStack(t)

    // Produce valid LFS record
    producer := kafka.NewProducer(stack.ProxyAddr())
    producer.Produce(ctx, &kafka.Record{
        Topic:   "test",
        Value:   []byte("original data"),
        Headers: []kafka.Header{{Key: "LFS_BLOB", Value: nil}},
    })

    // Corrupt the S3 object
    stack.MinIO().CorruptObject("kafscale-lfs", /* key */)

    // Consumer should detect corruption
    consumer := lfs.NewConsumer(kafka.NewConsumer(stack.BrokerAddr()), stack.LfsConfig())
    records := consumer.Poll(ctx)

    _, err := records[0].Value()
    assert.ErrorContains(t, err, "checksum mismatch")
}
```

#### 3.3 Performance Tests

| Test ID | Description | Load | Metrics | Target |
|---------|-------------|------|---------|--------|
| E2E-P01 | Throughput (small) | 1000 x 1MB blobs | Blobs/sec | >100/sec |
| E2E-P02 | Throughput (large) | 10 x 100MB blobs | MB/sec | >500 MB/sec |
| E2E-P03 | Latency (passthrough) | 10000 non-LFS | p99 latency | <5ms added |
| E2E-P04 | Memory stability | 100 concurrent x 50MB | Max RSS | <4GB |
| E2E-P05 | Consumer resolution | 100 concurrent fetch | Fetches/sec | >50/sec |

```go
func BenchmarkLfsUpload(b *testing.B) {
    stack := startFullStack(b)
    producer := kafka.NewProducer(stack.ProxyAddr())
    blob := randomBytes(1024 * 1024)  // 1MB

    b.ResetTimer()
    b.SetBytes(int64(len(blob)))

    for i := 0; i < b.N; i++ {
        producer.Produce(ctx, &kafka.Record{
            Topic:   "bench",
            Value:   blob,
            Headers: []kafka.Header{{Key: "LFS_BLOB", Value: nil}},
        })
    }
}
```

---

### 4. Security Tests

| Test ID | Description | Attack Vector | Expected |
|---------|-------------|---------------|----------|
| SEC-001 | S3 creds not in envelope | Inspect envelope | No credentials |
| SEC-002 | Bucket traversal | Key with `../` | Rejected or sanitized |
| SEC-003 | Oversized blob | Send 10GB | Rejected with 413 |
| SEC-004 | Invalid checksum format | Non-hex checksum | Rejected with 400 |

---

### 5. Compatibility Tests

| Test ID | Description | Client | Expected |
|---------|-------------|--------|----------|
| COMPAT-001 | Java producer | Kafka Java client | Works with header |
| COMPAT-002 | Python producer | confluent-kafka-python | Works with header |
| COMPAT-003 | Go producer | franz-go | Works with header |
| COMPAT-004 | librdkafka producer | librdkafka | Works with header |
| COMPAT-005 | Classic consumer | Any Kafka consumer | Receives envelope JSON |

---

## Test Data

### Standard Test Blobs

| Name | Size | Content | SHA256 |
|------|------|---------|--------|
| tiny.bin | 100 bytes | Repeated 'A' | `a1b2c3...` |
| small.bin | 1 KB | Random | Generated |
| medium.bin | 1 MB | Random | Generated |
| large.bin | 100 MB | Random | Generated |
| huge.bin | 1 GB | Random | Generated |

### Test Topics

| Topic | Partitions | Purpose |
|-------|------------|---------|
| lfs-test-small | 1 | Unit/integration tests |
| lfs-test-large | 3 | Partition distribution tests |
| lfs-perf | 10 | Performance tests |

---

## Test Automation

### CI Pipeline Integration

```yaml
# .github/workflows/lfs-tests.yml
name: LFS Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.22'
      - run: go test ./lfs/... -v -race

  integration-tests:
    runs-on: ubuntu-latest
    services:
      minio:
        image: minio/minio
        ports:
          - 9000:9000
    steps:
      - uses: actions/checkout@v4
      - run: make test-integration

  e2e-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: docker-compose -f docker-compose.test.yml up -d
      - run: make test-e2e
      - run: docker-compose -f docker-compose.test.yml down
```

### Test Coverage Requirements

| Component | Minimum Coverage |
|-----------|------------------|
| Proxy core | 80% |
| Consumer wrapper | 80% |
| Envelope handling | 90% |
| S3 client | 70% |

---

## Acceptance Criteria

### Phase 1 (MVP) Exit Criteria

- [ ] All unit tests pass
- [ ] Integration tests with MinIO pass
- [ ] E2E happy path (E2E-001, E2E-002) pass
- [ ] Passthrough latency <5ms (E2E-P03)
- [ ] No memory leaks under load (E2E-P04)

### Phase 2 (Streaming) Exit Criteria

- [ ] Streaming upload E2E (E2E-003) passes
- [ ] Chunked upload E2E (E2E-003A) passes
- [ ] Chunked resume E2E (E2E-003B) passes
- [ ] HTTP retry on proxy restart (E2E-006) passes
- [ ] HTTP 5xx error mapping (E2E-F06) validated
- [ ] Large file handling (1GB) works
- [ ] Consumer wrapper Java implementation passes

### Phase 3 (Production) Exit Criteria

- [ ] All security tests pass
- [ ] All compatibility tests pass
- [ ] Performance targets met
- [ ] 80% code coverage achieved
