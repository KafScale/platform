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

# LFS Business Usage Scenarios

## Overview

This document describes real-world business scenarios where KafScale LFS (Large File Support) provides significant value. These scenarios illustrate how LFS enables use cases that are impractical or impossible with standard Kafka.

---

## Scenario 1: Media Processing Pipeline

### Business Context

**Company:** StreamVision Media
**Industry:** Video streaming platform
**Challenge:** Process user-uploaded videos through a pipeline of transcoding, thumbnail generation, and content moderation.

### Current Pain Points

1. Users upload videos (100MB - 5GB) that need processing
2. Traditional Kafka limits messages to ~1MB without complex tuning
3. Tuning Kafka for large messages causes broker memory issues
4. Current workaround: Upload to S3 separately, pass URL in Kafka message
5. Problem: Two systems to coordinate, no atomicity, orphan files

### LFS Solution

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      VIDEO PROCESSING PIPELINE                               │
│                                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│  │   Upload     │     │   LFS        │     │   video-     │                │
│  │   Service    │────▶│   Proxy      │────▶│   uploads    │                │
│  │              │     │              │     │   (topic)    │                │
│  └──────────────┘     └──────┬───────┘     └──────┬───────┘                │
│                              │                    │                         │
│        Video bytes           │                    │  Pointer records        │
│        + LFS_BLOB            ▼                    │                         │
│        header         ┌──────────────┐            │                         │
│                       │     S3       │            │                         │
│                       │   (videos)   │            │                         │
│                       └──────────────┘            │                         │
│                              ▲                    │                         │
│                              │                    ▼                         │
│                              │           ┌──────────────┐                   │
│                              │           │  Transcoder  │                   │
│                              └───────────│  Service     │                   │
│                                (fetch)   │  (LFS SDK)   │                   │
│                                          └──────┬───────┘                   │
│                                                 │                           │
│                                                 ▼                           │
│                                          ┌──────────────┐                   │
│                                          │   video-     │                   │
│                                          │   processed  │                   │
│                                          │   (topic)    │                   │
│                                          └──────────────┘                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Implementation

**Producer (Upload Service):**

```java
// User uploads video via HTTP
@PostMapping("/upload")
public ResponseEntity<UploadResult> uploadVideo(
    @RequestParam("file") MultipartFile file,
    @RequestParam("userId") String userId
) {
    // Just send to Kafka with LFS_BLOB header - that's it!
    ProducerRecord<String, byte[]> record = new ProducerRecord<>(
        "video-uploads",
        userId,
        file.getBytes()
    );
    record.headers().add("LFS_BLOB", "".getBytes());
    record.headers().add("content-type", file.getContentType().getBytes());
    record.headers().add("filename", file.getOriginalFilename().getBytes());

    producer.send(record);

    return ResponseEntity.ok(new UploadResult("Processing started"));
}
```

**Consumer (Transcoder Service):**

```java
@Service
public class TranscoderService {
    private final LfsConsumer<String, byte[]> consumer;
    private final FFmpegWrapper ffmpeg;

    public void processVideos() {
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, byte[]> record : records) {
                // LFS SDK automatically fetches video from S3
                byte[] videoBytes = record.value();

                // Process video
                byte[] transcoded = ffmpeg.transcode(videoBytes, "720p");

                // Produce result (also via LFS)
                producer.send(new ProducerRecord<>(
                    "video-processed",
                    record.key(),
                    transcoded,
                    List.of(new Header("LFS_BLOB", new byte[0]))
                ));
            }
        }
    }
}
```

### Business Benefits

| Metric | Before LFS | After LFS | Improvement |
|--------|------------|-----------|-------------|
| Max video size | 1MB (or complex workarounds) | 5GB | 5000x |
| Code complexity | High (S3 + Kafka coordination) | Low (just add header) | 80% less code |
| Orphan files | Common (failed uploads) | Rare (atomic) | Near zero |
| Time to market | 3 weeks | 3 days | 7x faster |

---

## Scenario 2: Document Management System

### Business Context

**Company:** LegalDocs Inc.
**Industry:** Legal technology
**Challenge:** Store and process legal documents (contracts, court filings, evidence) with full audit trail.

### Requirements

1. Documents range from 1KB (text notes) to 500MB (scanned evidence bundles)
2. Every document change must be tracked in event log
3. Consumers include: search indexer, OCR processor, compliance auditor
4. Must handle 10,000+ documents/day during discovery periods

### LFS Solution Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     LEGAL DOCUMENT MANAGEMENT                                │
│                                                                              │
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │                    Document Ingestion                              │    │
│   │                                                                    │    │
│   │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐             │    │
│   │  │ Scanner │  │ Email   │  │ Upload  │  │ API     │             │    │
│   │  │ Import  │  │ Import  │  │ Portal  │  │ Import  │             │    │
│   │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘             │    │
│   │       │            │            │            │                    │    │
│   │       └────────────┴─────┬──────┴────────────┘                    │    │
│   │                          │                                         │    │
│   │                          ▼                                         │    │
│   │                   ┌──────────────┐                                 │    │
│   │                   │  LFS Proxy   │                                 │    │
│   │                   └──────┬───────┘                                 │    │
│   │                          │                                         │    │
│   └──────────────────────────┼─────────────────────────────────────────┘    │
│                              │                                               │
│                              ▼                                               │
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │                    documents-events (topic)                        │    │
│   │                                                                    │    │
│   │   Event Types:                                                     │    │
│   │   - DOCUMENT_CREATED (+ document bytes via LFS)                   │    │
│   │   - DOCUMENT_UPDATED (+ new version via LFS)                      │    │
│   │   - DOCUMENT_ACCESSED (metadata only)                             │    │
│   │   - DOCUMENT_DELETED (metadata only)                              │    │
│   │                                                                    │    │
│   └───────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│          ┌───────────────────┼───────────────────┬───────────────────┐      │
│          ▼                   ▼                   ▼                   ▼      │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────┐  │
│   │   Search    │     │    OCR      │     │  Compliance │     │ Archive │  │
│   │   Indexer   │     │  Processor  │     │   Auditor   │     │ Service │  │
│   │ (LFS SDK)   │     │ (LFS SDK)   │     │ (LFS SDK)   │     │(LFS SDK)│  │
│   └─────────────┘     └─────────────┘     └─────────────┘     └─────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Event Schema

```json
{
  "eventType": "DOCUMENT_CREATED",
  "documentId": "doc-12345",
  "caseId": "case-789",
  "metadata": {
    "filename": "contract-v2.pdf",
    "contentType": "application/pdf",
    "size": 15728640,
    "uploadedBy": "attorney@legaldocs.com",
    "uploadedAt": "2026-01-31T10:30:00Z"
  }
}
// Document bytes stored via LFS (message value is the PDF)
// Envelope automatically created by proxy
```

### Consumer Example: Search Indexer

```go
func (s *SearchIndexer) ProcessDocuments(ctx context.Context) {
    consumer := lfs.NewConsumer(s.kafkaConsumer, s.lfsConfig)

    for {
        records, err := consumer.Poll(ctx, time.Second)
        if err != nil {
            log.Error("poll failed", "error", err)
            continue
        }

        for _, record := range records {
            // Parse event metadata from headers
            eventType := string(record.Header("event-type"))

            if eventType == "DOCUMENT_CREATED" || eventType == "DOCUMENT_UPDATED" {
                // LFS SDK fetches document from S3 automatically
                docBytes, err := record.Value()
                if err != nil {
                    log.Error("failed to fetch document", "error", err)
                    continue
                }

                // Extract text and index
                text := s.extractText(docBytes, record.Header("content-type"))
                s.elasticsearch.Index(record.Key(), text, record.Headers())
            }
        }

        consumer.Commit()
    }
}
```

### Business Benefits

| Requirement | How LFS Addresses It |
|-------------|---------------------|
| Large documents | S3 storage, no Kafka size limits |
| Audit trail | Every event in Kafka, immutable log |
| Multiple consumers | Each consumer resolves independently |
| Peak load (discovery) | S3 scales, proxy scales horizontally |
| Compliance | Documents encrypted at rest (S3 SSE) |

---

## 1

### Business Context

**Company:** SmartFactory Corp.
**Industry:** Industrial IoT / Manufacturing
**Challenge:** Collect sensor data and quality inspection images from factory floor.

### Data Characteristics

| Data Type | Size | Frequency | Total Daily Volume |
|-----------|------|-----------|-------------------|
| Sensor readings | 100 bytes | 1000/sec | ~8 GB |
| Inspection images | 2-10 MB | 100/min | ~1.5 TB |
| Thermal scans | 50-200 MB | 10/min | ~2 TB |

### LFS Solution

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       FACTORY FLOOR DATA COLLECTION                          │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Edge Gateway                                 │   │
│  │                                                                      │   │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐       │   │
│  │  │ Temp      │  │ Vibration │  │ Camera    │  │ Thermal   │       │   │
│  │  │ Sensor    │  │ Sensor    │  │ (4K)      │  │ Imager    │       │   │
│  │  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘       │   │
│  │        │              │              │              │              │   │
│  │        ▼              ▼              ▼              ▼              │   │
│  │  ┌─────────────────────────────────────────────────────────────┐  │   │
│  │  │                   Edge Aggregator                            │  │   │
│  │  │                                                              │  │   │
│  │  │   Sensor data → Regular Kafka messages (no LFS)             │  │   │
│  │  │   Images      → Kafka messages + LFS_BLOB header            │  │   │
│  │  │                                                              │  │   │
│  │  └─────────────────────────────────────────────────────────────┘  │   │
│  │                              │                                     │   │
│  └──────────────────────────────┼─────────────────────────────────────┘   │
│                                 │                                          │
│                                 ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                        LFS Proxy Cluster                             │  │
│  │                                                                      │  │
│  │   Sensor data: passthrough (small, high frequency)                  │  │
│  │   Images: LFS handling (large, lower frequency)                     │  │
│  │                                                                      │  │
│  └──────────────────────────────┬──────────────────────────────────────┘  │
│                                 │                                          │
│              ┌──────────────────┼──────────────────┐                      │
│              ▼                  ▼                  ▼                       │
│       ┌─────────────┐    ┌─────────────┐    ┌─────────────┐              │
│       │  sensor-    │    │  quality-   │    │  thermal-   │              │
│       │  readings   │    │  images     │    │  scans      │              │
│       │  (topic)    │    │  (topic)    │    │  (topic)    │              │
│       │             │    │  (LFS)      │    │  (LFS)      │              │
│       │  ~8GB/day   │    │  ~1.5TB/day │    │  ~2TB/day   │              │
│       └─────────────┘    └─────────────┘    └─────────────┘              │
│                                                                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Edge Gateway Code

```python
import kafka
import struct

class EdgeAggregator:
    def __init__(self, proxy_address):
        self.producer = kafka.KafkaProducer(
            bootstrap_servers=proxy_address,
            max_request_size=200 * 1024 * 1024  # 200MB for thermal scans
        )

    def send_sensor_reading(self, sensor_id, value, timestamp):
        """Small sensor data - regular Kafka message"""
        data = struct.pack('!dq', value, timestamp)
        self.producer.send('sensor-readings', key=sensor_id.encode(), value=data)

    def send_inspection_image(self, line_id, image_bytes, metadata):
        """Large image - use LFS"""
        headers = [
            ('LFS_BLOB', b''),
            ('line-id', line_id.encode()),
            ('captured-at', str(metadata['timestamp']).encode()),
            ('camera-id', metadata['camera'].encode()),
        ]
        self.producer.send(
            'quality-images',
            key=line_id.encode(),
            value=image_bytes,
            headers=headers
        )

    def send_thermal_scan(self, machine_id, scan_bytes, checksum):
        """Very large thermal scan - use LFS with checksum validation"""
        headers = [
            ('LFS_BLOB', checksum.encode()),  # Proxy validates checksum
            ('machine-id', machine_id.encode()),
            ('scan-type', b'thermal-full'),
        ]
        self.producer.send(
            'thermal-scans',
            key=machine_id.encode(),
            value=scan_bytes,
            headers=headers
        )
```

### Analytics Consumer

```python
from lfs_sdk import LfsConsumer
import cv2
import numpy as np

class QualityAnalyzer:
    def __init__(self):
        self.consumer = LfsConsumer(
            kafka_config={'bootstrap.servers': 'broker:9092'},
            lfs_config={'s3_bucket': 'factory-lfs', 's3_region': 'us-east-1'}
        )
        self.model = load_defect_detection_model()

    def analyze_images(self):
        self.consumer.subscribe(['quality-images'])

        while True:
            records = self.consumer.poll(timeout_ms=1000)

            for record in records:
                # LFS SDK fetches image from S3 automatically
                image_bytes = record.value()

                # Decode and analyze
                image = cv2.imdecode(
                    np.frombuffer(image_bytes, np.uint8),
                    cv2.IMREAD_COLOR
                )

                defects = self.model.detect(image)

                if defects:
                    self.alert_quality_team(record.key(), defects)

            self.consumer.commit()
```

### Business Benefits

| Challenge | LFS Solution |
|-----------|--------------|
| 3.5 TB/day of images | S3 storage, virtually unlimited |
| Mixed data sizes | Same pipeline, LFS header for large data |
| Real-time analytics | Kafka semantics preserved, consumers get notified immediately |
| Cost | S3 storage ($0.023/GB) vs Kafka broker memory |
| Retention | S3 lifecycle policies, years of history |

---

## Scenario 4: Healthcare Medical Imaging

### Business Context

**Company:** RadiologyNet
**Industry:** Healthcare / Medical imaging
**Challenge:** Distribute DICOM medical images (CT, MRI, X-ray) to radiologists for diagnosis.

### Compliance Requirements

- HIPAA compliance (encryption, audit logs)
- Image integrity verification (checksums)
- Full audit trail of who accessed what
- Images must be immutable once stored

### LFS Solution with Compliance

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MEDICAL IMAGING DISTRIBUTION                              │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                        Imaging Modalities                              │  │
│  │                                                                        │  │
│  │   ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐          │  │
│  │   │ CT      │    │ MRI     │    │ X-Ray   │    │ Ultrasound│         │  │
│  │   │ Scanner │    │ Machine │    │ Machine │    │ Machine  │          │  │
│  │   └────┬────┘    └────┬────┘    └────┬────┘    └────┬────┘          │  │
│  │        │              │              │              │                 │  │
│  │        └──────────────┴──────┬───────┴──────────────┘                 │  │
│  │                              │                                         │  │
│  │                              ▼                                         │  │
│  │                    ┌───────────────────┐                              │  │
│  │                    │  DICOM Gateway    │                              │  │
│  │                    │                   │                              │  │
│  │                    │  - Receives DICOM │                              │  │
│  │                    │  - Computes SHA256│                              │  │
│  │                    │  - Adds LFS_BLOB  │                              │  │
│  │                    │  - Adds patient ID│                              │  │
│  │                    │    (encrypted)    │                              │  │
│  │                    └─────────┬─────────┘                              │  │
│  │                              │                                         │  │
│  └──────────────────────────────┼─────────────────────────────────────────┘  │
│                                 │                                             │
│                                 ▼                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                          LFS Proxy                                     │  │
│  │                                                                        │  │
│  │   - Validates checksum from header                                    │  │
│  │   - Uploads to S3 with SSE-KMS encryption                            │  │
│  │   - Creates envelope with checksum                                    │  │
│  │                                                                        │  │
│  └──────────────────────────────┬────────────────────────────────────────┘  │
│                                 │                                             │
│                                 ▼                                             │
│                    ┌───────────────────────┐                                 │
│                    │   medical-images      │                                 │
│                    │   (topic)             │                                 │
│                    │                       │                                 │
│                    │   - Immutable log     │                                 │
│                    │   - Patient ID header │                                 │
│                    │   - Study ID header   │                                 │
│                    │                       │                                 │
│                    └───────────────────────┘                                 │
│                                 │                                             │
│          ┌──────────────────────┼──────────────────────┐                     │
│          ▼                      ▼                      ▼                      │
│   ┌─────────────┐        ┌─────────────┐        ┌─────────────┐             │
│   │ Radiologist │        │ AI Triage   │        │ Audit       │             │
│   │ Workstation │        │ System      │        │ Logger      │             │
│   │ (LFS SDK)   │        │ (LFS SDK)   │        │ (metadata)  │             │
│   │             │        │             │        │             │             │
│   │ Fetches +   │        │ Fetches +   │        │ Records all │             │
│   │ displays    │        │ analyzes    │        │ access      │             │
│   └─────────────┘        └─────────────┘        └─────────────┘             │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

### HIPAA Compliance Features

| Requirement | LFS Implementation |
|-------------|-------------------|
| Encryption at rest | S3 SSE-KMS with customer-managed key |
| Encryption in transit | TLS 1.3 for all connections |
| Audit trail | Kafka topic = immutable log of all image events |
| Access control | S3 bucket policies, IAM roles |
| Integrity | SHA256 checksum validated on upload and download |
| Data retention | S3 lifecycle policies (7 years for HIPAA) |

### Image Integrity Verification

```java
public class DicomGateway {
    public void sendImage(DicomImage image, PatientInfo patient) {
        // Compute checksum for integrity
        String checksum = sha256(image.getBytes());

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
            "medical-images",
            patient.getStudyId(),
            image.getBytes()
        );

        // Headers for compliance
        record.headers().add("LFS_BLOB", checksum.getBytes());  // Checksum validation
        record.headers().add("patient-id", encrypt(patient.getId()).getBytes());
        record.headers().add("study-id", patient.getStudyId().getBytes());
        record.headers().add("modality", image.getModality().getBytes());
        record.headers().add("acquired-at", image.getAcquisitionTime().toString().getBytes());

        producer.send(record);

        // Log for audit
        auditLog.record("IMAGE_SENT", patient.getStudyId(), checksum);
    }
}
```

---

## Summary: When to Use LFS

### Use LFS When

| Scenario | Why LFS |
|----------|---------|
| Payloads > 1MB | Standard Kafka struggles without tuning |
| File attachments | Natural "file as message" semantics |
| Need audit trail | Kafka provides immutable event log |
| Multiple consumers | Each consumer resolves independently |
| Mixed sizes | LFS header opt-in, normal traffic unchanged |

### Don't Use LFS When

| Scenario | Alternative |
|----------|-------------|
| All data < 1MB | Standard Kafka is fine |
| No need for streaming semantics | Direct S3 upload |
| Single consumer only | Direct S3 with notifications |
| Latency-critical (< 10ms) | S3 adds latency |

### ROI Summary

| Benefit | Impact |
|---------|--------|
| Developer productivity | 70% less code for large file handling |
| Operational simplicity | Single pipeline for all sizes |
| Data integrity | Checksums validated automatically |
| Scalability | S3 handles storage, Kafka handles streaming |
| Cost efficiency | S3 storage much cheaper than Kafka broker memory |
