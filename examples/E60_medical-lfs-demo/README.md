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

# Medical LFS Demo (E60)

This demo showcases LFS (Large File Support) for healthcare workloads, demonstrating the **content explosion pattern** with DICOM-like medical imaging data.

## Quick Start

```bash
make lfs-demo-medical
```

## What This Demonstrates

- **Large blob handling**: 500MB+ DICOM-like medical images stored in S3 via LFS
- **Content explosion pattern**: Single upload spawns multiple derived topics
- **Metadata extraction**: Patient ID, modality, study date extracted to separate topic
- **Audit trail**: Access events logged for compliance
- **Checksum verification**: SHA256 integrity validation

## Architecture

```
DICOM Upload ──► LFS Proxy ──► S3 (blob) + Kafka (pointer)
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
            medical-images    medical-meta    medical-audit
            (LFS pointer)     (patient info)  (access log)
```

## Content Explosion Topics

| Topic | Purpose | Contains LFS? |
|-------|---------|---------------|
| `medical-images` | Original DICOM blob pointer | Yes |
| `medical-metadata` | Patient ID, modality, study info | No |
| `medical-audit` | Access timestamps, user actions | No |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MEDICAL_DEMO_NAMESPACE` | `kafscale-medical` | Kubernetes namespace |
| `MEDICAL_DEMO_BLOB_SIZE` | `524288000` (500MB) | DICOM blob size |
| `MEDICAL_DEMO_BLOB_COUNT` | `3` | Number of studies to upload |
| `MEDICAL_DEMO_CLEANUP` | `1` | Cleanup resources after demo |

## Sample Output

```
[1/8] Setting up medical LFS demo environment...
[2/8] Deploying LFS proxy and MinIO...
[3/8] Creating content explosion topics...
      - medical-images (LFS blobs)
      - medical-metadata (extracted info)
      - medical-audit (access log)
[4/8] Generating synthetic DICOM data...
      Patient: P-2026-001, Modality: CT, Size: 500MB
      Patient: P-2026-002, Modality: MRI, Size: 500MB
      Patient: P-2026-003, Modality: XRAY, Size: 500MB
[5/8] Uploading via LFS proxy...
[6/8] Consuming pointer records...
+------------------+------------------------------------------------------------------+--------+
| Patient          | SHA256                                                           | Status |
+------------------+------------------------------------------------------------------+--------+
| P-2026-001       | a1b2c3d4e5f6...                                                  | ok     |
| P-2026-002       | b2c3d4e5f6a1...                                                  | ok     |
| P-2026-003       | c3d4e5f6a1b2...                                                  | ok     |
+------------------+------------------------------------------------------------------+--------+
[7/8] Verifying blobs in MinIO...
      S3 blobs found: 3
[8/8] Content explosion summary:
      medical-images: 3 LFS pointers
      medical-metadata: 3 patient records
      medical-audit: 9 access events
```

## Real-World Use Cases

### Radiology Department
- CT/MRI scans uploaded by technicians
- Automatic metadata extraction for PACS integration
- Audit trail for HIPAA compliance

### Pathology Lab
- Whole slide images (1-5GB) stored efficiently
- AI inference results written to derived topic
- Chain of custody maintained via audit log

### Telehealth Platform
- Remote diagnostic imaging from clinics
- Real-time availability via Kafka consumers
- Compliance-ready access logging

## Why Healthcare Buyers Care

1. **Compliance**: HIPAA requires audit trails - built into the pattern
2. **Size**: Medical images are inherently large - LFS handles this
3. **Integrity**: SHA256 checksums ensure no corruption
4. **Auditability**: Every access is logged automatically

## Next Steps

- Connect to real DICOM source (Orthanc, dcm4chee)
- Add real metadata extraction with pydicom
- Integrate with Iceberg processor for analytics
- Add AI inference fan-out topic

## Files

| File | Description |
|------|-------------|
| `README.md` | This documentation |
| `../../scripts/medical-lfs-demo.sh` | Main demo script |
