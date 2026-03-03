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

# LFS Demo Gallery

This page highlights the LFS demo scripts and what each one showcases.

## Quick Start

All demos are environment-driven. You can override any setting via env vars (see `.env.example`).

Common prerequisites:
- A running Kubernetes cluster (kind or otherwise)
- `kubectl` access
- Demo images available locally or via registry

## Demos

### `scripts/lfs-demo.sh`
Baseline LFS proxy demo that uploads blobs, emits pointer records, and verifies objects in MinIO.

Use when you want:
- A minimal end‑to‑end LFS flow
- Pointer record parsing and verification output

### `scripts/industrial-lfs-demo.sh`
Manufacturing/IoT scenario that mixes small telemetry (passthrough) with large inspection images (LFS).

Use when you want:
- Mixed payload patterns
- Realistic industrial storytelling (telemetry + images)

### `scripts/medical-lfs-demo.sh`
Healthcare imaging workflow with DICOM‑like blobs and metadata/audit topics.

Use when you want:
- Very large payloads
- Compliance‑style audit trail narrative

### `scripts/video-lfs-demo.sh`
Media streaming scenario for large video blobs with codec metadata and keyframe references.

Use when you want:
- Large media files
- Content‑explosion narrative (raw video + metadata + frames)

## Recommended Environment Overrides

- `LFS_PROXY_IMAGE` / `E2E_CLIENT_IMAGE`
- `MINIO_BUCKET`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`
- `LFS_PROXY_HTTP_PORT`, `LFS_PROXY_KAFKA_PORT`
- `KAFSCALE_S3_NAMESPACE`

## Notes

- All demo scripts assume MinIO and LFS proxy are deployed into the same namespace.
- If you change service names or ports, override `LFS_PROXY_SERVICE_HOST`, `MINIO_SERVICE_HOST`, or the port env vars.
