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

# LFS Architecture Document

## Executive Summary

This document describes the architecture of the Large File Support (LFS) feature for KafScale. LFS enables efficient handling of large binary payloads (files, images, videos) by offloading storage to S3 while maintaining Kafka's streaming semantics.

**Key Architectural Principle:** The broker remains unchanged. All LFS logic is handled by a proxy layer and client SDKs.

---

## System Context

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           EXTERNAL SYSTEMS                                   │
│                                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │ Application │  │ Application │  │ Data        │  │ Monitoring  │        │
│  │ Producers   │  │ Consumers   │  │ Pipeline    │  │ Systems     │        │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘        │
│         │                │                │                │                │
└─────────┼────────────────┼────────────────┼────────────────┼────────────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           KAFSCALE LFS SYSTEM                                │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        LFS Proxy Cluster                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        KafScale Broker Cluster                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        S3 Object Storage                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Architecture

### High-Level Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│    ┌────────────────┐              ┌────────────────┐                       │
│    │   Producer     │              │   Consumer     │                       │
│    │   Application  │              │   Application  │                       │
│    └───────┬────────┘              └───────┬────────┘                       │
│            │                               │                                 │
│            │ Kafka Protocol                │ Kafka Protocol                  │
│            │ (+ LFS_BLOB header)           │                                 │
│            ▼                               │                                 │
│    ┌────────────────┐                      │                                 │
│    │   LFS Proxy    │                      │                                 │
│    │                │                      │                                 │
│    │  ┌──────────┐  │                      │                                 │
│    │  │ Request  │  │                      │                                 │
│    │  │ Router   │  │                      │                                 │
│    │  └────┬─────┘  │                      │                                 │
│    │       │        │                      │                                 │
│    │  ┌────▼─────┐  │                      │                                 │
│    │  │ LFS      │  │         ┌────────────▼───────────┐                    │
│    │  │ Handler  │──┼────────▶│   Consumer Wrapper     │                    │
│    │  └────┬─────┘  │         │   (LFS SDK)            │                    │
│    │       │        │         │                        │                    │
│    └───────┼────────┘         │  ┌──────────────────┐  │                    │
│            │                  │  │ Envelope Detector│  │                    │
│            │                  │  └────────┬─────────┘  │                    │
│            │                  │           │            │                    │
│            │                  │  ┌────────▼─────────┐  │                    │
│            │                  │  │ S3 Resolver      │  │                    │
│            │                  │  └────────┬─────────┘  │                    │
│            │                  │           │            │                    │
│            │                  └───────────┼────────────┘                    │
│            │                              │                                  │
│            ▼                              ▼                                  │
│    ┌────────────────────────────────────────────────────┐                   │
│    │                    S3 Bucket                        │                   │
│    │                                                     │                   │
│    │   namespace/topic/lfs/2026/01/31/obj-uuid          │                   │
│    │                                                     │                   │
│    └────────────────────────────────────────────────────┘                   │
│            │                                                                 │
│            │ Pointer Record                                                  │
│            ▼                                                                 │
│    ┌────────────────┐                                                        │
│    │   KafScale     │                                                        │
│    │   Broker       │                                                        │
│    │   (unchanged)  │                                                        │
│    └────────────────┘                                                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## LFS Proxy Architecture

### Internal Structure

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              LFS PROXY                                       │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Network Layer                                │   │
│  │                                                                      │   │
│  │   ┌──────────────────┐        ┌──────────────────┐                  │   │
│  │   │  Kafka Listener  │        │  HTTP Listener   │                  │   │
│  │   │     :9092        │        │     :8080        │                  │   │
│  │   └────────┬─────────┘        └────────┬─────────┘                  │   │
│  │            │                           │                             │   │
│  └────────────┼───────────────────────────┼─────────────────────────────┘   │
│               │                           │                                  │
│               ▼                           ▼                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Request Router                               │   │
│  │                                                                      │   │
│  │   ┌──────────────────────────────────────────────────────────────┐  │   │
│  │   │                    Header Detector                            │  │   │
│  │   │                                                               │  │   │
│  │   │   if headers.contains("LFS_BLOB") → LFS Handler               │  │   │
│  │   │   else → Passthrough                                          │  │   │
│  │   │                                                               │  │   │
│  │   └──────────────────────────────────────────────────────────────┘  │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│               │                           │                                  │
│       ┌───────┴───────┐                   │                                  │
│       ▼               ▼                   ▼                                  │
│  ┌─────────┐    ┌─────────────────────────────────────────────────────┐    │
│  │Passthru │    │                  LFS Handler                         │    │
│  │         │    │                                                      │    │
│  │ Forward │    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │    │
│  │ to      │    │  │ Checksum    │  │ S3 Uploader │  │ Envelope    │  │    │
│  │ Broker  │    │  │ Computer    │  │ (Multipart) │  │ Creator     │  │    │
│  │         │    │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  │    │
│  └────┬────┘    │         │                │                │         │    │
│       │         │         └────────────────┼────────────────┘         │    │
│       │         │                          │                          │    │
│       │         │                          ▼                          │    │
│       │         │                   ┌─────────────┐                   │    │
│       │         │                   │ Kafka       │                   │    │
│       │         │                   │ Producer    │                   │    │
│       │         │                   └──────┬──────┘                   │    │
│       │         │                          │                          │    │
│       │         └──────────────────────────┼──────────────────────────┘    │
│       │                                    │                                │
│       └────────────────┬───────────────────┘                                │
│                        │                                                     │
│                        ▼                                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         Broker Connection                            │   │
│  │                                                                      │   │
│  │   ┌──────────────────┐                                              │   │
│  │   │  Kafka Client    │ ──────────────────▶ KafScale Broker          │   │
│  │   │  (franz-go)      │                                              │   │
│  │   └──────────────────┘                                              │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Request Flow State Machine

```
                    ┌─────────────────┐
                    │  Request        │
                    │  Received       │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
              ┌─────│  Check LFS      │─────┐
              │     │  Header         │     │
              │     └─────────────────┘     │
              │                             │
         No Header                     Has LFS_BLOB
              │                             │
              ▼                             ▼
     ┌─────────────────┐           ┌─────────────────┐
     │  Passthrough    │           │  Validate       │
     │  Mode           │           │  Request        │
     └────────┬────────┘           └────────┬────────┘
              │                             │
              │                             ▼
              │                    ┌─────────────────┐
              │                    │  Init S3        │
              │                    │  Multipart      │
              │                    └────────┬────────┘
              │                             │
              │                             ▼
              │                    ┌─────────────────┐
              │                    │  Stream to S3   │
              │                    │  + Compute Hash │
              │                    └────────┬────────┘
              │                             │
              │                    ┌────────┴────────┐
              │                    │                 │
              │               Success             Failure
              │                    │                 │
              │                    ▼                 ▼
              │           ┌─────────────────┐ ┌─────────────────┐
              │           │  Validate       │ │  Abort          │
              │           │  Checksum       │ │  Multipart      │
              │           └────────┬────────┘ └────────┬────────┘
              │                    │                   │
              │               ┌────┴────┐              │
              │               │         │              │
              │            Valid     Invalid           │
              │               │         │              │
              │               ▼         ▼              │
              │      ┌─────────────┐ ┌─────────────┐   │
              │      │ Complete S3 │ │ Abort S3    │   │
              │      │ Upload      │ │ Return Err  │   │
              │      └──────┬──────┘ └─────────────┘   │
              │             │                          │
              │             ▼                          │
              │      ┌─────────────────┐               │
              │      │  Create         │               │
              │      │  Envelope       │               │
              │      └────────┬────────┘               │
              │               │                        │
              └───────┬───────┘                        │
                      │                                │
                      ▼                                │
             ┌─────────────────┐                       │
             │  Produce to     │                       │
             │  Broker         │                       │
             └────────┬────────┘                       │
                      │                                │
             ┌────────┴────────┐                       │
             │                 │                       │
          Success           Failure                    │
             │                 │                       │
             ▼                 ▼                       │
    ┌─────────────────┐ ┌─────────────────┐           │
    │  Return ACK     │ │  Log Orphan     │           │
    │  to Client      │ │  Return Error   │◀──────────┘
    └─────────────────┘ └─────────────────┘
```

---

## Consumer Wrapper Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CONSUMER WRAPPER SDK                                 │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                           Public API                                   │  │
│  │                                                                        │  │
│  │   NewConsumer(baseConsumer, config) → LfsConsumer                     │  │
│  │   consumer.Poll(timeout) → []Record                                   │  │
│  │   record.Value() → []byte                                             │  │
│  │   record.ValueStream() → io.ReadCloser                                │  │
│  │                                                                        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                     │                                        │
│                                     ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                        Internal Components                             │  │
│  │                                                                        │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │  │
│  │  │ Base Consumer   │  │ Envelope        │  │ S3 Client       │       │  │
│  │  │ Wrapper         │  │ Detector        │  │                 │       │  │
│  │  │                 │  │                 │  │ - GetObject     │       │  │
│  │  │ - Poll()        │  │ - IsLfs()       │  │ - Streaming     │       │  │
│  │  │ - Subscribe()   │  │ - Parse()       │  │ - Retry         │       │  │
│  │  │ - Commit()      │  │ - Validate()    │  │                 │       │  │
│  │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘       │  │
│  │           │                    │                    │                 │  │
│  │           └────────────────────┼────────────────────┘                 │  │
│  │                                │                                      │  │
│  │                                ▼                                      │  │
│  │  ┌───────────────────────────────────────────────────────────────┐   │  │
│  │  │                      Record Resolver                           │   │  │
│  │  │                                                                │   │  │
│  │  │   for each record:                                             │   │  │
│  │  │     if IsLfsEnvelope(record.value):                            │   │  │
│  │  │       envelope = ParseEnvelope(record.value)                   │   │  │
│  │  │       blob = S3.GetObject(envelope.bucket, envelope.key)       │   │  │
│  │  │       ValidateChecksum(blob, envelope.sha256)                  │   │  │
│  │  │       return ResolvedRecord(blob)                              │   │  │
│  │  │     else:                                                      │   │  │
│  │  │       return record  // passthrough                            │   │  │
│  │  │                                                                │   │  │
│  │  └───────────────────────────────────────────────────────────────┘   │  │
│  │                                                                        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Architecture

### Message Flow

```
Producer Message                    Stored in Kafka                Consumer Receives
─────────────────                   ───────────────                ─────────────────

┌─────────────────┐                ┌─────────────────┐            ┌─────────────────┐
│ Key: "order-1"  │                │ Key: "order-1"  │            │ Key: "order-1"  │
│                 │                │                 │            │                 │
│ Value:          │                │ Value:          │            │ Value:          │
│ <100MB PDF>     │  ─────────▶    │ {               │ ─────────▶ │ <100MB PDF>     │
│                 │                │   "kfs_lfs": 1, │   (SDK)    │                 │
│ Headers:        │    Proxy       │   "bucket":...  │            │ Headers:        │
│   LFS_BLOB: ""  │  transforms    │   "key":...     │            │   LFS_BLOB: ""  │
│                 │                │   "sha256":...  │            │                 │
└─────────────────┘                │ }               │            └─────────────────┘
                                   │                 │
                                   │ Headers:        │
                                   │   LFS_BLOB: ""  │
                                   └─────────────────┘

                                   S3 Storage
                                   ──────────

                                   ┌─────────────────┐
                                   │ Bucket:         │
                                   │   kafscale-lfs  │
                                   │                 │
                                   │ Key:            │
                                   │   ns/topic/lfs/ │
                                   │   2026/01/31/   │
                                   │   obj-uuid      │
                                   │                 │
                                   │ Content:        │
                                   │   <100MB PDF>   │
                                   └─────────────────┘
```

### S3 Object Layout

```
kafscale-lfs/
├── namespace-1/
│   ├── topic-a/
│   │   └── lfs/
│   │       ├── 2026/
│   │       │   ├── 01/
│   │       │   │   ├── 31/
│   │       │   │   │   ├── obj-abc123
│   │       │   │   │   ├── obj-def456
│   │       │   │   │   └── obj-ghi789
│   │       │   │   └── 30/
│   │       │   │       └── obj-...
│   │       │   └── 02/
│   │       │       └── ...
│   │       └── 2025/
│   │           └── ...
│   └── topic-b/
│       └── lfs/
│           └── ...
└── namespace-2/
    └── ...
```

---

## Deployment Architecture

### Production Topology

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              KUBERNETES CLUSTER                              │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                            Ingress / Load Balancer                     │  │
│  │                                                                        │  │
│  │   External :9092 ─────────────────────▶ LFS Proxy Service :9092       │  │
│  │   External :8080 ─────────────────────▶ LFS Proxy Service :8080       │  │
│  │                                                                        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                      │                                       │
│                                      ▼                                       │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                         LFS Proxy Deployment                           │  │
│  │                         (StatefulSet, 3 replicas)                      │  │
│  │                                                                        │  │
│  │   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐                │  │
│  │   │ lfs-proxy-0 │   │ lfs-proxy-1 │   │ lfs-proxy-2 │                │  │
│  │   │             │   │             │   │             │                │  │
│  │   │ CPU: 2      │   │ CPU: 2      │   │ CPU: 2      │                │  │
│  │   │ Mem: 4Gi    │   │ Mem: 4Gi    │   │ Mem: 4Gi    │                │  │
│  │   └─────────────┘   └─────────────┘   └─────────────┘                │  │
│  │                                                                        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                      │                                       │
│                                      ▼                                       │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                       KafScale Broker StatefulSet                      │  │
│  │                       (3 replicas)                                     │  │
│  │                                                                        │  │
│  │   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐                │  │
│  │   │ kafscale-0  │   │ kafscale-1  │   │ kafscale-2  │                │  │
│  │   └─────────────┘   └─────────────┘   └─────────────┘                │  │
│  │                                                                        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              AWS / S3-Compatible                             │
│                                                                              │
│   ┌───────────────────────────────────────────────────────────────────┐    │
│   │                         S3 Bucket                                  │    │
│   │                         kafscale-lfs                               │    │
│   │                                                                    │    │
│   │   - Server-side encryption (SSE-S3)                               │    │
│   │   - Lifecycle policy (90 day retention)                           │    │
│   │   - Versioning disabled                                           │    │
│   │   - Cross-region replication (optional)                           │    │
│   │                                                                    │    │
│   └───────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Kubernetes Resources

```yaml
# LFS Proxy Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lfs-proxy
spec:
  replicas: 3
  selector:
    matchLabels:
      app: lfs-proxy
  template:
    spec:
      containers:
      - name: lfs-proxy
        image: kafscale/lfs-proxy:v1.0.0
        ports:
        - containerPort: 9092  # Kafka
        - containerPort: 8080  # HTTP
        - containerPort: 9090  # Metrics
        resources:
          requests:
            cpu: "1"
            memory: "2Gi"
          limits:
            cpu: "2"
            memory: "4Gi"
        env:
        - name: LFS_KAFKA_BROKER
          value: "kafscale-broker:9093"
        - name: LFS_S3_BUCKET
          value: "kafscale-lfs"
```

---

## Security Architecture

### Trust Boundaries

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            EXTERNAL (Untrusted)                              │
│                                                                              │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                    │
│   │ Producer    │    │ Consumer    │    │ Admin       │                    │
│   │ Application │    │ Application │    │ Tools       │                    │
│   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘                    │
│          │                  │                  │                            │
└──────────┼──────────────────┼──────────────────┼────────────────────────────┘
           │ TLS + SASL       │ TLS + SASL       │ TLS + SASL
           │                  │                  │
┌──────────┼──────────────────┼──────────────────┼────────────────────────────┐
│          ▼                  ▼                  ▼                            │
│   ════════════════════════════════════════════════════════                 │
│                        AUTHENTICATION BOUNDARY                              │
│   ════════════════════════════════════════════════════════                 │
│          │                  │                  │                            │
│          ▼                  ▼                  ▼                            │
│   ┌─────────────────────────────────────────────────────────────┐          │
│   │                     LFS Proxy                                │          │
│   │                     (validates SASL, forwards to broker)     │          │
│   └──────────────────────────┬──────────────────────────────────┘          │
│                              │                                              │
│   ════════════════════════════════════════════════════════                 │
│                        INTERNAL NETWORK BOUNDARY                            │
│   ════════════════════════════════════════════════════════                 │
│                              │                                              │
│          ┌───────────────────┼───────────────────┐                         │
│          ▼                   ▼                   ▼                          │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                 │
│   │  KafScale   │     │  S3 Bucket  │     │  etcd       │                 │
│   │  Broker     │     │  (IAM auth) │     │  (mTLS)     │                 │
│   └─────────────┘     └─────────────┘     └─────────────┘                 │
│                                                                             │
│                            INTERNAL (Trusted)                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Credential Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                           │
│  1. Producer authenticates to Proxy via SASL                             │
│                                                                           │
│     Producer ──── SASL_PLAIN(user, pass) ────▶ LFS Proxy                 │
│                                                                           │
│  2. Proxy authenticates to Broker (passthrough)                          │
│                                                                           │
│     LFS Proxy ──── SASL_PLAIN(user, pass) ────▶ KafScale Broker          │
│                                                                           │
│  3. Proxy authenticates to S3 (IAM / access keys)                        │
│                                                                           │
│     LFS Proxy ──── AWS SigV4 (IAM role) ────▶ S3                         │
│                                                                           │
│  4. Consumer SDK authenticates to S3 (separate credentials)              │
│                                                                           │
│     Consumer SDK ──── AWS SigV4 (access key) ────▶ S3                    │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Quality Attributes

### Scalability

| Component | Scaling Strategy | Limits |
|-----------|------------------|--------|
| LFS Proxy | Horizontal (stateless) | Limited by S3 throughput |
| Consumer Wrapper | Per-application | Limited by S3 bandwidth |
| S3 Storage | Managed by AWS/MinIO | Effectively unlimited |

### Availability

| Component | Availability Target | Strategy |
|-----------|---------------------|----------|
| LFS Proxy | 99.9% | Multi-replica, health checks |
| S3 | 99.99% | Managed service (AWS) |
| Broker | 99.9% | Existing KafScale HA |

### Performance

| Metric | Target | Notes |
|--------|--------|-------|
| Passthrough latency | <5ms p99 | Non-LFS traffic |
| LFS upload latency | S3 latency + 20ms | Dominated by S3 |
| Consumer resolution | S3 latency + 10ms | Dominated by S3 |

---

## Appendix: Alternatives Considered

### Alternative 1: Broker-Side LFS

**Description:** Implement LFS logic directly in the broker.

**Rejected because:**
- Increases broker complexity
- Requires broker code changes and redeployment
- Harder to upgrade/maintain independently

### Alternative 2: Client-Side Upload

**Description:** Clients upload to S3 directly, then produce pointer.

**Rejected because:**
- Two-phase commit problem (orphan objects)
- Requires SDK for all producers
- Exposes S3 credentials to clients

### Alternative 3: Embedded Proxy in Client

**Description:** SDK handles S3 upload transparently.

**Rejected because:**
- Every producer needs SDK
- Credential management per client
- No central control/observability
