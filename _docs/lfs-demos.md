---
layout: doc
title: "KafScale LFS Demos: Large File Examples"
description: Runnable demos for the KafScale Large File Support system, from Docker Compose to a kind cluster.
permalink: /lfs-demos/
nav_title: LFS Demos
nav_order: 4
nav_group: LFS
---

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

# LFS Demos

KafScale ships two supported ways to try Large File Support: `make lfs-demo` on a kind cluster, and the Docker Compose stack in `deploy/docker-compose`.

## Quick reference

| Command | What it runs | Needs cluster? |
|---|---|---|
| `make lfs-demo` | Kind cluster with a broker, MinIO, and the LFS proxy | Yes (kind) |
| `docker compose up -d` in `deploy/docker-compose` | Local broker, MinIO, LFS HTTP API, and the browser demo profile | No |

Industry-specific make targets such as `lfs-demo-idoc`, `lfs-demo-medical`, `lfs-demo-video`, `lfs-demo-industrial`, and `e72-browser-demo` are not in this repository on main.

---

## Kind demo

```bash
make lfs-demo
```

`make lfs-demo` is a wrapper around `demo-platform-bootstrap`. It starts a kind cluster and deploys MinIO, a broker, and the LFS proxy into the demo namespace (`KAFSCALE_DEMO_NAMESPACE`, default `kafscale-demo`). The advertised Kafka address defaults to `kafscale-broker.<namespace>.svc.cluster.local:9092`.

The target sets `KAFSCALE_DEMO_PROXY=0` and `KAFSCALE_DEMO_CONSOLE=0`, and uses a single broker replica. After it is up, use the LFS HTTP API on the proxy as described in [LFS on Kubernetes](/lfs-helm/#http-api) and the [Docker Compose README](https://github.com/KafScale/platform/blob/main/deploy/docker-compose/README.md).

---

## Docker Compose (local, no cluster)

```bash
cd deploy/docker-compose
docker compose up -d
```

This starts MinIO, a KafScale broker, and the LFS proxy. The Compose README documents the ports (LFS HTTP API on `http://localhost:8080`) and example `/lfs/produce` and `/lfs/download` calls. The `demo` profile also starts the browser LFS UI on port 3072.

---

## IDoc explode

`idoc-explode` (`cmd/idoc-explode`) reads SAP IDoc XML or JSONL envelopes and writes one file per topic. It is a command-line processor, not a make demo. Flags are `-input` (file path; empty reads stdin) and `-out` (output directory). See [idoc-explode](/operations/#idoc-explode) in the operations guide.

---

## Related docs

- [LFS Proxy](/lfs-proxy/): architecture and configuration
- [LFS Helm Deployment](/lfs-helm/): Kubernetes deployment guide
- [LFS Client SDKs](/lfs-sdks/): SDK API reference for Go, Java, Python, and JavaScript
