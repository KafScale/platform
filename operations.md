---
layout: doc
title: Operations Guide
description: "Production operations guidance: monitoring, scaling, backups, upgrades, and troubleshooting for KafScale."
permalink: /operations/
---

# Operations Guide

## Monitoring with Prometheus

KafScale exposes Prometheus metrics on `/metrics` from both brokers and the operator:

- Broker metrics: `http://<broker-host>:9093/metrics`
- Operator metrics: `http://<operator-host>:8080/metrics`

Key metrics include:

- `kafscale_s3_health_state`
- `kafscale_s3_latency_ms_avg`
- `kafscale_s3_error_rate`
- `kafscale_produce_rps`
- `kafscale_fetch_rps`
- `kafscale_operator_etcd_snapshot_age_seconds`

Grafana templates live in `docs/grafana/broker-dashboard.json` in the main branch.

## Scaling brokers

Stateless brokers scale horizontally with Kubernetes HPA. Apply HPA against broker deployment CPU or custom metrics and rely on S3 for durability instead of disk rebalancing.

Example (CPU-based HPA):

```bash
kubectl autoscale deployment demo-broker --cpu-percent=70 --min=3 --max=12
```

## Backup and disaster recovery

The operator can manage etcd snapshots to S3. Recommended alerts:

- `KafscaleSnapshotAccessFailed` – snapshot writes failing
- `KafscaleSnapshotStale` – last successful snapshot older than threshold
- `KafscaleSnapshotNeverSucceeded` – no successful snapshots recorded

## Upgrading KafScale versions

Use `helm upgrade --install` with pinned image tags. The operator drains brokers through the gRPC control plane before restarting pods. Rollbacks use `helm rollback`.

## Troubleshooting common issues

- Check `/metrics` for S3 health state transitions (healthy/degraded/unavailable).
- Verify etcd endpoints and credentials (see `/configuration`).
- Confirm the operator can write to the snapshot bucket and that `KAFSCALE_OPERATOR_ETCD_SNAPSHOT_*` values are set.
