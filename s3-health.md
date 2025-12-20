---
layout: doc
title: S3 Health States
description: Definitions, metrics, and alert thresholds for KafScale S3 health state transitions.
---

# S3 Health States

KafScale brokers publish a live S3 health state based on latency and error-rate sampling windows.

## State definitions

- **HEALTHY**: S3 latency and error rate below warning thresholds.
- **DEGRADED**: Latency or error rate above warning thresholds.
- **UNAVAILABLE**: Latency or error rate above critical thresholds.

## Metrics and thresholds

Metrics:

- `kafscale_s3_health_state` (label `state`)
- `kafscale_s3_latency_ms_avg`
- `kafscale_s3_error_rate`
- `kafscale_s3_state_duration_seconds`

Thresholds (configurable):

- `KAFSCALE_S3_LATENCY_WARN_MS`
- `KAFSCALE_S3_LATENCY_CRIT_MS`
- `KAFSCALE_S3_ERROR_RATE_WARN`
- `KAFSCALE_S3_ERROR_RATE_CRIT`
- `KAFSCALE_S3_HEALTH_WINDOW_SEC`

## Alerting integration

Wire S3 health transitions into Prometheus rules or your alerting stack. A common pattern:

- Alert on `state="unavailable"` immediately.
- Alert on `state="degraded"` if sustained for more than a few minutes.
- Track latency/error trends to tune thresholds per region.
