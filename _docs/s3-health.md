---
layout: doc
title: "KafScale S3 Health States: Metrics and Alerts"
description: Definitions, metrics, and alert thresholds for KafScale S3 health state transitions.
permalink: /s3-health/
nav_title: S3 Health
nav_order: 7
nav_group: References
---

# S3 Health States

KafScale brokers continuously monitor S3 availability and publish a health state based on latency and error-rate sampling. This state controls broker behavior and provides observability into storage health.

---

## State definitions

| State | Metric | Condition | Broker behavior |
|-------|--------|-----------|-----------------|
| Healthy | `kafscale_s3_health_state{state="healthy"} 1` | Average latency below warn and error rate below warn | Produce and fetch proceed as usual |
| Degraded | `kafscale_s3_health_state{state="degraded"} 1` | Average latency at or above warn, or error rate at or above warn, and neither has reached crit | Produce and fetch are rejected with a retriable `REQUEST_TIMED_OUT` |
| Unavailable | `kafscale_s3_health_state{state="unavailable"} 1` | Average latency at or above crit, or error rate at or above crit | Produce and fetch are rejected with a retriable `KAFKA_STORAGE_ERROR` |

The metric is a labeled gauge: the active state is `1` and the other two states are `0`. It is not a single 0/1/2 value.

<div class="diagram">
  <div class="diagram-label">State transitions</div>
  <svg viewBox="0 0 700 180" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="S3 health state machine">
    <defs>
      <marker id="ah-h" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="var(--diagram-stroke)"/></marker>
      <marker id="ag-h" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#34d399"/></marker>
      <marker id="ay-h" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#fbbf24"/></marker>
      <marker id="ar-h" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0,0 L10,5 L0,10 z" fill="#f87171"/></marker>
    </defs>

    <!-- Healthy -->
    <rect x="50" y="50" width="140" height="70" rx="10" fill="rgba(52, 211, 153, 0.15)" stroke="#34d399" stroke-width="1.5"/>
    <text x="120" y="78" font-size="12" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Healthy</text>
    <text x="120" y="98" font-size="10" fill="var(--diagram-label)" text-anchor="middle">healthy</text>

    <!-- Degraded -->
    <rect x="280" y="50" width="140" height="70" rx="10" fill="rgba(251, 191, 36, 0.15)" stroke="#fbbf24" stroke-width="1.5"/>
    <text x="350" y="78" font-size="12" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Degraded</text>
    <text x="350" y="98" font-size="10" fill="var(--diagram-label)" text-anchor="middle">degraded</text>

    <!-- Unavailable -->
    <rect x="510" y="50" width="140" height="70" rx="10" fill="rgba(248, 113, 113, 0.15)" stroke="#f87171" stroke-width="1.5"/>
    <text x="580" y="78" font-size="12" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Unavailable</text>
    <text x="580" y="98" font-size="10" fill="var(--diagram-label)" text-anchor="middle">unavailable</text>

    <!-- Forward arrows -->
    <path d="M190,75 L275,75" stroke="#fbbf24" stroke-width="2" fill="none" marker-end="url(#ay-h)"/>
    <text x="232" y="65" font-size="9" fill="var(--diagram-label)" text-anchor="middle">warn exceeded</text>

    <path d="M420,75 L505,75" stroke="#f87171" stroke-width="2" fill="none" marker-end="url(#ar-h)"/>
    <text x="462" y="65" font-size="9" fill="var(--diagram-label)" text-anchor="middle">crit exceeded</text>

    <!-- Recovery arrow -->
    <path d="M510,105 Q350,165 190,105" stroke="#34d399" stroke-width="1.5" stroke-dasharray="4,2" fill="none" marker-end="url(#ag-h)"/>
    <text x="350" y="160" font-size="9" fill="var(--diagram-label)" text-anchor="middle">metrics recover below thresholds</text>
  </svg>
</div>

---

## Threshold configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFSCALE_S3_LATENCY_WARN_MS` | `500` | Average latency (ms) that enters degraded |
| `KAFSCALE_S3_LATENCY_CRIT_MS` | `3000` | Average latency (ms) that enters unavailable |
| `KAFSCALE_S3_ERROR_RATE_WARN` | `0.2` | Error rate that enters degraded (20%) |
| `KAFSCALE_S3_ERROR_RATE_CRIT` | `0.6` | Error rate that enters unavailable (60%) |
| `KAFSCALE_S3_HEALTH_WINDOW_SEC` | `60` | Sampling window for the average latency and error rate |

Tune these based on your S3 region and latency expectations. Cross-region S3 access may require higher latency thresholds.

---

## Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kafscale_s3_health_state` | Gauge | `state` | `1` for the active state (`healthy`, `degraded`, `unavailable`), `0` otherwise |
| `kafscale_s3_latency_ms_avg` | Gauge | | Average S3 latency (ms) over the sliding window |
| `kafscale_s3_error_rate` | Gauge | | Fraction of failed S3 operations in the sliding window |
| `kafscale_s3_state_duration_seconds` | Gauge | | Seconds spent in the current state |

These are the S3 health series the broker exports on the metrics port. See [Metrics](/metrics/) for the full broker list. There is no `kafscale_s3_latency_ms_p99`, `kafscale_s3_state_transitions_total`, or `kafscale_s3_operations_total` metric.

---

## Alerting rules

Wire S3 health into Prometheus Alertmanager:

{% raw %}
```yaml
groups:
  - name: kafscale-s3-health
    rules:
      - alert: KafScaleS3Unavailable
        expr: kafscale_s3_health_state{state="unavailable"} == 1
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "KafScale S3 unavailable"
          description: "Broker {{ $labels.pod }} cannot reach S3. Produce and fetch are rejected."

      - alert: KafScaleS3Degraded
        expr: kafscale_s3_health_state{state="degraded"} == 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "KafScale S3 degraded"
          description: "Broker {{ $labels.pod }} S3 latency or error rate elevated for 5+ minutes. Produce and fetch are rejected."

      - alert: KafScaleS3LatencyHigh
        expr: kafscale_s3_latency_ms_avg > 300
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "KafScale S3 latency elevated"
          description: "Average S3 latency {{ $value }}ms on {{ $labels.pod }}."

      - alert: KafScaleS3ErrorRateHigh
        expr: kafscale_s3_error_rate > 0.005
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "KafScale S3 error rate elevated"
          description: "S3 error rate {{ $value | humanizePercentage }} on {{ $labels.pod }}."
```
{% endraw %}

---

## Behavior by state

### Healthy

Normal operation. All produce and fetch requests are processed.

### Degraded

Brokers reject produce and fetch with `REQUEST_TIMED_OUT` so clients retry. This is not a warning-only state.

### Unavailable

Brokers reject produce and fetch with `KAFKA_STORAGE_ERROR`. Clients should retry with exponential backoff. Fetch is not served from cache while the broker is in this state.

```
# Client sees this error during unavailable state
ERROR: [kafka] produce failed: KAFKA_STORAGE_ERROR (retriable)
```

---

## How to read the state

There is no `/ops/health/s3` endpoint on the broker. Read the health series from the metrics port (the operator sets `KAFSCALE_METRICS_ADDR=:9093` on broker pods):

```bash
curl -s http://127.0.0.1:9093/metrics | grep kafscale_s3_
```

`/healthz` on that port reports process liveness and includes the current S3 state in the body (`ok state=healthy`). `/readyz` returns HTTP 503 when S3 is unavailable or etcd cannot be reached; a degraded S3 state stays ready.

---

## Tuning recommendations

| Scenario | Recommended thresholds |
|----------|------------------------|
| Same-region S3 | warn: 500ms / 20%, crit: 3000ms / 60% (defaults) |
| Cross-region S3 | warn: 1000ms / 20%, crit: 5000ms / 60% |
| Stricter | warn: 300ms / 10%, crit: 1000ms / 30% |

---

## Next steps

- [Operations](/operations/) for monitoring and scaling
- [Environment variable index](/operations/#environment-variable-index)
- [Metrics](/metrics/) for complete metrics reference
