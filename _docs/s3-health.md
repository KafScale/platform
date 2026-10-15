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

| State | Value | Condition | Broker behavior |
|-------|-------|-----------|-----------------|
| Healthy | 0 | Latency < warn AND error rate < warn | Normal operation |
| Degraded | 1 | Latency >= warn OR error rate >= warn | Accepts requests, emits warnings |
| Unavailable | 2 | Latency >= crit OR error rate >= crit | Rejects produces, serves cached fetches |

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
    <text x="120" y="98" font-size="10" fill="var(--diagram-label)" text-anchor="middle">state = 0</text>

    <!-- Degraded -->
    <rect x="280" y="50" width="140" height="70" rx="10" fill="rgba(251, 191, 36, 0.15)" stroke="#fbbf24" stroke-width="1.5"/>
    <text x="350" y="78" font-size="12" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Degraded</text>
    <text x="350" y="98" font-size="10" fill="var(--diagram-label)" text-anchor="middle">state = 1</text>

    <!-- Unavailable -->
    <rect x="510" y="50" width="140" height="70" rx="10" fill="rgba(248, 113, 113, 0.15)" stroke="#f87171" stroke-width="1.5"/>
    <text x="580" y="78" font-size="12" font-weight="600" fill="var(--diagram-text)" text-anchor="middle">Unavailable</text>
    <text x="580" y="98" font-size="10" fill="var(--diagram-label)" text-anchor="middle">state = 2</text>

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
| `KAFSCALE_S3_LATENCY_WARN_MS` | `500` | Latency threshold for degraded state |
| `KAFSCALE_S3_LATENCY_CRIT_MS` | `2000` | Latency threshold for unavailable state |
| `KAFSCALE_S3_ERROR_RATE_WARN` | `0.01` | Error rate threshold for degraded (1%) |
| `KAFSCALE_S3_ERROR_RATE_CRIT` | `0.05` | Error rate threshold for unavailable (5%) |
| `KAFSCALE_S3_HEALTH_WINDOW_SEC` | `60` | Sampling window for health calculation |

Tune these based on your S3 region and latency expectations. Cross-region S3 access may require higher latency thresholds.

---

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `kafscale_s3_health_state` | Gauge | Current health state (0, 1, or 2) |
| `kafscale_s3_latency_ms_avg` | Gauge | Average S3 operation latency over window |
| `kafscale_s3_latency_ms_p99` | Gauge | p99 S3 operation latency |
| `kafscale_s3_error_rate` | Gauge | Error rate over sampling window (0.0 to 1.0) |
| `kafscale_s3_state_duration_seconds` | Gauge | Time in current state |
| `kafscale_s3_state_transitions_total` | Counter | Total state transitions (label: `from`, `to`) |
| `kafscale_s3_operations_total` | Counter | Total S3 operations (label: `operation`, `status`) |

---

## Alerting rules

Wire S3 health into Prometheus Alertmanager:

{% raw %}
```yaml
groups:
  - name: kafscale-s3-health
    rules:
      - alert: KafScaleS3Unavailable
        expr: kafscale_s3_health_state == 2
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "KafScale S3 unavailable"
          description: "Broker {{ $labels.pod }} cannot reach S3. Produces are rejected."

      - alert: KafScaleS3Degraded
        expr: kafscale_s3_health_state == 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "KafScale S3 degraded"
          description: "Broker {{ $labels.pod }} S3 latency or error rate elevated for 5+ minutes."

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

Brokers continue to accept requests but emit warning logs and increment `kafscale_s3_degraded_requests_total`. Monitor this state to catch issues before they escalate.

### Unavailable

Brokers protect data integrity by rejecting produce requests with a retriable error code. Clients should retry with exponential backoff. Fetch requests are served from cache when possible.

```
# Client sees this error during unavailable state
ERROR: [kafka] produce failed: KAFKA_STORAGE_ERROR (retriable)
```

---

## Ops API endpoints

Query health state via the ops API:

```bash
# Get current health state
curl http://localhost:9093/ops/health/s3
```

Response:

```json
{
  "state": "healthy",
  "state_value": 0,
  "latency_ms_avg": 87,
  "latency_ms_p99": 142,
  "error_rate": 0.0,
  "state_duration_seconds": 3847,
  "window_seconds": 60
}
```

Query S3 health history:

```bash
# Get health history (last hour)
curl http://localhost:9093/ops/health/s3/history?minutes=60
```


---

## Tuning recommendations

| Scenario | Recommended thresholds |
|----------|------------------------|
| Same-region S3 | warn: 500ms / 1%, crit: 2000ms / 5% (defaults) |
| Cross-region S3 | warn: 1000ms / 1%, crit: 5000ms / 5% |
| High-throughput | warn: 300ms / 0.5%, crit: 1000ms / 2% |
| Cost-optimized (S3 Standard-IA) | warn: 800ms / 1%, crit: 3000ms / 5% |

---

## Next steps

- [Operations](/operations/) for monitoring and scaling
- [Runtime Settings](/configuration/) for all environment variables
- [Metrics](/metrics/) for complete metrics reference
