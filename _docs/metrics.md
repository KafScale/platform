---
layout: doc
title: Metrics and Dashboards
description: Prometheus metrics and Grafana dashboards for KafScale.
permalink: /metrics/
nav_title: Metrics
nav_order: 4
nav_group: References
---

<!--
Copyright 2025 Alexander Alten (novatechflow), KafScale (novatechflow.com).
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

# Metrics and Dashboards

KafScale exposes Prometheus metrics on `/metrics` from both brokers and the operator.
The console UI and Grafana dashboard templates are built on the same metrics.

## Endpoints

- **Broker metrics** – `http://<broker-host>:9093/metrics`
- **Operator metrics** – `http://<operator-host>:8080/metrics`

In local development, the console can scrape broker metrics if you set
`KAFSCALE_CONSOLE_BROKER_METRICS_URL`. Operator metrics can be wired into the
console via `KAFSCALE_CONSOLE_OPERATOR_METRICS_URL`.

## ISR Terminology

The broker advertises ISR (in-sync replica) values as part of metadata responses
and exposes related counts in metrics/UI. This reflects KafScale's **logical**
replica set in etcd metadata, not Kafka's internal `LeaderAndIsr` protocol. We
do not implement Kafka ISR management or any internal replication APIs; ISR here
is a metadata indicator used for client compatibility and visibility.

## Broker Metrics

Broker metrics are emitted directly by the broker process.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kafscale_s3_health_state` | Gauge | `state` | 1 for the active S3 health state (`healthy`, `degraded`, `unavailable`). |
| `kafscale_s3_latency_ms_avg` | Gauge | - | Average S3 latency (ms) over the sliding window. |
| `kafscale_s3_error_rate` | Gauge | - | Fraction of failed S3 operations in the sliding window. |
| `kafscale_s3_state_duration_seconds` | Gauge | - | Seconds spent in the current S3 health state. |
| `kafscale_produce_rps` | Gauge | - | Produce requests per second (sliding window). |
| `kafscale_fetch_rps` | Gauge | - | Fetch requests per second (sliding window). |
| `kafscale_produce_latency_ms` | Histogram | - | Produce request latency distribution (use p95 in PromQL). |
| `kafscale_consumer_lag` | Histogram | - | Consumer lag distribution (use p95 in PromQL). |
| `kafscale_consumer_lag_max` | Gauge | - | Maximum observed consumer lag. |
| `kafscale_broker_uptime_seconds` | Gauge | - | Seconds since broker start. |
| `kafscale_broker_cpu_percent` | Gauge | - | Process CPU usage percent between scrapes. |
| `kafscale_broker_mem_alloc_bytes` | Gauge | - | Allocated heap bytes. |
| `kafscale_broker_mem_sys_bytes` | Gauge | - | Memory obtained from the OS. |
| `kafscale_broker_heap_inuse_bytes` | Gauge | - | Heap in-use bytes. |
| `kafscale_broker_goroutines` | Gauge | - | Number of goroutines. |
| `kafscale_admin_requests_total` | Counter | `api` | Count of admin API requests by API name. |
| `kafscale_admin_request_errors_total` | Counter | `api` | Count of admin API errors by API name. |
| `kafscale_admin_request_latency_ms_avg` | Gauge | `api` | Average admin API latency (ms). |
| `kafscale_authz_denied_total` | Counter | `action`, `resource` | Count of authorization denials by action/resource. |

Admin API label values are human-readable for common ops APIs
(`DescribeGroups`, `ListGroups`, `OffsetForLeaderEpoch`, `DescribeConfigs`,
`AlterConfigs`). Less common keys show as `api_<id>` (for example,
`api_37` for CreatePartitions).

## Operator Metrics

Operator metrics are exported by the controller runtime metrics server.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kafscale_operator_clusters` | Gauge | - | Count of managed `KafScaleCluster` resources. |
| `kafscale_operator_snapshot_publish_total` | Counter | `result` | Snapshot publish attempts (`success` or `error`). |
| `kafscale_operator_etcd_snapshot_age_seconds` | Gauge | `cluster` | Seconds since last successful etcd snapshot upload. |
| `kafscale_operator_etcd_snapshot_last_success_timestamp` | Gauge | `cluster` | Unix timestamp of last successful snapshot upload. |
| `kafscale_operator_etcd_snapshot_last_schedule_timestamp` | Gauge | `cluster` | Unix timestamp of last scheduled snapshot job. |
| `kafscale_operator_etcd_snapshot_stale` | Gauge | `cluster` | 1 when the snapshot age exceeds the staleness threshold. |
| `kafscale_operator_etcd_snapshot_success` | Gauge | `cluster` | 1 if at least one successful snapshot was recorded. |
| `kafscale_operator_etcd_snapshot_access_ok` | Gauge | `cluster` | 1 if the snapshot bucket preflight succeeds. |

The `cluster` label uses `namespace/name`.

## Kubernetes ServiceMonitor

If you're using the Prometheus Operator, create a `ServiceMonitor` to scrape KafScale:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: kafscale-brokers
  labels:
    app: kafscale
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: kafscale
      app.kubernetes.io/component: broker
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: kafscale-operator
  labels:
    app: kafscale
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: kafscale
      app.kubernetes.io/component: operator
  endpoints:
    - port: metrics
      interval: 30s
      path: /metrics
```

## Recommended Alert Rules

Example Prometheus alerting rules for production deployments:

{% raw %}
```yaml
groups:
  - name: kafscale
    rules:
      - alert: KafScaleS3Unhealthy
        expr: kafscale_s3_health_state{state="healthy"} != 1
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "KafScale S3 connection unhealthy"
          description: "Broker {{ $labels.instance }} has been in non-healthy S3 state for 2+ minutes."

      - alert: KafScaleHighProduceLatency
        expr: histogram_quantile(0.95, rate(kafscale_produce_latency_ms_bucket[5m])) > 500
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "KafScale produce latency elevated"
          description: "P95 produce latency on {{ $labels.instance }} exceeds 500ms."

      - alert: KafScaleConsumerLagHigh
        expr: kafscale_consumer_lag_max > 100000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "KafScale consumer lag high"
          description: "Consumer lag on {{ $labels.instance }} exceeds 100k messages."

      - alert: KafScaleEtcdSnapshotStale
        expr: kafscale_operator_etcd_snapshot_stale == 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "KafScale etcd snapshot stale"
          description: "Cluster {{ $labels.cluster }} has not had a successful snapshot recently."

      - alert: KafScaleS3ErrorRateHigh
        expr: kafscale_s3_error_rate > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "KafScale S3 error rate elevated"
          description: "S3 error rate on {{ $labels.instance }} exceeds 5%."

      - alert: KafScaleBrokerDown
        expr: up{job="kafscale-brokers"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "KafScale broker unreachable"
          description: "Broker {{ $labels.instance }} is not responding to scrapes."
```
{% endraw %}

Tune thresholds based on your workload. KafScale's S3-native architecture means
produce latencies are inherently higher than traditional Kafka (expect 200-400ms
typical), so adjust `KafScaleHighProduceLatency` accordingly.

## Grafana Dashboard

The Grafana template lives in `docs/grafana/broker-dashboard.json`. It expects
Prometheus to scrape both broker and operator metrics endpoints.

Import the dashboard via Grafana UI or provision it in your Grafana deployment:

```yaml
# Example provisioning config
apiVersion: 1
providers:
  - name: KafScale
    folder: KafScale
    type: file
    options:
      path: /var/lib/grafana/dashboards/kafscale
```

## Metric Coverage

Metric names and behavior evolve as the platform grows. When in doubt, consult
the `/metrics` endpoint in your environment to see the current exported series.