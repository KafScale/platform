<!--
Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

Kafscale exposes Prometheus metrics on `/metrics` from both brokers and the operator.
The console UI and Grafana dashboard templates are built on the same metrics.

## Endpoints

- **Broker metrics** – `http://<broker-host>:9093/metrics`
- **Operator metrics** – `http://<operator-host>:8080/metrics`

In local development, the console can scrape broker metrics if you set
`KAFSCALE_CONSOLE_BROKER_METRICS_URL`. Operator metrics can be wired into the
console via `KAFSCALE_CONSOLE_OPERATOR_METRICS_URL`.

## ISR Terminology

The broker advertises ISR (in-sync replica) values as part of metadata responses
and exposes related counts in metrics/UI. This reflects Kafscale's **logical**
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
| `kafscale_admin_requests_total` | Counter | `api` | Count of admin API requests by API name. |
| `kafscale_admin_request_errors_total` | Counter | `api` | Count of admin API errors by API name. |
| `kafscale_admin_request_latency_ms_avg` | Gauge | `api` | Average admin API latency (ms). |
| `kafscale_produce_latency_ms` | Histogram | - | Produce request latency distribution (use p95 in PromQL). |
| `kafscale_consumer_lag` | Histogram | - | Consumer lag distribution (use p95 in PromQL). |
| `kafscale_consumer_lag_max` | Gauge | - | Maximum observed consumer lag. |
| `kafscale_broker_uptime_seconds` | Gauge | - | Seconds since broker start. |
| `kafscale_broker_cpu_percent` | Gauge | - | Process CPU usage percent between scrapes. |
| `kafscale_broker_mem_alloc_bytes` | Gauge | - | Allocated heap bytes. |
| `kafscale_broker_mem_sys_bytes` | Gauge | - | Memory obtained from the OS. |
| `kafscale_broker_heap_inuse_bytes` | Gauge | - | Heap in-use bytes. |
| `kafscale_broker_goroutines` | Gauge | - | Number of goroutines. |

Admin API label values are human-readable for common ops APIs
(`DescribeGroups`, `ListGroups`, `OffsetForLeaderEpoch`, `DescribeConfigs`,
`AlterConfigs`). Less common keys show as `api_<id>` (for example,
`api_37` for CreatePartitions).

## Operator Metrics

Operator metrics are exported by the controller runtime metrics server.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kafscale_operator_clusters` | Gauge | - | Count of managed `KafscaleCluster` resources. |
| `kafscale_operator_snapshot_publish_total` | Counter | `result` | Snapshot publish attempts (`success` or `error`). |
| `kafscale_operator_etcd_snapshot_age_seconds` | Gauge | `cluster` | Seconds since last successful etcd snapshot upload. |
| `kafscale_operator_etcd_snapshot_last_success_timestamp` | Gauge | `cluster` | Unix timestamp of last successful snapshot upload. |
| `kafscale_operator_etcd_snapshot_last_schedule_timestamp` | Gauge | `cluster` | Unix timestamp of last scheduled snapshot job. |
| `kafscale_operator_etcd_snapshot_stale` | Gauge | `cluster` | 1 when the snapshot age exceeds the staleness threshold. |
| `kafscale_operator_etcd_snapshot_success` | Gauge | `cluster` | 1 if at least one successful snapshot was recorded. |
| `kafscale_operator_etcd_snapshot_access_ok` | Gauge | `cluster` | 1 if the snapshot bucket preflight succeeds. |

The `cluster` label uses `namespace/name`.

## Grafana Dashboard

The Grafana template lives in `docs/grafana/broker-dashboard.json`. It expects
Prometheus to scrape both broker and operator metrics endpoints.

## Metric Coverage

Metric names and behavior evolve as the platform grows. When in doubt, consult
the `/metrics` endpoint in your environment to see the current exported series.
