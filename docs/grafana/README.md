# Grafana Dashboards

The `broker-dashboard.json` definition ships a minimal overview for the Kafscale broker:

- S3 health/pressure tiles backed by `kafscale_s3_health_state`
- Produce throughput (success rate per topic)
- S3 latency/error time series

Import it into Grafana via **Dashboards → Import** and point the Prometheus data source to the
cluster scraping the broker and operator `/metrics` endpoints.
