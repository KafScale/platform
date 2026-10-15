---
layout: doc
title: "KafScale Grafana Dashboards: Broker Overview"
description: "Import the broker dashboard and the metrics it shows."
permalink: /grafana/
nav_title: Grafana
nav_order: 11
nav_group: References
---

<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# Grafana Dashboards

The `broker-dashboard.json` definition ships a minimal overview for the Kafscale broker:

- S3 health/pressure tiles backed by `kafscale_s3_health_state`
- Produce throughput (success rate per topic)
- S3 latency/error time series
- Admin API request/error rates

Import it into Grafana via **Dashboards → Import** and point the Prometheus data source to the
cluster scraping the broker and operator `/metrics` endpoints.
