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

# Iceberg Processor (Addon)

Storage-native processor that reads completed KafScale segments from S3 and
writes to Apache Iceberg tables. It is not a Kafka broker. It trades latency
(seconds to minutes) for simpler operations and lower cost.

## Quick Links

- User guide: `addons/processors/iceberg-processor/user-guide.md`
- Developer guide: `addons/processors/iceberg-processor/developer.md`
- Reference config: `addons/processors/iceberg-processor/config/config.yaml`
- Helm values: `addons/processors/iceberg-processor/deploy/helm/iceberg-processor/values.yaml`

## What It Does (High Level)

- Discovers completed KafScale segments in S3.
- Decodes segments and batches records by topic.
- Maps topics to Iceberg tables via YAML.
- Writes in append mode with at-least-once semantics.
- Persists offsets via a lease-per-partition model.
- Evolves Iceberg schemas from mapping-defined columns or a schema registry.

## Segment Layout

```
s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.kfs
s3://{bucket}/{namespace}/{topic}/{partition}/segment-{base_offset}.index
```

Segment formats are defined in `kafscale-spec.md`.

## Minimal Config (Example)

```yaml
s3:
  bucket: kafscale-data
  namespace: production

iceberg:
  catalog:
    type: rest
    uri: https://iceberg-catalog.example.com
  warehouse: s3://iceberg-warehouse/production

etcd:
  endpoints:
    - http://etcd.kafscale.svc.cluster.local:2379

mappings:
  - topic: orders
    table: prod.orders
    mode: append
    create_table_if_missing: true
```

For full configuration and operational guidance, use the user guide.
