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

# Kafscale Roadmap

This roadmap tracks completed work and open gaps. It is intentionally high level; detailed specs live in `kafscale-spec.md`.

## Milestones (Completed)

- Core protocol parsing and metadata support
- Produce and fetch paths with S3-backed durability
- Consumer group coordination with offset and group persistence
- DescribeGroups/ListGroups ops visibility
- OffsetForLeaderEpoch consumer recovery
- DescribeConfigs/AlterConfigs ops tuning
- CreatePartitions/DeleteGroups ops APIs
- etcd topic/partition management
- Observability (structured logging, Grafana dashboard templates, expanded Prometheus metrics)
- Kubernetes operator with managed etcd + snapshot backups
- End-to-end tests for broker durability and operator resilience
- Admin ops API e2e coverage
- Security review (TLS/auth)
- End-to-end tests multi-segment restart durability
- Authentication / ACL and security hardening