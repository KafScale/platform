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

# Addons

This directory hosts optional components that extend Kafscale without changing
core broker behavior. Addons are designed to be deployed separately (often in a
different namespace) and communicate with Kafscale through durable stores or
read-only interfaces.

## Design Goals

- Keep the core broker minimal and stateless.
- Allow extensions without requiring Kafka protocol compatibility.
- Favor storage-native access to S3 and metadata where possible.
- Encourage safe separation of concerns and isolation.
- Make security and deployment boundaries explicit per addon.

## Security and Separation

- Run each addon in its own namespace.
- Scope IAM and Kubernetes RBAC to only the resources the addon needs.
- Ship addons with separate Helm charts or standalone manifests.
- Avoid direct broker access unless explicitly required.
- Document all external dependencies and credentials.

## Structure

- `processors/` contains stream or batch processors that consume Kafscale
  segments directly from S3 and write to downstream systems.
- `processors/iceberg-processor/` describes the storage-native processor that
  maps topics to Iceberg tables.
- `processors/skeleton/` provides a template for building custom processors.
