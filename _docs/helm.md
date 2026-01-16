---
layout: doc
title: Helm
description: Helm chart installation and configuration for KafScale.
permalink: /helm/
nav_title: Helm
nav_order: 8
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

# Helm

This chart deploys the Kafscale operator (which in turn manages broker pods) and the lightweight operations console. CRDs are packaged with the release so `helm install` takes care of registering the `KafscaleCluster` and `KafscaleTopic` resources before the operator starts.

### Prerequisites

- Kubernetes 1.26+
- Helm 3.12+
- Access to an etcd cluster that the operator can reach
- IAM credentials/secret containing the S3 access keys that the operator will mount into broker pods

### Quickstart

```bash
helm upgrade --install kafscale ./deploy/helm/kafscale \
  --namespace kafscale --create-namespace \
  --set operator.etcdEndpoints[0]=http://etcd.kafscale.svc:2379 \
  --set operator.image.tag=v0.1.0 \
  --set console.image.tag=v0.1.0
```

For a dev/latest install, set `operator.image.useLatest=true`, `console.image.useLatest=true`, and `operator.brokerImage.useLatest=true` (this also forces `imagePullPolicy=Always`).

After the chart is running you can create a cluster by applying a `KafscaleCluster` resource (see `config/samples/` for an example). The console service is exposed as a ClusterIP by default; enable ingress by toggling `.Values.console.ingress`.

### MCP

The MCP service is optional and disabled by default. Enable it with `mcp.enabled=true`; it will deploy into the dedicated namespace defined by `mcp.namespace.name`.

### Values Overview

| Key | Description | Default |
|-----|-------------|---------|
| `operator.replicaCount` | Number of operator replicas (each performs leader election via etcd). | `2` |
| `operator.leaderKey` | Kubernetes leader election lock name. | `kafscale-operator` |
| `operator.etcdEndpoints` | List of etcd endpoints the operator will connect to. | `["http://etcd:2379"]` |
| `console.service.type` | Kubernetes service type for the console. | `ClusterIP` |
| `console.ingress.*` | Optional ingress configuration for exposing the console. | disabled |
| `mcp.enabled` | Deploy the MCP service. | `false` |
| `mcp.namespace.name` | Namespace to deploy the MCP service into. | `kafscale-mcp` |
| `mcp.ingress.*` | Optional ingress configuration for exposing the MCP service. | disabled |

Consult `values.yaml` for all tunables, including resource requests, node selectors, and pull secrets.
