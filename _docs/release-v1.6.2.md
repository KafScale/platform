---
layout: doc
title: "KafScale Release v1.6.2"
description: "Release notes for KafScale v1.6.2."
permalink: /releases/v1.6.2/
nav_title: v1.6.2
nav_order: 18
nav_group: Releases
---

<!--
Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# v1.6.2

Release date: 2026-10-16

## Highlights

- Faster partition recovery from S3 with bounded parallel restore, progress reporting, and atomic publication of restored state (#187).
- Per-cluster broker image overrides and independent affinity overrides for brokers and managed etcd (#189, #192).
- Operator-managed workloads now satisfy Kubernetes Pod Security Admission's restricted policy (#191).
- Release publishing now includes the unified proxy and MCP images (#186).

## Upgrade notes

- The main Helm chart is version `0.4.5`; processor charts are version `0.1.2`. All charts default to application images tagged `v1.6.2`.
- Update the KafscaleCluster CRD before using the new broker image and affinity fields. Helm does not automatically upgrade existing CRDs; apply `deploy/helm/kafscale/crds/kafscaleclusters.yaml` from this release before upgrading the chart.
- Broker and managed etcd affinity can be configured independently. Omit or remove an override to retain the default soft anti-affinity, supply `{}` to disable affinity, or supply a full affinity object to replace the default. External etcd endpoints are unaffected.
- Operator-managed containers now run as UID/GID 10001 with a read-only root filesystem. Writable volumes use `fsGroup: 10001`; existing volumes require a storage driver that applies fsGroup permissions. For hostPath or drivers without that support, prepare volume ownership and permissions before upgrading. See [the security guide](/security/).

## Bug fixes

- Fixed ListOffsets to validate topics and partitions for every timestamp mode, return appropriate protocol errors, and avoid creating topics during offset lookups (#190).
- Fixed concurrent cache access during partition restore and retained consistent state when restore fails (#187).
- Fixed managed etcd maintenance shell compatibility and the defragmentation command (#188, #191).
- Fixed per-cluster broker image selection, including explicit tags and digests (#189).
- Fixed the operator's default LFS proxy image and runtime configuration (#191).

## Security fixes

- Hardened broker, managed etcd, LFS proxy, initialization, backup, restore, and maintenance containers with non-root execution, dropped capabilities, disabled privilege escalation, and RuntimeDefault seccomp (#191).
- No specific CVEs are addressed by this release.

## Maintenance

- Fixed the Scorecard action image pull and added proxy and MCP images to release builds (#185, #186).
- Added real Kubernetes API validation for affinity overrides and a restricted-policy Kubernetes runtime test covering existing-volume upgrades, backup/restore, and proxy produce/consume (#191, #192).

[Full changelog](https://github.com/KafScale/platform/compare/v1.6.1...v1.6.2)
