---
layout: doc
title: "KafScale Release v1.4.2"
description: "Release notes for KafScale v1.4.2."
permalink: /releases/v1.4.2/
nav_title: v1.4.2
nav_order: 15
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

# v1.4.2

Release date: 2026-01-17

## Highlights

- Introduced Kafsql as a first-class SQL processor with Helm charts, proxy mode, guardrails, and caching.
- Expanded Kafsql query capabilities with manifest/time index tooling, backfill support, and broader protocol coverage.
- Added SQL processor documentation plus an end-to-end demo flow and Docker build wiring.

## Bug fixes

- Fixed coordinator broker identity mismatches.
- Corrected int32 conversion for S3 partitions.
- Bounded SQL partition parsing for CodeQL findings.

## Maintenance

- Bumped AWS SDK dependencies and GitHub Actions tooling (CodeQL, setup-go).
- Refreshed organization/module references across tooling and docs.
- Bumped Helm chart versions for the SQL, Iceberg, and skeleton processors to align with v1.4.2.

## Security fixes

- No known runtime vulnerabilities were fixed in this release.
