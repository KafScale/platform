---
layout: doc
title: "KafScale Release v2.0.1"
description: "Release notes for KafScale v2.0.1."
permalink: /releases/v2.0.1/
nav_title: v2.0.1
nav_order: 21
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

# v2.0.1

Release date: 2026-11-21

The main Helm chart is version `2.0.1`; processor charts are version `0.2.1`. All charts default to application images tagged `v2.0.1`.

## Upgrade notes

### Behavior changes

- The Go module path is `github.com/KafScale/platform/v2`, and v2.0.1 is the first release you can fetch with Go modules: `go get github.com/KafScale/platform/v2@v2.0.1`. Change imports from `github.com/KafScale/platform/...` to `github.com/KafScale/platform/v2/...`. `go get github.com/KafScale/platform@v2.0.0` fails because that tag has no `/v2` suffix, and v2.0.0 cannot be fetched through `/v2` either. v1.6.3 stays `github.com/KafScale/platform`. The Iceberg, skeleton, and SQL processor modules use the same suffix. Standalone Iceberg image builds require `PLATFORM_VERSION` naming a published release with a valid `/v2` module declaration, such as `v2.0.1`; missing or invalid versions fail before building. `USE_LOCAL_PLATFORM=1` builds from the checkout. The published Go API check still compares exported symbols with v1.6.3 after rewriting that checkout onto `/v2`. Do not tag v1.7.0 on this module path. See [Upgrade and compatibility guide](https://github.com/KafScale/platform/blob/v2.0.1/docs/upgrade-compatibility.md) (#409).

- Added the E72 browser LFS demo source and the `ghcr.io/kafscale/kafscale-e72-browser-demo` image, with upload, envelope inspection, and verified downloads. Relevant pull requests build every release image once on native Linux in one check; version tags publish amd64 and arm64 from the same build catalog. The image and browser workflows use path filters and cancel superseded PR runs; browser tests exercise the Compose demo profile (#410).

[Full changelog](https://github.com/KafScale/platform/compare/v2.0.0...v2.0.1)
