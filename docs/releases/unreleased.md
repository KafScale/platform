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

# Unreleased

## Upgrade notes

### Behavior changes

- The Go module path is `github.com/KafScale/platform/v2`. `go get github.com/KafScale/platform@v2.0.0` fails because that tag has no `/v2` suffix. Future 2.x releases must use this path; the existing `v2.0.0` tag is also invalid when fetched through `/v2`. v1.6.3 stays `github.com/KafScale/platform`. The Iceberg, skeleton, and SQL processor modules use the same suffix. Standalone Iceberg image builds require `PLATFORM_VERSION` naming a published release with a valid `/v2` module declaration; missing or invalid versions fail before building. No valid release is assumed by default. `USE_LOCAL_PLATFORM=1` builds from the checkout. The published Go API check still compares exported symbols with v1.6.3 after rewriting that checkout onto `/v2`. Do not tag v1.7.0 on this module path. See [Upgrade and compatibility guide](../upgrade-compatibility.md) (#409).

- Added the E72 browser LFS demo source and image, with upload, envelope inspection, and verified downloads. Release images now build on pull requests for amd64 and arm64 using the same matrix as tag publishing; browser tests exercise the Compose demo profile (#410).
