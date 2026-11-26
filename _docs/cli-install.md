---
layout: doc
title: "Install the native CLI"
permalink: /cli-install/
nav_title: "Install the native CLI"
nav_order: 4
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

# Install the native KafScale CLI

`kafscale-cli` runs directly on Linux or macOS, on amd64 or arm64. It needs
neither Docker nor a Go toolchain when installed from a release archive.
Its commands are `restore`, `prune` and `schema import`; use a Kafka client
such as [kcat](https://github.com/edenhill/kcat#install) for produce and consume.

## Homebrew

On macOS or Linux, install the latest final release with Homebrew:

```bash
brew tap kafscale/platform https://github.com/KafScale/platform
brew install kafscale-cli
```

If brew says the tap is untrusted, run `brew trust kafscale/platform` once and
install again. A `-rc` or `-dev` tag is a GitHub pre-release and does not
update the Homebrew formula.

## Download a release

Open [GitHub releases](https://github.com/KafScale/platform/releases) and select
a release containing CLI assets. `v2.1.0` is the first release that carries
them; older releases, including `v2.0.1`, have no binary attachments.

Each such release provides:

- `kafscale-cli_<version>_linux_amd64.tar.gz`
- `kafscale-cli_<version>_linux_arm64.tar.gz`
- `kafscale-cli_<version>_darwin_amd64.tar.gz`
- `kafscale-cli_<version>_darwin_arm64.tar.gz`
- `docker-compose.yaml`, with product images pinned to that release
- `SHA256SUMS`, covering the four archives and Compose file

The block below picks the archive for your operating system and CPU, verifies
its checksum, and extracts it only when the check passes. It downloads `v2.1.0`
unless you set `VERSION` to another tag with CLI assets first.

```bash
VERSION="${VERSION:-v2.1.0}"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in
  x86_64 | amd64) ARCH=amd64 ;;
  aarch64 | arm64) ARCH=arm64 ;;
  *) ARCH="$(uname -m)" ;;
esac
ARCHIVE="kafscale-cli_${VERSION}_${OS}_${ARCH}.tar.gz"
BASE="https://github.com/KafScale/platform/releases/download/${VERSION}"
curl -fLO "$BASE/$ARCHIVE"
curl -fLO "$BASE/SHA256SUMS"
if command -v sha256sum >/dev/null 2>&1; then
  SHA256_CHECK="sha256sum -c -"
else
  SHA256_CHECK="shasum -a 256 -c -"
fi
grep "  ${ARCHIVE}$" SHA256SUMS | $SHA256_CHECK && tar -xzf "$ARCHIVE"
./kafscale-cli --version
./kafscale-cli --help
```

Supported platforms are `linux` and `darwin` on `amd64` and `arm64`. Install
the binary in a directory on your `PATH`, or continue using `./kafscale-cli`.

For restore/prune, configure [S3 and etcd access](/s3-connection/#native-cli).
The [operations guide](/operations/#cli-access-from-a-laptop) covers Kubernetes
port-forwarding. `kafscale-cli <command> --help` lists each command's flags.

## Build from source

For unreleased changes, install the Go version in `go.mod` and run:

```bash
go build -o bin/kafscale-cli ./cmd/kafscale-cli
./bin/kafscale-cli --help
```
