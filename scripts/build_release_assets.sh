#!/usr/bin/env bash
# Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail
cd "$(dirname "$0")/.."
release_tag="${RELEASE_TAG:?Set RELEASE_TAG to the version being packaged}"
if [[ ! "$release_tag" =~ ^v[0-9][A-Za-z0-9._-]*$ ]]; then
  echo "Invalid release tag: $release_tag" >&2
  exit 1
fi
output_dir="${1:-dist/release}"
mkdir -p "$output_dir"
if [ -n "$(ls -A "$output_dir")" ]; then
  echo "Release output directory must be empty: $output_dir" >&2
  exit 1
fi
output_dir=$(cd "$output_dir" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64; do
  target_os=${target%/*}
  target_arch=${target#*/}
  bundle="$work/$target_os-$target_arch"
  mkdir -p "$bundle"
  CGO_ENABLED=0 GOOS="$target_os" GOARCH="$target_arch" go build \
    -trimpath -buildvcs=false -ldflags="-s -w -X main.version=$release_tag" \
    -o "$bundle/kafscale-cli" ./cmd/kafscale-cli
  cp LICENSE NOTICE "$bundle/"
  COPYFILE_DISABLE=1 tar -czf "$output_dir/kafscale-cli_${release_tag}_${target_os}_${target_arch}.tar.gz" \
    -C "$bundle" kafscale-cli LICENSE NOTICE
done
python3 - "$release_tag" "$output_dir" <<'PYASSETS'
import hashlib
import pathlib
import re
import sys

version, output = sys.argv[1], pathlib.Path(sys.argv[2])
source = pathlib.Path("deploy/docker-compose/docker-compose.release.yaml").read_text()
compose, count = re.subn(r"\$\{TAG:-[^}]+\}", version, source)
if count != 3:
    raise SystemExit(f"expected three product image tags, found {count}")
(output / "docker-compose.yaml").write_text(compose)
files = [output / f"kafscale-cli_{version}_{os}_{arch}.tar.gz"
         for os in ("linux", "darwin") for arch in ("amd64", "arm64")]
files.append(output / "docker-compose.yaml")
(output / "SHA256SUMS").write_text("".join(
    f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
    for path in sorted(files)))
PYASSETS
