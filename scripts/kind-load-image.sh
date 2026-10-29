#!/usr/bin/env bash
# Copyright 2026 KafScale team.
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

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <image> <kind-cluster>" >&2
  exit 2
fi

image="$1"
cluster="$2"

if ! docker save --help | grep -q -- '--platform'; then
  kind load docker-image "$image" --name "$cluster"
  exit 0
fi

arch="$(docker info -f '{{.Architecture}}' | sed 's/x86_64/amd64/;s/aarch64/arm64/')"
archive="$(mktemp)"
trap 'rm -f "$archive"' EXIT

docker save --platform "linux/${arch}" "$image" -o "$archive"
kind load image-archive "$archive" --name "$cluster"
