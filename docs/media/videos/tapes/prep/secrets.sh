#!/usr/bin/env bash
# Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
source "$(dirname "${BASH_SOURCE[0]}")/rc.sh"
umask 077
mkdir -p "$KSV_SECRETS"
chmod 700 "$KSV_SECRETS"
for name in minio-root-user minio-root-pass s3-secret s3-secret-2 console-pass; do
  if [ ! -s "$KSV_SECRETS/$name" ]; then
    openssl rand -hex 16 > "$KSV_SECRETS/$name"
  fi
done
for name in s3-secret console-pass; do
  value="$(cat "$KSV_SECRETS/$name")"
  printf 'Type@5ms "%s"\n' "$value" > "$KSV_SECRETS/$name.tape"
done
unset value
ls -l "$KSV_SECRETS"
