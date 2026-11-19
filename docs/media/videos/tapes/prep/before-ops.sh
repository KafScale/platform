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
if [ -f "$KSV_DIR/load.pid" ]; then
  kill "$(cat "$KSV_DIR/load.pid")" 2>/dev/null || true
  rm -f "$KSV_DIR/load.pid"
fi
umask 077
mkdir -p "$KSV_DIR/new-s3"
printf 'kafscale-demo-2' > "$KSV_DIR/new-s3/AWS_ACCESS_KEY_ID"
cp "$KSV_SECRETS/s3-secret-2" "$KSV_DIR/new-s3/AWS_SECRET_ACCESS_KEY"
"$KSV_PREP/minio-user.sh" add kafscale-demo-2 "$KSV_SECRETS/s3-secret-2"
ksv_port_forward pod/demo-broker-0 9093:9093
curl -s http://127.0.0.1:9093/metrics | grep -E '^kafscale_(s3_health_state|s3_latency_ms_avg|broker_uptime_seconds)'
