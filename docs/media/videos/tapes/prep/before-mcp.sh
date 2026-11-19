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
kubectl -n kafscale exec kcat -- timeout 30 kcat -b kafscale-proxy:9092 -G billing-worker -o beginning -e -q orders >/dev/null || true
if [ -f "$KSV_DIR/load.pid" ]; then
  kill "$(cat "$KSV_DIR/load.pid")" 2>/dev/null || true
fi
nohup bash -c 'i=0; while :; do echo "m$i" | kubectl -n kafscale exec -i kcat -- kcat -b kafscale-proxy:9092 -P -t orders; i=$((i+1)); sleep 0.3; done' >"$KSV_DIR/logs/load.log" 2>&1 &
echo $! > "$KSV_DIR/load.pid"
echo "load generator pid $(cat "$KSV_DIR/load.pid")"
