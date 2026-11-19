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

set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/rc.sh"
sudo_cmd="${KSV_SUDO:-}"
if [ -f "$KSV_DIR/load.pid" ]; then
  kill "$(cat "$KSV_DIR/load.pid")" 2>/dev/null
  rm -f "$KSV_DIR/load.pid"
fi
pkill -f "kubectl -n kafscale port-forward" 2>/dev/null
$sudo_cmd docker rm -f kafscale-videos-apicurio >/dev/null 2>&1
$sudo_cmd kind delete cluster --name "$KSV_CLUSTER"
rm -rf "$KSV_DIR/new-s3" "$KSV_DIR/kubeconfig"
