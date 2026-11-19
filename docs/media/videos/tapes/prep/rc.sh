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

KSV_REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"
export KSV_REPO
export KSV_PREP="$KSV_REPO/docs/media/videos/tapes/prep"
export KSV_DIR="${KSV_DIR:-/tmp/kafscale-videos}"
export KSV_SECRETS="${KSV_SECRETS:-/tmp/kafscale-videos/secrets}"
export KSV_CLUSTER="${KSV_CLUSTER:-kafscale-videos}"
export KUBECONFIG="$KSV_DIR/kubeconfig"
export HELM_CACHE_HOME="$KSV_DIR/.helmcache"
export MC_CONFIG_DIR="$KSV_DIR/.mc"
export PATH="$KSV_DIR/bin:$KSV_DIR/venv/bin:$PATH"
export PS1='\[\e[1;32m\]$\[\e[0m\] '
alias k='kubectl -n kafscale'
mkdir -p "$KSV_DIR/bin" "$KSV_DIR/logs"
ln -sfn "$KSV_REPO" "$KSV_DIR/platform"

ksv_port_forward() {
  local target="$1" ports="$2" ns="${3:-kafscale}"
  pkill -f "port-forward $target $ports" 2>/dev/null || true
  nohup kubectl -n "$ns" port-forward "$target" "$ports" >"$KSV_DIR/logs/pf-${ports%%:*}.log" 2>&1 &
  sleep 3
}

cd "$KSV_DIR"
