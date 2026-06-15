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

# Chart template test for the proxy Service nodePort behaviour.
#
# Asserts that deploy/helm/kafscale/templates/proxy-service.yaml renders
# spec.ports[0].nodePort only when proxy.service.type == NodePort AND
# proxy.service.nodePort is set. Self-contained: needs only helm and awk,
# no helm plugins. Run directly or via `make test-chart-proxy-nodeport`.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART_DIR="${ROOT_DIR}/deploy/helm/kafscale"

command -v helm >/dev/null 2>&1 || { echo "helm is required"; exit 1; }

fail=0

# Render the proxy Service object only. The chart guards the proxy template
# behind proxy.enabled, so every case enables it.
render_proxy_service() {
  helm template kafscale "${CHART_DIR}" \
    --show-only templates/proxy-service.yaml \
    --set proxy.enabled=true \
    "$@"
}

# nodePort value rendered on the kafka port, or empty string if absent.
proxy_nodeport() {
  render_proxy_service "$@" | awk '/^[[:space:]]*nodePort:/ { print $2; exit }'
}

assert_eq() {
  local desc="$1" want="$2" got="$3"
  if [ "${got}" = "${want}" ]; then
    echo "PASS: ${desc} (nodePort=\"${got}\")"
  else
    echo "FAIL: ${desc} (want nodePort=\"${want}\", got \"${got}\")"
    fail=1
  fi
}

echo "==> chart proxy nodePort template test"

# Case 1: NodePort + explicit nodePort -> renders nodePort: 30092.
got="$(proxy_nodeport --set proxy.service.type=NodePort --set proxy.service.nodePort=30092)"
assert_eq "NodePort + nodePort=30092 renders the value" "30092" "${got}"

# Case 2: NodePort + empty nodePort -> line omitted (Kubernetes auto-assigns).
got="$(proxy_nodeport --set proxy.service.type=NodePort --set 'proxy.service.nodePort=')"
assert_eq "NodePort + empty nodePort omits the line" "" "${got}"

# Case 3: LoadBalancer + value -> line suppressed (only NodePort honours it).
got="$(proxy_nodeport --set proxy.service.type=LoadBalancer --set proxy.service.nodePort=30092)"
assert_eq "LoadBalancer + nodePort=30092 suppresses the line" "" "${got}"

if [ "${fail}" -ne 0 ]; then
  echo "==> chart proxy nodePort template test FAILED"
  exit 1
fi
echo "==> chart proxy nodePort template test passed"
