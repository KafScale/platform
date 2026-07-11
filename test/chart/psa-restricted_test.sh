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

# Chart conformance gate for the PodSecurity "restricted" profile.
#
# Asserts that every chart-templated Deployment (operator, proxy, console,
# mcp) renders the five restricted controls:
#   1. pod-level    runAsNonRoot: true
#   2. container    allowPrivilegeEscalation: false
#   3. container    capabilities.drop includes ALL
#   4. pod-level    seccompProfile.type: RuntimeDefault
#   5. pod-level    runAsUser: <non-zero>  (a non-root UID)
#   6. container    readOnlyRootFilesystem: true
#
# Proxy additionally mounts a writable emptyDir at /tmp so the LFS verify path
# (cmd/proxy/lfs_http.go, os.CreateTemp) works with a read-only root FS.
#
# This is the gate that catches a future Deployment being added without the
# securityContext blocks (e.g. the mcp gap this test was written to close).
# Self-contained: needs only helm and awk, no helm plugins, no cluster.
# Run directly or via `make test-chart-psa`.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART_DIR="${ROOT_DIR}/deploy/helm/kafscale"

command -v helm >/dev/null 2>&1 || { echo "helm is required"; exit 1; }

fail=0

# Render a single Deployment template with every workload enabled. The proxy
# and mcp are opt-in (enabled: false by default), so we enable all four here.
render_deployment() {
  local template="$1"
  helm template kafscale "${CHART_DIR}" \
    --show-only "templates/${template}" \
    --set proxy.enabled=true \
    --set console.enabled=true \
    --set mcp.enabled=true
}

# assert_present <component> <description> <rendered-yaml> <grep-pattern>
assert_present() {
  local component="$1" desc="$2" yaml="$3" pattern="$4"
  if printf '%s\n' "${yaml}" | grep -Eq "${pattern}"; then
    echo "PASS: ${component}: ${desc}"
  else
    echo "FAIL: ${component}: ${desc} (pattern: ${pattern})"
    fail=1
  fi
}

# assert_runasuser_nonzero <component> <rendered-yaml>
# runAsUser must be present and not 0 (0 is root, which violates restricted).
assert_runasuser_nonzero() {
  local component="$1" yaml="$2" uid
  uid="$(printf '%s\n' "${yaml}" | awk '/^[[:space:]]*runAsUser:/ { print $2; exit }')"
  if [ -n "${uid}" ] && [ "${uid}" != "0" ]; then
    echo "PASS: ${component}: runAsUser is a non-root UID (runAsUser=${uid})"
  else
    echo "FAIL: ${component}: runAsUser must be a non-root UID (got \"${uid:-<absent>}\")"
    fail=1
  fi
}

echo "==> chart PSA-restricted conformance gate"

# The chart-templated Deployments and their template files. Broker/etcd are
# operator-reconciled (label app=kafscale-broker), not chart-templated, and
# are intentionally out of scope for this chart-level gate.
declare -a COMPONENTS=(
  "operator:operator-deployment.yaml"
  "proxy:proxy-deployment.yaml"
  "console:console-deployment.yaml"
  "mcp:mcp-deployment.yaml"
)

for entry in "${COMPONENTS[@]}"; do
  component="${entry%%:*}"
  template="${entry##*:}"
  yaml="$(render_deployment "${template}")"

  # 1. runAsNonRoot: true
  assert_present "${component}" "runAsNonRoot: true" "${yaml}" \
    '^[[:space:]]*runAsNonRoot:[[:space:]]*true[[:space:]]*$'
  # 2. allowPrivilegeEscalation: false
  assert_present "${component}" "allowPrivilegeEscalation: false" "${yaml}" \
    '^[[:space:]]*allowPrivilegeEscalation:[[:space:]]*false[[:space:]]*$'
  # 3. capabilities.drop includes ALL
  assert_present "${component}" "capabilities.drop includes ALL" "${yaml}" \
    '^[[:space:]]*-[[:space:]]*ALL[[:space:]]*$'
  # 4. seccompProfile RuntimeDefault
  assert_present "${component}" "seccompProfile.type: RuntimeDefault" "${yaml}" \
    '^[[:space:]]*type:[[:space:]]*RuntimeDefault[[:space:]]*$'
  # 5. non-root runAsUser
  assert_runasuser_nonzero "${component}" "${yaml}"
  # 6. readOnlyRootFilesystem: true
  assert_present "${component}" "readOnlyRootFilesystem: true" "${yaml}" \
    '^[[:space:]]*readOnlyRootFilesystem:[[:space:]]*true[[:space:]]*$'
  if [ "${component}" = "proxy" ]; then
    assert_present "${component}" "writable /tmp emptyDir mount" "${yaml}" \
      'mountPath:[[:space:]]*/tmp'
    assert_present "${component}" "tmp emptyDir volume" "${yaml}" \
      '^[[:space:]]*- name:[[:space:]]*tmp[[:space:]]*$'
  fi
done

if [ "${fail}" -ne 0 ]; then
  echo "==> chart PSA-restricted conformance gate FAILED"
  exit 1
fi
echo "==> chart PSA-restricted conformance gate passed (4 Deployments x 6 controls; proxy /tmp mount)"
