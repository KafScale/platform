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

# Chart template test for the proxy Deployment default podAntiAffinity.
#
# The default anti-affinity rule only spreads replicas if its label selector
# matches the proxy's own pod labels. A selector hardcoded to "kafscale-proxy"
# silently matches nothing under nameOverride (the pod label becomes
# "<override>-proxy"), disabling HA with no error. This test asserts that the
# rendered anti-affinity selector EQUALS the rendered pod-template labels, in
# the default case AND under --set nameOverride=foo (the case that would have
# caught the drift). It also asserts that an explicit proxy.affinity replaces
# the default. Self-contained: needs only helm and awk, no helm plugins. Run
# directly or via `make test-chart-antiaffinity`.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART_DIR="${ROOT_DIR}/deploy/helm/kafscale"

command -v helm >/dev/null 2>&1 || { echo "helm is required"; exit 1; }

fail=0

render_proxy() {
  helm template kafscale "${CHART_DIR}" \
    --show-only templates/proxy-deployment.yaml \
    --set proxy.enabled=true \
    "$@"
}

# Extract the three app.kubernetes.io label values (name/instance/component)
# from a given YAML block. We scope to a section by streaming the lines that
# belong to it. The values are emitted as "name=..,instance=..,component=.."
# so two blocks can be compared as a single string.
labels_triplet() {
  awk '
    /app\.kubernetes\.io\/name:/      { name=$2 }
    /app\.kubernetes\.io\/instance:/  { inst=$2 }
    /app\.kubernetes\.io\/component:/ { comp=$2 }
    END { printf "name=%s,instance=%s,component=%s", name, inst, comp }
  '
}

# The pod-template label block: lines after "  template:" up to (not including)
# the "    spec:" line. Within that we read the matchLabels-free label map.
pod_template_labels() {
  render_proxy "$@" | awk '
    /^  template:/ { intpl=1; next }
    intpl && /^    spec:/ { intpl=0 }
    intpl { print }
  ' | labels_triplet
}

# The anti-affinity selector label block: lines after the podAntiAffinity
# "matchLabels:" up to the "topologyKey:" line.
antiaffinity_selector_labels() {
  render_proxy "$@" | awk '
    /matchLabels:/   { insel=1; next }
    insel && /topologyKey:/ { insel=0 }
    insel { print }
  ' | labels_triplet
}

# Whether the render contains a soft (preferredDuring) podAntiAffinity at all.
has_soft_antiaffinity() {
  render_proxy "$@" | grep -q "preferredDuringSchedulingIgnoredDuringExecution"
}

assert_eq() {
  local desc="$1" want="$2" got="$3"
  if [ "${got}" = "${want}" ]; then
    echo "PASS: ${desc}"
  else
    echo "FAIL: ${desc}"
    echo "       want: ${want}"
    echo "       got:  ${got}"
    fail=1
  fi
}

assert_true() {
  local desc="$1"; shift
  if "$@"; then
    echo "PASS: ${desc}"
  else
    echo "FAIL: ${desc}"
    fail=1
  fi
}

assert_false() {
  local desc="$1"; shift
  if "$@"; then
    echo "FAIL: ${desc}"
    fail=1
  else
    echo "PASS: ${desc}"
  fi
}

echo "==> chart proxy anti-affinity template test"

# Case 1: default install. Selector must equal the pod labels (kafscale-proxy).
pod="$(pod_template_labels)"
sel="$(antiaffinity_selector_labels)"
assert_eq "default: anti-affinity selector equals pod labels" "${pod}" "${sel}"
assert_eq "default: selector targets the proxy pods" "name=kafscale-proxy,instance=kafscale,component=proxy" "${sel}"

# Case 2: nameOverride=foo. The pod label name becomes foo-proxy; the selector
# must move with it. The old hardcoded "kafscale-proxy" selector would fail here.
pod_ov="$(pod_template_labels --set nameOverride=foo)"
sel_ov="$(antiaffinity_selector_labels --set nameOverride=foo)"
assert_eq "nameOverride=foo: anti-affinity selector equals pod labels" "${pod_ov}" "${sel_ov}"
assert_eq "nameOverride=foo: selector tracks the overridden name" "name=foo-proxy,instance=kafscale,component=proxy" "${sel_ov}"

# Case 3: soft-only. The default must be preferredDuring (never requiredDuring),
# so single-node clusters still schedule every replica.
assert_true  "default anti-affinity is soft (preferredDuring present)" has_soft_antiaffinity
assert_false "default anti-affinity is not hard (requiredDuring absent)" \
  bash -c 'helm template kafscale "'"${CHART_DIR}"'" --show-only templates/proxy-deployment.yaml --set proxy.enabled=true | grep -q requiredDuringSchedulingIgnoredDuringExecution'

# Case 4: explicit proxy.affinity replaces the default (no podAntiAffinity).
assert_false "explicit proxy.affinity replaces the default soft anti-affinity" \
  has_soft_antiaffinity --set 'proxy.affinity.nodeAffinity.foo=bar'

if [ "${fail}" -ne 0 ]; then
  echo "==> chart proxy anti-affinity template test FAILED"
  exit 1
fi
echo "==> chart proxy anti-affinity template test passed"
