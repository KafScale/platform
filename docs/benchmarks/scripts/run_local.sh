#!/usr/bin/env bash
# Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# Local benchmark runner for the demo-platform stack.
# Assumes kcat, python3, kind, kubectl, and helm are installed.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RESULTS_DIR="${RESULTS_DIR:-${ROOT_DIR}/docs/benchmarks/results/$(date +%Y%m%d-%H%M%S)}"
BROKER_ADDR="${BROKER_ADDR:-127.0.0.1:39092}"
KAFSCALE_NAMESPACE="${KAFSCALE_NAMESPACE:-kafscale-demo}"

N="${N:-2000}"
SIZE="${SIZE:-256}"
TOPICS="${TOPICS:-bench-hot:3 bench-s3:2 bench-multi:6}"

mkdir -p "${RESULTS_DIR}"

log() {
  printf '%s\n' "$*" | tee -a "${RESULTS_DIR}/run.log"
}

run_kcat_produce() {
  local topic="$1"
  local n="$2"
  local size="$3"
  python3 - <<"PY" | kcat -P -b "${BROKER_ADDR}" -t "${topic}" -q
import sys
n=int("${n}")
size=int("${size}")
line=("x"*(size-1)) + "\n"
for _ in range(n):
    sys.stdout.write(line)
PY
}

run_kcat_consume() {
  local topic="$1"
  local count="$2"
  local offset="$3"
  kcat -C -b "${BROKER_ADDR}" -t "${topic}" -o "${offset}" -c "${count}" -e -q
}

capture_metrics() {
  local label="$1"
  if command -v rg >/dev/null 2>&1; then
    curl -s http://127.0.0.1:9093/metrics | rg 'kafscale_(produce|fetch)_rps|kafscale_s3_latency_ms_avg|kafscale_s3_error_rate' \
      > "${RESULTS_DIR}/metrics-${label}.txt" || true
  else
    curl -s http://127.0.0.1:9093/metrics | grep -E 'kafscale_(produce|fetch)_rps|kafscale_s3_latency_ms_avg|kafscale_s3_error_rate' \
      > "${RESULTS_DIR}/metrics-${label}.txt" || true
  fi
}

log "Results directory: ${RESULTS_DIR}"

log "Stopping demo workload if running"
if pgrep -f demo-workload >/dev/null 2>&1; then
  pkill -f demo-workload || true
  sleep 1
fi

ensure_topics() {
  local kafka_topics=""
  if command -v kafka-topics.sh >/dev/null 2>&1; then
    kafka_topics="kafka-topics.sh"
  elif [[ -n "${KAFKA_TOPICS:-}" ]]; then
    kafka_topics="${KAFKA_TOPICS}"
  fi

  if [[ -z "${kafka_topics}" ]]; then
    log "kafka-topics.sh not found; skipping explicit topic creation"
    return 0
  fi

  for entry in ${TOPICS}; do
    local name="${entry%%:*}"
    local parts="${entry##*:}"
    log "Ensuring topic ${name} with ${parts} partitions"
    "${kafka_topics}" --bootstrap-server "${BROKER_ADDR}" \
      --create --if-not-exists --topic "${name}" \
      --partitions "${parts}" --replication-factor 1 >/dev/null
  done
}

ensure_topics

log "Scenario: broker produce+consume (topic=bench-hot)"
START=$(date +%s)
time {
  run_kcat_consume bench-hot "${N}" end >/tmp/bench-consume.log &
  cons=$!
  sleep 1
  run_kcat_produce bench-hot "${N}" "${SIZE}"
  wait "${cons}"
} 2>&1 | tee "${RESULTS_DIR}/broker-produce-consume.txt"
END=$(date +%s)
log "Duration: $((END-START))s"

capture_metrics "broker"

log "Scenario: produce backlog (topic=bench-s3)"
time run_kcat_produce bench-s3 "${N}" "${SIZE}" \
  2>&1 | tee "${RESULTS_DIR}/produce-backlog.txt"

log "Scenario: consume from beginning (topic=bench-s3)"
time run_kcat_consume bench-s3 "${N}" beginning \
  2>&1 | tee "${RESULTS_DIR}/consume-from-beginning.txt"

capture_metrics "s3"

log "Done"
