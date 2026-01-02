#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
example_dir="${repo_root}/examples/E30_flink-kafscale-demo"
log_file="/tmp/kafscale-demo-run.log"

keep_demo="${KEEP_DEMO:-0}"
flink_only="${FLINK_JOB_ONLY:-0}"
submit_target="${SUBMIT_TARGET:-submit-local}"
build_jar="${BUILD_JAR:-0}"

cleanup() {
  if [[ "${keep_demo}" != "1" ]]; then
    if [[ -n "${demo_pid:-}" ]]; then
      kill "${demo_pid}" >/dev/null 2>&1 || true
    fi
  fi
}
trap cleanup EXIT

if [[ "${flink_only}" != "1" ]]; then
  printf "Starting local KafScale demo...\n"
  (if command -v lsof >/dev/null 2>&1; then
    if lsof -ti :2380 >/dev/null 2>&1 || lsof -ti :2379 >/dev/null 2>&1; then
      echo "Ports 2379/2380 are in use. Stop the existing demo or free etcd ports, then retry." >&2
      echo "Tip: run 'make stop-containers' or find the process with 'lsof -i :2380'." >&2
      exit 1
    fi
  fi)
  ( cd "${repo_root}" && make demo-bridge >"${log_file}" 2>&1 ) &
  demo_pid=$!

  printf "Waiting for demo readiness...\n"
  for _ in $(seq 1 120); do
    if command -v rg >/dev/null 2>&1; then
      ready=$(rg -q "demo stack running" "${log_file}" && echo 1 || echo 0)
    else
      ready=$(grep -q "demo stack running" "${log_file}" && echo 1 || echo 0)
    fi
    if [[ "${ready}" == "1" ]]; then
      break
    fi
    sleep 1
    if ! kill -0 "${demo_pid}" >/dev/null 2>&1; then
      echo "Local demo exited early. Check ${log_file}" >&2
      exit 1
    fi
  done
else
  printf "FLINK_JOB_ONLY=1 set: skipping local demo startup.\n"
fi

printf "Seeding demo-topic-1 with kcat...\n"
if ! command -v kcat >/dev/null 2>&1; then
  echo "kcat is required for seeding messages. Install it or skip by setting SKIP_SEED=1." >&2
  exit 1
fi
if [[ "${SKIP_SEED:-0}" != "1" ]]; then
  broker="${KAFSCALE_BOOTSTRAP_SERVERS:-127.0.0.1:39092}"
  kcat -b "${broker}" -t demo-topic-1 -L >/dev/null 2>&1 || true
  printf "message-1\nmessage-2\nmessage-3\nmessage-4\nmessage-5\n" | kcat -b "${broker}" -t demo-topic-1 -P
  printf "Reading back seeded messages...\n"
  kcat -b "${broker}" -t demo-topic-1 -C -o -5 -e
else
  echo "SKIP_SEED=1 set: skipping kcat seeding."
fi

printf "Starting Flink standalone (Docker) and submitting job...\n"
cd "${example_dir}"
make up
make status

make "${submit_target}" BUILD_JAR="${build_jar}" KAFSCALE_SETUP_PROFILE="${KAFSCALE_SETUP_PROFILE:-}"

ui_port="${KAFSCALE_FLINK_REST_PORT:-8081}"
printf "Flink JobManager UI: http://localhost:%s\n" "${ui_port}"
printf "Done.\n"
if [[ "${keep_demo}" == "1" ]]; then
  printf "Local demo kept running (KEEP_DEMO=1). Log: %s\n" "${log_file}"
fi
