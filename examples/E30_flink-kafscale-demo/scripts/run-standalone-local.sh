#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
example_dir="${repo_root}/examples/E30_flink-kafscale-demo"
log_file="/tmp/kafscale-demo-run.log"

printf "Seeding demo-topic-1 with kcat...\n"
if ! command -v kcat >/dev/null 2>&1; then
  echo "kcat is required for seeding messages. Install it or skip by setting SKIP_SEED=1." >&2
  exit 1
fi
if [[ "${SKIP_SEED:-0}" != "1" ]]; then
  broker="${KAFSCALE_BOOTSTRAP_SERVERS:-127.0.0.1:9092}"
  kcat -b "${broker}" -t demo-topic-1 -L >/dev/null 2>&1 || true
  printf "message-1\nmessage-2\nmessage-3\nmessage-4\nmessage-5\n" | kcat -b "${broker}" -t demo-topic-1 -P
  printf "Reading back seeded messages...\n"
  kcat -b "${broker}" -t demo-topic-1 -C -o -5 -e
else
  echo "SKIP_SEED=1 set: skipping kcat seeding."
fi

printf "Running Flink job in standalone JVM via Maven (fat jar)...\n"
cd "${example_dir}"
# Build fat jar and run it via exec:exec to keep Flink runtime on classpath.
mvn -Pstandalone -DskipTests package \
  exec:exec -Dexec.executable=java -Dexec.args="-jar target/flink-kafscale-demo.jar"

ui_port="${KAFSCALE_FLINK_REST_PORT:-8081}"
printf "Flink JobManager UI: http://localhost:%s\n" "${ui_port}"
printf "Done.\n"
if [[ "${keep_demo}" == "1" ]]; then
  printf "Local demo kept running (KEEP_DEMO=1). Log: %s\n" "${log_file}"
fi
