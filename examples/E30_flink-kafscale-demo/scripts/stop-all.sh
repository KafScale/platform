#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

printf "Stopping Flink standalone containers...\n"
cd "${repo_root}/examples/E30_flink-kafscale-demo"
make down || true

printf "Stopping local KafScale demo helpers...\n"
cd "${repo_root}"
make stop-containers || true

printf "Done.\n"
