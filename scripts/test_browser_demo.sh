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
cd "$(dirname "$0")/.."
root="$PWD"
work=$(mktemp -d)
project="kafscale-browser-test-$$"
compose=(docker compose -p "$project" -f "$work/compose.json" --profile demo)
cleanup() {
  result=$?
  if [ "$result" != 0 ] && [ -f "$work/compose.json" ]; then
    "${compose[@]}" logs --no-color || true
  fi
  if [ -f "$work/compose.json" ]; then
    "${compose[@]}" down --volumes --remove-orphans || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

if [ "${DEMO_TEST_SKIP_BUILD:-0}" != 1 ]; then
  docker build -t kafscale-e72-browser-demo:e2e -f deploy/docker/e72-browser-demo.Dockerfile .
  docker build -t kafscale-broker:browser-e2e -f deploy/docker/broker.Dockerfile .
  docker build -t kafscale-proxy:browser-e2e -f deploy/docker/proxy.Dockerfile .
  docker build -t kafscale-minio:browser-e2e -f test/e2e/minio.Dockerfile .
fi

# Start the actual Compose demo profile with isolated names, storage and ports.
docker compose -p "$project" -f deploy/docker-compose/docker-compose.yaml --profile demo config --format json > "$work/base.json"
python3 - "$work/base.json" "$work/compose.json" <<'PYCONFIG'
import json, sys
config = json.load(open(sys.argv[1]))
images = {"broker": "kafscale-broker:browser-e2e", "proxy": "kafscale-proxy:browser-e2e",
          "minio": "kafscale-minio:browser-e2e", "e72-browser-demo": "kafscale-e72-browser-demo:e2e"}
config["services"].pop("console", None)
for name, service in config["services"].items():
    service.pop("container_name", None)
    service.pop("ports", None)
    service.pop("build", None)
    if name in images:
        service["image"] = images[name]
config["services"]["e72-browser-demo"]["ports"] = ["127.0.0.1::8080"]
json.dump(config, open(sys.argv[2], "w"))
PYCONFIG
"${compose[@]}" up -d --wait --wait-timeout 180 etcd minio broker proxy e72-browser-demo
port=$("${compose[@]}" port e72-browser-demo 8080)
cd "$root/lfs-client-sdk/js-browser"
DEMO_BASE_URL="http://$port" npm run test:demo
