#!/usr/bin/env bash
# Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
compose_file="${1:?Pass the generated release Compose file}"
export KAFSCALE_S3_BUCKET=release-compose-check
export KAFSCALE_S3_REGION=us-east-1
export KAFSCALE_S3_ACCESS_KEY=minioadmin
export KAFSCALE_S3_SECRET_KEY=minioadmin
export KAFSCALE_S3_PATH_STYLE=true
export KAFSCALE_UI_PASSWORD=compose-test-password
export KAFSCALE_S3_ENDPOINT=http://external-s3:9000
cleanup() {
  result=$?
  if [ "$result" -ne 0 ]; then
    docker compose -p release-smoke -f "$compose_file" logs --no-color || true
    docker logs release-smoke-s3 || true
  fi
  docker rm -f release-smoke-s3 || true
  docker compose -p release-smoke -f "$compose_file" down --volumes || true
  return "$result"
}
trap cleanup EXIT
# MinIO is a disposable CI fixture, never a release asset or service.
docker build -t kafscale-minio:release-smoke -f test/e2e/minio.Dockerfile .
docker run -d --name release-smoke-s3 -p 127.0.0.1:9000:9000 \
  -e MINIO_ROOT_USER="$KAFSCALE_S3_ACCESS_KEY" -e MINIO_ROOT_PASSWORD="$KAFSCALE_S3_SECRET_KEY" \
  kafscale-minio:release-smoke
for _ in $(seq 1 60); do
  curl -fsS http://127.0.0.1:9000/minio/health/ready >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS http://127.0.0.1:9000/minio/health/ready
# Provision and verify the required bucket before starting any product container.
curl -fsS --aws-sigv4 "aws:amz:$KAFSCALE_S3_REGION:s3" \
  --user "$KAFSCALE_S3_ACCESS_KEY:$KAFSCALE_S3_SECRET_KEY" \
  -X PUT "http://127.0.0.1:9000/$KAFSCALE_S3_BUCKET"
curl -fsSI --aws-sigv4 "aws:amz:$KAFSCALE_S3_REGION:s3" \
  --user "$KAFSCALE_S3_ACCESS_KEY:$KAFSCALE_S3_SECRET_KEY" \
  "http://127.0.0.1:9000/$KAFSCALE_S3_BUCKET"
docker compose -p release-smoke -f "$compose_file" create
docker network connect --alias external-s3 release-smoke_default release-smoke-s3
docker compose -p release-smoke -f "$compose_file" up -d --wait --wait-timeout 180
curl -fsS http://127.0.0.1:9094/readyz
curl -fsS http://127.0.0.1:3080/healthz
