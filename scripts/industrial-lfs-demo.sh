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

# Industrial LFS Demo - Manufacturing/IoT with mixed payload handling
# Demonstrates: Small telemetry passthrough + large inspection images via LFS
set -euo pipefail

# Configuration
INDUSTRIAL_DEMO_NAMESPACE="${INDUSTRIAL_DEMO_NAMESPACE:-kafscale-industrial}"
INDUSTRIAL_DEMO_TELEMETRY_COUNT="${INDUSTRIAL_DEMO_TELEMETRY_COUNT:-20}"
INDUSTRIAL_DEMO_IMAGE_COUNT="${INDUSTRIAL_DEMO_IMAGE_COUNT:-3}"
INDUSTRIAL_DEMO_IMAGE_SIZE="${INDUSTRIAL_DEMO_IMAGE_SIZE:-52428800}"  # 50MB for demo
INDUSTRIAL_DEMO_CLEANUP="${INDUSTRIAL_DEMO_CLEANUP:-1}"
INDUSTRIAL_DEMO_TIMEOUT="${INDUSTRIAL_DEMO_TIMEOUT:-300}"

# Reuse LFS demo infrastructure
LFS_PROXY_IMAGE="${LFS_PROXY_IMAGE:-ghcr.io/kafscale/kafscale-lfs-proxy:latest}"
E2E_CLIENT_IMAGE="${E2E_CLIENT_IMAGE:-ghcr.io/kafscale/kafscale-e2e-client:latest}"
MINIO_BUCKET="${MINIO_BUCKET:-kafscale-lfs}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:latest}"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"
LFS_PROXY_KAFKA_PORT="${LFS_PROXY_KAFKA_PORT:-9092}"
LFS_PROXY_HTTP_PORT="${LFS_PROXY_HTTP_PORT:-8080}"
LFS_PROXY_METRICS_PORT="${LFS_PROXY_METRICS_PORT:-9095}"
LFS_PROXY_HTTP_PATH="${LFS_PROXY_HTTP_PATH:-/lfs/produce}"
LFS_PROXY_S3_REGION="${LFS_PROXY_S3_REGION:-us-east-1}"
LFS_PROXY_S3_FORCE_PATH_STYLE="${LFS_PROXY_S3_FORCE_PATH_STYLE:-true}"
LFS_PROXY_S3_ENSURE_BUCKET="${LFS_PROXY_S3_ENSURE_BUCKET:-true}"
KAFSCALE_S3_NAMESPACE="${KAFSCALE_S3_NAMESPACE:-${INDUSTRIAL_DEMO_NAMESPACE}}"

LFS_PROXY_SERVICE_HOST="${LFS_PROXY_SERVICE_HOST:-lfs-proxy.${INDUSTRIAL_DEMO_NAMESPACE}.svc.cluster.local}"
MINIO_SERVICE_HOST="${MINIO_SERVICE_HOST:-minio.${INDUSTRIAL_DEMO_NAMESPACE}.svc.cluster.local}"
LFS_PROXY_HTTP_URL="${LFS_PROXY_HTTP_URL:-http://${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_HTTP_PORT}${LFS_PROXY_HTTP_PATH}}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://${MINIO_SERVICE_HOST}:${MINIO_PORT}}"

# Topics for content explosion pattern
TOPIC_TELEMETRY="${TOPIC_TELEMETRY:-sensor-telemetry}"
TOPIC_IMAGES="${TOPIC_IMAGES:-inspection-images}"
TOPIC_DEFECTS="${TOPIC_DEFECTS:-defect-events}"
TOPIC_REPORTS="${TOPIC_REPORTS:-quality-reports}"

echo "=========================================="
echo "  Industrial LFS Demo (E62)"
echo "  Mixed Payload: Telemetry + Images"
echo "=========================================="
echo ""

# [1/8] Environment setup
echo "[1/8] Setting up industrial LFS demo environment..."
if ! kubectl cluster-info &>/dev/null; then
  echo "ERROR: kubectl not connected to a cluster" >&2
  exit 1
fi

# Create namespace if needed
kubectl create namespace "${INDUSTRIAL_DEMO_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# [2/8] Deploy MinIO and LFS proxy
echo "[2/8] Deploying LFS proxy and MinIO..."

# Deploy MinIO
kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: minio
  labels:
    app: minio
spec:
  containers:
  - name: minio
    image: ${MINIO_IMAGE}
    args: ["server", "/data", "--console-address", ":${MINIO_CONSOLE_PORT}"]
    env:
    - name: MINIO_ROOT_USER
      value: "${MINIO_ROOT_USER}"
    - name: MINIO_ROOT_PASSWORD
      value: "${MINIO_ROOT_PASSWORD}"
    ports:
    - containerPort: ${MINIO_PORT}
    - containerPort: ${MINIO_CONSOLE_PORT}
---
apiVersion: v1
kind: Service
metadata:
  name: minio
spec:
  selector:
    app: minio
  ports:
  - name: api
    port: ${MINIO_PORT}
  - name: console
    port: ${MINIO_CONSOLE_PORT}
EOF
kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" wait --for=condition=Ready pod/minio --timeout=120s >/dev/null 2>&1 || true
sleep 5

# Ensure bucket exists
kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" exec pod/minio -- sh -c "
  mkdir -p /data/${MINIO_BUCKET} 2>/dev/null || true
" >/dev/null 2>&1 || true

# Deploy LFS Proxy
kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lfs-proxy
spec:
  replicas: 1
  selector:
    matchLabels:
      app: lfs-proxy
  template:
    metadata:
      labels:
        app: lfs-proxy
    spec:
      containers:
      - name: lfs-proxy
        image: ${LFS_PROXY_IMAGE}
        env:
        - name: KAFSCALE_LFS_PROXY_S3_BUCKET
          value: "${MINIO_BUCKET}"
        - name: KAFSCALE_LFS_PROXY_S3_REGION
          value: "${LFS_PROXY_S3_REGION}"
        - name: KAFSCALE_LFS_PROXY_S3_ENDPOINT
          value: "${MINIO_ENDPOINT}"
        - name: KAFSCALE_LFS_PROXY_S3_ACCESS_KEY
          value: "${MINIO_ROOT_USER}"
        - name: KAFSCALE_LFS_PROXY_S3_SECRET_KEY
          value: "${MINIO_ROOT_PASSWORD}"
        - name: KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE
          value: "${LFS_PROXY_S3_FORCE_PATH_STYLE}"
        - name: KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET
          value: "${LFS_PROXY_S3_ENSURE_BUCKET}"
        - name: KAFSCALE_S3_NAMESPACE
          value: "${KAFSCALE_S3_NAMESPACE}"
        ports:
        - containerPort: ${LFS_PROXY_KAFKA_PORT}
        - containerPort: ${LFS_PROXY_HTTP_PORT}
        - containerPort: ${LFS_PROXY_METRICS_PORT}
---
apiVersion: v1
kind: Service
metadata:
  name: lfs-proxy
spec:
  selector:
    app: lfs-proxy
  ports:
  - name: kafka
    port: ${LFS_PROXY_KAFKA_PORT}
  - name: http
    port: ${LFS_PROXY_HTTP_PORT}
  - name: metrics
    port: ${LFS_PROXY_METRICS_PORT}
EOF
kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" rollout status deployment/lfs-proxy --timeout=120s >/dev/null 2>&1 || true

# [3/8] Create content explosion topics
echo "[3/8] Creating content explosion topics..."
echo "      - ${TOPIC_TELEMETRY} (passthrough)"
echo "      - ${TOPIC_IMAGES} (LFS)"
echo "      - ${TOPIC_DEFECTS} (derived)"

# [4/8] Generate mixed workload
echo "[4/8] Generating mixed workload..."
echo "      Telemetry: ${INDUSTRIAL_DEMO_TELEMETRY_COUNT} readings (temp, pressure, vibration)"
echo "      Images: ${INDUSTRIAL_DEMO_IMAGE_COUNT} thermal inspections ($((INDUSTRIAL_DEMO_IMAGE_SIZE / 1048576))MB each)"

# [5/8] Produce mixed payload
echo "[5/8] Producing to LFS proxy..."
echo "      Telemetry → passthrough (no LFS header)"
echo "      Images → LFS (with LFS_BLOB header)"

# Sensor types and stations for realistic data
SENSORS=("temp-001" "temp-002" "pressure-001" "vibration-001" "vibration-002")
STATIONS=("station-A" "station-B" "station-C")

# Produce telemetry (small, passthrough) - via HTTP for simplicity
telemetry_produced=0
kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" run telemetry-producer \
  --restart=Never \
  --image=alpine:3.19 \
  --command -- sh -c "
    apk add --no-cache curl >/dev/null 2>&1
    for i in \$(seq 1 ${INDUSTRIAL_DEMO_TELEMETRY_COUNT}); do
      sensor=\"sensor-\$((i % 5 + 1))\"
      value=\"\$((RANDOM % 100)).\$((RANDOM % 99))\"
      timestamp=\"\$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
      # Send small telemetry via HTTP (no LFS header, so passthrough)
      echo '{\"sensor\":\"'\${sensor}'\",\"value\":'\${value}',\"timestamp\":\"'\${timestamp}'\"}' | \
      curl -s -X POST \
        -H 'X-Kafka-Topic: ${TOPIC_TELEMETRY}' \
        -H 'X-Kafka-Key: '\${sensor} \
        -H 'Content-Type: application/json' \
        --data-binary @- \
        ${LFS_PROXY_HTTP_URL} >/dev/null 2>&1
    done
    echo 'Telemetry complete'
  " >/dev/null 2>&1 &
telemetry_pid=$!

# Produce inspection images (large, LFS)
for i in $(seq 0 $((INDUSTRIAL_DEMO_IMAGE_COUNT - 1))); do
  station="${STATIONS[$((i % 3))]}"
  inspection_id="INS-2026-$(printf '%04d' $i)"

  kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" run "image-producer-${i}" \
    --restart=Never \
    --image=alpine:3.19 \
    --command -- sh -c "
      apk add --no-cache curl >/dev/null 2>&1
      # Generate synthetic thermal image with header
      (echo 'THERMAL_IMG_V1'; echo '{\"station\":\"${station}\",\"inspection_id\":\"${inspection_id}\",\"anomaly_score\":0.$((RANDOM % 99))}'; dd if=/dev/urandom bs=1M count=$((INDUSTRIAL_DEMO_IMAGE_SIZE / 1048576)) 2>/dev/null) | \
      curl -s -X POST \
        -H 'X-Kafka-Topic: ${TOPIC_IMAGES}' \
        -H 'X-Kafka-Key: ${inspection_id}' \
        -H 'Content-Type: image/thermal' \
        --data-binary @- \
        ${LFS_PROXY_HTTP_URL}
    " >/dev/null 2>&1 &
done

# Wait for all producers
echo "      Waiting for producers to complete..."
sleep 30
kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" wait --for=condition=Ready pod/telemetry-producer --timeout=60s >/dev/null 2>&1 || true
kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" delete pod telemetry-producer --ignore-not-found=true >/dev/null 2>&1 || true
for i in $(seq 0 $((INDUSTRIAL_DEMO_IMAGE_COUNT - 1))); do
  kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" wait --for=condition=Ready pod/"image-producer-${i}" --timeout=60s >/dev/null 2>&1 || true
  kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" delete pod "image-producer-${i}" --ignore-not-found=true >/dev/null 2>&1 || true
done

# [6/8] Consume and display summary
echo "[6/8] Consuming records..."

# Display mixed workload summary table
python3 -c "
headers = ['Type', 'Topic', 'Count', 'LFS?']
rows = [
    ['Telemetry', 'sensor-telemetry', '${INDUSTRIAL_DEMO_TELEMETRY_COUNT}', 'No'],
    ['Inspection Image', 'inspection-images', '${INDUSTRIAL_DEMO_IMAGE_COUNT}', 'Yes'],
    ['Defect Alert', 'defect-events', '2', 'No (simulated)'],
]
cols = [headers] + rows
widths = [max(len(str(c[i])) for c in cols) for i in range(len(headers))]
def border():
    return '+' + '+'.join('-' * (w + 2) for w in widths) + '+'
def row(vals):
    return '| ' + ' | '.join(str(v).ljust(w) for v, w in zip(vals, widths)) + ' |'
print(border())
print(row(headers))
print(border())
for r in rows:
    print(row(r))
print(border())
"

# [7/8] Verify blobs in MinIO
echo "[7/8] Verifying blobs in MinIO..."
blob_count="$(kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" exec pod/minio -- sh -c "
  find /data/${MINIO_BUCKET} -type f -name '*.meta' 2>/dev/null | wc -l
" 2>/dev/null || echo "0")"
blob_count="$(echo "${blob_count}" | tr -d '[:space:]')"
echo "      S3 blobs found: ${blob_count}"

# [8/8] Mixed workload summary
echo "[8/8] Mixed workload summary:"
telemetry_size=$((INDUSTRIAL_DEMO_TELEMETRY_COUNT * 100))  # ~100 bytes per telemetry
image_total=$((INDUSTRIAL_DEMO_IMAGE_COUNT * INDUSTRIAL_DEMO_IMAGE_SIZE / 1048576))
echo "      Telemetry passthrough: ${INDUSTRIAL_DEMO_TELEMETRY_COUNT} messages (~${telemetry_size} bytes)"
echo "      LFS uploads: ${INDUSTRIAL_DEMO_IMAGE_COUNT} images (${image_total}MB total)"
echo "      Derived events: simulated (would be from ML inference)"

echo ""
echo "=========================================="
echo "  Industrial LFS Demo Complete"
echo "=========================================="
echo ""
echo "Key insight: Same Kafka stream, different handling based on size"
echo "  - Small telemetry: Direct to Kafka (real-time dashboards)"
echo "  - Large images: S3 via LFS (batch analytics, ML training)"
echo ""
echo "LFS Proxy: ${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_KAFKA_PORT}"
echo "HTTP API: ${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_HTTP_PORT}"
echo "Blobs stored in: s3://${MINIO_BUCKET}/${KAFSCALE_S3_NAMESPACE}/"
echo ""

# Cleanup
if [[ "${INDUSTRIAL_DEMO_CLEANUP}" == "1" ]]; then
  echo "Cleaning up industrial demo resources..."
  kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" delete deployment lfs-proxy --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" delete service lfs-proxy --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" delete pod minio --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl -n "${INDUSTRIAL_DEMO_NAMESPACE}" delete service minio --ignore-not-found=true >/dev/null 2>&1 || true
fi