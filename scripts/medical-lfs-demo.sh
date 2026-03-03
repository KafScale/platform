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

# Medical LFS Demo - Healthcare imaging with content explosion pattern
# Demonstrates: DICOM-like blobs, metadata extraction, audit trails
set -euo pipefail

# Configuration
MEDICAL_DEMO_NAMESPACE="${MEDICAL_DEMO_NAMESPACE:-kafscale-medical}"
MEDICAL_DEMO_BLOB_SIZE="${MEDICAL_DEMO_BLOB_SIZE:-524288000}"  # 500MB
MEDICAL_DEMO_BLOB_COUNT="${MEDICAL_DEMO_BLOB_COUNT:-3}"
MEDICAL_DEMO_CLEANUP="${MEDICAL_DEMO_CLEANUP:-1}"
MEDICAL_DEMO_TIMEOUT="${MEDICAL_DEMO_TIMEOUT:-300}"

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
KAFSCALE_S3_NAMESPACE="${KAFSCALE_S3_NAMESPACE:-${MEDICAL_DEMO_NAMESPACE}}"

LFS_PROXY_SERVICE_HOST="${LFS_PROXY_SERVICE_HOST:-lfs-proxy.${MEDICAL_DEMO_NAMESPACE}.svc.cluster.local}"
MINIO_SERVICE_HOST="${MINIO_SERVICE_HOST:-minio.${MEDICAL_DEMO_NAMESPACE}.svc.cluster.local}"
LFS_PROXY_HTTP_URL="${LFS_PROXY_HTTP_URL:-http://${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_HTTP_PORT}${LFS_PROXY_HTTP_PATH}}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://${MINIO_SERVICE_HOST}:${MINIO_PORT}}"

# Topics for content explosion pattern
TOPIC_IMAGES="${TOPIC_IMAGES:-medical-images}"
TOPIC_METADATA="${TOPIC_METADATA:-medical-metadata}"
TOPIC_AUDIT="${TOPIC_AUDIT:-medical-audit}"

echo "=========================================="
echo "  Medical LFS Demo (E60)"
echo "  Content Explosion Pattern for Healthcare"
echo "=========================================="
echo ""

# [1/8] Environment setup
echo "[1/8] Setting up medical LFS demo environment..."
if ! kubectl cluster-info &>/dev/null; then
  echo "ERROR: kubectl not connected to a cluster" >&2
  exit 1
fi

# Create namespace if needed
kubectl create namespace "${MEDICAL_DEMO_NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# [2/8] Deploy MinIO and LFS proxy (reuse from base lfs-demo)
echo "[2/8] Deploying LFS proxy and MinIO..."

# Deploy MinIO
kubectl -n "${MEDICAL_DEMO_NAMESPACE}" apply -f - <<EOF
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
kubectl -n "${MEDICAL_DEMO_NAMESPACE}" wait --for=condition=Ready pod/minio --timeout=120s >/dev/null 2>&1 || true
sleep 5

# Ensure bucket exists
kubectl -n "${MEDICAL_DEMO_NAMESPACE}" exec pod/minio -- sh -c "
  mkdir -p /data/${MINIO_BUCKET} 2>/dev/null || true
" >/dev/null 2>&1 || true

# Deploy LFS Proxy
kubectl -n "${MEDICAL_DEMO_NAMESPACE}" apply -f - <<EOF
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
kubectl -n "${MEDICAL_DEMO_NAMESPACE}" rollout status deployment/lfs-proxy --timeout=120s >/dev/null 2>&1 || true

# [3/8] Create content explosion topics
echo "[3/8] Creating content explosion topics..."
echo "      - ${TOPIC_IMAGES} (LFS blobs)"
echo "      - ${TOPIC_METADATA} (extracted info)"
echo "      - ${TOPIC_AUDIT} (access log)"

# Note: In production, these would be KafscaleTopic CRDs
# For demo purposes, topics are auto-created by the producer

# [4/8] Generate synthetic DICOM data
echo "[4/8] Generating synthetic DICOM data..."

# Medical study metadata
PATIENTS=("P-2026-001" "P-2026-002" "P-2026-003")
MODALITIES=("CT" "MRI" "XRAY")
STUDY_DATES=("2026-02-01" "2026-02-01" "2026-01-31")

for i in $(seq 0 $((MEDICAL_DEMO_BLOB_COUNT - 1))); do
  patient="${PATIENTS[$i]:-P-2026-00$i}"
  modality="${MODALITIES[$i]:-CT}"
  study_date="${STUDY_DATES[$i]:-2026-02-01}"
  echo "      Patient: ${patient}, Modality: ${modality}, Size: $((MEDICAL_DEMO_BLOB_SIZE / 1048576))MB"
done

# [5/8] Upload via LFS proxy HTTP endpoint
echo "[5/8] Uploading via LFS proxy..."

# Create producer pod that sends DICOM-like blobs
for i in $(seq 0 $((MEDICAL_DEMO_BLOB_COUNT - 1))); do
  patient="${PATIENTS[$i]:-P-2026-00$i}"
  modality="${MODALITIES[$i]:-CT}"
  study_date="${STUDY_DATES[$i]:-2026-02-01}"

  # Generate and upload blob via HTTP streaming endpoint
  kubectl -n "${MEDICAL_DEMO_NAMESPACE}" run "medical-producer-${i}" \
    --restart=Never \
    --image=alpine:3.19 \
    --command -- sh -c "
      apk add --no-cache curl >/dev/null 2>&1
      # Generate random DICOM-like data with metadata header
      (echo 'DICM'; echo '{\"patient_id\":\"${patient}\",\"modality\":\"${modality}\",\"study_date\":\"${study_date}\"}'; dd if=/dev/urandom bs=1M count=$((MEDICAL_DEMO_BLOB_SIZE / 1048576)) 2>/dev/null) | \
      curl -s -X POST \
        -H 'X-Kafka-Topic: ${TOPIC_IMAGES}' \
        -H 'X-Kafka-Key: ${patient}' \
        -H 'Content-Type: application/dicom' \
        --data-binary @- \
        ${LFS_PROXY_HTTP_URL}
    " >/dev/null 2>&1 &
done

# Wait for producers to complete
echo "      Waiting for uploads to complete..."
sleep 30
for i in $(seq 0 $((MEDICAL_DEMO_BLOB_COUNT - 1))); do
  kubectl -n "${MEDICAL_DEMO_NAMESPACE}" wait --for=condition=Ready pod/"medical-producer-${i}" --timeout=60s >/dev/null 2>&1 || true
  kubectl -n "${MEDICAL_DEMO_NAMESPACE}" logs pod/"medical-producer-${i}" 2>/dev/null || true
  kubectl -n "${MEDICAL_DEMO_NAMESPACE}" delete pod "medical-producer-${i}" --ignore-not-found=true >/dev/null 2>&1 || true
done

# [6/8] Consume and display pointer records
echo "[6/8] Consuming pointer records..."

# Create consumer to read back the LFS envelopes
kubectl -n "${MEDICAL_DEMO_NAMESPACE}" run medical-consumer \
  --restart=Never \
  --image="${E2E_CLIENT_IMAGE}" \
  --env="KAFSCALE_E2E_MODE=consume" \
  --env="KAFSCALE_E2E_BROKER=${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_KAFKA_PORT}" \
  --env="KAFSCALE_E2E_TOPIC=${TOPIC_IMAGES}" \
  --env="KAFSCALE_E2E_COUNT=${MEDICAL_DEMO_BLOB_COUNT}" \
  --env="KAFSCALE_E2E_TIMEOUT=30s" \
  >/dev/null 2>&1 || true

sleep 15
consumer_logs="$(kubectl -n "${MEDICAL_DEMO_NAMESPACE}" logs pod/medical-consumer --tail=100 2>/dev/null || echo "")"

# Parse and display results
if [[ -n "${consumer_logs}" ]]; then
  echo "${consumer_logs}" | python3 -c "
import json, sys, re
lines = sys.stdin.read().splitlines()
rows = []
json_re = re.compile(r'\{.*\}')
for line in lines:
    m = json_re.search(line)
    if not m:
        continue
    try:
        data = json.loads(m.group(0))
        if 'key' in data and 'sha256' in data:
            # Extract patient from key path
            patient = data.get('key', '').split('/')[-1][:12] if '/' in data.get('key', '') else 'unknown'
            rows.append([patient, data['sha256'][:20]+'...', 'ok'])
    except:
        pass
if rows:
    headers = ['Patient', 'SHA256', 'Status']
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
else:
    print('(no pointer records found)')
" 2>/dev/null || echo "(parsing failed)"
fi
kubectl -n "${MEDICAL_DEMO_NAMESPACE}" delete pod medical-consumer --ignore-not-found=true >/dev/null 2>&1 || true

# [7/8] Verify blobs in MinIO
echo "[7/8] Verifying blobs in MinIO..."
blob_count="$(kubectl -n "${MEDICAL_DEMO_NAMESPACE}" exec pod/minio -- sh -c "
  find /data/${MINIO_BUCKET} -type f -name '*.meta' 2>/dev/null | wc -l
" 2>/dev/null || echo "0")"
blob_count="$(echo "${blob_count}" | tr -d '[:space:]')"
echo "      S3 blobs found: ${blob_count}"

# [8/8] Content explosion summary
echo "[8/8] Content explosion summary:"
echo "      ${TOPIC_IMAGES}: ${MEDICAL_DEMO_BLOB_COUNT} LFS pointers"
echo "      ${TOPIC_METADATA}: ${MEDICAL_DEMO_BLOB_COUNT} patient records (simulated)"
echo "      ${TOPIC_AUDIT}: $((MEDICAL_DEMO_BLOB_COUNT * 3)) access events (simulated)"

echo ""
echo "=========================================="
echo "  Medical LFS Demo Complete"
echo "=========================================="
echo ""
echo "LFS Proxy: ${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_KAFKA_PORT}"
echo "HTTP API: ${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_HTTP_PORT}"
echo "Blobs stored in: s3://${MINIO_BUCKET}/${KAFSCALE_S3_NAMESPACE}/"
echo ""

# Cleanup
if [[ "${MEDICAL_DEMO_CLEANUP}" == "1" ]]; then
  echo "Cleaning up medical demo resources..."
  kubectl -n "${MEDICAL_DEMO_NAMESPACE}" delete deployment lfs-proxy --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl -n "${MEDICAL_DEMO_NAMESPACE}" delete service lfs-proxy --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl -n "${MEDICAL_DEMO_NAMESPACE}" delete pod minio --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl -n "${MEDICAL_DEMO_NAMESPACE}" delete service minio --ignore-not-found=true >/dev/null 2>&1 || true
fi