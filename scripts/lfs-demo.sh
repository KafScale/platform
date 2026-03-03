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

LFS_DEMO_NAMESPACE="${LFS_DEMO_NAMESPACE:-kafscale-demo}"
LFS_DEMO_TOPIC="${LFS_DEMO_TOPIC:-lfs-demo-topic}"
LFS_DEMO_BLOB_SIZE="${LFS_DEMO_BLOB_SIZE:-10485760}"
LFS_DEMO_BLOB_COUNT="${LFS_DEMO_BLOB_COUNT:-5}"
LFS_DEMO_TIMEOUT_SEC="${LFS_DEMO_TIMEOUT_SEC:-120}"
KAFSCALE_S3_NAMESPACE="${KAFSCALE_S3_NAMESPACE:-default}"
LFS_PROXY_LOG_LEVEL="${LFS_PROXY_LOG_LEVEL:-debug}"
LFS_PROXY_KAFKA_PORT="${LFS_PROXY_KAFKA_PORT:-9092}"
LFS_PROXY_HTTP_PORT="${LFS_PROXY_HTTP_PORT:-8080}"
LFS_PROXY_HEALTH_PORT="${LFS_PROXY_HEALTH_PORT:-9094}"
LFS_PROXY_METRICS_PORT="${LFS_PROXY_METRICS_PORT:-9095}"
LFS_PROXY_HTTP_PATH="${LFS_PROXY_HTTP_PATH:-/lfs/produce}"
LFS_PROXY_SERVICE_HOST="${LFS_PROXY_SERVICE_HOST:-lfs-proxy.${LFS_DEMO_NAMESPACE}.svc.cluster.local}"
BROKER_SERVICE_HOST="${BROKER_SERVICE_HOST:-kafscale-broker.${LFS_DEMO_NAMESPACE}.svc.cluster.local}"
ETCD_SERVICE_HOST="${ETCD_SERVICE_HOST:-kafscale-etcd-client.${LFS_DEMO_NAMESPACE}.svc.cluster.local}"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_SERVICE_HOST="${MINIO_SERVICE_HOST:-minio.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://${MINIO_SERVICE_HOST}:${MINIO_PORT}}"
LFS_PROXY_S3_FORCE_PATH_STYLE="${LFS_PROXY_S3_FORCE_PATH_STYLE:-true}"
LFS_PROXY_S3_ENSURE_BUCKET="${LFS_PROXY_S3_ENSURE_BUCKET:-true}"
METRICS_LOCAL_PORT="${METRICS_LOCAL_PORT:-19095}"
BUSYBOX_IMAGE="${BUSYBOX_IMAGE:-busybox:1.36}"
LFS_PROXY_BACKENDS="${LFS_PROXY_BACKENDS:-${BROKER_SERVICE_HOST}:${LFS_PROXY_KAFKA_PORT}}"
LFS_PROXY_ETCD_ENDPOINTS="${LFS_PROXY_ETCD_ENDPOINTS:-http://${ETCD_SERVICE_HOST}:2379}"
LFS_PROXY_ADVERTISED_HOST="${LFS_PROXY_ADVERTISED_HOST:-${LFS_PROXY_SERVICE_HOST}}"
LFS_PROXY_ADVERTISED_PORT="${LFS_PROXY_ADVERTISED_PORT:-${LFS_PROXY_KAFKA_PORT}}"
TMP_ROOT="${TMPDIR:-/tmp}"

cleanup_kubeconfigs() {
  find "${TMP_ROOT}" -maxdepth 1 -type f -name 'kafscale-kind-kubeconfig.*' -delete 2>/dev/null || true
}

wait_for_dns() {
  local name="$1"
  local attempts="${2:-20}"
  local sleep_sec="${3:-3}"
  for _ in $(seq 1 "$attempts"); do
    if kubectl -n "${LFS_DEMO_NAMESPACE}" run "lfs-dns-check-$$" \
      --restart=Never --rm -i --image=${BUSYBOX_IMAGE} \
      --command -- nslookup "${name}" >/dev/null 2>&1; then
      return 0
    fi
    sleep "${sleep_sec}"
  done
  echo "dns lookup failed for ${name}" >&2
  return 1
}

required_vars=(
  KUBECONFIG
  KAFSCALE_DEMO_NAMESPACE
  KAFSCALE_KIND_CLUSTER
  LFS_DEMO_NAMESPACE
  LFS_DEMO_TOPIC
  LFS_PROXY_IMAGE
  E2E_CLIENT_IMAGE
  MINIO_BUCKET
  MINIO_REGION
  MINIO_ROOT_USER
  MINIO_ROOT_PASSWORD
)

for var in "${required_vars[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    echo "missing required env var: ${var}" >&2
    exit 1
  fi
done

cleanup_kubeconfigs
kubeconfig_file="$(mktemp)"
if ! kind get kubeconfig --name "${KAFSCALE_KIND_CLUSTER}" > "${kubeconfig_file}"; then
  echo "failed to load kubeconfig for kind cluster ${KAFSCALE_KIND_CLUSTER}" >&2
  rm -f "${kubeconfig_file}"
  exit 1
fi
export KUBECONFIG="${kubeconfig_file}"

echo ""
echo "=========================================="
echo " LFS Proxy Demo"
echo "=========================================="
echo ""

# 1. Load LFS proxy image to kind
echo "[1/7] Loading LFS proxy image..."
kind load docker-image "${LFS_PROXY_IMAGE}" --name "${KAFSCALE_KIND_CLUSTER}"
kind load docker-image "${E2E_CLIENT_IMAGE}" --name "${KAFSCALE_KIND_CLUSTER}"

# Ensure namespace exists
kubectl create namespace "${LFS_DEMO_NAMESPACE}" --dry-run=client -o yaml | \
  kubectl apply --validate=false -f -

# Create S3 credentials secret
kubectl -n "${LFS_DEMO_NAMESPACE}" create secret generic kafscale-s3-credentials \
  --from-literal=AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}" \
  --from-literal=KAFSCALE_S3_ACCESS_KEY="${MINIO_ROOT_USER}" \
  --from-literal=KAFSCALE_S3_SECRET_KEY="${MINIO_ROOT_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply --validate=false -f -

# 2. Deploy LFS proxy
echo "[2/7] Deploying LFS proxy..."
kubectl -n "${LFS_DEMO_NAMESPACE}" apply -f - <<EOF
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
        ports:
        - containerPort: ${LFS_PROXY_KAFKA_PORT}
        - containerPort: ${LFS_PROXY_HTTP_PORT}
        - containerPort: ${LFS_PROXY_HEALTH_PORT}
        - containerPort: ${LFS_PROXY_METRICS_PORT}
        env:
        - name: KAFSCALE_LFS_PROXY_LOG_LEVEL
          value: "${LFS_PROXY_LOG_LEVEL}"
        - name: KAFSCALE_LFS_PROXY_ADDR
          value: ":${LFS_PROXY_KAFKA_PORT}"
        - name: KAFSCALE_LFS_PROXY_HTTP_ADDR
          value: ":${LFS_PROXY_HTTP_PORT}"
        - name: KAFSCALE_LFS_PROXY_HEALTH_ADDR
          value: ":${LFS_PROXY_HEALTH_PORT}"
        - name: KAFSCALE_LFS_PROXY_METRICS_ADDR
          value: ":${LFS_PROXY_METRICS_PORT}"
        - name: KAFSCALE_LFS_PROXY_BACKENDS
          value: "${LFS_PROXY_BACKENDS}"
        - name: KAFSCALE_LFS_PROXY_ADVERTISED_HOST
          value: "${LFS_PROXY_ADVERTISED_HOST}"
        - name: KAFSCALE_LFS_PROXY_ADVERTISED_PORT
          value: "${LFS_PROXY_ADVERTISED_PORT}"
        - name: KAFSCALE_LFS_PROXY_ETCD_ENDPOINTS
          value: "${LFS_PROXY_ETCD_ENDPOINTS}"
        - name: KAFSCALE_LFS_PROXY_S3_BUCKET
          value: "${MINIO_BUCKET}"
        - name: KAFSCALE_LFS_PROXY_S3_REGION
          value: "${MINIO_REGION}"
        - name: KAFSCALE_LFS_PROXY_S3_ENDPOINT
          value: "${MINIO_ENDPOINT}"
        - name: KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE
          value: "${LFS_PROXY_S3_FORCE_PATH_STYLE}"
        - name: KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET
          value: "${LFS_PROXY_S3_ENSURE_BUCKET}"
        - name: KAFSCALE_S3_NAMESPACE
          value: "${KAFSCALE_S3_NAMESPACE}"
        - name: KAFSCALE_LFS_PROXY_S3_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: kafscale-s3-credentials
              key: KAFSCALE_S3_ACCESS_KEY
        - name: KAFSCALE_LFS_PROXY_S3_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: kafscale-s3-credentials
              key: KAFSCALE_S3_SECRET_KEY
        readinessProbe:
          httpGet:
            path: /readyz
            port: ${LFS_PROXY_HEALTH_PORT}
          initialDelaySeconds: 5
          periodSeconds: 5
        livenessProbe:
          httpGet:
            path: /livez
            port: ${LFS_PROXY_HEALTH_PORT}
          initialDelaySeconds: 10
          periodSeconds: 10
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
    targetPort: ${LFS_PROXY_KAFKA_PORT}
  - name: http
    port: ${LFS_PROXY_HTTP_PORT}
    targetPort: ${LFS_PROXY_HTTP_PORT}
  - name: health
    port: ${LFS_PROXY_HEALTH_PORT}
    targetPort: ${LFS_PROXY_HEALTH_PORT}
  - name: metrics
    port: ${LFS_PROXY_METRICS_PORT}
    targetPort: ${LFS_PROXY_METRICS_PORT}
EOF

# 3. Wait for LFS proxy
echo "[3/7] Waiting for LFS proxy..."
if ! kubectl -n "${LFS_DEMO_NAMESPACE}" rollout status deployment/lfs-proxy --timeout=120s; then
  kubectl -n "${LFS_DEMO_NAMESPACE}" describe deployment/lfs-proxy || true
  kubectl -n "${LFS_DEMO_NAMESPACE}" logs deployment/lfs-proxy --tail=100 || true
  exit 1
fi

# Wait for LFS proxy DNS
wait_for_dns "${LFS_PROXY_SERVICE_HOST}"

# 4. Create demo topic via e2e-client probe
echo "[4/7] Creating demo topic..."
if ! kubectl get crd kafscaletopics.kafscale.io >/dev/null 2>&1; then
  echo "Installing KafscaleTopic CRD..."
  kubectl apply -f deploy/helm/kafscale/crds/kafscaletopics.yaml
fi
kubectl -n "${LFS_DEMO_NAMESPACE}" apply -f - <<EOF
apiVersion: kafscale.io/v1alpha1
kind: KafscaleTopic
metadata:
  name: ${LFS_DEMO_TOPIC}
spec:
  clusterRef: kafscale
  partitions: 1
EOF

if ! kubectl -n "${LFS_DEMO_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Ready kafscaletopic/${LFS_DEMO_TOPIC} --timeout=120s; then
  kubectl -n "${LFS_DEMO_NAMESPACE}" describe kafscaletopic/${LFS_DEMO_TOPIC} || true
  exit 1
fi

kubectl -n "${LFS_DEMO_NAMESPACE}" delete pod lfs-topic-create --ignore-not-found=true >/dev/null
kubectl -n "${LFS_DEMO_NAMESPACE}" run lfs-topic-create --restart=Never \
  --image="${E2E_CLIENT_IMAGE}" \
  --env="KAFSCALE_E2E_MODE=probe" \
  --env="KAFSCALE_E2E_ADDRS=${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_KAFKA_PORT}" \
  --env="KAFSCALE_E2E_BROKER_ADDR=${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_KAFKA_PORT}" \
  --env="KAFSCALE_E2E_TOPIC=${LFS_DEMO_TOPIC}" \
  --env="KAFSCALE_E2E_PROBE_RETRIES=30" \
  --env="KAFSCALE_E2E_PROBE_SLEEP_MS=1000" \
  --command -- /usr/local/bin/kafscale-e2e-client

if ! kubectl -n "${LFS_DEMO_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/lfs-topic-create --timeout=120s; then
  kubectl -n "${LFS_DEMO_NAMESPACE}" logs pod/lfs-topic-create --tail=100 || true
  kubectl -n "${LFS_DEMO_NAMESPACE}" describe pod/lfs-topic-create || true
  exit 1
fi
kubectl -n "${LFS_DEMO_NAMESPACE}" delete pod lfs-topic-create --ignore-not-found=true >/dev/null

# 5. Produce LFS messages with blob headers
echo "[5/7] Producing ${LFS_DEMO_BLOB_COUNT} LFS messages (${LFS_DEMO_BLOB_SIZE} bytes each) to ${LFS_DEMO_TOPIC}..."
kubectl -n "${LFS_DEMO_NAMESPACE}" delete pod lfs-demo-producer --ignore-not-found=true >/dev/null

# Produce messages via e2e-client with LFS_BLOB header
kubectl -n "${LFS_DEMO_NAMESPACE}" run lfs-demo-producer --restart=Never \
  --image="${E2E_CLIENT_IMAGE}" \
  --env="KAFSCALE_E2E_MODE=produce" \
  --env="KAFSCALE_E2E_ADDRS=${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_KAFKA_PORT}" \
  --env="KAFSCALE_E2E_BROKER_ADDR=${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_KAFKA_PORT}" \
  --env="KAFSCALE_E2E_TOPIC=${LFS_DEMO_TOPIC}" \
  --env="KAFSCALE_E2E_COUNT=${LFS_DEMO_BLOB_COUNT}" \
  --env="KAFSCALE_E2E_LFS_BLOB=true" \
  --env="KAFSCALE_E2E_MSG_SIZE=${LFS_DEMO_BLOB_SIZE}" \
  --command -- /usr/local/bin/kafscale-e2e-client

echo "Waiting for lfs-demo-producer to complete..."
if ! kubectl -n "${LFS_DEMO_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/lfs-demo-producer --timeout="${LFS_DEMO_TIMEOUT_SEC}s"; then
  kubectl -n "${LFS_DEMO_NAMESPACE}" logs pod/lfs-demo-producer --tail=200 || true
  kubectl -n "${LFS_DEMO_NAMESPACE}" describe pod/lfs-demo-producer || true
  exit 1
fi
kubectl -n "${LFS_DEMO_NAMESPACE}" logs pod/lfs-demo-producer --tail=20 || true
echo "Produced ${LFS_DEMO_BLOB_COUNT} messages to ${LFS_DEMO_TOPIC}"

# 6. Show pointer records
echo "[6/8] Pointer records (topic ${LFS_DEMO_TOPIC}):"
kubectl -n "${LFS_DEMO_NAMESPACE}" delete pod lfs-demo-consumer --ignore-not-found=true >/dev/null
kubectl -n "${LFS_DEMO_NAMESPACE}" run lfs-demo-consumer --restart=Never \
  --image="${E2E_CLIENT_IMAGE}" \
  --env="KAFSCALE_E2E_MODE=consume" \
  --env="KAFSCALE_E2E_BROKER_ADDR=${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_KAFKA_PORT}" \
  --env="KAFSCALE_E2E_TOPIC=${LFS_DEMO_TOPIC}" \
  --env="KAFSCALE_E2E_COUNT=${LFS_DEMO_BLOB_COUNT}" \
  --env="KAFSCALE_E2E_TIMEOUT_SEC=30" \
  --env="KAFSCALE_E2E_PRINT_VALUES=true" \
  --env="KAFSCALE_E2E_PRINT_LIMIT=512" \
  --command -- /usr/local/bin/kafscale-e2e-client
if ! kubectl -n "${LFS_DEMO_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/lfs-demo-consumer --timeout=120s; then
  kubectl -n "${LFS_DEMO_NAMESPACE}" logs pod/lfs-demo-consumer --tail=200 || true
  kubectl -n "${LFS_DEMO_NAMESPACE}" describe pod/lfs-demo-consumer || true
  pointer_keys=""
else
  echo ""
  consumer_logs="$(kubectl -n "${LFS_DEMO_NAMESPACE}" logs pod/lfs-demo-consumer --tail=500 || true)"
  pointer_keys_file="$(mktemp)"
  pointer_meta_file="$(mktemp)"

  # Parse pointer records from consumer logs
  PARSE_PYCODE=$(cat <<'PYCODE'
import json, sys, re
out = sys.stdin.read().splitlines()
rows = []
keys = []
meta = []
json_re = re.compile(r"\{.*\}")
for line in out:
    if "record" not in line:
        continue
    payload = ""
    m = json_re.search(line)
    if m:
        payload = m.group(0)
    else:
        parts = line.split("\t", 3)
        if len(parts) >= 4:
            payload = parts[3]
        else:
            parts = re.split(r"\s+", line, maxsplit=3)
            if len(parts) >= 4:
                payload = parts[3]
    if not payload:
        continue
    idx = "?"
    size = "?"
    parts = line.split("\t")
    if len(parts) >= 3 and parts[0] == "record":
        idx, size = parts[1], parts[2]
    else:
        parts = re.split(r"\s+", line, maxsplit=3)
        if len(parts) >= 3 and parts[0] == "record":
            idx, size = parts[1], parts[2]
    try:
        data = json.loads(payload)
        key = data.get("key","")
        sha = data.get("sha256","")
        bucket = data.get("bucket","")
    except Exception:
        key = ""
        sha = ""
        bucket = ""
    rows.append([idx, size, key, sha])
    if key:
        keys.append(key)
        meta.append((key, sha, bucket))
with open(sys.argv[1], "w") as fh:
    for k in keys:
        fh.write(k + "\n")
with open(sys.argv[2], "w") as fh:
    for k, sha, bucket in meta:
        fh.write(f"{k}\t{sha}\t{bucket}\n")
def border(widths):
    return "+" + "+".join("-" * (w + 2) for w in widths) + "+"
def row(values, widths):
    return "| " + " | ".join(v.ljust(w) for v, w in zip(values, widths)) + " |"
headers = ["Record", "Bytes", "Key", "SHA256"]
cols = [headers] + rows
widths = [max(len(c[i]) for c in cols) for i in range(len(headers))]
print(border(widths))
print(row(headers, widths))
print(border(widths))
for r in rows:
    print(row(r, widths))
print(border(widths))
PYCODE
)
  printf '%s\n' "${consumer_logs}" | python3 -c "${PARSE_PYCODE}" "${pointer_keys_file}" "${pointer_meta_file}"
  pointer_keys="$(cat "${pointer_keys_file}" 2>/dev/null || true)"
  pointer_meta="$(cat "${pointer_meta_file}" 2>/dev/null || true)"
  rm -f "${pointer_keys_file}"
  rm -f "${pointer_meta_file}"
  if [[ -z "${pointer_keys}" ]]; then
    echo "(no pointer keys parsed; raw logs below)"
    echo "${consumer_logs}"
  fi
fi
kubectl -n "${LFS_DEMO_NAMESPACE}" delete pod lfs-demo-consumer --ignore-not-found=true >/dev/null

# 7. Verify blobs in MinIO using pointer keys + checksum
echo "[7/8] Verifying blobs in MinIO..."
blob_count="0"
verify_rows=""
if [[ -n "${pointer_meta:-}" ]]; then
  # Use alpine image and install mc for verification via exec (more reliable than startup command)
  kubectl -n "${LFS_DEMO_NAMESPACE}" delete pod lfs-verify --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl -n "${LFS_DEMO_NAMESPACE}" run lfs-verify --restart=Never --image=alpine:3.19 --command -- sleep 300 >/dev/null 2>&1
  kubectl -n "${LFS_DEMO_NAMESPACE}" wait --for=condition=Ready pod/lfs-verify --timeout=60s >/dev/null 2>&1 || true

  # Install curl and mc, then setup alias (run via exec for reliability)
  kubectl -n "${LFS_DEMO_NAMESPACE}" exec pod/lfs-verify -- sh -c "apk add --no-cache curl >/dev/null 2>&1" >/dev/null 2>&1
  kubectl -n "${LFS_DEMO_NAMESPACE}" exec pod/lfs-verify -- sh -c "curl -sL https://dl.min.io/client/mc/release/linux-amd64/mc -o /tmp/mc && chmod +x /tmp/mc" >/dev/null 2>&1
  kubectl -n "${LFS_DEMO_NAMESPACE}" exec pod/lfs-verify -- /tmp/mc alias set minio "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1

  minio_endpoint="${MINIO_ENDPOINT}"

  while IFS=$'\t' read -r key expected_sha bucket; do
    [[ -z "${key}" ]] && continue
    actual_sha=""
    # Use mc to fetch object (handles auth) and pipe to sha256sum
    actual_sha="$(kubectl -n "${LFS_DEMO_NAMESPACE}" exec pod/lfs-verify -- sh -c "
      /tmp/mc cat minio/${MINIO_BUCKET}/${key} 2>/dev/null | sha256sum | cut -d ' ' -f1
    " 2>/dev/null || true)"
    actual_sha="$(echo "${actual_sha}" | tr -d '[:space:]')"
    status="mismatch"
    url="${minio_endpoint}/${MINIO_BUCKET}/${key}"
    if [[ -n "${actual_sha}" && "${actual_sha}" != "" && "${actual_sha}" != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" ]]; then
      # Note: e3b0c44298fc... is sha256 of empty string (mc failure)
      if [[ "${actual_sha}" == "${expected_sha}" ]]; then
        status="ok"
        blob_count="$((blob_count + 1))"
      fi
    else
      status="missing"
    fi
    verify_rows+="${key}\t${expected_sha}\t${actual_sha}\t${status}\t${url}\n"
  done <<< "${pointer_meta}"

  kubectl -n "${LFS_DEMO_NAMESPACE}" delete pod lfs-verify --ignore-not-found=true >/dev/null 2>&1 || true
else
  echo "No pointer keys available to verify."
fi
if [[ -n "${verify_rows}" ]]; then
  TABLE_PYCODE=$(cat <<'PYCODE'
import sys
rows = [line.strip().split("\t") for line in sys.stdin if line.strip()]
headers = ["Key", "Expected", "Actual", "Status", "URL"]
cols = [headers] + rows
widths = [max(len(c[i]) for c in cols) for i in range(len(headers))]
def border():
    return "+" + "+".join("-" * (w + 2) for w in widths) + "+"
def row(vals):
    return "| " + " | ".join(v.ljust(w) for v, w in zip(vals, widths)) + " |"
print(border())
print(row(headers))
print(border())
for r in rows:
    print(row(r))
print(border())
PYCODE
)
  printf "%b" "${verify_rows}" | python3 -c "${TABLE_PYCODE}"
fi
echo "S3 blobs found: ${blob_count}"

# 8. Show metrics
echo "[8/8] LFS Proxy Metrics:"
kubectl -n "${LFS_DEMO_NAMESPACE}" port-forward svc/lfs-proxy ${METRICS_LOCAL_PORT}:${LFS_PROXY_METRICS_PORT} &
PF_PID=$!
sleep 3
echo ""
echo "--- LFS Metrics ---"
curl -s http://localhost:${METRICS_LOCAL_PORT}/metrics 2>/dev/null | grep -E "^kafscale_lfs" || echo "(no LFS metrics yet)"
echo ""
kill $PF_PID 2>/dev/null || true

echo ""
echo "=========================================="
echo " LFS Demo Complete!"
echo "=========================================="
echo ""
echo "LFS Proxy: ${LFS_PROXY_SERVICE_HOST}:${LFS_PROXY_KAFKA_PORT}"
echo "Blobs stored in: s3://${MINIO_BUCKET}/${KAFSCALE_S3_NAMESPACE}/${LFS_DEMO_TOPIC}/lfs/"
echo ""
echo "To access LFS proxy from local machine:"
echo "  kubectl -n ${LFS_DEMO_NAMESPACE} port-forward svc/lfs-proxy ${LFS_PROXY_KAFKA_PORT}:${LFS_PROXY_KAFKA_PORT}"
echo ""
echo "To verify blobs from local machine:"
echo "  kubectl -n ${LFS_DEMO_NAMESPACE} port-forward svc/minio ${MINIO_PORT}:${MINIO_PORT} &"
echo "  kubectl -n ${LFS_DEMO_NAMESPACE} logs pod/lfs-demo-consumer > records.txt"
echo "  scripts/verify-lfs-urls.sh records.txt"
echo ""

# Cleanup
if [[ "${LFS_DEMO_CLEANUP:-0}" == "1" ]]; then
  echo "Cleaning up LFS demo resources..."
  kubectl -n "${LFS_DEMO_NAMESPACE}" delete job lfs-demo-producer --ignore-not-found=true || true
  kubectl -n "${LFS_DEMO_NAMESPACE}" delete deployment lfs-proxy --ignore-not-found=true || true
  kubectl -n "${LFS_DEMO_NAMESPACE}" delete service lfs-proxy --ignore-not-found=true || true
  cleanup_kubeconfigs
fi
