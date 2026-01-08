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

ICEBERG_DEMO_CLUSTER="${ICEBERG_DEMO_CLUSTER:-kafscale}"
ICEBERG_BROKER_SERVICE="${ICEBERG_DEMO_CLUSTER}-broker"
TMP_ROOT="${TMPDIR:-/tmp}"

cleanup_kubeconfigs() {
  find "${TMP_ROOT}" -maxdepth 1 -type f -name 'kafscale-kind-kubeconfig.*' -delete 2>/dev/null || true
}

wait_for_dns() {
  local name="$1"
  local attempts="${2:-20}"
  local sleep_sec="${3:-3}"
  for i in $(seq 1 "$attempts"); do
    if kubectl -n "${ICEBERG_DEMO_NAMESPACE}" run "iceberg-dns-check-$$" \
      --restart=Never --rm -i --image=busybox:1.36 \
      --command -- nslookup "${name}" >/dev/null 2>&1; then
      return 0
    fi
    sleep "${sleep_sec}"
  done
  echo "dns lookup failed for ${name}" >&2
  return 1
}

wait_for_etcd_leader() {
  local attempts="${ICEBERG_DEMO_ETCD_WAIT_ATTEMPTS:-80}"
  local sleep_sec="${ICEBERG_DEMO_ETCD_WAIT_SLEEP_SEC:-3}"
  local replicas
  replicas="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get statefulset "${ICEBERG_DEMO_CLUSTER}-etcd" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)"
  if [[ -z "${replicas}" || "${replicas}" == "0" ]]; then
    echo "etcd statefulset not found or replicas=0" >&2
    return 1
  fi

  for i in $(seq 0 $((replicas - 1))); do
    wait_for_dns "${ICEBERG_DEMO_CLUSTER}-etcd-${i}.${ICEBERG_DEMO_CLUSTER}-etcd.${ICEBERG_DEMO_NAMESPACE}.svc.cluster.local"
  done

  local endpoints=()
  for i in $(seq 0 $((replicas - 1))); do
    endpoints+=("http://${ICEBERG_DEMO_CLUSTER}-etcd-${i}.${ICEBERG_DEMO_CLUSTER}-etcd.${ICEBERG_DEMO_NAMESPACE}.svc.cluster.local:2379")
  done
  local service_endpoint="http://${ICEBERG_DEMO_CLUSTER}-etcd-client.${ICEBERG_DEMO_NAMESPACE}.svc.cluster.local:2379"

  for i in $(seq 1 "${attempts}"); do
    if kubectl -n "${ICEBERG_DEMO_NAMESPACE}" run "iceberg-etcd-health-$$-${RANDOM}" \
      --restart=Never --rm -i --image=kubesphere/etcd:3.6.4-0 \
      --command -- /bin/sh -c "out=\$(etcdctl --endpoints=${service_endpoint} endpoint status -w json 2>/dev/null || true); case \"\$out\" in *'\"Leader\":true'*|*'\"leader\":true'* ) exit 0 ;; *'\"leader\":0'*|*'\"Leader\":0'* ) exit 1 ;; *'\"leader\":'*|*'\"Leader\":'* ) exit 0 ;; * ) exit 1 ;; esac" >/dev/null 2>&1; then
      return 0
    fi
    for endpoint in "${endpoints[@]}"; do
      if kubectl -n "${ICEBERG_DEMO_NAMESPACE}" run "iceberg-etcd-health-$$-${RANDOM}" \
        --restart=Never --rm -i --image=kubesphere/etcd:3.6.4-0 \
        --command -- /bin/sh -c "out=\$(etcdctl --endpoints=${endpoint} endpoint status -w json 2>/dev/null || true); case \"\$out\" in *'\"Leader\":true'*|*'\"leader\":true'* ) exit 0 ;; *'\"leader\":0'*|*'\"Leader\":0'* ) exit 1 ;; *'\"leader\":'*|*'\"Leader\":'* ) exit 0 ;; * ) exit 1 ;; esac" >/dev/null 2>&1; then
        return 0
      fi
    done
    sleep "${sleep_sec}"
  done
  echo "etcd leader not ready for endpoints: ${endpoints[*]}" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" run "iceberg-etcd-health-$$-${RANDOM}" \
    --restart=Never --rm -i --image=kubesphere/etcd:3.6.4-0 \
    --command -- /bin/sh -c "etcdctl --endpoints=${endpoints[0]} endpoint status --cluster -w table || true" >&2 || true
  return 1
}

get_etcd_statefulset_pods() {
  local sts_name="$1"
  local selector="$2"
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l "${selector}" \
    -o go-template="{{range .items}}{{\$pod := .}}{{range .metadata.ownerReferences}}{{if and (eq .kind \"StatefulSet\") (eq .name \"${sts_name}\")}}{{\$pod.metadata.name}} {{end}}{{end}}{{end}}"
}

wait_for_etcd_pods_ready() {
  local sts_name="$1"
  local selector="$2"
  local timeout="${3:-180s}"
  local pods
  pods="$(get_etcd_statefulset_pods "${sts_name}" "${selector}")"
  if [[ -z "${pods}" ]]; then
    echo "no etcd statefulset pods found for ${sts_name}" >&2
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l "${selector}" -o wide || true
    return 1
  fi
  for p in ${pods}; do
    if ! kubectl -n "${ICEBERG_DEMO_NAMESPACE}" wait --for=condition=Ready "pod/${p}" --timeout="${timeout}"; then
      echo "etcd pod not Ready: ${p}" >&2
      kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe "pod/${p}" || true
      return 1
    fi
  done
}

etcd_client_endpoints() {
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get endpoints "${ICEBERG_DEMO_CLUSTER}-etcd-client" \
    -o jsonpath="{.subsets[*].addresses[*].ip}" 2>/dev/null | \
    awk '{for (i=1;i<=NF;i++) printf "http://%s:2379%s", $i, (i<NF?",":"")}'
}

wait_for_etcd_stable() {
  local attempts="${ICEBERG_DEMO_ETCD_STABLE_ATTEMPTS:-30}"
  local sleep_sec="${ICEBERG_DEMO_ETCD_STABLE_SLEEP_SEC:-2}"
  local need_consecutive="${ICEBERG_DEMO_ETCD_STABLE_CONSECUTIVE:-3}"
  local consecutive=0
  local endpoints
  for i in $(seq 1 "${attempts}"); do
    endpoints="$(etcd_client_endpoints)"
    if [[ -n "${endpoints}" ]]; then
      if kubectl -n "${ICEBERG_DEMO_NAMESPACE}" run "iceberg-etcd-stable-$$-${RANDOM}" \
        --restart=Never --rm -i --image=kubesphere/etcd:3.6.4-0 \
        --command -- /bin/sh -c "etcdctl --endpoints=${endpoints} endpoint status --cluster -w json >/dev/null 2>&1"; then
        consecutive=$((consecutive + 1))
        if [[ "${consecutive}" -ge "${need_consecutive}" ]]; then
          return 0
        fi
      else
        consecutive=0
      fi
    fi
    sleep "${sleep_sec}"
  done
  echo "etcd cluster not stable after ${attempts} attempts (needed ${need_consecutive} consecutive successes)" >&2
  if [[ -n "${endpoints}" ]]; then
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" run "iceberg-etcd-stable-$$-${RANDOM}" \
      --restart=Never --rm -i --image=kubesphere/etcd:3.6.4-0 \
      --command -- /bin/sh -c "etcdctl --endpoints=${endpoints} endpoint status --cluster -w table || true" >&2 || true
  fi
  return 1
}

required_vars=(
  KUBECONFIG
  KAFSCALE_DEMO_NAMESPACE
  KAFSCALE_KIND_CLUSTER
  ICEBERG_DEMO_NAMESPACE
  ICEBERG_DEMO_CLUSTER
  ICEBERG_REST_IMAGE
  ICEBERG_REST_PORT
  ICEBERG_WAREHOUSE_BUCKET
  ICEBERG_WAREHOUSE_PREFIX
  MINIO_REGION
  MINIO_ROOT_USER
  MINIO_ROOT_PASSWORD
  ICEBERG_DEMO_TABLE_NAMESPACE
  ICEBERG_DEMO_TABLE_NAME
  ICEBERG_DEMO_TABLE
  ICEBERG_DEMO_TOPIC
  ICEBERG_DEMO_RECORDS
  ICEBERG_DEMO_TIMEOUT_SEC
  ICEBERG_DEMO_TIMEOUT_SEC
  ICEBERG_PROCESSOR_RELEASE
  ICEBERG_PROCESSOR_IMAGE
  E2E_CLIENT_IMAGE
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
ensure_kube_system_ready() {
  local timeout="${ICEBERG_DEMO_KUBE_SYSTEM_TIMEOUT:-120s}"
  local control_plane="kube-scheduler-${KAFSCALE_KIND_CLUSTER}-control-plane"
  local controller="kube-controller-manager-${KAFSCALE_KIND_CLUSTER}-control-plane"
  local apiserver="kube-apiserver-${KAFSCALE_KIND_CLUSTER}-control-plane"
  local failed=0

  kubectl -n kube-system wait --for=condition=Ready "pod/${apiserver}" --timeout="${timeout}" || failed=1
  kubectl -n kube-system wait --for=condition=Ready "pod/${controller}" --timeout="${timeout}" || failed=1
  kubectl -n kube-system wait --for=condition=Ready "pod/${control_plane}" --timeout="${timeout}" || failed=1
  kubectl -n kube-system wait --for=condition=Ready pods -l k8s-app=kube-dns --timeout="${timeout}" || failed=1
  if [[ "${failed}" -ne 0 ]]; then
    echo "kube-system components not ready; dumping kube-system diagnostics" >&2
    kubectl -n kube-system get pods -o wide || true
    kubectl -n kube-system describe "pod/${apiserver}" || true
    kubectl -n kube-system describe "pod/${controller}" || true
    kubectl -n kube-system describe "pod/${control_plane}" || true
    kubectl -n kube-system logs "pod/${control_plane}" --tail=200 || true
    kubectl -n kube-system logs deployment/coredns --tail=200 || true
    return 1
  fi
  return 0
}
cleanup_kind_cluster() {
  if [[ "${ICEBERG_DEMO_CLEANUP_KIND:-1}" == "1" ]]; then
    echo "cleaning kind cluster ${KAFSCALE_KIND_CLUSTER}"
    kind delete cluster --name "${KAFSCALE_KIND_CLUSTER}" >/dev/null 2>&1 || true
  fi
}
cleanup_kubeconfig() {
  if [[ "${ICEBERG_DEMO_KEEP_KUBECONFIG:-0}" == "1" ]]; then
    echo "keeping kubeconfig at ${kubeconfig_file}" >&2
    return 0
  fi
  rm -f "${kubeconfig_file}"
}
trap 'status=$?; cleanup_kubeconfig; if [[ "${status}" -eq 0 ]]; then cleanup_kind_cluster; fi; exit ${status}' EXIT

if ! ensure_kube_system_ready; then
  exit 1
fi

operator_deploy="$(kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" get deployments -l app.kubernetes.io/component=operator -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || true)"
if [[ -z "${operator_deploy}" ]]; then
  echo "operator deployment not found in ${KAFSCALE_DEMO_NAMESPACE}" >&2
  kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" get deployments || true
  exit 1
fi
kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" set env deployment/"${operator_deploy}" \
  KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET=1

kind load docker-image "${ICEBERG_PROCESSOR_IMAGE}" --name "${KAFSCALE_KIND_CLUSTER}"
kind load docker-image "${E2E_CLIENT_IMAGE}" --name "${KAFSCALE_KIND_CLUSTER}"

kubectl create namespace "${ICEBERG_DEMO_NAMESPACE}" --dry-run=client -o yaml | \
  kubectl apply --validate=false -f -

kubectl -n "${ICEBERG_DEMO_NAMESPACE}" create secret generic iceberg-s3-credentials \
  --from-literal=AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}" \
  --from-literal=KAFSCALE_S3_ACCESS_KEY="${MINIO_ROOT_USER}" \
  --from-literal=KAFSCALE_S3_SECRET_KEY="${MINIO_ROOT_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply --validate=false -f -

if ! kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get "kafscalecluster/${ICEBERG_DEMO_CLUSTER}" >/dev/null 2>&1; then
  echo "kafscalecluster/${ICEBERG_DEMO_CLUSTER} not found in ${ICEBERG_DEMO_NAMESPACE}; run make demo-platform-bootstrap first" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get kafscaleclusters -o wide || true
  exit 1
fi

topic_yaml="$(printf '%s\n' \
  'apiVersion: kafscale.io/v1alpha1' \
  'kind: KafscaleTopic' \
  'metadata:' \
  "  name: ${ICEBERG_DEMO_TOPIC}" \
  "  namespace: ${ICEBERG_DEMO_NAMESPACE}" \
  'spec:' \
  "  clusterRef: ${ICEBERG_DEMO_CLUSTER}" \
  '  partitions: 1' \
)"
kubectl apply --validate=false -f - <<<"${topic_yaml}"
echo "waiting for kafscaletopic/${ICEBERG_DEMO_TOPIC} to be Ready..."
topic_wait_timeout="${ICEBERG_DEMO_TOPIC_WAIT_TIMEOUT:-180s}"
if ! kubectl -n "${ICEBERG_DEMO_NAMESPACE}" wait \
  --for=jsonpath='{.status.phase}'=Ready \
  "kafscaletopic/${ICEBERG_DEMO_TOPIC}" --timeout="${topic_wait_timeout}"; then
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get "kafscaletopic/${ICEBERG_DEMO_TOPIC}" -o yaml || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe "kafscaletopic/${ICEBERG_DEMO_TOPIC}" || true
  exit 1
fi

for i in $(seq 1 30); do
  if kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get "statefulset/${ICEBERG_DEMO_CLUSTER}-etcd" >/dev/null 2>&1; then
    break
  fi
  sleep 3
done
if ! kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get "statefulset/${ICEBERG_DEMO_CLUSTER}-etcd" >/dev/null 2>&1; then
  echo "etcd statefulset not found: ${ICEBERG_DEMO_CLUSTER}-etcd" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get statefulsets -o wide || true
  exit 1
fi
echo "waiting for statefulset/${ICEBERG_DEMO_CLUSTER}-etcd rollout..."
if ! kubectl -n "${ICEBERG_DEMO_NAMESPACE}" rollout status "statefulset/${ICEBERG_DEMO_CLUSTER}-etcd" --timeout=180s; then
  echo "etcd statefulset rollout failed" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get "statefulset/${ICEBERG_DEMO_CLUSTER}-etcd" -o yaml || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe "statefulset/${ICEBERG_DEMO_CLUSTER}-etcd" || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l "app=kafscale-etcd,cluster=${ICEBERG_DEMO_CLUSTER}" -o wide || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get events --sort-by=.lastTimestamp | tail -n 80 || true
  etcd_pods="$(get_etcd_statefulset_pods "${ICEBERG_DEMO_CLUSTER}-etcd" \
    "app=kafscale-etcd,cluster=${ICEBERG_DEMO_CLUSTER}")"
  for p in ${etcd_pods}; do
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe "pod/${p}" || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" -c snapshot-download --tail=200 || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" -c snapshot-restore --tail=200 || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" -c etcd --tail=200 || true
  done
  exit 1
fi
echo "waiting for etcd pods to be Ready..."
if ! wait_for_etcd_pods_ready "${ICEBERG_DEMO_CLUSTER}-etcd" \
  "app=kafscale-etcd,cluster=${ICEBERG_DEMO_CLUSTER}" "180s"; then
  echo "etcd pods not Ready" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l "app=kafscale-etcd,cluster=${ICEBERG_DEMO_CLUSTER}" -o wide || true
  exit 1
fi
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get svc "${ICEBERG_DEMO_CLUSTER}-etcd-client" >/dev/null
echo "waiting for etcd leader..."
if ! wait_for_etcd_leader; then
  echo "etcd leader not ready for ${ICEBERG_DEMO_CLUSTER}" >&2
  etcd_pods="$(get_etcd_statefulset_pods "${ICEBERG_DEMO_CLUSTER}-etcd" \
    "app=kafscale-etcd,cluster=${ICEBERG_DEMO_CLUSTER}")"
  for p in ${etcd_pods}; do
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe "pod/${p}" || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" -c snapshot-download --tail=200 || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" -c snapshot-restore --tail=200 || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" -c etcd --tail=200 || true
  done
  exit 1
fi
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get svc "${ICEBERG_BROKER_SERVICE}" >/dev/null
echo "waiting for statefulset/${ICEBERG_BROKER_SERVICE} rollout..."
if ! kubectl -n "${ICEBERG_DEMO_NAMESPACE}" rollout status \
  "statefulset/${ICEBERG_BROKER_SERVICE}" --timeout=180s; then
  echo "broker statefulset rollout failed" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get "statefulset/${ICEBERG_BROKER_SERVICE}" -o yaml || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l "app.kubernetes.io/component=broker,cluster=${ICEBERG_DEMO_CLUSTER}" -o wide || true
  exit 1
fi
selector="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get svc "${ICEBERG_BROKER_SERVICE}" \
  -o go-template='{{range $k,$v := .spec.selector}}{{printf "%s=%s," $k $v}}{{end}}')"
selector="${selector%,}"
if [[ -z "${selector}" ]]; then
  echo "broker service has no selector labels" >&2
  exit 1
fi
echo "waiting for broker pods to be Ready..."
if ! kubectl -n "${ICEBERG_DEMO_NAMESPACE}" wait --for=condition=Ready pod -l "${selector}" --timeout=180s; then
  echo "broker pods not Ready" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l "${selector}" -o wide || true
  exit 1
fi

echo "waiting for kafscalecluster/${ICEBERG_DEMO_CLUSTER} to be Ready..."
if ! kubectl -n "${ICEBERG_DEMO_NAMESPACE}" wait --for=condition=Ready \
  "kafscalecluster/${ICEBERG_DEMO_CLUSTER}" --timeout=120s; then
  echo "kafscalecluster/${ICEBERG_DEMO_CLUSTER} not Ready" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get "kafscalecluster/${ICEBERG_DEMO_CLUSTER}" -o yaml || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe "kafscalecluster/${ICEBERG_DEMO_CLUSTER}" || true
  echo "cluster pods:" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -o wide || true
  echo "etcd init container logs (if any):" >&2
  etcd_pods="$(get_etcd_statefulset_pods "${ICEBERG_DEMO_CLUSTER}-etcd" \
    "app=kafscale-etcd,cluster=${ICEBERG_DEMO_CLUSTER}")"
  for p in ${etcd_pods}; do
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe "pod/${p}" || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" -c snapshot-download --tail=200 || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" -c snapshot-restore --tail=200 || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" -c etcd --tail=200 || true
  done
  echo "operator logs (tail):" >&2
  kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" logs deployment/"${operator_deploy}" --tail=200 || true
  echo "namespace events:" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get events --sort-by=.lastTimestamp || true
  exit 1
fi

etcd_endpoint="http://${ICEBERG_DEMO_CLUSTER}-etcd-client.${ICEBERG_DEMO_NAMESPACE}.svc.cluster.local:2379"
echo "checking etcd endpoint: ${etcd_endpoint}"
etcd_ips="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get endpoints "${ICEBERG_DEMO_CLUSTER}-etcd-client" -o jsonpath="{.subsets[*].addresses[*].ip}" 2>/dev/null || true)"
if [[ -z "${etcd_ips}" ]]; then
  echo "etcd endpoints not ready in namespace ${ICEBERG_DEMO_NAMESPACE}" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get svc "${ICEBERG_DEMO_CLUSTER}-etcd-client" -o yaml || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get endpoints "${ICEBERG_DEMO_CLUSTER}-etcd-client" -o yaml || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l "app=kafscale-etcd,cluster=${ICEBERG_DEMO_CLUSTER}" -o wide || true
  exit 1
fi
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" delete pod etcd-health --ignore-not-found=true >/dev/null
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" run etcd-health --restart=Never \
  --image=curlimages/curl:8.6.0 -- \
  sh -c "set -e; curl -fsS --max-time 5 ${etcd_endpoint}/health >/dev/null"
if ! kubectl -n "${ICEBERG_DEMO_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/etcd-health --timeout=60s; then
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs pod/etcd-health --tail=200 || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe pod/etcd-health || true
  exit 1
fi
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" delete pod etcd-health --ignore-not-found=true >/dev/null

echo "waiting for etcd cluster stability..."
if ! wait_for_etcd_stable; then
  etcd_pods="$(get_etcd_statefulset_pods "${ICEBERG_DEMO_CLUSTER}-etcd" \
    "app=kafscale-etcd,cluster=${ICEBERG_DEMO_CLUSTER}")"
  for p in ${etcd_pods}; do
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe "pod/${p}" || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" -c snapshot-download --tail=200 || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" -c snapshot-restore --tail=200 || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" -c etcd --tail=200 || true
  done
  exit 1
fi

echo "ensuring bucket ${ICEBERG_WAREHOUSE_BUCKET} exists in MinIO..."
if [[ "${ICEBERG_DEMO_CREATE_BUCKET:-0}" == "1" ]]; then
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" delete pod iceberg-minio-bucket --ignore-not-found=true >/dev/null
  bucket_cmd=$(cat <<EOF
set -e
echo "aws-cli: \$(aws --version 2>&1 || true)"
export AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}"
export AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}"
export AWS_DEFAULT_REGION="${MINIO_REGION}"
export AWS_EC2_METADATA_DISABLED=true
endpoint="http://minio.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local:9000"
echo "endpoint=\${endpoint}"
aws s3api --endpoint-url "\${endpoint}" create-bucket --bucket "${ICEBERG_WAREHOUSE_BUCKET}" >/dev/null 2>&1 || true
aws s3api --endpoint-url "\${endpoint}" head-bucket --bucket "${ICEBERG_WAREHOUSE_BUCKET}" >/dev/null
EOF
)
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" run iceberg-minio-bucket --restart=Never \
    --image=amazon/aws-cli:2.15.0 --command -- /bin/sh -c "${bucket_cmd}"
  if ! kubectl -n "${ICEBERG_DEMO_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/iceberg-minio-bucket --timeout=90s; then
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs pod/iceberg-minio-bucket --tail=200 || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe pod/iceberg-minio-bucket || true
    exit 1
  fi
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" delete pod iceberg-minio-bucket --ignore-not-found=true >/dev/null
fi

iceberg_rest_yaml="$(printf '%s\n' \
  'apiVersion: apps/v1' \
  'kind: Deployment' \
  'metadata:' \
  '  name: iceberg-rest' \
  "  namespace: ${ICEBERG_DEMO_NAMESPACE}" \
  'spec:' \
  '  replicas: 1' \
  '  selector:' \
  '    matchLabels:' \
  '      app: iceberg-rest' \
  '  template:' \
  '    metadata:' \
  '      labels:' \
  '        app: iceberg-rest' \
  '    spec:' \
  '      containers:' \
  '        - name: iceberg-rest' \
  "          image: ${ICEBERG_REST_IMAGE}" \
  '          env:' \
  '            - name: CATALOG_WAREHOUSE' \
  "              value: s3://${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_WAREHOUSE_PREFIX}" \
  '            - name: CATALOG_IO__IMPL' \
  '              value: org.apache.iceberg.aws.s3.S3FileIO' \
  '            - name: CATALOG_S3_REGION' \
  "              value: ${MINIO_REGION}" \
  '            - name: AWS_REGION' \
  "              value: ${MINIO_REGION}" \
  '            - name: AWS_DEFAULT_REGION' \
  "              value: ${MINIO_REGION}" \
  '            - name: CATALOG_S3_ENDPOINT' \
  "              value: http://minio.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local:9000" \
  '            - name: CATALOG_S3_PATH__STYLE__ACCESS' \
  '              value: "true"' \
  '            - name: CATALOG_S3_ACCESS__KEY__ID' \
  '              valueFrom:' \
  '                secretKeyRef:' \
  '                  name: iceberg-s3-credentials' \
  '                  key: AWS_ACCESS_KEY_ID' \
  '            - name: CATALOG_S3_SECRET__ACCESS__KEY' \
  '              valueFrom:' \
  '                secretKeyRef:' \
  '                  name: iceberg-s3-credentials' \
  '                  key: AWS_SECRET_ACCESS_KEY' \
  '          ports:' \
  "            - containerPort: ${ICEBERG_REST_PORT}" \
  '---' \
  'apiVersion: v1' \
  'kind: Service' \
  'metadata:' \
  '  name: iceberg-rest' \
  "  namespace: ${ICEBERG_DEMO_NAMESPACE}" \
  'spec:' \
  '  selector:' \
  '    app: iceberg-rest' \
  '  ports:' \
  '    - name: http' \
  "      port: ${ICEBERG_REST_PORT}" \
  "      targetPort: ${ICEBERG_REST_PORT}" \
)"
kubectl apply --validate=false -f - <<<"${iceberg_rest_yaml}"

echo "waiting for iceberg-rest deployment rollout..."
if ! kubectl -n "${ICEBERG_DEMO_NAMESPACE}" rollout status deployment/iceberg-rest --timeout=120s; then
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l app=iceberg-rest -o wide
  pod="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l app=iceberg-rest -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || true)"
  if [[ -n "${pod}" ]]; then
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "${pod}" --all-containers=true --tail=200
  fi
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe deployment/iceberg-rest
  exit 1
fi

rest_ip="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get svc iceberg-rest -o jsonpath="{.spec.clusterIP}")"
if [[ -z "${rest_ip}" ]]; then
  echo "iceberg-rest service has no clusterIP" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get svc iceberg-rest -o yaml
  exit 1
fi

rest_endpoint="http://${rest_ip}:${ICEBERG_REST_PORT}"
table_location="s3://${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_WAREHOUSE_PREFIX}/${ICEBERG_DEMO_TABLE_NAMESPACE}/${ICEBERG_DEMO_TABLE_NAME}"
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" delete pod iceberg-rest-setup --ignore-not-found=true >/dev/null
setup_script=$(cat <<'EOS'
set -e
schema='{"type":"struct","schema-id":1,"identifier-field-ids":[],"fields":[{"id":1,"name":"record_id","required":true,"type":"string"},{"id":2,"name":"topic","required":true,"type":"string"},{"id":3,"name":"partition","required":true,"type":"int"},{"id":4,"name":"offset","required":true,"type":"long"},{"id":5,"name":"timestamp_ms","required":true,"type":"long"},{"id":6,"name":"key","required":false,"type":"binary"},{"id":7,"name":"value","required":false,"type":"binary"},{"id":8,"name":"headers","required":false,"type":"string"}]}'
payload=$(printf '{"name":"%s","schema":%s,"location":"%s"}' "$ICEBERG_TABLE_NAME" "$schema" "$ICEBERG_TABLE_LOCATION")
for i in $(seq 1 10); do
  resp=$(curl -sS --connect-timeout 3 --max-time 10 -X POST -H "Content-Type: application/json" -d "{\"namespace\":[\"${ICEBERG_NAMESPACE}\"]}" "${REST_ENDPOINT}/v1/namespaces" -w "\n%{http_code}" || true)
  code=$(printf "%s" "${resp}" | tail -n 1)
  body=$(printf "%s" "${resp}" | sed "$d")
  if [ "${code}" = "200" ] || [ "${code}" = "409" ]; then
    echo "namespace create ok (${code})"
    break
  fi
  echo "namespace create failed (attempt ${i}): ${code}"
  if [ -n "${body}" ]; then echo "${body}"; fi
  sleep 3
done
for i in $(seq 1 10); do
  resp=$(curl -sS --connect-timeout 3 --max-time 20 -X POST -H "Content-Type: application/json" -d "${payload}" "${REST_ENDPOINT}/v1/namespaces/${ICEBERG_NAMESPACE}/tables" -w "\n%{http_code}" || true)
  code=$(printf "%s" "${resp}" | tail -n 1)
  body=$(printf "%s" "${resp}" | sed "$d")
  if [ "${code}" = "200" ] || [ "${code}" = "409" ]; then
    echo "table create ok (${code})"
    exit 0
  fi
  echo "table create failed (attempt ${i}): ${code}"
  if [ -n "${body}" ]; then echo "${body}"; fi
  sleep 3
done
exit 1
EOS
)
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" run iceberg-rest-setup --restart=Never \
  --image=curlimages/curl:8.6.0 \
  --env REST_ENDPOINT="${rest_endpoint}" \
  --env ICEBERG_NAMESPACE="${ICEBERG_DEMO_TABLE_NAMESPACE}" \
  --env ICEBERG_TABLE_NAME="${ICEBERG_DEMO_TABLE_NAME}" \
  --env ICEBERG_TABLE_LOCATION="${table_location}" -- \
  sh -c "${setup_script}"

echo "waiting for iceberg-rest-setup pod..."
for i in $(seq 1 40); do
  phase="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pod iceberg-rest-setup -o jsonpath="{.status.phase}" 2>/dev/null || true)"
  if [[ "${phase}" == "Succeeded" ]]; then
    break
  fi
  if [[ "${phase}" == "Failed" ]]; then
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs pod/iceberg-rest-setup --tail=200 || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe pod/iceberg-rest-setup || true
    exit 1
  fi
  sleep 3
done

if ! kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pod iceberg-rest-setup -o jsonpath="{.status.phase}" 2>/dev/null | grep -q Succeeded; then
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs pod/iceberg-rest-setup --tail=200 || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe pod/iceberg-rest-setup || true
  exit 1
fi
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs pod/iceberg-rest-setup --tail=200 || true
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" delete pod iceberg-rest-setup --ignore-not-found=true >/dev/null

proc_repo="${ICEBERG_PROCESSOR_IMAGE%:*}"
proc_tag="${ICEBERG_PROCESSOR_IMAGE##*:}"
if [[ -z "${proc_repo}" || -z "${proc_tag}" ]]; then
  echo "invalid ICEBERG_PROCESSOR_IMAGE: ${ICEBERG_PROCESSOR_IMAGE}" >&2
  exit 1
fi

helm upgrade --install "${ICEBERG_PROCESSOR_RELEASE}" addons/processors/iceberg-processor/deploy/helm/iceberg-processor \
  --namespace "${ICEBERG_DEMO_NAMESPACE}" \
  --set fullnameOverride="${ICEBERG_PROCESSOR_RELEASE}" \
  --set image.repository="${proc_repo}" \
  --set image.tag="${proc_tag}" \
  --set s3.credentialsSecretRef=iceberg-s3-credentials \
  --set config.s3.bucket="${ICEBERG_WAREHOUSE_BUCKET}" \
  --set config.s3.namespace="${ICEBERG_DEMO_NAMESPACE}" \
  --set config.s3.region="${MINIO_REGION}" \
  --set config.s3.endpoint="http://minio.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local:9000" \
  --set config.s3.path_style=true \
  --set config.iceberg.catalog.uri="http://iceberg-rest.${ICEBERG_DEMO_NAMESPACE}.svc.cluster.local:${ICEBERG_REST_PORT}" \
  --set config.iceberg.warehouse="s3://${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_WAREHOUSE_PREFIX}" \
  --set-string config.schema.registry.base_url= \
  --set config.offsets.backend=etcd \
  --set config.discovery.mode=s3 \
  --set config.processor.poll_interval_seconds=1 \
  --set config.etcd.endpoints[0]="${etcd_endpoint}" \
  --set env[0].name=ICEBERG_PROCESSOR_REST_DEBUG \
  --set-string env[0].value="${ICEBERG_PROCESSOR_REST_DEBUG:-}" \
  --set config.mappings[0].topic="${ICEBERG_DEMO_TOPIC}" \
  --set config.mappings[0].table="${ICEBERG_DEMO_TABLE}" \
  --set config.mappings[0].mode=append \
  --set config.mappings[0].schema.source=none \
  --set config.mappings[0].create_table_if_missing=true

deploy="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get deployments -l app.kubernetes.io/name=iceberg-processor,app.kubernetes.io/instance="${ICEBERG_PROCESSOR_RELEASE}" -o name | head -n 1)"
if [[ -z "${deploy}" ]]; then
  echo "iceberg-processor deployment not found for release ${ICEBERG_PROCESSOR_RELEASE}" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get deployments
  exit 1
fi
dump_iceberg_processor_debug() {
  echo "iceberg-processor rollout failed; collecting debug output..." >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe "${deploy}" || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get rs -l app.kubernetes.io/name=iceberg-processor,app.kubernetes.io/instance="${ICEBERG_PROCESSOR_RELEASE}" -o wide || true
  rs_list="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get rs -l app.kubernetes.io/name=iceberg-processor,app.kubernetes.io/instance="${ICEBERG_PROCESSOR_RELEASE}" -o name || true)"
  if [[ -n "${rs_list}" ]]; then
    for rs in ${rs_list}; do
      kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe "${rs}" || true
    done
  fi
  pods="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l app.kubernetes.io/name=iceberg-processor,app.kubernetes.io/instance="${ICEBERG_PROCESSOR_RELEASE}" -o name || true)"
  if [[ -n "${pods}" ]]; then
    for pod in ${pods}; do
      kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe "${pod}" || true
      kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "${pod}" --all-containers=true --tail=200 || true
      kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "${pod}" --all-containers=true --previous --tail=200 || true
    done
  else
    echo "no iceberg-processor pods found for ${ICEBERG_PROCESSOR_RELEASE}" >&2
  fi
  echo "namespace events (tail):" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get events --sort-by=.lastTimestamp | tail -n 80 || true
}
echo "waiting for iceberg-processor rollout..."
set +e
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" rollout status "${deploy}" --timeout=180s
rollout_status=$?
set -e
if [[ "${rollout_status}" -ne 0 ]]; then
  dump_iceberg_processor_debug
  exit 1
fi
echo "iceberg-processor startup logs:"
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "${deploy}" --tail=120 || true

echo "re-checking etcd stability before producer job..."
if ! wait_for_etcd_stable; then
  exit 1
fi

kubectl -n "${ICEBERG_DEMO_NAMESPACE}" delete job iceberg-demo-producer --ignore-not-found=true
producer_yaml="$(printf '%s\n' \
  'apiVersion: batch/v1' \
  'kind: Job' \
  'metadata:' \
  '  name: iceberg-demo-producer' \
  "  namespace: ${ICEBERG_DEMO_NAMESPACE}" \
  'spec:' \
  '  ttlSecondsAfterFinished: 300' \
  '  backoffLimit: 0' \
  '  template:' \
  '    spec:' \
  '      restartPolicy: Never' \
  '      containers:' \
  '        - name: producer' \
  "          image: ${E2E_CLIENT_IMAGE}" \
  '          command:' \
  '            - sh' \
  '            - -c' \
  '          args:' \
  '            - |' \
  '              set -e' \
  '              KAFSCALE_E2E_MODE=probe /usr/local/bin/kafscale-e2e-client' \
  '              KAFSCALE_E2E_MODE=produce /usr/local/bin/kafscale-e2e-client' \
  '          env:' \
  '            - name: KAFSCALE_E2E_ADDRS' \
  "              value: ${ICEBERG_BROKER_SERVICE}.${ICEBERG_DEMO_NAMESPACE}.svc.cluster.local:9092" \
  '            - name: KAFSCALE_E2E_BROKER_ADDR' \
  "              value: ${ICEBERG_BROKER_SERVICE}.${ICEBERG_DEMO_NAMESPACE}.svc.cluster.local:9092" \
  '            - name: KAFSCALE_E2E_TOPIC' \
  "              value: ${ICEBERG_DEMO_TOPIC}" \
  '            - name: KAFSCALE_E2E_COUNT' \
  "              value: \"${ICEBERG_DEMO_RECORDS}\"" \
  '            - name: KAFSCALE_E2E_TIMEOUT_SEC' \
  "              value: \"${ICEBERG_DEMO_TIMEOUT_SEC}\"" \
  '            - name: KAFSCALE_E2E_PROBE_RETRIES' \
  '              value: "20"' \
  '            - name: KAFSCALE_E2E_PROBE_SLEEP_MS' \
  '              value: "500"' \
)"
kubectl apply --validate=false -f - <<<"${producer_yaml}"

echo "waiting for iceberg-demo-producer job to complete..."
job_wait_timeout="${ICEBERG_DEMO_JOB_WAIT_TIMEOUT:-180s}"
set +e
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" wait \
  --for=jsonpath='{.status.phase}'=Succeeded \
  pod -l job-name=iceberg-demo-producer --timeout="${job_wait_timeout}"
wait_status=$?
set -e
if [[ "${wait_status}" -ne 0 ]]; then
  pod_phases="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l job-name=iceberg-demo-producer \
    -o jsonpath="{.items[*].status.phase}" 2>/dev/null || true)"
  if [[ "${pod_phases}" == *"Succeeded"* ]]; then
    wait_status=0
  else
    failed_condition="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get job iceberg-demo-producer \
      -o jsonpath="{.status.conditions[?(@.type=='Failed')].status}" 2>/dev/null || true)"
    if [[ "${failed_condition}" == "True" ]] || [[ "${pod_phases}" == *"Failed"* ]] || [[ "${pod_phases}" == *"Error"* ]]; then
      echo "iceberg-demo-producer failed" >&2
    else
      echo "timed out waiting for iceberg-demo-producer to complete" >&2
    fi
  fi
fi
if [[ "${wait_status}" -ne 0 ]]; then
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get job iceberg-demo-producer -o yaml || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l job-name=iceberg-demo-producer -o wide || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe job/iceberg-demo-producer || true
  pod_list="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l job-name=iceberg-demo-producer \
    -o jsonpath="{.items[*].metadata.name}" 2>/dev/null || true)"
  if [[ -n "${pod_list}" ]]; then
    for p in ${pod_list}; do
      kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe "pod/${p}" || true
      kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" --all-containers=true --tail=200 || true
      kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${p}" --all-containers=true --previous --tail=200 || true
    done
  fi
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs job/iceberg-demo-producer --all-containers=true --tail=200 || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get events --sort-by=.lastTimestamp || true
  echo "kube-system status (control plane diagnostics):" >&2
  kubectl -n kube-system get pods -o wide || true
  kubectl -n kube-system logs "pod/kube-controller-manager-${KAFSCALE_KIND_CLUSTER}-control-plane" --tail=200 || true
  kubectl -n kube-system logs "pod/kube-controller-manager-${KAFSCALE_KIND_CLUSTER}-control-plane" --previous --tail=200 || true
  kubectl -n kube-system logs "pod/kube-scheduler-${KAFSCALE_KIND_CLUSTER}-control-plane" --tail=200 || true
  kubectl -n kube-system logs "pod/kube-scheduler-${KAFSCALE_KIND_CLUSTER}-control-plane" --previous --tail=200 || true
  exit 1
fi
pod="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l job-name=iceberg-demo-producer -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || true)"

echo "post-producer S3 check (topic ${ICEBERG_DEMO_TOPIC}):"
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs job/iceberg-demo-producer --tail=200 || true
kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- \
  sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_DEMO_NAMESPACE}/${ICEBERG_DEMO_TOPIC} 2>/dev/null && \
         ls /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_DEMO_NAMESPACE}/${ICEBERG_DEMO_TOPIC}/* 2>/dev/null | head -n 20" || true

echo "waiting for segments + iceberg data files..."
for i in $(seq 1 30); do
  seg="$(kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- \
    sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_DEMO_NAMESPACE}/${ICEBERG_DEMO_TOPIC}/*/segment-*.kfs 2>/dev/null | head -n 1")"
  data="$(kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- \
    sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_WAREHOUSE_PREFIX}/${ICEBERG_DEMO_TABLE_NAMESPACE}/${ICEBERG_DEMO_TABLE_NAME}/data/*.parquet 2>/dev/null | head -n 1")"
  if [[ -n "${seg}" && -n "${data}" ]]; then
    break
  fi
  sleep 3
done

if [[ -z "${seg}" ]]; then
  echo "no kafka segments found for topic ${ICEBERG_DEMO_TOPIC} under s3://${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_DEMO_NAMESPACE}" >&2
  echo "bucket contents (top level):"
  kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET} 2>/dev/null | head -n 50" || true
  echo "namespace contents:"
  kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_DEMO_NAMESPACE} 2>/dev/null | head -n 50" || true
  echo "topic path listing:"
  kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- sh -c "ls -R /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_DEMO_NAMESPACE}/${ICEBERG_DEMO_TOPIC} 2>/dev/null | head -n 200" || true
  echo "segment files found anywhere in bucket:"
  kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET}/*/*/*/segment-*.kfs 2>/dev/null | head -n 50" || true
  exit 1
fi

if [[ -z "${data}" ]]; then
  echo "no iceberg parquet data files found" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs deployment/iceberg-rest --tail=200
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs deployment/"${ICEBERG_PROCESSOR_RELEASE}" --tail=200
  exit 1
fi

if ! kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET} >/dev/null 2>&1"; then
  echo "warehouse bucket missing: ${ICEBERG_WAREHOUSE_BUCKET}" >&2
  exit 1
fi
if ! kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_WAREHOUSE_PREFIX} >/dev/null 2>&1"; then
  echo "iceberg warehouse prefix missing: s3://${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_WAREHOUSE_PREFIX}" >&2
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs deployment/iceberg-rest --tail=200
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs deployment/"${ICEBERG_PROCESSOR_RELEASE}" --tail=200
  exit 1
fi
echo "iceberg data files (count):"
kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- \
  sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_WAREHOUSE_PREFIX}/${ICEBERG_DEMO_TABLE_NAMESPACE}/${ICEBERG_DEMO_TABLE_NAME}/data/*.parquet 2>/dev/null | wc -l"
echo "kfs segments (count):"
kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- \
  sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_DEMO_NAMESPACE}/${ICEBERG_DEMO_TOPIC}/*/segment-*.kfs 2>/dev/null | wc -l"
echo "kfs segments (sample):"
kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- \
  sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_DEMO_NAMESPACE}/${ICEBERG_DEMO_TOPIC}/*/segment-*.kfs 2>/dev/null | head -n 20" || true
echo "parquet files (sample):"
kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- \
  sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_WAREHOUSE_PREFIX}/${ICEBERG_DEMO_TABLE_NAMESPACE}/${ICEBERG_DEMO_TABLE_NAME}/data/*.parquet 2>/dev/null | head -n 20" || true
echo "iceberg manifest avro files (count):"
kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- \
  sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_WAREHOUSE_PREFIX}/${ICEBERG_DEMO_TABLE_NAMESPACE}/${ICEBERG_DEMO_TABLE_NAME}/metadata/*.avro 2>/dev/null | wc -l"
echo "iceberg manifest avro files (sample):"
kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- \
  sh -c "ls /data/${ICEBERG_WAREHOUSE_BUCKET}/${ICEBERG_WAREHOUSE_PREFIX}/${ICEBERG_DEMO_TABLE_NAMESPACE}/${ICEBERG_DEMO_TABLE_NAME}/metadata/*.avro 2>/dev/null | head -n 20" || true

kubectl -n "${ICEBERG_DEMO_NAMESPACE}" delete pod iceberg-rest-curl --ignore-not-found=true >/dev/null
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" run iceberg-rest-curl --restart=Never \
  --image=curlimages/curl:8.6.0 -- \
  sh -c 'set -e; \
    CURL="curl -fsS --max-time 20 --connect-timeout 5 --retry 5 --retry-delay 3 --retry-all-errors"; \
    ns_url="http://iceberg-rest.'"${ICEBERG_DEMO_NAMESPACE}"'.svc.cluster.local:'"${ICEBERG_REST_PORT}"'/v1/namespaces"; \
    tbls_url="http://iceberg-rest.'"${ICEBERG_DEMO_NAMESPACE}"'.svc.cluster.local:'"${ICEBERG_REST_PORT}"'/v1/namespaces/'"${ICEBERG_DEMO_TABLE_NAMESPACE}"'/tables"; \
    table_url="http://iceberg-rest.'"${ICEBERG_DEMO_NAMESPACE}"'.svc.cluster.local:'"${ICEBERG_REST_PORT}"'/v1/namespaces/'"${ICEBERG_DEMO_TABLE_NAMESPACE}"'/tables/'"${ICEBERG_DEMO_TABLE_NAME}"'"; \
    for i in $(seq 1 6); do \
      if ${CURL} "${ns_url}" >/dev/null 2>&1; then break; fi; \
      echo "iceberg rest not ready yet (attempt ${i})"; \
      sleep 5; \
    done; \
    echo "iceberg catalog namespaces:"; \
    ns_json=$(${CURL} "${ns_url}" 2>/dev/null || true); \
    if [ -n "${ns_json}" ]; then \
      echo "${ns_json}" | sed -n "s/.*\\\"namespace\\\"[[:space:]]*:[[:space:]]*\\[\\\"\\([^\\\"]*\\)\\\"\\].*/\\1/p" | sort -u; \
    else \
      echo "(none)"; \
    fi; \
    echo; \
    echo "iceberg catalog tables (namespace '"${ICEBERG_DEMO_TABLE_NAMESPACE}"'):"; \
    tbls_json=$(${CURL} "${tbls_url}" 2>/dev/null || true); \
    if [ -n "${tbls_json}" ]; then \
      echo "${tbls_json}" | sed -n "s/.*\\\"name\\\"[[:space:]]*:[[:space:]]*\\\"\\([^\\\"]*\\)\\\".*/\\1/p" | sort -u; \
    else \
      echo "(none)"; \
    fi; \
    echo; \
    expected='"${ICEBERG_DEMO_RECORDS}"'; \
    table=""; \
    got="0"; \
    for i in $(seq 1 20); do \
      table=$(${CURL} "${table_url}" 2>/dev/null || true); \
      if [ -n "${table}" ]; then \
        got=$(echo "${table}" | sed -n "s/.*\\\"total-records\\\"[[:space:]]*:[[:space:]]*\\\"\\{0,1\\}\\([0-9][0-9]*\\).*/\\1/p" | head -n 1); \
        got=${got:-0}; \
        if [ "${got}" -ge "${expected}" ]; then \
          echo "iceberg catalog table (namespace '"${ICEBERG_DEMO_TABLE_NAMESPACE}"'):"; \
          break; \
        fi; \
        echo "waiting for iceberg records: ${got}/${expected}"; \
      else \
        echo "waiting for iceberg table metadata (attempt ${i})"; \
      fi; \
      sleep 6; \
    done; \
    if [ -z "${table}" ]; then \
      echo "iceberg table fetch failed"; \
      exit 1; \
    fi; \
    echo "iceberg table schema (name, type):"; \
    echo "${table}" | sed -n "s/.*\\\"name\\\":\\\"\\([^\\\"]*\\)\\\"[^\\\"]*\\\"type\\\":\\\"\\([^\\\"]*\\)\\\".*/\\1\\t\\2/p" | \
      awk '\''BEGIN{printf "%-24s %s\n","column","type"} {printf "%-24s %s\n",$1,$2}'\''; \
    echo; \
    echo "iceberg table summary:"; \
    data_files=$(echo "${table}" | sed -n "s/.*\\\"total-data-files\\\"[[:space:]]*:[[:space:]]*\\\"\\{0,1\\}\\([0-9][0-9]*\\).*/\\1/p" | head -n 1); \
    snapshot_id=$(echo "${table}" | sed -n "s/.*\\\"current-snapshot-id\\\"[[:space:]]*:[[:space:]]*\\([0-9][0-9]*\\).*/\\1/p" | head -n 1); \
    data_path=$(echo "${table}" | sed -n "s/.*\\\"write.data.path\\\"[[:space:]]*:[[:space:]]*\\\"\\([^\\\"]*\\)\\\".*/\\1/p" | head -n 1); \
    metadata_path=$(echo "${table}" | sed -n "s/.*\\\"write.metadata.path\\\"[[:space:]]*:[[:space:]]*\\\"\\([^\\\"]*\\)\\\".*/\\1/p" | head -n 1); \
    printf "data_files\\t%s\\nrecords\\t%s\\ndata_path\\t%s\\nmetadata_path\\t%s\\nsnapshot_id\\t%s\\n" \
      "${data_files:-0}" "${got:-0}" "${data_path:-}" "${metadata_path:-}" "${snapshot_id:-}"; \
    echo; \
    echo "total-records: ${got} (expected ${expected})"; \
    '

curl_deadline="$((SECONDS + 420))"
while true; do
  phase="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pod iceberg-rest-curl -o jsonpath="{.status.phase}" 2>/dev/null || true)"
  if [[ "${phase}" == "Succeeded" ]]; then
    break
  fi
  if [[ "${phase}" == "Failed" ]]; then
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs pod/iceberg-rest-curl --tail=200 || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" describe pod/iceberg-rest-curl || true
    exit 1
  fi
  if (( SECONDS >= curl_deadline )); then
    echo "timed out waiting for iceberg-rest-curl pod" >&2
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pod iceberg-rest-curl -o yaml || true
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs pod/iceberg-rest-curl --tail=200 || true
    echo "iceberg-processor logs (tail):" >&2
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs deployment/"${ICEBERG_PROCESSOR_RELEASE}" --tail=200 || true
    echo "iceberg-rest logs (tail):" >&2
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs deployment/iceberg-rest --tail=200 || true
    echo "broker logs (tail):" >&2
    broker_pod="$(kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get pods -l app.kubernetes.io/component=broker -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || true)"
    if [[ -n "${broker_pod}" ]]; then
      kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs "pod/${broker_pod}" --tail=200 || true
    fi
    echo "etcd endpoints:" >&2
    kubectl -n "${ICEBERG_DEMO_NAMESPACE}" get endpoints "${ICEBERG_DEMO_CLUSTER}-etcd-client" -o yaml || true
    exit 1
  fi
  sleep 5
done
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" logs pod/iceberg-rest-curl --tail=200 || true
kubectl -n "${ICEBERG_DEMO_NAMESPACE}" delete pod iceberg-rest-curl --ignore-not-found=true >/dev/null

if [[ "${ICEBERG_DEMO_CLEANUP:-1}" == "1" ]]; then
  echo "cleaning up iceberg demo cluster (${ICEBERG_DEMO_CLUSTER}) in ${ICEBERG_DEMO_NAMESPACE}"
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" delete kafscaletopic "${ICEBERG_DEMO_TOPIC}" --ignore-not-found=true || true
  kubectl -n "${ICEBERG_DEMO_NAMESPACE}" delete kafscalecluster "${ICEBERG_DEMO_CLUSTER}" --ignore-not-found=true || true
  cleanup_kubeconfigs
fi
