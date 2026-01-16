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

KAFSQL_DEMO_CLUSTER="${KAFSQL_DEMO_CLUSTER:-kafsql}"
KAFSQL_BROKER_SERVICE="${KAFSQL_DEMO_CLUSTER}-broker"
KAFSQL_DEMO_TIMEOUT_SEC="${KAFSQL_DEMO_TIMEOUT_SEC:-120}"
TMP_ROOT="${TMPDIR:-/tmp}"

cleanup_kubeconfigs() {
  find "${TMP_ROOT}" -maxdepth 1 -type f -name 'kafscale-kind-kubeconfig.*' -delete 2>/dev/null || true
}

wait_for_dns() {
  local name="$1"
  local attempts="${2:-20}"
  local sleep_sec="${3:-3}"
  for _ in $(seq 1 "$attempts"); do
    if kubectl -n "${KAFSQL_DEMO_NAMESPACE}" run "kafsql-dns-check-$$" \
      --restart=Never --rm -i --image=busybox:1.36 \
      --command -- nslookup "${name}" >/dev/null 2>&1; then
      return 0
    fi
    sleep "${sleep_sec}"
  done
  echo "dns lookup failed for ${name}" >&2
  return 1
}

get_etcd_statefulset_pods() {
  local sts_name="$1"
  local selector="$2"
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" get pods -l "${selector}" \
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
    kubectl -n "${KAFSQL_DEMO_NAMESPACE}" get pods -l "${selector}" -o wide || true
    return 1
  fi
  for p in ${pods}; do
    if ! kubectl -n "${KAFSQL_DEMO_NAMESPACE}" wait --for=condition=Ready "pod/${p}" --timeout="${timeout}"; then
      echo "etcd pod not Ready: ${p}" >&2
      kubectl -n "${KAFSQL_DEMO_NAMESPACE}" describe "pod/${p}" || true
      return 1
    fi
  done
}

wait_for_etcd_leader() {
  local attempts="${KAFSQL_DEMO_ETCD_WAIT_ATTEMPTS:-80}"
  local sleep_sec="${KAFSQL_DEMO_ETCD_WAIT_SLEEP_SEC:-3}"
  local replicas
  replicas="$(kubectl -n "${KAFSQL_DEMO_NAMESPACE}" get statefulset "${KAFSQL_DEMO_CLUSTER}-etcd" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)"
  if [[ -z "${replicas}" || "${replicas}" == "0" ]]; then
    echo "etcd statefulset not found or replicas=0" >&2
    return 1
  fi

  for i in $(seq 0 $((replicas - 1))); do
    wait_for_dns "${KAFSQL_DEMO_CLUSTER}-etcd-${i}.${KAFSQL_DEMO_CLUSTER}-etcd.${KAFSQL_DEMO_NAMESPACE}.svc.cluster.local"
  done

  local endpoints=()
  for i in $(seq 0 $((replicas - 1))); do
    endpoints+=("http://${KAFSQL_DEMO_CLUSTER}-etcd-${i}.${KAFSQL_DEMO_CLUSTER}-etcd.${KAFSQL_DEMO_NAMESPACE}.svc.cluster.local:2379")
  done
  local service_endpoint="http://${KAFSQL_DEMO_CLUSTER}-etcd-client.${KAFSQL_DEMO_NAMESPACE}.svc.cluster.local:2379"

  for _ in $(seq 1 "${attempts}"); do
    if kubectl -n "${KAFSQL_DEMO_NAMESPACE}" run "kafsql-etcd-health-$$-${RANDOM}" \
      --restart=Never --rm -i --image=kubesphere/etcd:3.6.4-0 \
      --command -- /bin/sh -c "out=\$(etcdctl --endpoints=${service_endpoint} endpoint status -w json 2>/dev/null || true); case \"\$out\" in *'\"Leader\":true'*|*'\"leader\":true'* ) exit 0 ;; *'\"leader\":0'*|*'\"Leader\":0'* ) exit 1 ;; *'\"leader\":'*|*'\"Leader\":'* ) exit 0 ;; * ) exit 1 ;; esac" >/dev/null 2>&1; then
      return 0
    fi
    for endpoint in "${endpoints[@]}"; do
      if kubectl -n "${KAFSQL_DEMO_NAMESPACE}" run "kafsql-etcd-health-$$-${RANDOM}" \
        --restart=Never --rm -i --image=kubesphere/etcd:3.6.4-0 \
        --command -- /bin/sh -c "out=\$(etcdctl --endpoints=${endpoint} endpoint status -w json 2>/dev/null || true); case \"\$out\" in *'\"Leader\":true'*|*'\"leader\":true'* ) exit 0 ;; *'\"leader\":0'*|*'\"Leader\":0'* ) exit 1 ;; *'\"leader\":'*|*'\"Leader\":'* ) exit 0 ;; * ) exit 1 ;; esac" >/dev/null 2>&1; then
        return 0
      fi
    done
    sleep "${sleep_sec}"
  done
  echo "etcd leader not ready for endpoints: ${endpoints[*]}" >&2
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" run "kafsql-etcd-health-$$-${RANDOM}" \
    --restart=Never --rm -i --image=kubesphere/etcd:3.6.4-0 \
    --command -- /bin/sh -c "etcdctl --endpoints=${endpoints[0]} endpoint status --cluster -w table || true" >&2 || true
  return 1
}

required_vars=(
  KUBECONFIG
  KAFSCALE_DEMO_NAMESPACE
  KAFSCALE_KIND_CLUSTER
  KAFSQL_DEMO_NAMESPACE
  KAFSQL_DEMO_CLUSTER
  KAFSQL_DEMO_TOPIC
  KAFSQL_DEMO_RECORDS
  KAFSQL_PROCESSOR_RELEASE
  SQL_PROCESSOR_IMAGE
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

operator_deploy="$(kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" get deployments -l app.kubernetes.io/component=operator -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || true)"
if [[ -z "${operator_deploy}" ]]; then
  echo "operator deployment not found in ${KAFSCALE_DEMO_NAMESPACE}" >&2
  kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" get deployments || true
  exit 1
fi

kind load docker-image "${SQL_PROCESSOR_IMAGE}" --name "${KAFSCALE_KIND_CLUSTER}"
kind load docker-image "${E2E_CLIENT_IMAGE}" --name "${KAFSCALE_KIND_CLUSTER}"

kubectl create namespace "${KAFSQL_DEMO_NAMESPACE}" --dry-run=client -o yaml | \
  kubectl apply --validate=false -f -

kubectl -n "${KAFSQL_DEMO_NAMESPACE}" create secret generic kafsql-s3-credentials \
  --from-literal=AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}" \
  --from-literal=KAFSCALE_S3_ACCESS_KEY="${MINIO_ROOT_USER}" \
  --from-literal=KAFSCALE_S3_SECRET_KEY="${MINIO_ROOT_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply --validate=false -f -

cluster_yaml="$(printf '%s\n' \
  'apiVersion: kafscale.io/v1alpha1' \
  'kind: KafscaleCluster' \
  'metadata:' \
  "  name: ${KAFSQL_DEMO_CLUSTER}" \
  "  namespace: ${KAFSQL_DEMO_NAMESPACE}" \
  'spec:' \
  '  brokers:' \
  "    advertisedHost: ${KAFSQL_BROKER_SERVICE}.${KAFSQL_DEMO_NAMESPACE}.svc.cluster.local" \
  '    advertisedPort: 9092' \
  '    replicas: 1' \
  '  s3:' \
  "    bucket: ${MINIO_BUCKET}" \
  "    region: ${MINIO_REGION}" \
  "    endpoint: http://minio.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local:9000" \
  '    credentialsSecretRef: kafsql-s3-credentials' \
  '  etcd:' \
  '    endpoints: []' \
)"
kubectl apply --validate=false -f - <<<"${cluster_yaml}"

topic_yaml="$(printf '%s\n' \
  'apiVersion: kafscale.io/v1alpha1' \
  'kind: KafscaleTopic' \
  'metadata:' \
  "  name: ${KAFSQL_DEMO_TOPIC}" \
  "  namespace: ${KAFSQL_DEMO_NAMESPACE}" \
  'spec:' \
  "  clusterRef: ${KAFSQL_DEMO_CLUSTER}" \
  '  partitions: 1' \
)"
kubectl apply --validate=false -f - <<<"${topic_yaml}"

echo "waiting for kafscalecluster/${KAFSQL_DEMO_CLUSTER} to be Ready..."
if ! kubectl -n "${KAFSQL_DEMO_NAMESPACE}" wait --for=condition=Ready \
  "kafscalecluster/${KAFSQL_DEMO_CLUSTER}" --timeout=180s; then
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" get "kafscalecluster/${KAFSQL_DEMO_CLUSTER}" -o yaml || true
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" describe "kafscalecluster/${KAFSQL_DEMO_CLUSTER}" || true
  exit 1
fi

echo "waiting for kafscaletopic/${KAFSQL_DEMO_TOPIC} to be Ready..."
if ! kubectl -n "${KAFSQL_DEMO_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Ready \
  "kafscaletopic/${KAFSQL_DEMO_TOPIC}" --timeout=180s; then
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" get "kafscaletopic/${KAFSQL_DEMO_TOPIC}" -o yaml || true
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" describe "kafscaletopic/${KAFSQL_DEMO_TOPIC}" || true
  exit 1
fi

echo "waiting for etcd statefulset rollout..."
if ! kubectl -n "${KAFSQL_DEMO_NAMESPACE}" rollout status "statefulset/${KAFSQL_DEMO_CLUSTER}-etcd" --timeout=180s; then
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" describe "statefulset/${KAFSQL_DEMO_CLUSTER}-etcd" || true
  exit 1
fi
if ! wait_for_etcd_pods_ready "${KAFSQL_DEMO_CLUSTER}-etcd" \
  "app=kafscale-etcd,cluster=${KAFSQL_DEMO_CLUSTER}" "180s"; then
  exit 1
fi
kubectl -n "${KAFSQL_DEMO_NAMESPACE}" get svc "${KAFSQL_DEMO_CLUSTER}-etcd-client" >/dev/null
echo "waiting for etcd leader..."
if ! wait_for_etcd_leader; then
  exit 1
fi

echo "ensuring bucket ${MINIO_BUCKET} exists in MinIO..."
if [[ "${KAFSQL_DEMO_CREATE_BUCKET:-1}" == "1" ]]; then
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" delete pod kafsql-minio-bucket --ignore-not-found=true >/dev/null
  bucket_cmd=$(cat <<EOF
set -e
export AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}"
export AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}"
export AWS_DEFAULT_REGION="${MINIO_REGION}"
export AWS_EC2_METADATA_DISABLED=true
endpoint="http://minio.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local:9000"
aws s3api --endpoint-url "\${endpoint}" create-bucket --bucket "${MINIO_BUCKET}" >/dev/null 2>&1 || true
aws s3api --endpoint-url "\${endpoint}" head-bucket --bucket "${MINIO_BUCKET}" >/dev/null
EOF
)
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" run kafsql-minio-bucket --restart=Never \
    --image=amazon/aws-cli:2.15.0 --command -- /bin/sh -c "${bucket_cmd}"
  if ! kubectl -n "${KAFSQL_DEMO_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/kafsql-minio-bucket --timeout=90s; then
    kubectl -n "${KAFSQL_DEMO_NAMESPACE}" logs pod/kafsql-minio-bucket --tail=200 || true
    kubectl -n "${KAFSQL_DEMO_NAMESPACE}" describe pod/kafsql-minio-bucket || true
    exit 1
  fi
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" delete pod kafsql-minio-bucket --ignore-not-found=true >/dev/null
fi

kubectl -n "${KAFSQL_DEMO_NAMESPACE}" delete job kafsql-demo-producer --ignore-not-found=true
producer_yaml="$(printf '%s\n' \
  'apiVersion: batch/v1' \
  'kind: Job' \
  'metadata:' \
  '  name: kafsql-demo-producer' \
  "  namespace: ${KAFSQL_DEMO_NAMESPACE}" \
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
  "              value: ${KAFSQL_BROKER_SERVICE}.${KAFSQL_DEMO_NAMESPACE}.svc.cluster.local:9092" \
  '            - name: KAFSCALE_E2E_BROKER_ADDR' \
  "              value: ${KAFSQL_BROKER_SERVICE}.${KAFSQL_DEMO_NAMESPACE}.svc.cluster.local:9092" \
  '            - name: KAFSCALE_E2E_TOPIC' \
  "              value: ${KAFSQL_DEMO_TOPIC}" \
  '            - name: KAFSCALE_E2E_COUNT' \
  "              value: \"${KAFSQL_DEMO_RECORDS}\"" \
  '            - name: KAFSCALE_E2E_TIMEOUT_SEC' \
  "              value: \"${KAFSQL_DEMO_TIMEOUT_SEC}\"" \
  '            - name: KAFSCALE_E2E_PROBE_RETRIES' \
  '              value: "20"' \
  '            - name: KAFSCALE_E2E_PROBE_SLEEP_MS' \
  '              value: "500"' \
)"
kubectl apply --validate=false -f - <<<"${producer_yaml}"

echo "waiting for kafsql-demo-producer job to complete..."
job_wait_timeout="${KAFSQL_DEMO_JOB_WAIT_TIMEOUT:-180s}"
set +e
kubectl -n "${KAFSQL_DEMO_NAMESPACE}" wait \
  --for=jsonpath='{.status.phase}'=Succeeded \
  pod -l job-name=kafsql-demo-producer --timeout="${job_wait_timeout}"
wait_status=$?
set -e
if [[ "${wait_status}" -ne 0 ]]; then
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" logs job/kafsql-demo-producer --all-containers=true --tail=200 || true
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" describe job/kafsql-demo-producer || true
  exit 1
fi
echo "produced ${KAFSQL_DEMO_RECORDS} messages to ${KAFSQL_DEMO_TOPIC}"

echo "waiting for KFS segments in S3..."
segment_count=""
for i in $(seq 1 30); do
  segment_count="$(kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- sh -c "ls /data/${MINIO_BUCKET}/${KAFSQL_DEMO_NAMESPACE}/${KAFSQL_DEMO_TOPIC}/*/segment-*.kfs 2>/dev/null | wc -l" || true)"
  segment_count="$(echo "${segment_count}" | tr -d '[:space:]')"
  if [[ -n "${segment_count}" && "${segment_count}" != "0" ]]; then
    break
  fi
  sleep 3
done
if [[ -z "${segment_count}" || "${segment_count}" == "0" ]]; then
  echo "no segments found for ${KAFSQL_DEMO_TOPIC} under s3://${MINIO_BUCKET}/${KAFSQL_DEMO_NAMESPACE}" >&2
  kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" exec deploy/minio -- sh -c "ls -R /data/${MINIO_BUCKET}/${KAFSQL_DEMO_NAMESPACE} 2>/dev/null | head -n 200" || true
  exit 1
fi
echo "S3 segments: ${segment_count}"

proc_repo="${SQL_PROCESSOR_IMAGE%:*}"
proc_tag="${SQL_PROCESSOR_IMAGE##*:}"
if [[ -z "${proc_repo}" || -z "${proc_tag}" ]]; then
  echo "invalid SQL_PROCESSOR_IMAGE: ${SQL_PROCESSOR_IMAGE}" >&2
  exit 1
fi

etcd_endpoint="http://${KAFSQL_DEMO_CLUSTER}-etcd-client.${KAFSQL_DEMO_NAMESPACE}.svc.cluster.local:2379"
helm upgrade --install "${KAFSQL_PROCESSOR_RELEASE}" addons/processors/sql-processor/deploy/helm/sql-processor \
  --namespace "${KAFSQL_DEMO_NAMESPACE}" \
  --set fullnameOverride="${KAFSQL_PROCESSOR_RELEASE}" \
  --set image.repository="${proc_repo}" \
  --set image.tag="${proc_tag}" \
  --set envFromSecret=kafsql-s3-credentials \
  --set env[0].name=KAFSQL_S3_BUCKET \
  --set env[0].value="${MINIO_BUCKET}" \
  --set env[1].name=KAFSQL_S3_NAMESPACE \
  --set env[1].value="${KAFSQL_DEMO_NAMESPACE}" \
  --set env[2].name=KAFSQL_S3_REGION \
  --set env[2].value="${MINIO_REGION}" \
  --set env[3].name=KAFSQL_S3_ENDPOINT \
  --set env[3].value="http://minio.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local:9000" \
  --set env[4].name=KAFSQL_S3_PATH_STYLE \
  --set-string env[4].value="true" \
  --set env[5].name=KAFSQL_METADATA_DISCOVERY \
  --set env[5].value="etcd" \
  --set env[6].name=KAFSQL_METADATA_ETCD_ENDPOINTS \
  --set env[6].value="${etcd_endpoint}"

deploy="$(kubectl -n "${KAFSQL_DEMO_NAMESPACE}" get deployments -l app.kubernetes.io/name=sql-processor,app.kubernetes.io/instance="${KAFSQL_PROCESSOR_RELEASE}" -o name | head -n 1)"
if [[ -z "${deploy}" ]]; then
  echo "sql-processor deployment not found for release ${KAFSQL_PROCESSOR_RELEASE}" >&2
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" get deployments
  exit 1
fi
if ! kubectl -n "${KAFSQL_DEMO_NAMESPACE}" rollout status "${deploy}" --timeout=180s; then
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" describe "${deploy}" || true
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" logs "${deploy}" --tail=200 || true
  exit 1
fi

service_yaml="$(printf '%s\n' \
  'apiVersion: v1' \
  'kind: Service' \
  'metadata:' \
  "  name: ${KAFSQL_PROCESSOR_RELEASE}" \
  "  namespace: ${KAFSQL_DEMO_NAMESPACE}" \
  'spec:' \
  '  type: ClusterIP' \
  '  selector:' \
  '    app.kubernetes.io/name: sql-processor' \
  "    app.kubernetes.io/instance: ${KAFSQL_PROCESSOR_RELEASE}" \
  '  ports:' \
  '    - name: sql' \
  '      port: 5432' \
  '      targetPort: 5432' \
)"
kubectl apply --validate=false -f - <<<"${service_yaml}"

kubectl -n "${KAFSQL_DEMO_NAMESPACE}" delete pod kafsql-demo-query --ignore-not-found=true >/dev/null
query_cmd=$(cat <<EOF
set -e
host="${KAFSQL_PROCESSOR_RELEASE}.${KAFSQL_DEMO_NAMESPACE}.svc.cluster.local"
echo "KAFSQL SQL checks (host=\${host})"
echo "psql \\dt"
psql -X "host=\${host} port=5432 user=kafsql dbname=kfs sslmode=disable" -c "\\dt"
echo "SHOW TOPICS"
psql -X "host=\${host} port=5432 user=kafsql dbname=kfs sslmode=disable" -c "SHOW TOPICS;"
echo "SHOW PARTITIONS FROM ${KAFSQL_DEMO_TOPIC}"
psql -X "host=\${host} port=5432 user=kafsql dbname=kfs sslmode=disable" -c "SHOW PARTITIONS FROM ${KAFSQL_DEMO_TOPIC};"
echo "DESCRIBE ${KAFSQL_DEMO_TOPIC}"
psql -X "host=\${host} port=5432 user=kafsql dbname=kfs sslmode=disable" -c "DESCRIBE ${KAFSQL_DEMO_TOPIC};"
echo "information_schema.tables"
psql -X "host=\${host} port=5432 user=kafsql dbname=kfs sslmode=disable" -c "SELECT * FROM information_schema.tables;"
echo "information_schema.columns"
psql -X "host=\${host} port=5432 user=kafsql dbname=kfs sslmode=disable" -c "SELECT * FROM information_schema.columns;"
echo "TAIL 5"
psql -X "host=\${host} port=5432 user=kafsql dbname=kfs sslmode=disable" -c "SELECT _offset, _ts, _value FROM ${KAFSQL_DEMO_TOPIC} TAIL 5;"
echo "LAST 1h LIMIT 5"
psql -X "host=\${host} port=5432 user=kafsql dbname=kfs sslmode=disable" -c "SELECT _offset, _ts FROM ${KAFSQL_DEMO_TOPIC} LAST 1h LIMIT 5;"
echo "OFFSET FILTER"
psql -X "host=\${host} port=5432 user=kafsql dbname=kfs sslmode=disable" -c "SELECT count(*) FROM ${KAFSQL_DEMO_TOPIC} WHERE _offset >= 0 AND _offset <= 9 LAST 1h;"
count=\$(psql -tA "host=\${host} port=5432 user=kafsql dbname=kfs sslmode=disable" -c "SELECT count(*) FROM ${KAFSQL_DEMO_TOPIC} LAST 1h;" | tr -d '[:space:]')
if [ "\${count}" != "${KAFSQL_DEMO_RECORDS}" ]; then
  echo "unexpected count \${count}, expected ${KAFSQL_DEMO_RECORDS}" >&2
  exit 1
fi
max_offset=\$(psql -tA "host=\${host} port=5432 user=kafsql dbname=kfs sslmode=disable" -c "SELECT max(_offset) FROM ${KAFSQL_DEMO_TOPIC} LAST 1h;" | tr -d '[:space:]')
expected_offset=$((KAFSQL_DEMO_RECORDS - 1))
if [ "\${max_offset}" != "\${expected_offset}" ]; then
  echo "unexpected max offset \${max_offset}, expected \${expected_offset}" >&2
  exit 1
fi
topics=\$(psql -tA "host=\${host} port=5432 user=kafsql dbname=kfs sslmode=disable" -c "SHOW TOPICS;" | tr -d '\r')
if ! echo "\${topics}" | grep -q "${KAFSQL_DEMO_TOPIC}"; then
  echo "topic ${KAFSQL_DEMO_TOPIC} not listed in SHOW TOPICS" >&2
  exit 1
fi
echo "KAFSQL E2E checks passed."
EOF
)
kubectl -n "${KAFSQL_DEMO_NAMESPACE}" run kafsql-demo-query --restart=Never \
  --image=postgres:15 --command -- /bin/sh -c "${query_cmd}"
if ! kubectl -n "${KAFSQL_DEMO_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Succeeded pod/kafsql-demo-query --timeout=120s; then
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" logs pod/kafsql-demo-query --tail=200 || true
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" describe pod/kafsql-demo-query || true
  exit 1
fi
kubectl -n "${KAFSQL_DEMO_NAMESPACE}" logs pod/kafsql-demo-query --tail=200 || true
kubectl -n "${KAFSQL_DEMO_NAMESPACE}" delete pod kafsql-demo-query --ignore-not-found=true >/dev/null

if [[ "${KAFSQL_DEMO_CLEANUP:-1}" == "1" ]]; then
  echo "cleaning up KAFSQL demo resources (${KAFSQL_DEMO_NAMESPACE})"
  helm -n "${KAFSQL_DEMO_NAMESPACE}" uninstall "${KAFSQL_PROCESSOR_RELEASE}" >/dev/null 2>&1 || true
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" delete service "${KAFSQL_PROCESSOR_RELEASE}" --ignore-not-found=true || true
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" delete job kafsql-demo-producer --ignore-not-found=true || true
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" delete kafscaletopic "${KAFSQL_DEMO_TOPIC}" --ignore-not-found=true || true
  kubectl -n "${KAFSQL_DEMO_NAMESPACE}" delete kafscalecluster "${KAFSQL_DEMO_CLUSTER}" --ignore-not-found=true || true
  cleanup_kubeconfigs
fi
