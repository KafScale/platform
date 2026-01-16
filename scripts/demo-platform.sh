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

KAFSCALE_DEMO_NAMESPACE="${KAFSCALE_DEMO_NAMESPACE:-kafscale-demo}"
MINIO_IMAGE="${MINIO_IMAGE:-quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_REGION="${MINIO_REGION:-us-east-1}"
MODE="${1:-}"
KAFSCALE_DEMO_BROKER_REPLICAS="${KAFSCALE_DEMO_BROKER_REPLICAS:-2}"
KAFSCALE_DEMO_PROXY="${KAFSCALE_DEMO_PROXY:-0}"
KAFSCALE_METALLB_RANGE="${KAFSCALE_METALLB_RANGE:-}"

wait_for_dns() {
	local name="$1"
	local attempts="${2:-20}"
	local sleep_sec="${3:-3}"
	for i in $(seq 1 "$attempts"); do
		if kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" run "kafscale-dns-check-$$" \
			--restart=Never --rm -i --image=busybox:1.36 \
			--command -- nslookup "${name}" >/dev/null 2>&1; then
			return 0
		fi
		sleep "${sleep_sec}"
	done
	echo "dns lookup failed for ${name}" >&2
	return 1
}

wait_for_proxy_ready() {
	local url="http://kafscale-proxy.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local:9094/readyz"
	for i in $(seq 1 20); do
		if kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" run "kafscale-proxy-ready-$$" \
			--restart=Never --rm -i --image=busybox:1.36 \
			--command -- wget -qO- "${url}" >/dev/null 2>&1; then
			return 0
		fi
		sleep 3
	done
	echo "proxy readiness endpoint not ready at ${url}" >&2
	return 1
}

wait_for_etcd_leader() {
	local attempts="${KAFSCALE_DEMO_ETCD_WAIT_ATTEMPTS:-80}"
	local sleep_sec="${KAFSCALE_DEMO_ETCD_WAIT_SLEEP_SEC:-3}"
	local replicas
	replicas="$(kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" get statefulset kafscale-etcd -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0)"
	if [[ -z "${replicas}" || "${replicas}" == "0" ]]; then
		echo "etcd statefulset not found or replicas=0" >&2
		return 1
	fi

	for i in $(seq 0 $((replicas - 1))); do
		wait_for_dns "kafscale-etcd-${i}.kafscale-etcd.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local"
	done

	local endpoints=()
	for i in $(seq 0 $((replicas - 1))); do
		endpoints+=("http://kafscale-etcd-${i}.kafscale-etcd.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local:2379")
	done
	local service_endpoint="http://kafscale-etcd-client.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local:2379"

	for i in $(seq 1 "${attempts}"); do
		if kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" run "kafscale-etcd-health-$$" \
			--restart=Never --rm -i --image=kubesphere/etcd:3.6.4-0 \
			--command -- /bin/sh -c "out=\$(etcdctl --endpoints=${service_endpoint} endpoint status -w json 2>/dev/null || true); case \"\$out\" in *'\"Leader\":true'*|*'\"leader\":true'* ) exit 0 ;; *'\"leader\":0'*|*'\"Leader\":0'* ) exit 1 ;; *'\"leader\":'*|*'\"Leader\":'* ) exit 0 ;; * ) exit 1 ;; esac" >/dev/null 2>&1; then
			return 0
		fi
		for endpoint in "${endpoints[@]}"; do
			if kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" run "kafscale-etcd-health-$$" \
				--restart=Never --rm -i --image=kubesphere/etcd:3.6.4-0 \
				--command -- /bin/sh -c "out=\$(etcdctl --endpoints=${endpoint} endpoint status -w json 2>/dev/null || true); case \"\$out\" in *'\"Leader\":true'*|*'\"leader\":true'* ) exit 0 ;; *'\"leader\":0'*|*'\"Leader\":0'* ) exit 1 ;; *'\"leader\":'*|*'\"Leader\":'* ) exit 0 ;; * ) exit 1 ;; esac" >/dev/null 2>&1; then
				return 0
			fi
		done
		sleep "${sleep_sec}"
	done
	echo "etcd leader not ready for endpoints: ${endpoints[*]}" >&2
	kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" run "kafscale-etcd-health-$$" \
		--restart=Never --rm -i --image=kubesphere/etcd:3.6.4-0 \
		--command -- /bin/sh -c "etcdctl --endpoints=${endpoints[0]} endpoint status --cluster -w table || true" >&2 || true
	return 1
}

wait_for_etcd_pods_ready() {
	local sts_name="$1"
	local selector="$2"
	local timeout="${3:-180s}"
	local pods
	pods="$(kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" get pods -l "${selector}" \
		-o go-template="{{range .items}}{{\$pod := .}}{{range .metadata.ownerReferences}}{{if and (eq .kind \"StatefulSet\") (eq .name \"${sts_name}\")}}{{\$pod.metadata.name}} {{end}}{{end}}{{end}}")"
	if [[ -z "${pods}" ]]; then
		echo "no etcd statefulset pods found for ${sts_name}" >&2
		kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" get pods -l "${selector}" -o wide || true
		return 1
	fi
	for p in ${pods}; do
		if ! kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" wait --for=condition=Ready "pod/${p}" --timeout="${timeout}"; then
			echo "etcd pod not Ready: ${p}" >&2
			kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" describe "pod/${p}" || true
			return 1
		fi
	done
}

if [[ "$MODE" == "minio" ]]; then
	cat <<EOF | kubectl apply --validate=false -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: ${KAFSCALE_DEMO_NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
        - name: minio
          image: ${MINIO_IMAGE}
          args: ["server", "/data", "--console-address", ":9001"]
          env:
            - name: MINIO_ROOT_USER
              value: ${MINIO_ROOT_USER}
            - name: MINIO_ROOT_PASSWORD
              value: ${MINIO_ROOT_PASSWORD}
          ports:
            - containerPort: 9000
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: ${KAFSCALE_DEMO_NAMESPACE}
spec:
  selector:
    app: minio
  ports:
    - name: api
      port: 9000
      targetPort: 9000
EOF

elif [[ "$MODE" == "cluster" ]]; then
	advertised_block=""
	if [[ -n "${KAFSCALE_DEMO_ADVERTISED_HOST:-}" ]]; then
		advertised_port="${KAFSCALE_DEMO_ADVERTISED_PORT:-9092}"
		advertised_block=$'    advertisedHost: '"${KAFSCALE_DEMO_ADVERTISED_HOST}"$'\n    advertisedPort: '"${advertised_port}"
	elif [[ "$KAFSCALE_DEMO_PROXY" == "1" ]]; then
		advertised_block=$'    advertisedPort: 9092'
	else
		advertised_block=$'    advertisedHost: 127.0.0.1\n    advertisedPort: 39092'
	fi
	cat <<EOF | kubectl apply --validate=false -f -
apiVersion: kafscale.io/v1alpha1
kind: KafscaleCluster
metadata:
  name: kafscale
  namespace: ${KAFSCALE_DEMO_NAMESPACE}
spec:
  brokers:
${advertised_block}
    replicas: ${KAFSCALE_DEMO_BROKER_REPLICAS}
  s3:
    bucket: kafscale-snapshots
    region: ${MINIO_REGION}
    endpoint: http://minio.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local:9000
    credentialsSecretRef: kafscale-s3-credentials
  etcd:
    endpoints: []
EOF

	cat <<EOF | kubectl apply --validate=false -f -
apiVersion: kafscale.io/v1alpha1
kind: KafscaleTopic
metadata:
  name: demo-topic-1
  namespace: ${KAFSCALE_DEMO_NAMESPACE}
spec:
  clusterRef: kafscale
  partitions: 3
---
apiVersion: kafscale.io/v1alpha1
kind: KafscaleTopic
metadata:
  name: demo-topic-2
  namespace: ${KAFSCALE_DEMO_NAMESPACE}
spec:
  clusterRef: kafscale
  partitions: 2
EOF
elif [[ "$MODE" == "wait" ]]; then
	kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" wait --for=condition=Ready kafscalecluster/kafscale --timeout=180s
	wait_for_etcd_pods_ready "kafscale-etcd" "app=kafscale-etcd,cluster=kafscale" "180s"
	kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" get svc kafscale-etcd-client >/dev/null
	wait_for_dns "kafscale-etcd-client.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local"
	wait_for_etcd_leader
	if [[ "${KAFSCALE_DEMO_PROXY}" == "1" ]]; then
		kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" rollout status deployment/kafscale-proxy --timeout=180s
		kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" wait --for=condition=Ready pod -l app=kafscale-proxy --timeout=180s
		wait_for_proxy_ready
	fi
	kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" get svc kafscale-broker >/dev/null
	selector="$(kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" get svc kafscale-broker \
		-o go-template='{{range $k,$v := .spec.selector}}{{printf "%s=%s," $k $v}}{{end}}')"
	selector="${selector%,}"
	if [[ -z "$selector" ]]; then
		echo "broker service has no selector labels" >&2
		exit 1
	fi
	kubectl -n "${KAFSCALE_DEMO_NAMESPACE}" wait --for=condition=Ready pod -l "${selector}" --timeout=180s
elif [[ "$MODE" == "metallb" ]]; then
	kubectl apply --validate=false -f deploy/kind/metallb-native.yaml
	kubectl -n metallb-system rollout status deployment/controller --timeout=180s
	kubectl -n metallb-system rollout status daemonset/speaker --timeout=180s
	if [[ -z "$KAFSCALE_METALLB_RANGE" ]]; then
		subnet="$(docker network inspect kind --format '{{(index .IPAM.Config 0).Subnet}}' 2>/dev/null || true)"
		base="${subnet%%/*}"
		mask="${subnet##*/}"
		if [[ -n "$base" ]]; then
			IFS='.' read -r o1 o2 o3 o4 <<<"$base"
			if [[ "${mask:-24}" -le 16 ]]; then
				KAFSCALE_METALLB_RANGE="${o1}.${o2}.255.200-${o1}.${o2}.255.250"
			else
				KAFSCALE_METALLB_RANGE="${o1}.${o2}.${o3}.200-${o1}.${o2}.${o3}.250"
			fi
		else
			KAFSCALE_METALLB_RANGE="172.18.255.200-172.18.255.250"
		fi
	fi
	cat <<EOF | kubectl apply --validate=false -f -
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: kind-pool
  namespace: metallb-system
spec:
  addresses:
    - ${KAFSCALE_METALLB_RANGE}
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: kind-l2
  namespace: metallb-system
spec:
  ipAddressPools:
    - kind-pool
EOF
else
	echo "usage: $0 [minio|cluster|wait|metallb]" >&2
	exit 1
fi
