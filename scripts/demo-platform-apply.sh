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

KAFSCALE_DEMO_NAMESPACE="${KAFSCALE_DEMO_NAMESPACE:-kafscale-demo}"
MINIO_IMAGE="${MINIO_IMAGE:-quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_REGION="${MINIO_REGION:-us-east-1}"
MODE="${1:-}"

if [[ "$MODE" == "minio" ]]; then
	cat <<EOF | kubectl apply -f -
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
	cat <<EOF | kubectl apply -f -
apiVersion: kafscale.io/v1alpha1
kind: KafscaleCluster
metadata:
  name: kafscale
  namespace: ${KAFSCALE_DEMO_NAMESPACE}
spec:
  brokers:
    advertisedHost: 127.0.0.1
    advertisedPort: 39092
    replicas: 1
  s3:
    bucket: kafscale-snapshots
    region: ${MINIO_REGION}
    endpoint: http://minio.${KAFSCALE_DEMO_NAMESPACE}.svc.cluster.local:9000
    credentialsSecretRef: kafscale-s3-credentials
  etcd:
    endpoints: []
EOF

	cat <<EOF | kubectl apply -f -
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
else
	echo "usage: $0 [minio|cluster]" >&2
	exit 1
fi
