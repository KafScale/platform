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
source "$(dirname "${BASH_SOURCE[0]}")/rc.sh"
sudo_cmd="${KSV_SUDO:-}"
"$KSV_PREP/secrets.sh" >/dev/null
mkdir -p "$KSV_DIR/demo"
for f in "$KSV_PREP"/demo/*; do
  sed '1,/^$/d' "$f" > "$KSV_DIR/demo/$(basename "$f")"
done
printf 'kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnetworking:\n  kubeProxyMode: nftables\n' > "$KSV_DIR/kind.yaml"
$sudo_cmd kind create cluster --name "$KSV_CLUSTER" --config "$KSV_DIR/kind.yaml"
$sudo_cmd kind get kubeconfig --name "$KSV_CLUSTER" > "$KUBECONFIG"
images=(
  ghcr.io/kafscale/kafscale-operator:dev
  ghcr.io/kafscale/kafscale-broker:dev
  ghcr.io/kafscale/kafscale-proxy:dev
  ghcr.io/kafscale/kafscale-console:dev
  ghcr.io/kafscale/kafscale-mcp:dev
  ghcr.io/kafscale/kafscale-schema-registry:dev
  ghcr.io/kafscale/kafscale-etcd-tools:dev
  kafscale-minio:local
  amazon/aws-cli:2.15.0
  edenhill/kcat:1.7.1
)
for img in "${images[@]}"; do
  $sudo_cmd docker image inspect "$img" >/dev/null 2>&1 || $sudo_cmd docker pull "$img"
  (cd "$KSV_REPO" && $sudo_cmd scripts/kind-load-image.sh "$img" "$KSV_CLUSTER" >/dev/null)
  echo "loaded $img"
done
kubectl create namespace kafscale
kubectl -n kafscale create secret generic minio-root \
  --from-file=user="$KSV_SECRETS/minio-root-user" \
  --from-file=password="$KSV_SECRETS/minio-root-pass"
kubectl -n kafscale apply -f "$KSV_PREP/minio.yaml"
kubectl -n kafscale rollout status deploy/minio --timeout=180s
"$KSV_PREP/minio-user.sh" add kafscale-demo "$KSV_SECRETS/s3-secret"
if [ ! -x "$KSV_DIR/venv/bin/python3" ]; then
  python3 -m venv "$KSV_DIR/venv"
  "$KSV_DIR/venv/bin/pip" install -q "confluent-kafka[avro,schemaregistry]"
fi
kubectl get nodes
