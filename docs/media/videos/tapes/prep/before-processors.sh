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
export KSV_CLUSTER=kafscale-demo
source "$(dirname "${BASH_SOURCE[0]}")/rc.sh"
cd "$KSV_REPO"
sudo_cmd="${KSV_SUDO:-}"
ns=kafscale-demo
pkill -f "kubectl -n $ns port-forward" 2>/dev/null || true
$sudo_cmd env "PATH=$PATH" KAFSCALE_KIND_KUBE_PROXY_MODE=nftables make demo-platform-bootstrap
$sudo_cmd kind get kubeconfig --name kafscale-demo > "$KUBECONFIG"
kubectl -n "$ns" get kafscalecluster,kafscaletopic
ice_tag="$(sed -n 's/^appVersion: "\(.*\)"/\1/p' addons/processors/iceberg-processor/deploy/helm/iceberg-processor/Chart.yaml)"
sql_tag="$(sed -n 's/^appVersion: "\(.*\)"/\1/p' addons/processors/sql-processor/deploy/helm/sql-processor/Chart.yaml)"
for image in tabulario/iceberg-rest:1.6.0 \
  "ghcr.io/kafscale/kafscale-iceberg-processor:$ice_tag" \
  "ghcr.io/kafscale/kafscale-sql-processor:$sql_tag" \
  edenhill/kcat:1.7.1; do
  $sudo_cmd docker pull "$image" >/dev/null && $sudo_cmd scripts/kind-load-image.sh "$image" kafscale-demo
done
for name in iceberg sql; do
  (
    umask 077
    printf 'AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n' \
      "$(kubectl -n "$ns" get secret kafscale-s3-credentials -o jsonpath='{.data.KAFSCALE_S3_ACCESS_KEY}' | base64 -d)" \
      "$(kubectl -n "$ns" get secret kafscale-s3-credentials -o jsonpath='{.data.KAFSCALE_S3_SECRET_KEY}' | base64 -d)" \
      > "$KSV_DIR/$name-s3.env"
  )
  kubectl -n "$ns" create secret generic "$name-s3-credentials" \
    --from-env-file="$KSV_DIR/$name-s3.env" --dry-run=client -o yaml | kubectl apply -f -
  rm -f "$KSV_DIR/$name-s3.env"
done
kubectl apply -f addons/processors/iceberg-processor/deploy/kind/iceberg-rest.yaml
kubectl -n "$ns" rollout status deployment/iceberg-rest --timeout=300s
helm upgrade --install iceberg-processor addons/processors/iceberg-processor/deploy/helm/iceberg-processor \
  --namespace "$ns" \
  -f addons/processors/iceberg-processor/deploy/kind/values-kind.yaml
kubectl -n "$ns" rollout status deployment/iceberg-processor --timeout=300s
helm upgrade --install kafsql addons/processors/sql-processor/deploy/helm/sql-processor \
  --namespace "$ns" \
  -f addons/processors/sql-processor/deploy/kind/values-kind.yaml
kubectl -n "$ns" rollout status deployment/kafsql --timeout=300s
kubectl -n "$ns" run kcat-produce --rm -i --restart=Never --image=edenhill/kcat:1.7.1 -- -L -b kafscale-broker:9092 </dev/null >/dev/null
duckdb -no-agent -c "INSTALL httpfs; INSTALL aws; INSTALL iceberg; INSTALL avro;" >/dev/null
kubectl -n "$ns" get deployment iceberg-rest iceberg-processor kafsql
