#!/usr/bin/env bash
<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
This project is supported and financed by Scalytics, Inc. (www.scalytics.io).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
example_dir="${root_dir}/examples/E30_flink-kafscale-demo"
image="${FLINK_DEMO_IMAGE:-ghcr.io/novatechflow/kafscale-flink-demo:dev}"
kind_cluster="${KAFSCALE_KIND_CLUSTER:-kafscale-demo}"
skip_build="${SKIP_BUILD:-0}"
profile="${KAFSCALE_PROFILE:-cluster}"

printf "Starting KafScale platform demo (kind + Helm)...\n"
( cd "${root_dir}" && make demo-guide-pf )

printf "Building Flink demo image...\n"
if [[ "${skip_build}" != "1" ]]; then
  cd "${example_dir}"
  docker build -t "${image}" .

  printf "Loading image into kind cluster %s...\n" "${kind_cluster}"
  kind load docker-image "${image}" --name "${kind_cluster}"
else
  printf "SKIP_BUILD=1 set: using existing image %s\n" "${image}"
fi

printf "Deploying Flink word count job into the cluster...\n"
cd "${root_dir}"
kubectl apply -f deploy/demo/flink-wordcount-app.yaml
kubectl -n kafscale-demo set env deployment/flink-wordcount-app KAFSCALE_PROFILE="${profile}"

printf "Done. Use: kubectl -n kafscale-demo logs deployment/flink-wordcount-app -f\n"
