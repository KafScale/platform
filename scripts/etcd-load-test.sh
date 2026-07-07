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

TOPIC="${TOPIC:-etcd-load}"
NUM_RECORDS="${NUM_RECORDS:-5000000}"
RECORD_SIZE="${RECORD_SIZE:-256}"
THROUGHPUT="${THROUGHPUT:-5000}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
NAMESPACE="${NAMESPACE:-kafscale}"
CLUSTER="${CLUSTER:-kafscale}"

echo "==> producer load: topic=${TOPIC} records=${NUM_RECORDS} throughput=${THROUGHPUT}"
kafka-producer-perf-test \
  --topic "${TOPIC}" \
  --num-records "${NUM_RECORDS}" \
  --record-size "${RECORD_SIZE}" \
  --throughput "${THROUGHPUT}" \
  --producer-props "bootstrap.servers=${BOOTSTRAP}"

echo "==> etcd endpoint status"
kubectl -n "${NAMESPACE}" exec "${CLUSTER}-etcd-0" -- \
  etcdctl --endpoints="http://127.0.0.1:2379" endpoint status --write-out=table

echo "==> maintenance cronjobs"
kubectl -n "${NAMESPACE}" get cronjob | rg 'etcd-maintenance|NAME'

echo "==> recent maintenance jobs"
kubectl -n "${NAMESPACE}" get jobs | rg 'etcd-maintenance' || true