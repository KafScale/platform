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
kubectl -n kafscale get pod kcat >/dev/null 2>&1 || kubectl -n kafscale run kcat --image=edenhill/kcat:1.7.1 --restart=Never --command -- sleep 86400
kubectl -n kafscale wait --for=condition=Ready pod/kcat --timeout=120s
seq 1 30 | awk '{printf "o%d:order-%d\n",$1,$1}' | kubectl -n kafscale exec -i kcat -- kcat -b kafscale-proxy:9092 -P -t orders -K:
printf 'orders: '
kubectl -n kafscale exec kcat -- kcat -b kafscale-proxy:9092 -C -t orders -o beginning -e -q | wc -l
ksv_port_forward svc/demo-etcd-client 2379:2379
ksv_port_forward svc/minio 9000:9000
curl -s -o /dev/null -w 'minio %{http_code}\n' http://127.0.0.1:9000/minio/health/live
curl -s http://127.0.0.1:2379/version
echo
