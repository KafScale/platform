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

Output "docs/media/videos/ops/ops.gif"
Output "docs/media/videos/ops/ops.webm"
Output "docs/media/videos/ops/ops.mp4"
Set Shell bash
Set FontSize 21
Set Width 960
Set Height 540
Set Padding 12
Set Theme "Dracula"
Set Framerate 25
Set TypingSpeed 35ms
Hide
Type "source docs/media/videos/tapes/prep/rc.sh && clear" Enter
Sleep 1s
Show
Type "curl -s localhost:9093/metrics | grep -E \" Enter
Type "  '^kafscale_(s3_health_state|s3_latency_ms_avg|broker_uptime_seconds)'" Enter
Wait+Line@30s /^\$\s*$/
Sleep 4s
Type "clear; kubectl -n kafscale exec demo-etcd-0 -c etcd -- \" Enter
Type "  etcdctl get --prefix /kafscale/partition-leases/orders/" Enter
Wait+Line@30s /^\$\s*$/
Sleep 4s
Type "clear; kubectl -n kafscale delete pod demo-broker-1   # restart a broker" Enter
Wait+Line@90s /^\$\s*$/
Type "for p in 0 1 2; do echo r$p | kubectl -n kafscale exec -i kcat -- \" Enter
Type "  kcat -b kafscale-proxy:9092 -P -t orders -p $p; done; echo produced" Enter
Wait+Line@120s /^\$\s*$/
Sleep 1.5s
Type "kubectl -n kafscale exec demo-etcd-0 -c etcd -- \" Enter
Type "  etcdctl get --prefix /kafscale/partition-leases/orders/" Enter
Wait+Line@30s /^\$\s*$/
Sleep 4s
Hide
Type "k rollout status sts/demo-broker --timeout=300s >/dev/null; clear" Enter
Wait+Line@6m /^\$\s*$/
Show
Type "ls -l new-s3/   # new MinIO key pair, one file per key, mode 600" Enter
Wait+Line@10s /^\$\s*$/
Sleep 2.5s
Type "kubectl -n kafscale create secret generic kafscale-s3 \" Enter
Type "  --from-file=new-s3/AWS_ACCESS_KEY_ID \" Enter
Type "  --from-file=new-s3/AWS_SECRET_ACCESS_KEY \" Enter
Type "  --dry-run=client -o yaml | kubectl replace -f -" Enter
Wait+Line@30s /^\$\s*$/
Sleep 2s
Hide
Type "sleep 10; k rollout status sts/demo-broker --timeout=300s >/dev/null; clear" Enter
Wait+Line@6m /^\$\s*$/
Show
Type "kubectl -n kafscale rollout status sts/demo-broker   # the operator rolled them" Enter
Wait+Line@5m /^\$\s*$/
Sleep 2s
Type "DEPLOY=$(kubectl get deployment -n kafscale -l \" Enter
Type "  'app.kubernetes.io/instance=kafscale,app.kubernetes.io/component=schema-registry' -o name)" Enter
Wait+Line@30s /^\$\s*$/
Type `kubectl rollout restart "$DEPLOY" -n kafscale && kubectl rollout status "$DEPLOY" -n kafscale` Enter
Wait+Line@5m /^\$\s*$/
Sleep 2s
Hide
Type `"$KSV_REPO"/docs/media/videos/tapes/prep/minio-user.sh remove kafscale-demo >/dev/null; clear` Enter
Wait+Line@30s /^\$\s*$/
Show
Type "# rollout done, so the old key kafscale-demo is now revoked in MinIO" Enter
Type "echo after-rotation | kubectl -n kafscale exec -i kcat -- \" Enter
Type "  kcat -b kafscale-proxy:9092 -P -t orders && echo produced" Enter
Wait+Line@60s /^\$\s*$/
Sleep 3s
Type "clear; namespace='kafscale'; cluster='demo'" Enter
Type `kubectl -n "$namespace" create job --from=cronjob/"$cluster"-etcd-snapshot snapshot-now` Enter
Wait+Line@30s /^\$\s*$/
Type `kubectl -n "$namespace" wait --for=condition=complete --timeout=5m job/snapshot-now` Enter
Wait+Line@6m /^\$\s*$/
Type `kubectl -n "$namespace" logs job/snapshot-now | tail -4` Enter
Wait+Line@30s /^\$\s*$/
Sleep 5s
Type "clear; kubectl -n kafscale patch kafscalecluster demo --type merge \" Enter
Type `  -p '{"spec":{"brokers":{"replicas":4}}}'   # scale out 3 to 4` Enter
Wait+Line@30s /^\$\s*$/
Sleep 1.5s
Hide
Type "until k get pod demo-broker-3 >/dev/null 2>&1; do sleep 2; done; k wait --for=condition=Ready pod/demo-broker-3 --timeout=300s >/dev/null; clear" Enter
Wait+Line@8m /^\$\s*$/
Show
Type "kubectl -n kafscale get pods -l app=kafscale-broker; kubectl -n kafscale get hpa" Enter
Wait+Line@30s /^\$\s*$/
Sleep 6s
Hide
Type "clear" Enter
