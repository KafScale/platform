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

Output "docs/media/videos/admin/admin.gif"
Output "docs/media/videos/admin/admin.webm"
Output "docs/media/videos/admin/admin.mp4"
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
Type "kubectl get pods -n kafscale   # step 1 done, S3 is MinIO here" Enter
Wait+Line@60s /^\$\s*$/
Sleep 2s
Type "clear; printf 'AWS secret access key: '; read -rs AWS_SECRET_ACCESS_KEY; echo" Enter
Wait+Line@10s /secret access key:\s*$/
Hide
Source "/tmp/kafscale-videos/secrets/s3-secret.tape"
Enter
Show
Type `printf '%s' "$AWS_SECRET_ACCESS_KEY" | \` Enter
Type "  kubectl -n kafscale create secret generic kafscale-s3 \" Enter
Type "  --from-literal=AWS_ACCESS_KEY_ID=kafscale-demo \" Enter
Type "  --from-file=AWS_SECRET_ACCESS_KEY=/dev/stdin" Enter
Wait+Line@60s /^\$\s*$/
Type "unset AWS_SECRET_ACCESS_KEY" Enter
Sleep 1s
Type "printf 'Console password: '; read -rs CONSOLE_PASSWORD; echo" Enter
Wait+Line@10s /Console password:\s*$/
Hide
Source "/tmp/kafscale-videos/secrets/console-pass.tape"
Enter
Show
Type `printf '%s' "$CONSOLE_PASSWORD" | kubectl -n kafscale create secret generic \` Enter
Type "  kafscale-console-auth --from-literal=username=kafscaleadmin \" Enter
Type "  --from-file=password=/dev/stdin; unset CONSOLE_PASSWORD" Enter
Wait+Line@60s /^\$\s*$/
Sleep 2s
Type "clear; cat demo/dev-images.yaml" Enter
Sleep 4.5s
Type "clear; cat demo/kafscale-values.yaml   # console and 2 Kafka proxies" Enter
Sleep 4.5s
Type "clear; helm upgrade --install kafscale platform/deploy/helm/kafscale \" Enter
Type "  --namespace kafscale --create-namespace \" Enter
Type "  --set 'operator.etcdEndpoints={}' \" Enter
Type "  -f demo/dev-images.yaml -f demo/kafscale-values.yaml | head -6" Enter
Wait+Line@120s /^\$\s*$/
Sleep 3s
Type "clear; cat demo/cluster.yaml" Enter
Sleep 3.5s
Type "kubectl apply -n kafscale -f demo/cluster.yaml" Enter
Wait+Line@60s /^\$\s*$/
Sleep 1.5s
Type "clear; kubectl apply -n kafscale -f demo/s3-mb.yaml   # bucket Job, quickstart" Enter
Wait+Line@60s /^\$\s*$/
Type "kubectl -n kafscale wait --for=condition=complete job/s3-mb --timeout=120s" Enter
Wait+Line@150s /^\$\s*$/
Type "kubectl -n kafscale logs job/s3-mb; kubectl -n kafscale delete job s3-mb" Enter
Wait+Line@60s /^\$\s*$/
Sleep 2.5s
Hide
Type "until k get sts demo-broker >/dev/null 2>&1; do sleep 2; done; k rollout status sts/demo-broker --timeout=600s >/dev/null; k rollout status deploy/kafscale-proxy --timeout=600s >/dev/null; k rollout status sts/demo-etcd --timeout=600s >/dev/null; k wait --for=condition=Ready pod --all --field-selector=status.phase=Running --timeout=600s >/dev/null; clear" Enter
Wait+Line@15m /^\$\s*$/
Show
Type "kubectl get pods -n kafscale -o custom-columns=POD:.metadata.name,PHASE:.status.phase" Enter
Wait+Line@60s /^\$\s*$/
Sleep 4s
Type "clear; kubectl get kafscalecluster demo -n kafscale -o jsonpath=\" Enter
Type `'{range .status.conditions[*]}{.type}={.status} ({.reason}){"\n"}{end}'` Enter
Wait+Line@60s /^\$\s*$/
Sleep 4s
Type "clear; cat demo/topic.yaml; kubectl apply -n kafscale -f demo/topic.yaml" Enter
Wait+Line@60s /^\$\s*$/
Sleep 3s
Hide
Type "sleep 8; clear" Enter
Wait+Line@60s /^\$\s*$/
Show
Type "kubectl -n kafscale run kcat --image=edenhill/kcat:1.7.1 --restart=Never \" Enter
Type "  --command -- sleep 86400   # a client pod in the cluster" Enter
Wait+Line@60s /^\$\s*$/
Type "kubectl -n kafscale wait --for=condition=Ready pod/kcat --timeout=120s" Enter
Wait+Line@150s /^\$\s*$/
Sleep 1.5s
Type "clear; kubectl -n kafscale exec kcat -- kcat -b demo-broker:9092 -L" Enter
Wait+Line@60s /^\$\s*$/
Sleep 5s
Type "clear; echo hello | kubectl -n kafscale exec -i kcat -- \" Enter
Type "  kcat -b demo-broker:9092 -t orders -P" Enter
Wait+Line@60s /^\$\s*$/
Sleep 1s
Type "kubectl -n kafscale exec kcat -- kcat -b demo-broker:9092 -t orders -C -o beginning -e" Enter
Wait+Line@60s /^\$\s*$/
Sleep 3s
Type "kubectl -n kafscale exec kcat -- kcat -b kafscale-proxy:9092 \" Enter
Type "  -t orders -C -o beginning -e   # same read through the Kafka proxy" Enter
Wait+Line@60s /^\$\s*$/
Sleep 5s
