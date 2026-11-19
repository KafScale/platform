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

Output "docs/media/videos/schema-registry/schema-registry.gif"
Output "docs/media/videos/schema-registry/schema-registry.webm"
Output "docs/media/videos/schema-registry/schema-registry.mp4"
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
Type "cat demo/registry.yaml" Enter
Wait+Line@30s /^\$\s*$/
Sleep 5s
Type "helm upgrade kafscale platform/deploy/helm/kafscale -n kafscale \" Enter
Type "  --reuse-values -f demo/registry.yaml | grep -E 'STATUS|REVISION'" Enter
Wait+Line@120s /^\$\s*$/
Sleep 2s
Hide
Type "k rollout status deploy/kafscale-schema-registry --timeout=300s >/dev/null; (kubectl -n kafscale port-forward svc/kafscale-schema-registry 8081:8081 >/dev/null 2>&1 &); sleep 3; clear" Enter
Wait+Line@6m /^\$\s*$/
Show
Type "kubectl get pods -n kafscale -l app.kubernetes.io/component=schema-registry" Enter
Wait+Line@30s /^\$\s*$/
Sleep 3s
Type "clear; # source: Apicurio Registry 3.0.6 on :8085, KafScale registry on :8081" Enter
Type "SRC=http://127.0.0.1:8085/apis/ccompat/v7; curl -s $SRC/subjects; echo" Enter
Wait+Line@30s /^\$\s*$/
Sleep 2s
Type "kafscale-cli schema import --from $SRC --to http://127.0.0.1:8081 --dry-run" Enter
Wait+Line@60s /^\$\s*$/
Sleep 4s
Type "clear; kafscale-cli schema import --from $SRC --to http://127.0.0.1:8081" Enter
Wait+Line@60s /^\$\s*$/
Sleep 4s
Type "clear; curl -s localhost:8081/subjects/orders-value/versions/2 | jq -c '{subject,version,id}'" Enter
Wait+Line@30s /^\$\s*$/
Sleep 3s
Type `curl -s -X PUT -H 'Content-Type: application/json' -d '{"mode":"READWRITE"}' \` Enter
Type "  localhost:8081/mode; echo" Enter
Wait+Line@30s /^\$\s*$/
Sleep 1.5s
Type "curl -s localhost:8081/config; echo   # the import copied Apicurio's global level" Enter
Wait+Line@30s /^\$\s*$/
Sleep 2s
Type `curl -s -X PUT -H 'Content-Type: application/json' -d '{"compatibility":"BACKWARD"}' \` Enter
Type "  localhost:8081/config; echo" Enter
Wait+Line@30s /^\$\s*$/
Sleep 2s
Type "clear; python3 demo/sr_demo.py" Enter
Wait+Line@60s /^\$\s*$/
Sleep 6s
Hide
Type "clear" Enter
