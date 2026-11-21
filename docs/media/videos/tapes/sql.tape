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

Output "docs/media/videos/sql/sql.gif"
Output "docs/media/videos/sql/sql.webm"
Output "docs/media/videos/sql/sql.mp4"
Set Shell bash
Set FontSize 20
Set Width 960
Set Height 540
Set Padding 12
Set Theme "Dracula"
Set Framerate 25
Set TypingSpeed 35ms
Hide
Type "export KSV_CLUSTER=kafscale-demo; unset CURSOR_AGENT; source docs/media/videos/tapes/prep/rc.sh && cd platform && clear" Enter
Sleep 1s
Show
Type "# query your Kafka topics with psql, no ksqlDB cluster needed" Enter
Sleep 1s
Type "kubectl -n kafscale-demo get deployment,service kafsql" Enter
Wait+Line@30s /^\$\s*$/
Sleep 3s
Hide
Type "clear" Enter
Sleep 0.3s
Show
Type `seq 1 200 | awk '{printf "{\"order_id\":%d,\"status\":\"new\"}\n", $1}' | \` Enter
Type "  kubectl -n kafscale-demo run kcat-produce --rm -i --restart=Never --image=edenhill/kcat:1.7.1 -- \" Enter
Type "  -b kafscale-broker:9092 -t demo-topic-2 -P" Enter
Wait+Line@120s /^\$\s*$/
Sleep 2s
Hide
Type "for i in $(seq 1 60); do kubectl -n kafscale-demo exec deploy/minio -- sh -c 'ls /data/kafscale-snapshots/kafscale-demo/demo-topic-2/*/ 2>/dev/null' | grep -q '\.kfs' && break; sleep 5; done; kubectl -n kafscale-demo port-forward svc/kafsql 15432:5432 >/tmp/kafsql-port-forward.log 2>&1 & sleep 3; clear" Enter
Wait+Line@6m /^\$\s*$/
Show
Type "kubectl -n kafscale-demo exec deploy/minio -- sh -c 'ls /data/kafscale-snapshots/kafscale-demo/demo-topic-2/*/'" Enter
Wait+Line@30s /^\$\s*$/
Sleep 2s
Hide
Type "clear" Enter
Sleep 0.3s
Show
Type `psql "host=localhost port=15432 user=kafsql dbname=kfs sslmode=disable" -c "SHOW TOPICS;"` Enter
Wait+Line@30s /^\$\s*$/
Sleep 2.5s
Type `psql "host=localhost port=15432 user=kafsql dbname=kfs sslmode=disable" -c "SELECT _partition, _offset, _ts, json_value(_value, '$.order_id') AS order_id, json_value(_value, '$.status') AS status FROM demo-topic-2 TAIL 5;"` Enter
Wait+Line@30s /^\$\s*$/
Sleep 5s
Hide
Type "clear" Enter
Sleep 0.3s
Show
Type `psql "host=localhost port=15432 user=kafsql dbname=kfs sslmode=disable" -c "SELECT count(*) FROM demo-topic-2 LAST 1h;"` Enter
Wait+Line@30s /^\$\s*$/
Sleep 6s
Hide
Type "pkill -f 'kubectl -n kafscale-demo port-forward svc/kafsql'; clear" Enter
