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

Output "docs/media/videos/iceberg/iceberg.gif"
Output "docs/media/videos/iceberg/iceberg.webm"
Output "docs/media/videos/iceberg/iceberg.mp4"
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
Type "kubectl -n kafscale-demo get deployment iceberg-rest iceberg-processor" Enter
Wait+Line@30s /^\$\s*$/
Sleep 3s
Hide
Type "clear" Enter
Sleep 0.3s
Show
Type `seq 1 50 | awk '{printf "{\"order_id\":%d,\"status\":\"new\"}\n", $1}' | \` Enter
Type "  kubectl -n kafscale-demo run kcat-produce --rm -i --restart=Never --image=edenhill/kcat:1.7.1 -- \" Enter
Type "  -b kafscale-broker:9092 -t demo-topic-1 -P" Enter
Wait+Line@120s /^\$\s*$/
Sleep 2s
Hide
Type "clear" Enter
Sleep 0.3s
Show
Type "kubectl -n kafscale-demo exec deploy/minio -- sh -c 'ls /data/kafscale-snapshots/kafscale-demo/demo-topic-1/*/'" Enter
Wait+Line@30s /^\$\s*$/
Sleep 2s
Hide
Type "for i in $(seq 1 60); do kubectl -n kafscale-demo exec deploy/minio -- sh -c 'ls /data/kafscale-snapshots/iceberg/demo/demo_topic_1/data/ 2>/dev/null' | grep -q parquet && break; sleep 5; done; kubectl -n kafscale-demo port-forward svc/iceberg-rest 8181:8181 >/tmp/iceberg-rest-port-forward.log 2>&1 & kubectl -n kafscale-demo port-forward svc/minio 9000:9000 >/tmp/minio-port-forward.log 2>&1 & sleep 3; clear" Enter
Wait+Line@6m /^\$\s*$/
Show
Type "kubectl -n kafscale-demo exec deploy/minio -- ls /data/kafscale-snapshots/iceberg/demo/demo_topic_1/data/" Enter
Wait+Line@30s /^\$\s*$/
Sleep 3s
Hide
Type "clear" Enter
Sleep 0.3s
Show
Type "curl -fsS http://localhost:8181/v1/namespaces/demo/tables | jq -c ." Enter
Type "curl -fsS http://localhost:8181/v1/namespaces/demo/tables/demo_topic_1 | jq '{" Enter
Type "  location: .metadata.location," Enter
Type `  columns: [.metadata.schemas[-1].fields[] | "\(.name) \(.type)"],` Enter
Type `  "total-records": .metadata.snapshots[-1].summary["total-records"]` Enter
Type "}'" Enter
Wait+Line@30s /^\$\s*$/
Sleep 5s
Hide
Type "clear" Enter
Sleep 0.3s
Show
Type `export AWS_ACCESS_KEY_ID="$(kubectl -n kafscale-demo get secret iceberg-s3-credentials -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)"` Enter
Wait+Line@30s /^\$\s*$/
Type `export AWS_SECRET_ACCESS_KEY="$(kubectl -n kafscale-demo get secret iceberg-s3-credentials -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)"` Enter
Wait+Line@30s /^\$\s*$/
Sleep 1s
Hide
Type "clear" Enter
Sleep 0.3s
Show
Type "duckdb <<'SQL'" Enter
Type "INSTALL httpfs;" Enter
Type "LOAD httpfs;" Enter
Type "INSTALL aws;" Enter
Type "LOAD aws;" Enter
Type "INSTALL iceberg;" Enter
Type "LOAD iceberg;" Enter
Type "CREATE SECRET minio (TYPE s3, PROVIDER credential_chain, CHAIN 'env', ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');" Enter
Type "ATTACH 'warehouse' AS lake (TYPE iceberg, URI 'http://localhost:8181', AUTHORIZATION_TYPE 'none');" Enter
Type "SELECT count(*) AS records FROM lake.demo.demo_topic_1;" Enter
Type `SELECT record_id, "partition", "offset", decode(value) AS value FROM lake.demo.demo_topic_1 ORDER BY "partition", "offset" LIMIT 5;` Enter
Type "SQL" Enter
Wait+Line@120s /^\$\s*$/
Sleep 6s
Hide
Type "unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY; pkill -f 'kubectl -n kafscale-demo port-forward svc/iceberg-rest'; pkill -f 'kubectl -n kafscale-demo port-forward svc/minio'; clear" Enter
