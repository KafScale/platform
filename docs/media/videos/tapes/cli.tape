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

Output "docs/media/videos/cli/cli.gif"
Output "docs/media/videos/cli/cli.webm"
Output "docs/media/videos/cli/cli.mp4"
Set Shell bash
Set FontSize 21
Set Width 960
Set Height 540
Set Padding 12
Set Theme "Dracula"
Set Framerate 25
Set TypingSpeed 35ms
Hide
Type "source docs/media/videos/tapes/prep/rc.sh && rm -f bin/kafscale-cli console.cookies && clear" Enter
Sleep 1s
Show
Type `go -C platform build -o "$PWD/bin/kafscale-cli" ./cmd/kafscale-cli` Enter
Wait+Line@180s /^\$\s*$/
Type "kafscale-cli --help 2>&1 | head -8" Enter
Wait+Line@30s /^\$\s*$/
Sleep 5s
Type "clear; # port-forwards running: etcd on :2379, MinIO (S3) on :9000" Enter
Type "export KAFSCALE_ETCD_ENDPOINTS=http://127.0.0.1:2379 \" Enter
Type "  KAFSCALE_S3_BUCKET=kafscale-demo KAFSCALE_S3_REGION=us-east-1 \" Enter
Type "  KAFSCALE_S3_ENDPOINT=http://127.0.0.1:9000 KAFSCALE_S3_ACCESS_KEY=kafscale-demo" Enter
Wait+Line@10s /^\$\s*$/
Type "printf 'S3 secret key: '; read -rs KAFSCALE_S3_SECRET_KEY; echo" Enter
Wait+Line@10s /S3 secret key:\s*$/
Hide
Source "/tmp/kafscale-videos/secrets/s3-secret.tape"
Enter
Show
Type "export KAFSCALE_S3_SECRET_KEY" Enter
Sleep 1.5s
Type "kafscale-cli restore --topic orders --target-topic orders-restored \" Enter
Type `  --namespace kafscale --to "$(date -u +%FT%TZ)"` Enter
Wait+Line@120s /^\$\s*$/
Sleep 3s
Type "clear; for t in orders orders-restored; do printf '%s: ' $t; kubectl -n kafscale \" Enter
Type "  exec kcat -- kcat -b kafscale-proxy:9092 -C -t $t -o beginning -e -q | wc -l; done" Enter
Wait+Line@120s /^\$\s*$/
Sleep 4s
Type "clear; kubectl -n kafscale port-forward svc/kafscale-console 8080:80 >/dev/null &" Enter
Sleep 2s
Type "printf 'Console password: '; read -rs CONSOLE_PASSWORD; echo" Enter
Wait+Line@10s /Console password:\s*$/
Hide
Source "/tmp/kafscale-videos/secrets/console-pass.tape"
Enter
Show
Type `printf '%s' "$CONSOLE_PASSWORD" | jq -cRs '{username:"kafscaleadmin",password:.}' | \` Enter
Type "  curl -sS -c console.cookies -X POST -H 'Content-Type: application/json' \" Enter
Type "  -d @- http://127.0.0.1:8080/ui/api/auth/login; echo" Enter
Wait+Line@30s /^\$\s*$/
Sleep 1.5s
Type "curl -sS -b console.cookies -X DELETE \" Enter
Type "  http://127.0.0.1:8080/ui/api/status/topics/orders-restored; echo" Enter
Wait+Line@30s /^\$\s*$/
Type "unset CONSOLE_PASSWORD" Enter
Sleep 3s
Type "clear; kafscale-cli prune" Enter
Wait+Line@60s /^\$\s*$/
Sleep 4.5s
Type "kafscale-cli prune --topic orders-restored --force" Enter
Wait+Line@60s /^\$\s*$/
Sleep 5s
Hide
Type "kill %1; rm -f console.cookies; clear" Enter
