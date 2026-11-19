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

Output "docs/media/videos/mcp/mcp.gif"
Output "docs/media/videos/mcp/mcp.webm"
Output "docs/media/videos/mcp/mcp.mp4"
Set Shell bash
Set FontSize 21
Set Width 960
Set Height 540
Set Padding 12
Set Theme "Dracula"
Set Framerate 25
Set TypingSpeed 35ms
Hide
Type "source docs/media/videos/tapes/prep/rc.sh && rm -f mcp-inspector.json && clear" Enter
Sleep 1s
Show
Type "kubectl create namespace kafscale-mcp" Enter
Wait+Line@30s /^\$\s*$/
Type "KAFSCALE_MCP_AUTH_TOKEN=$(openssl rand -base64 32)" Enter
Wait+Line@10s /^\$\s*$/
Type `printf '%s' "$KAFSCALE_MCP_AUTH_TOKEN" | kubectl -n kafscale-mcp create secret \` Enter
Type "  generic kafscale-mcp-token --from-file=mcp-token=/dev/stdin" Enter
Wait+Line@30s /^\$\s*$/
Sleep 2s
Type "clear; cat demo/mcp.yaml" Enter
Wait+Line@30s /^\$\s*$/
Sleep 6s
Type "clear; helm upgrade kafscale platform/deploy/helm/kafscale -n kafscale \" Enter
Type "  --reuse-values -f demo/mcp.yaml | grep -E 'STATUS|REVISION'" Enter
Wait+Line@120s /^\$\s*$/
Sleep 2s
Hide
Type "kubectl -n kafscale-mcp rollout status deploy/kafscale-mcp --timeout=300s >/dev/null; clear" Enter
Wait+Line@6m /^\$\s*$/
Show
Type "kubectl -n kafscale-mcp get svc" Enter
Wait+Line@30s /^\$\s*$/
Sleep 2s
Type "kubectl -n kafscale-mcp port-forward svc/kafscale-mcp 8090:80 >/dev/null 2>&1 &" Enter
Sleep 2.5s
Type "(umask 077; cat > mcp-inspector.json <<EOF" Enter
Type `{"mcpServers":{"kafscale":{"type":"streamable-http","url":"http://127.0.0.1:8090/mcp","headers":{"Authorization":"Bearer ${KAFSCALE_MCP_AUTH_TOKEN}"}}}}` Enter
Type "EOF" Enter
Type ")" Enter
Wait+Line@10s /^\$\s*$/
Sleep 2s
Type "clear; mcp() { npx -y @modelcontextprotocol/inspector --cli \" Enter
Type `  --config mcp-inspector.json --server kafscale "$@" 2>/dev/null; }` Enter
Type "curl -s -o /dev/null -w 'no token: %{http_code}\n' -X POST localhost:8090/mcp" Enter
Wait+Line@30s /^\$\s*$/
Sleep 2s
Type `mcp --method tools/list | jq -r '[.tools[].name] | join(" ")'` Enter
Wait+Line@120s /^\$\s*$/
Sleep 4s
Type "clear; mcp --method tools/call --tool-name cluster_status \" Enter
Type "  | jq -c '.structuredContent | del(.topics)'" Enter
Wait+Line@90s /^\$\s*$/
Sleep 5s
Type "clear; mcp --method tools/call --tool-name describe_group \" Enter
Type "  --tool-arg 'group_id=billing-worker' | jq -c '.structuredContent.offsets[]'" Enter
Wait+Line@90s /^\$\s*$/
Sleep 4s
Type "mcp --method tools/call --tool-name cluster_metrics | jq .structuredContent" Enter
Wait+Line@90s /^\$\s*$/
Sleep 6s
Type "rm mcp-inspector.json; kill %1" Enter
Sleep 1.5s
Hide
Type "clear" Enter
