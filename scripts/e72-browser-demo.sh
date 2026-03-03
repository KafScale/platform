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
#
# Deploys the E72 Browser LFS Demo inside the kind cluster.

set -euo pipefail

NAMESPACE="${E72_NAMESPACE:-kafscale-demo}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MANIFEST="$REPO_ROOT/examples/E72_browser-lfs-sdk-demo/k8s-deploy.yaml"

echo "=== E72 Browser LFS Demo (In-Cluster) ==="
echo ""

# Check kubectl
if ! command -v kubectl &>/dev/null; then
    echo "❌ kubectl not found"
    exit 1
fi

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" &>/dev/null; then
    echo "❌ Namespace '$NAMESPACE' not found."
    echo "   Run 'make lfs-demo' first to create the LFS stack."
    exit 1
fi

# Check if LFS proxy is running
if ! kubectl -n "$NAMESPACE" get svc lfs-proxy &>/dev/null; then
    echo "❌ LFS proxy service not found in namespace '$NAMESPACE'."
    echo "   Run 'make lfs-demo' first to create the LFS stack."
    exit 1
fi

echo "[1/4] Deploying E72 browser demo to namespace: $NAMESPACE"
kubectl -n "$NAMESPACE" apply -f "$MANIFEST"

echo ""
echo "[2/4] Waiting for pod to be ready..."
kubectl -n "$NAMESPACE" rollout status deployment/e72-browser-demo --timeout=60s

echo ""
echo "[3/4] Getting NodePort..."
NODE_PORT=$(kubectl -n "$NAMESPACE" get svc e72-browser-demo -o jsonpath='{.spec.ports[0].nodePort}')
echo "   NodePort: $NODE_PORT"

echo ""
echo "[4/4] Setting up port-forward for browser access..."

# For kind clusters, we need to port-forward since NodePort isn't directly accessible
echo "   Starting port-forward on localhost:3072 -> e72-browser-demo:80"
kubectl -n "$NAMESPACE" port-forward svc/e72-browser-demo 3072:80 &
PF_PID=$!
sleep 2

echo ""
echo "=============================================="
echo "✅ E72 Browser Demo is ready!"
echo ""
echo "   Open in browser: http://localhost:3072"
echo ""
echo "   The demo is running INSIDE the cluster and"
echo "   connects directly to: http://lfs-proxy:8080"
echo ""
echo "   Press Ctrl+C to stop the port-forward"
echo "=============================================="

# Open browser
if command -v open &>/dev/null; then
    open "http://localhost:3072"
elif command -v xdg-open &>/dev/null; then
    xdg-open "http://localhost:3072"
fi

# Wait for port-forward to be interrupted
wait $PF_PID 2>/dev/null || true
