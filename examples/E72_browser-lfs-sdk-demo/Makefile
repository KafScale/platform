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

.PHONY: serve serve-bg test open help

PORT ?= 3000
LFS_ENDPOINT ?= http://localhost:8080/lfs/produce

serve: ## Start the demo server (foreground)
	@echo "Starting E72 Browser LFS Demo on http://localhost:$(PORT)"
	@echo "Press Ctrl+C to stop"
	@pgrep -f "http.server $(PORT)" | xargs kill 2>/dev/null || true
	python3 -m http.server $(PORT)

serve-bg: ## Start the demo server (background)
	@echo "Starting E72 Browser LFS Demo on http://localhost:$(PORT)"
	@pgrep -f "http.server $(PORT)" | xargs kill 2>/dev/null || true
	@python3 -m http.server $(PORT) &
	@sleep 1
	@echo "Server running in background (PID: $$!)"

open: ## Open demo in browser
	@open http://localhost:$(PORT) 2>/dev/null || xdg-open http://localhost:$(PORT) 2>/dev/null || echo "Open http://localhost:$(PORT) in your browser"

test: serve-bg open ## Start server and open browser
	@echo "Demo running at http://localhost:$(PORT)"
	@echo "Click 'Run E2E Tests' in the browser to execute automated tests"

check-proxy: ## Verify LFS proxy is reachable
	@echo "Checking LFS proxy at $(LFS_ENDPOINT)..."
	@curl -s -o /dev/null -w "%{http_code}" $(LFS_ENDPOINT:produce=readyz) | grep -q "200" && echo "✓ LFS proxy is ready" || echo "✗ LFS proxy not reachable - run: kubectl -n kafscale-demo port-forward svc/lfs-proxy 8080:8080"

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "%-15s %s\n", $$1, $$2}'
