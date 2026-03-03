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

.PHONY: all run run-all run-demo clean clean-all help
.PHONY: install-sdk install-sdk-if-changed build-demo
.PHONY: build-proxy load-proxy restart-proxy refresh-proxy refresh-proxy-if-changed
.PHONY: start-stack start-stack-if-missing wait-stack port-forward
.PHONY: wait-ready wait-http list-pods check-deps
.PHONY: rebuild-all

# ==============================================================================
# Configuration
# ==============================================================================
DEMO_NAMESPACE ?= kafscale-demo
KIND_CLUSTER ?= kafscale-demo
LFS_PROXY_IMAGE ?= ghcr.io/kafscale/kafscale-lfs-proxy:dev

# Paths
REPO_ROOT := ../..
SDK_DIR := $(REPO_ROOT)/lfs-client-sdk/java
PROXY_DIR := $(REPO_ROOT)/cmd/lfs-proxy
PKG_LFS_DIR := $(REPO_ROOT)/pkg/lfs
BUILD_STAMP_DIR := .build

# Stamp files for incremental builds
SDK_STAMP := $(BUILD_STAMP_DIR)/sdk.stamp
PROXY_STAMP := $(BUILD_STAMP_DIR)/proxy.stamp
DEMO_STAMP := $(BUILD_STAMP_DIR)/demo.stamp

# Source file patterns
SDK_SOURCES := $(shell find $(SDK_DIR)/src -name '*.java' 2>/dev/null)
SDK_POM := $(SDK_DIR)/pom.xml
PROXY_SOURCES := $(shell find $(PROXY_DIR) -name '*.go' 2>/dev/null)
PKG_LFS_SOURCES := $(shell find $(PKG_LFS_DIR) -name '*.go' 2>/dev/null)
DEMO_SOURCES := $(shell find src -name '*.java' 2>/dev/null)
DEMO_POM := pom.xml

# ==============================================================================
# Main targets
# ==============================================================================

all: run ## Default: build and run the demo

run: check-deps install-sdk-if-changed refresh-proxy-if-changed build-demo ## Build and run E70 demo (incremental)
	@echo "==> Running E70 Java LFS Demo..."
	mvn -q -Dexec.mainClass=io.kafscale.examples.lfs.E70JavaLfsDemo exec:java

run-all: check-deps install-sdk-if-changed refresh-proxy-if-changed build-demo ## Run with background port-forwards
	@mkdir -p .tmp
	@echo "==> Starting port-forwards..."
	@kubectl -n $(DEMO_NAMESPACE) port-forward svc/lfs-proxy 8080:8080 > .tmp/port-forward-lfs-proxy.log 2>&1 & echo $$! > .tmp/port-forward-lfs-proxy.pid
	@kubectl -n $(DEMO_NAMESPACE) port-forward svc/lfs-proxy 9094:9094 > .tmp/port-forward-lfs-proxy-health.log 2>&1 & echo $$! > .tmp/port-forward-lfs-proxy-health.pid
	@kubectl -n $(DEMO_NAMESPACE) port-forward svc/kafscale-broker 9092:9092 > .tmp/port-forward-broker.log 2>&1 & echo $$! > .tmp/port-forward-broker.pid
	@kubectl -n $(DEMO_NAMESPACE) port-forward svc/minio 9000:9000 > .tmp/port-forward-minio.log 2>&1 & echo $$! > .tmp/port-forward-minio.pid
	@trap 'for f in .tmp/port-forward-*.pid; do if [ -f $$f ]; then kill $$(cat $$f) >/dev/null 2>&1 || true; fi; done' EXIT; \
		sleep 2; \
		$(MAKE) wait-ready; \
		$(MAKE) wait-http; \
		$(MAKE) run

run-demo: run ## Alias for run

# ==============================================================================
# Incremental build targets
# ==============================================================================

$(BUILD_STAMP_DIR):
	@mkdir -p $(BUILD_STAMP_DIR)

# SDK: only rebuild if sources changed
$(SDK_STAMP): $(SDK_SOURCES) $(SDK_POM) | $(BUILD_STAMP_DIR)
	@echo "==> SDK sources changed, rebuilding..."
	cd $(SDK_DIR) && mvn -DskipTests clean install
	@touch $(SDK_STAMP)

install-sdk-if-changed: $(SDK_STAMP) ## Install SDK only if sources changed

install-sdk: ## Force install SDK (always rebuild)
	@echo "==> Force rebuilding SDK..."
	cd $(SDK_DIR) && mvn -DskipTests clean install
	@mkdir -p $(BUILD_STAMP_DIR) && touch $(SDK_STAMP)

# Proxy: only rebuild if Go sources changed
$(PROXY_STAMP): $(PROXY_SOURCES) $(PKG_LFS_SOURCES) | $(BUILD_STAMP_DIR)
	@echo "==> Proxy sources changed, rebuilding and redeploying..."
	$(MAKE) -C $(REPO_ROOT) docker-build-lfs-proxy
	kind load docker-image $(LFS_PROXY_IMAGE) --name $(KIND_CLUSTER)
	kubectl -n $(DEMO_NAMESPACE) rollout restart deployment/lfs-proxy || true
	kubectl -n $(DEMO_NAMESPACE) rollout status deployment/lfs-proxy --timeout=180s || true
	@touch $(PROXY_STAMP)

refresh-proxy-if-changed: $(PROXY_STAMP) ## Rebuild proxy only if sources changed

# Demo: rebuild if demo sources or SDK changed
$(DEMO_STAMP): $(DEMO_SOURCES) $(DEMO_POM) $(SDK_STAMP) | $(BUILD_STAMP_DIR)
	@echo "==> Demo sources changed, rebuilding..."
	mvn -q -DskipTests package
	@touch $(DEMO_STAMP)

build-demo: $(DEMO_STAMP) ## Build demo JAR

# ==============================================================================
# Force rebuild targets
# ==============================================================================

build-proxy: ## Force build LFS proxy image
	$(MAKE) -C $(REPO_ROOT) docker-build-lfs-proxy

load-proxy: ## Load LFS proxy image into kind
	kind load docker-image $(LFS_PROXY_IMAGE) --name $(KIND_CLUSTER)

restart-proxy: ## Restart LFS proxy deployment
	kubectl -n $(DEMO_NAMESPACE) rollout restart deployment/lfs-proxy
	kubectl -n $(DEMO_NAMESPACE) rollout status deployment/lfs-proxy --timeout=180s

refresh-proxy: build-proxy load-proxy restart-proxy ## Force rebuild and redeploy proxy
	@mkdir -p $(BUILD_STAMP_DIR) && touch $(PROXY_STAMP)

rebuild-all: check-deps ## Force rebuild SDK, proxy, and demo (ignores stamps)
	@echo "==> Rebuilding all components..."
	@echo "[1/3] Rebuilding SDK..."
	cd $(SDK_DIR) && mvn -DskipTests clean install
	@echo "[2/3] Rebuilding and redeploying LFS Proxy..."
	$(MAKE) -C $(REPO_ROOT) docker-build-lfs-proxy
	kind load docker-image $(LFS_PROXY_IMAGE) --name $(KIND_CLUSTER)
	kubectl -n $(DEMO_NAMESPACE) rollout restart deployment/lfs-proxy || true
	kubectl -n $(DEMO_NAMESPACE) rollout status deployment/lfs-proxy --timeout=180s || true
	@echo "[3/3] Rebuilding Demo..."
	mvn -DskipTests clean package
	@mkdir -p $(BUILD_STAMP_DIR)
	@touch $(SDK_STAMP) $(PROXY_STAMP) $(DEMO_STAMP)
	@echo "==> All components rebuilt successfully."

# ==============================================================================
# Stack management
# ==============================================================================

start-stack: ## Start the LFS demo stack (keeps running)
	LFS_DEMO_CLEANUP=0 KAFSCALE_LFS_PROXY_CHUNK_SIZE=5242880 $(MAKE) -C $(REPO_ROOT) lfs-demo
	$(MAKE) wait-stack

start-stack-if-missing: ## Start stack only if lfs-proxy deployment missing
	@if kubectl -n $(DEMO_NAMESPACE) get deployment/lfs-proxy >/dev/null 2>&1; then \
		$(MAKE) wait-stack; \
	else \
		$(MAKE) start-stack; \
	fi

wait-stack: ## Wait for MinIO + LFS proxy to be ready
	kubectl -n $(DEMO_NAMESPACE) rollout status deployment/lfs-proxy --timeout=180s
	kubectl -n $(DEMO_NAMESPACE) rollout status deployment/minio --timeout=180s

port-forward: ## Port-forward all services (foreground)
	@echo "Starting port-forwards (Ctrl+C to stop)..."
	@echo "  LFS Proxy: localhost:8080"
	@echo "  Kafka:     localhost:9092"
	@echo "  MinIO:     localhost:9000"
	kubectl -n $(DEMO_NAMESPACE) port-forward svc/lfs-proxy 8080:8080 &
	kubectl -n $(DEMO_NAMESPACE) port-forward svc/kafscale-broker 9092:9092 &
	kubectl -n $(DEMO_NAMESPACE) port-forward svc/minio 9000:9000 &
	wait

# ==============================================================================
# Health checks
# ==============================================================================

wait-ready: ## Wait for LFS proxy /readyz
	@for i in $$(seq 1 30); do \
		if curl -fsS http://localhost:9094/readyz >/dev/null 2>&1; then \
			echo "lfs-proxy ready"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "ERROR: lfs-proxy not ready on http://localhost:9094/readyz"; \
	exit 1

wait-http: ## Wait for LFS proxy HTTP port
	@for i in $$(seq 1 30); do \
		if nc -z 127.0.0.1 8080 >/dev/null 2>&1; then \
			echo "lfs-proxy HTTP ready"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "ERROR: lfs-proxy HTTP not reachable on 127.0.0.1:8080"; \
	exit 1

check-deps: ## Check required tools
	@command -v kubectl >/dev/null || (echo "ERROR: kubectl not found" && exit 1)
	@command -v kind >/dev/null || (echo "ERROR: kind not found" && exit 1)
	@command -v mvn >/dev/null || (echo "ERROR: mvn not found" && exit 1)
	@command -v docker >/dev/null || (echo "ERROR: docker not found" && exit 1)

# ==============================================================================
# Diagnostics
# ==============================================================================

list-pods: ## List pods and services in demo namespace
	@echo "=== Pods ==="
	kubectl -n $(DEMO_NAMESPACE) get pods -o wide
	@echo ""
	@echo "=== Services ==="
	kubectl -n $(DEMO_NAMESPACE) get svc
	@echo ""
	@echo "=== Endpoints ==="
	kubectl -n $(DEMO_NAMESPACE) get endpoints

# ==============================================================================
# Cleanup
# ==============================================================================

clean: ## Remove local build artifacts
	rm -rf target .tmp $(BUILD_STAMP_DIR)

clean-all: clean ## Remove all build artifacts including SDK
	cd $(SDK_DIR) && mvn clean || true

# ==============================================================================
# Help
# ==============================================================================

help: ## Show this help
	@echo "E70 Java LFS SDK Demo - Makefile targets:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Quick start:"
	@echo "  1. make start-stack      # Start LFS demo stack (once)"
	@echo "  2. make run-all          # Build and run with port-forwards"
	@echo ""
	@echo "Development workflow:"
	@echo "  make run                 # Incremental build + run (needs port-forwards)"
	@echo "  make install-sdk         # Force rebuild SDK"
	@echo "  make refresh-proxy       # Force rebuild proxy"
