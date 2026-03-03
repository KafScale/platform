.PHONY: help kind-kubeconfig lfs-demo-up lfs-demo-run pf-start pf-stop wait-http test-lfs-sdk-kind run-all \
	build-java test-java build-js test-js build-python test-python build-js-browser test-js-browser build-all test-all

KAFSCALE_KIND_CLUSTER ?= kafscale-demo
KAFSCALE_DEMO_NAMESPACE ?= kafscale-demo
KAFSCALE_KIND_KUBECONFIG ?= $(shell mktemp -t kafscale-kind-kubeconfig.XXXXXX 2>/dev/null || mktemp)

BROKER_LOCAL_PORT ?= 39092
LFS_PROXY_HTTP_LOCAL_PORT ?= 8080
MINIO_LOCAL_PORT ?= 9000

LFS_PROXY_HTTP_PORT ?= 8080
LFS_PROXY_HTTP_PATH ?= /lfs/produce

KAFSCALE_LFS_PROXY_S3_BUCKET ?= kafscale
KAFSCALE_LFS_PROXY_S3_REGION ?= us-east-1
KAFSCALE_LFS_PROXY_S3_ENDPOINT ?= http://127.0.0.1:$(MINIO_LOCAL_PORT)
KAFSCALE_LFS_PROXY_S3_ACCESS_KEY ?= minioadmin
KAFSCALE_LFS_PROXY_S3_SECRET_KEY ?= minioadmin
KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE ?= true
PYTHON ?= python3

help:
	@echo "Targets:"
	@echo "  build-java          Build Java SDK (skip tests)."
	@echo "  test-java           Run Java SDK tests."
	@echo "  build-js            Build Node.js SDK."
	@echo "  test-js             Run Node.js SDK tests."
	@echo "  build-python        Build Python SDK (wheel/sdist)."
	@echo "  test-python         Run Python SDK tests."
	@echo "  build-js-browser    Build browser SDK."
	@echo "  test-js-browser     Run browser SDK tests."
	@echo "  build-all           Build Java, JS, Python (and browser) SDKs."
	@echo "  test-all            Run Java, JS, Python (and browser) SDK tests."
	@echo "  lfs-demo-up         Start/refresh the Kind LFS demo stack (keeps resources)."
	@echo "  lfs-demo-run        Run the LFS demo job in-cluster."
	@echo "  pf-start            Start port-forwards for broker, LFS proxy HTTP, and MinIO."
	@echo "  pf-stop             Stop port-forwards started by pf-start."
	@echo "  test-lfs-sdk-kind   Run Go SDK E2E test against Kind stack."
	@echo "  run-all             lfs-demo-up + pf-start + test-lfs-sdk-kind."

kind-kubeconfig:
	@kind get kubeconfig --name $(KAFSCALE_KIND_CLUSTER) > $(KAFSCALE_KIND_KUBECONFIG)

lfs-demo-up: kind-kubeconfig ## Start Kind LFS demo stack and keep it running.
	@KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) \
	  LFS_DEMO_CLEANUP=0 \
	  KAFSCALE_DEMO_NAMESPACE=$(KAFSCALE_DEMO_NAMESPACE) \
	  KAFSCALE_KIND_CLUSTER=$(KAFSCALE_KIND_CLUSTER) \
	  $(MAKE) -C .. lfs-demo

lfs-demo-run: kind-kubeconfig ## Run the in-cluster LFS demo job.
	@KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) \
	  LFS_DEMO_CLEANUP=0 \
	  KAFSCALE_DEMO_NAMESPACE=$(KAFSCALE_DEMO_NAMESPACE) \
	  KAFSCALE_KIND_CLUSTER=$(KAFSCALE_KIND_CLUSTER) \
	  $(MAKE) -C .. lfs-demo

pf-start: kind-kubeconfig ## Start port-forwards for broker, LFS proxy HTTP, and MinIO.
	@mkdir -p .pf
	@KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) bash -c 'set -euo pipefail; \
	  start_pf() { \
	    local name="$$1"; shift; \
	    local log="$$1"; shift; \
	    local pidfile=".pf/$$name.pid"; \
	    if [ -f "$$pidfile" ] && kill -0 "$$(cat "$$pidfile")" 2>/dev/null; then \
	      echo "$$name port-forward already running"; \
	      return; \
	    fi; \
	    kubectl -n $(KAFSCALE_DEMO_NAMESPACE) port-forward "$$@" >>"$$log" 2>&1 & \
	    echo $$! > "$$pidfile"; \
	  }; \
	  start_pf broker /tmp/kafscale-demo-broker.log svc/kafscale-broker $(BROKER_LOCAL_PORT):9092; \
	  start_pf lfs_http /tmp/kafscale-demo-lfs-http.log svc/lfs-proxy $(LFS_PROXY_HTTP_LOCAL_PORT):$(LFS_PROXY_HTTP_PORT); \
	  start_pf minio /tmp/kafscale-demo-minio.log svc/minio $(MINIO_LOCAL_PORT):9000; \
	  echo "Port-forwards running (broker=$(BROKER_LOCAL_PORT), lfs_http=$(LFS_PROXY_HTTP_LOCAL_PORT), minio=$(MINIO_LOCAL_PORT))"; \
	'

pf-stop: ## Stop port-forwards started by pf-start.
	@bash -c 'set -euo pipefail; \
	  for pidfile in .pf/*.pid; do \
	    [ -f "$$pidfile" ] || exit 0; \
	    pid="$$(cat "$$pidfile")"; \
	    kill "$$pid" 2>/dev/null || true; \
	    rm -f "$$pidfile"; \
	  done; \
	'

test-lfs-sdk-kind: ## Run Go SDK E2E test against Kind stack.
	@$(MAKE) wait-http
	@KAFSCALE_E2E=1 \
	  KAFSCALE_E2E_KIND=1 \
	  KAFSCALE_E2E_BROKER_ADDR=127.0.0.1:$(BROKER_LOCAL_PORT) \
	  LFS_PROXY_HTTP_URL=http://127.0.0.1:$(LFS_PROXY_HTTP_LOCAL_PORT) \
	  KAFSCALE_LFS_PROXY_S3_BUCKET=$(KAFSCALE_LFS_PROXY_S3_BUCKET) \
	  KAFSCALE_LFS_PROXY_S3_REGION=$(KAFSCALE_LFS_PROXY_S3_REGION) \
	  KAFSCALE_LFS_PROXY_S3_ENDPOINT=$(KAFSCALE_LFS_PROXY_S3_ENDPOINT) \
	  KAFSCALE_LFS_PROXY_S3_ACCESS_KEY=$(KAFSCALE_LFS_PROXY_S3_ACCESS_KEY) \
	  KAFSCALE_LFS_PROXY_S3_SECRET_KEY=$(KAFSCALE_LFS_PROXY_S3_SECRET_KEY) \
	  KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE=$(KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE) \
	  go test -tags=e2e ../test/e2e -run TestLfsSDKKindE2E -v

run-all: lfs-demo-up pf-start test-lfs-sdk-kind ## Start stack, port-forward, then run SDK test.

wait-http: kind-kubeconfig ## Ensure LFS proxy is ready, then verify HTTP port-forward.
	@KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) bash -c 'set -euo pipefail; \
	  if ! kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get svc lfs-proxy >/dev/null 2>&1; then \
	    echo "LFS proxy service not found in namespace $(KAFSCALE_DEMO_NAMESPACE). Run: make lfs-demo" >&2; \
	    exit 1; \
	  fi; \
	  if ! kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get deployment lfs-proxy >/dev/null 2>&1; then \
	    echo "LFS proxy deployment not found in namespace $(KAFSCALE_DEMO_NAMESPACE). Run: make lfs-demo" >&2; \
	    exit 1; \
	  fi; \
	  kubectl -n $(KAFSCALE_DEMO_NAMESPACE) rollout status deployment/lfs-proxy --timeout=120s; \
	  $(MAKE) pf-start; \
	  for i in $$(seq 1 30); do \
	    if nc -z 127.0.0.1 $(LFS_PROXY_HTTP_LOCAL_PORT) >/dev/null 2>&1; then \
	      exit 0; \
	    fi; \
	    sleep 1; \
	  done; \
	  echo "LFS proxy HTTP not reachable on 127.0.0.1:$(LFS_PROXY_HTTP_LOCAL_PORT)" >&2; \
	  exit 1; \
	'

build-java:
	@cd java && mvn -DskipTests package

test-java:
	@cd java && mvn test

build-js:
	@cd js && npm run build

test-js:
	@cd js && npm test

build-python:
	@cd python && $(PYTHON) -c "import build" >/dev/null 2>&1 || { echo "python -m pip install build"; exit 1; }
	@cd python && $(PYTHON) -m build

test-python:
	@cd python && $(PYTHON) -m pytest

build-js-browser:
	@cd js-browser && npm run build

test-js-browser:
	@cd js-browser && npm test

build-all: build-java build-js build-python build-js-browser

test-all: test-java test-js test-python test-js-browser
