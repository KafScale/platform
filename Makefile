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

.PHONY: proto build test tidy lint generate docker-build docker-build-e2e-client docker-build-etcd-tools docker-clean ensure-minio start-minio stop-containers release-broker-ports test-produce-consume test-produce-consume-debug test-consumer-group test-ops-api test-mcp test-multi-segment-durability test-full test-operator demo demo-platform demo-platform-bootstrap iceberg-demo kafsql-demo platform-demo help clean-kind-all

REGISTRY ?= ghcr.io/kafscale
STAMP_DIR ?= .build
BROKER_IMAGE ?= $(REGISTRY)/kafscale-broker:dev
OPERATOR_IMAGE ?= $(REGISTRY)/kafscale-operator:dev
CONSOLE_IMAGE ?= $(REGISTRY)/kafscale-console:dev
PROXY_IMAGE ?= $(REGISTRY)/kafscale-proxy:dev
SQL_PROCESSOR_IMAGE ?= $(REGISTRY)/kafscale-sql-processor:dev
MCP_IMAGE ?= $(REGISTRY)/kafscale-mcp:dev
E2E_CLIENT_IMAGE ?= $(REGISTRY)/kafscale-e2e-client:dev
ETCD_TOOLS_IMAGE ?= $(REGISTRY)/kafscale-etcd-tools:dev
ICEBERG_PROCESSOR_IMAGE ?= iceberg-processor:dev
ICEBERG_REST_IMAGE ?= tabulario/iceberg-rest:1.6.0
ICEBERG_REST_PORT ?= 8181
ICEBERG_WAREHOUSE_BUCKET ?= kafscale-snapshots
ICEBERG_WAREHOUSE_PREFIX ?= iceberg
ICEBERG_DEMO_TOPIC ?= demo-topic-1
ICEBERG_DEMO_TABLE ?= demo.demo_topic_1
ICEBERG_DEMO_TABLE_NAMESPACE ?= $(word 1,$(subst ., ,$(ICEBERG_DEMO_TABLE)))
ICEBERG_DEMO_TABLE_NAME ?= $(word 2,$(subst ., ,$(ICEBERG_DEMO_TABLE)))
ICEBERG_DEMO_RECORDS ?= 50
ICEBERG_DEMO_TIMEOUT_SEC ?= 120
ICEBERG_DEMO_NAMESPACE ?= $(KAFSCALE_DEMO_NAMESPACE)
ICEBERG_PROCESSOR_RELEASE ?= iceberg-processor-dev
ICEBERG_PROCESSOR_REST_DEBUG ?=
ICEBERG_PROCESSOR_BUILD_FLAGS ?=
KAFSQL_DEMO_NAMESPACE ?= kafsql-demo
KAFSQL_DEMO_CLUSTER ?= kafsql
KAFSQL_DEMO_TOPIC ?= kafsql-demo-topic
KAFSQL_DEMO_RECORDS ?= 200
KAFSQL_DEMO_TIMEOUT_SEC ?= 120
KAFSQL_PROCESSOR_RELEASE ?= kafsql-processor-dev
MINIO_CONTAINER ?= kafscale-minio
MINIO_IMAGE ?= quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z
MINIO_PORT ?= 9000
MINIO_CONSOLE_PORT ?= 9001
MINIO_REGION ?= us-east-1
MINIO_ROOT_USER ?= minioadmin
MINIO_ROOT_PASSWORD ?= minioadmin
MINIO_BUCKET ?= kafscale
KAFSCALE_KIND_CLUSTER ?= kafscale-demo
ifndef KAFSCALE_KIND_KUBECONFIG
KAFSCALE_KIND_KUBECONFIG := $(shell mktemp -t kafscale-kind-kubeconfig.XXXXXX 2>/dev/null || mktemp)
endif
KAFSCALE_DEMO_NAMESPACE ?= kafscale-demo
KAFSCALE_CONSOLE_OPERATOR_METRICS_URL ?=
KAFSCALE_UI_USERNAME ?= kafscaleadmin
KAFSCALE_UI_PASSWORD ?= kafscale
KAFSCALE_DEMO_BROKER_REPLICAS ?= 2
KAFSCALE_DEMO_PROXY ?= 1
KAFSCALE_DEMO_CONSOLE ?= 1
KAFSCALE_DEMO_ADVERTISED_HOST ?=
KAFSCALE_DEMO_ADVERTISED_PORT ?=
KAFSCALE_DEMO_ETCD_INMEM ?= 1
KAFSCALE_DEMO_ETCD_REPLICAS ?= 3
BROKER_PORT ?= 39092
BROKER_PORTS ?= 39092 39093 39094

proto: ## Generate protobuf + gRPC stubs
	buf generate

generate: proto

build: ## Build all binaries
	go build ./...

test: ## Run unit tests + vet + race
	go vet ./...
	go test -race ./...

docker-build: docker-build-broker docker-build-operator docker-build-console docker-build-proxy docker-build-mcp docker-build-e2e-client docker-build-etcd-tools docker-build-sql-processor ## Build all container images
	@mkdir -p $(STAMP_DIR)

DOCKER_BUILD_CMD := $(shell \
	if command -v docker-buildx >/dev/null 2>&1 && docker-buildx version >/dev/null 2>&1; then \
		echo "docker-buildx build --load"; \
	elif command -v docker >/dev/null 2>&1 && docker buildx version >/dev/null 2>&1; then \
		echo "docker buildx build --load"; \
	else \
		echo "DOCKER_BUILDKIT=1 docker build"; \
	fi)
HOST_ARCH := $(shell uname -m)
DOCKER_PLATFORM ?= $(if $(filter arm64 aarch64,$(HOST_ARCH)),linux/arm64,$(if $(filter x86_64 amd64,$(HOST_ARCH)),linux/amd64,))
DOCKER_BUILD_ARGS ?= $(if $(findstring buildx,$(DOCKER_BUILD_CMD)),--platform=$(DOCKER_PLATFORM),)


BROKER_SRCS := $(shell find cmd/broker pkg go.mod go.sum)
docker-build-broker: $(STAMP_DIR)/broker.image ## Build broker container image
$(STAMP_DIR)/broker.image: $(BROKER_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) $(DOCKER_BUILD_ARGS) -t $(BROKER_IMAGE) -f deploy/docker/broker.Dockerfile .
	@touch $(STAMP_DIR)/broker.image

OPERATOR_SRCS := $(shell find cmd/operator pkg/operator api config go.mod go.sum)
docker-build-operator: $(STAMP_DIR)/operator.image ## Build operator container image
$(STAMP_DIR)/operator.image: $(OPERATOR_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) $(DOCKER_BUILD_ARGS) -t $(OPERATOR_IMAGE) -f deploy/docker/operator.Dockerfile .
	@touch $(STAMP_DIR)/operator.image

CONSOLE_SRCS := $(shell find cmd/console internal/console ui go.mod go.sum)
docker-build-console: $(STAMP_DIR)/console.image ## Build console container image
$(STAMP_DIR)/console.image: $(CONSOLE_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) $(DOCKER_BUILD_ARGS) -t $(CONSOLE_IMAGE) -f deploy/docker/console.Dockerfile .
	@touch $(STAMP_DIR)/console.image

PROXY_SRCS := $(shell find cmd/proxy pkg go.mod go.sum)
docker-build-proxy: $(STAMP_DIR)/proxy.image ## Build proxy container image
$(STAMP_DIR)/proxy.image: $(PROXY_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) $(DOCKER_BUILD_ARGS) -t $(PROXY_IMAGE) -f deploy/docker/proxy.Dockerfile .
	@touch $(STAMP_DIR)/proxy.image

MCP_SRCS := $(shell find cmd/mcp internal/mcpserver go.mod go.sum)
docker-build-mcp: $(STAMP_DIR)/mcp.image ## Build MCP container image
$(STAMP_DIR)/mcp.image: $(MCP_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) $(DOCKER_BUILD_ARGS) -t $(MCP_IMAGE) -f deploy/docker/mcp.Dockerfile .
	@touch $(STAMP_DIR)/mcp.image

E2E_CLIENT_SRCS := $(shell find cmd/e2e-client go.mod go.sum)
docker-build-e2e-client: $(STAMP_DIR)/e2e-client.image ## Build e2e client container image
$(STAMP_DIR)/e2e-client.image: $(E2E_CLIENT_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) $(DOCKER_BUILD_ARGS) -t $(E2E_CLIENT_IMAGE) -f deploy/docker/e2e-client.Dockerfile .
	@touch $(STAMP_DIR)/e2e-client.image

ETCD_TOOLS_SRCS := deploy/docker/etcd-tools.Dockerfile
docker-build-etcd-tools: $(STAMP_DIR)/etcd-tools.image ## Build etcd tools container image (etcdctl/etcdutl + shell)
$(STAMP_DIR)/etcd-tools.image: $(ETCD_TOOLS_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) $(DOCKER_BUILD_ARGS) -t $(ETCD_TOOLS_IMAGE) -f deploy/docker/etcd-tools.Dockerfile .
	@touch $(STAMP_DIR)/etcd-tools.image

SQL_PROCESSOR_SRCS := $(shell find addons/processors/sql-processor -name '*.go' -o -name 'go.mod' -o -name 'go.sum' -o -name 'Dockerfile')
docker-build-sql-processor: $(STAMP_DIR)/sql-processor.image ## Build KAFSQL processor container image
$(STAMP_DIR)/sql-processor.image: $(SQL_PROCESSOR_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) $(DOCKER_BUILD_ARGS) -t $(SQL_PROCESSOR_IMAGE) -f addons/processors/sql-processor/Dockerfile addons/processors/sql-processor
	@touch $(STAMP_DIR)/sql-processor.image

docker-clean: ## Remove local dev images and prune dangling Docker data
	@echo "WARNING: this resets Docker build caches (buildx/builder) and removes local images."
	@printf "Type YES to continue: "; read ans; [ "$$ans" = "YES" ] || { echo "aborted"; exit 1; }
	-docker image rm -f $(BROKER_IMAGE) $(OPERATOR_IMAGE) $(CONSOLE_IMAGE) $(PROXY_IMAGE) $(MCP_IMAGE) $(E2E_CLIENT_IMAGE) $(ETCD_TOOLS_IMAGE) $(SQL_PROCESSOR_IMAGE)
	-rm -rf $(STAMP_DIR)
	docker system prune --force --volumes
	docker buildx prune --force


stop-containers: ## Stop lingering e2e containers (MinIO + kind control planes) and free tcp ports
	-ids=$$(docker ps -q --filter "name=kafscale-minio"); if [ -n "$$ids" ]; then docker stop $$ids >/dev/null; fi
	-ids=$$(docker ps -q --filter "name=kafscale-e2e"); if [ -n "$$ids" ]; then docker stop $$ids >/dev/null; fi
	-$(MAKE) release-broker-ports

clean-kind-e2e: ## Delete kafscale-e2e* kind clusters without stopping MinIO.
	@filter_cmd="rg '^kafscale-e2e'"; \
	if ! command -v rg >/dev/null 2>&1; then \
	  filter_cmd="grep -E '^kafscale-e2e'"; \
	fi; \
	clusters=$$(kind get clusters | eval $$filter_cmd); \
	if [ -n "$$clusters" ]; then \
	  echo "$$clusters" | while read -r name; do kind delete cluster --name $$name; done; \
	fi

clean-kind-all: ## Delete all kind clusters and stop any remaining kind containers.
	@clusters=$$(kind get clusters 2>/dev/null || true); \
	if [ -n "$$clusters" ]; then \
	  echo "$$clusters" | while read -r name; do kind delete cluster --name $$name; done; \
	fi
	-ids=$$(docker ps -q --filter "label=io.x-k8s.kind.cluster"); if [ -n "$$ids" ]; then docker stop $$ids >/dev/null; fi
	-ids=$$(docker ps -aq --filter "label=io.x-k8s.kind.cluster"); if [ -n "$$ids" ]; then docker rm -f $$ids >/dev/null; fi
	-rm -f /tmp/kafscale-demo-*.log
	-@TMP_ROOT=$${TMPDIR:-/tmp}; rm -f "$$TMP_ROOT"/kafscale-kind-kubeconfig.* /tmp/kafscale-kind-kubeconfig.* 2>/dev/null || true

ensure-minio: ## Ensure the local MinIO helper container is running and reachable
	@command -v docker >/dev/null 2>&1 || { echo "docker is required for MinIO-backed e2e tests"; exit 1; }
	@if docker ps --format '{{.Names}}' | grep -q '^$(MINIO_CONTAINER)$$'; then \
		if ! lsof -ti :$(MINIO_PORT) >/dev/null 2>&1; then \
			echo "MinIO container is running but port $(MINIO_PORT) is unavailable; restarting"; \
			docker rm -f $(MINIO_CONTAINER) >/dev/null; \
			$(MAKE) start-minio >/dev/null; \
		else \
			echo "MinIO container $(MINIO_CONTAINER) already running"; \
		fi; \
	else \
		$(MAKE) start-minio >/dev/null; \
	fi

start-minio:
	@echo "starting MinIO helper $(MINIO_CONTAINER) on port $(MINIO_PORT)"
	@docker rm -f $(MINIO_CONTAINER) >/dev/null 2>&1 || true
	@docker run -d --name $(MINIO_CONTAINER) -p $(MINIO_PORT):9000 -p $(MINIO_CONSOLE_PORT):9001 \
		-e MINIO_ROOT_USER=$(MINIO_ROOT_USER) -e MINIO_ROOT_PASSWORD=$(MINIO_ROOT_PASSWORD) \
		$(MINIO_IMAGE) server /data --console-address :9001 >/dev/null
	@echo "waiting for MinIO to become reachable..."
	@for i in $$(seq 1 30); do \
		if lsof -ti :$(MINIO_PORT) >/dev/null 2>&1; then \
			echo "MinIO ready"; \
			break; \
		fi; \
		sleep 1; \
	done; \
	if ! lsof -ti :$(MINIO_PORT) >/dev/null 2>&1; then \
		echo "MinIO failed to start"; \
		docker logs $(MINIO_CONTAINER); \
		exit 1; \
	fi
	@docker exec $(MINIO_CONTAINER) sh -c "mkdir -p /data/$(MINIO_BUCKET)"

release-broker-ports:
	@for port in $(BROKER_PORTS); do \
		pids=$$(lsof -ti :$$port -sTCP:LISTEN 2>/dev/null); \
		if [ -n "$$pids" ]; then \
			echo "killing process on port $$port ($$pids)"; \
			kill $$pids; \
		fi; \
	done

test-produce-consume: release-broker-ports ensure-minio ## Run produce/consume tests against local Go binaries (only MinIO/kind helpers require Docker).
	KAFSCALE_E2E=1 \
	KAFSCALE_E2E_KIND=1 \
	KAFSCALE_S3_BUCKET=$(MINIO_BUCKET) \
	KAFSCALE_S3_REGION=$(MINIO_REGION) \
	KAFSCALE_S3_NAMESPACE=default \
	KAFSCALE_S3_ENDPOINT=http://127.0.0.1:$(MINIO_PORT) \
	KAFSCALE_S3_PATH_STYLE=true \
	KAFSCALE_S3_ACCESS_KEY=$(MINIO_ROOT_USER) \
	KAFSCALE_S3_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	go test -tags=e2e ./test/e2e -run 'TestFranzGoProduceConsume|TestKafkaCliProduce|TestKafkaCliAdminTopics' -v

test-produce-consume-debug: release-broker-ports ensure-minio ## Run produce/consume tests with broker trace logging enabled for debugging.
	KAFSCALE_TRACE_KAFKA=true \
	KAFSCALE_LOG_LEVEL=debug \
	$(MAKE) test-produce-consume

test-consumer-group: release-broker-ports ## Run consumer group persistence e2e (embedded etcd + in-memory S3).
	KAFSCALE_E2E=1 \
	go test -tags=e2e ./test/e2e -run TestConsumerGroupMetadataPersistsInEtcd -v

test-ops-api: release-broker-ports ## Run ops/admin API e2e (embedded etcd + in-memory S3).
	KAFSCALE_E2E=1 \
	go test -tags=e2e ./test/e2e -run TestOpsAPI -v

test-mcp: ## Run MCP e2e tests (in-memory metadata store + streamable HTTP).
	KAFSCALE_E2E=1 \
	go test -tags=e2e ./test/e2e -run TestMCP -v

test-multi-segment-durability: release-broker-ports ensure-minio ## Run multi-segment restart durability e2e (embedded etcd + MinIO).
	KAFSCALE_E2E=1 \
	go test -tags=e2e ./test/e2e -run TestMultiSegmentRestartDurability -v

test-full: ## Run unit tests plus local + MinIO-backed e2e suites.
	$(MAKE) test
	$(MAKE) test-consumer-group
	$(MAKE) test-ops-api
	$(MAKE) test-mcp
	$(MAKE) test-multi-segment-durability
	$(MAKE) test-produce-consume

test-operator: docker-build ## Run operator envtest + kind snapshot e2e (requires kind/kubectl/helm for kind).
	@$(MAKE) clean-kind-e2e
	@SETUP_ENVTEST=$$(command -v setup-envtest || true); \
	if [ -z "$$SETUP_ENVTEST" ]; then \
		GOBIN=$$(go env GOBIN); \
		GOPATH=$$(go env GOPATH); \
		if [ -n "$$GOBIN" ] && [ -x "$$GOBIN/setup-envtest" ]; then \
			SETUP_ENVTEST="$$GOBIN/setup-envtest"; \
		elif [ -x "$$GOPATH/bin/setup-envtest" ]; then \
			SETUP_ENVTEST="$$GOPATH/bin/setup-envtest"; \
		else \
			echo "setup-envtest not found; attempting install"; \
			go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest; \
			if [ -n "$$GOBIN" ]; then \
				SETUP_ENVTEST="$$GOBIN/setup-envtest"; \
			else \
				SETUP_ENVTEST="$$GOPATH/bin/setup-envtest"; \
			fi; \
		fi; \
	fi; \
	if [ ! -x "$$SETUP_ENVTEST" ]; then \
		echo "setup-envtest not available on PATH or GOPATH/bin; please add it to PATH"; \
		exit 1; \
	fi; \
	export KUBEBUILDER_ASSETS="$$( "$$SETUP_ENVTEST" use -p path 1.29.x )"; \
	KAFSCALE_E2E=1 \
	go test -tags=e2e ./test/e2e -run 'TestOperator(ManagedEtcdResources|BrokerExternalAccessConfig)' -v
	KAFSCALE_E2E=1 \
	KAFSCALE_E2E_KIND=1 \
	KAFSCALE_KIND_RECREATE=1 \
	go test -tags=e2e ./test/e2e -run TestOperatorEtcdSnapshotKindE2E -v

demo-platform-bootstrap: docker-build ## Bootstrap a full platform demo on kind (operator HA + managed etcd + console).
	@command -v docker >/dev/null 2>&1 || { echo "docker is required"; exit 1; }
	@command -v kind >/dev/null 2>&1 || { echo "kind is required"; exit 1; }
	@command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required"; exit 1; }
	@command -v helm >/dev/null 2>&1 || { echo "helm is required"; exit 1; }
	@TMP_ROOT=$${TMPDIR:-/tmp}; rm -f "$$TMP_ROOT"/kafscale-kind-kubeconfig.* /tmp/kafscale-kind-kubeconfig.* 2>/dev/null || true
	@$(MAKE) clean-kind-all
	@kind delete cluster --name $(KAFSCALE_KIND_CLUSTER) >/dev/null 2>&1 || true
	@if ! kind get clusters | grep -q '^$(KAFSCALE_KIND_CLUSTER)$$'; then kind create cluster --name $(KAFSCALE_KIND_CLUSTER); fi
	@kind get kubeconfig --name $(KAFSCALE_KIND_CLUSTER) > $(KAFSCALE_KIND_KUBECONFIG)
	@if [ "$(KAFSCALE_DEMO_PROXY)" = "1" ]; then KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) KAFSCALE_DEMO_NAMESPACE=$(KAFSCALE_DEMO_NAMESPACE) scripts/demo-platform.sh metallb; fi
	@kind load docker-image $(BROKER_IMAGE) --name $(KAFSCALE_KIND_CLUSTER)
	@kind load docker-image $(OPERATOR_IMAGE) --name $(KAFSCALE_KIND_CLUSTER)
	@if [ "$(KAFSCALE_DEMO_CONSOLE)" = "1" ]; then kind load docker-image $(CONSOLE_IMAGE) --name $(KAFSCALE_KIND_CLUSTER); fi
	@kind load docker-image $(ETCD_TOOLS_IMAGE) --name $(KAFSCALE_KIND_CLUSTER)
	@if [ "$(KAFSCALE_DEMO_PROXY)" = "1" ]; then kind load docker-image $(PROXY_IMAGE) --name $(KAFSCALE_KIND_CLUSTER); fi
	@KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) kubectl create namespace $(KAFSCALE_DEMO_NAMESPACE) --dry-run=client -o yaml | \
	  KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) kubectl apply --validate=false -f -
	@KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) KAFSCALE_DEMO_NAMESPACE=$(KAFSCALE_DEMO_NAMESPACE) \
	  MINIO_IMAGE=$(MINIO_IMAGE) \
	  MINIO_ROOT_USER=$(MINIO_ROOT_USER) \
	  MINIO_ROOT_PASSWORD=$(MINIO_ROOT_PASSWORD) \
	  scripts/demo-platform.sh minio
	@KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) kubectl -n $(KAFSCALE_DEMO_NAMESPACE) rollout status deployment/minio --timeout=120s
	@KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) kubectl -n $(KAFSCALE_DEMO_NAMESPACE) create secret generic kafscale-s3-credentials \
	  --from-literal=KAFSCALE_S3_ACCESS_KEY=$(MINIO_ROOT_USER) \
	  --from-literal=KAFSCALE_S3_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	  --dry-run=client -o yaml | KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) kubectl apply --validate=false -f -
	@OPERATOR_REPO=$${OPERATOR_IMAGE%:*}; OPERATOR_TAG=$${OPERATOR_IMAGE##*:}; \
	if [ -z "$$OPERATOR_REPO" ] || [ "$$OPERATOR_REPO" = "$$OPERATOR_TAG" ]; then \
	  echo "OPERATOR_IMAGE missing repository, defaulting to $(REGISTRY)/kafscale-operator"; \
	  OPERATOR_REPO=$(REGISTRY)/kafscale-operator; \
	  if [ "$$OPERATOR_TAG" = "$$OPERATOR_IMAGE" ] || [ -z "$$OPERATOR_TAG" ]; then \
	    OPERATOR_TAG=dev; \
	  fi; \
	fi; \
	CONSOLE_REPO=$${CONSOLE_IMAGE%:*}; CONSOLE_TAG=$${CONSOLE_IMAGE##*:}; \
	if [ -z "$$CONSOLE_REPO" ] || [ "$$CONSOLE_REPO" = "$$CONSOLE_TAG" ]; then \
	  echo "CONSOLE_IMAGE missing repository, defaulting to $(REGISTRY)/kafscale-console"; \
	  CONSOLE_REPO=$(REGISTRY)/kafscale-console; \
	  if [ "$$CONSOLE_TAG" = "$$CONSOLE_IMAGE" ] || [ -z "$$CONSOLE_TAG" ]; then \
	    CONSOLE_TAG=dev; \
	  fi; \
	fi; \
	PROXY_REPO=$${PROXY_IMAGE%:*}; PROXY_TAG=$${PROXY_IMAGE##*:}; \
	if [ -z "$$PROXY_REPO" ] || [ "$$PROXY_REPO" = "$$PROXY_TAG" ]; then \
	  echo "PROXY_IMAGE missing repository, defaulting to $(REGISTRY)/kafscale-proxy"; \
	  PROXY_REPO=$(REGISTRY)/kafscale-proxy; \
	  if [ "$$PROXY_TAG" = "$$PROXY_IMAGE" ] || [ -z "$$PROXY_TAG" ]; then \
	    PROXY_TAG=dev; \
	  fi; \
	fi; \
	ETCD_TOOLS_REPO=$${ETCD_TOOLS_IMAGE%:*}; ETCD_TOOLS_TAG=$${ETCD_TOOLS_IMAGE##*:}; \
	if [ -z "$$ETCD_TOOLS_REPO" ] || [ "$$ETCD_TOOLS_REPO" = "$$ETCD_TOOLS_TAG" ]; then \
	  echo "ETCD_TOOLS_IMAGE missing repository, defaulting to $(REGISTRY)/kafscale-etcd-tools"; \
	  ETCD_TOOLS_REPO=$(REGISTRY)/kafscale-etcd-tools; \
	  if [ "$$ETCD_TOOLS_TAG" = "$$ETCD_TOOLS_IMAGE" ] || [ -z "$$ETCD_TOOLS_TAG" ]; then \
	    ETCD_TOOLS_TAG=dev; \
	  fi; \
	fi; \
	KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) helm upgrade --install kafscale deploy/helm/kafscale \
	  --namespace $(KAFSCALE_DEMO_NAMESPACE) \
	  --create-namespace \
	  --set operator.replicaCount=2 \
	  --set operator.image.repository=$$OPERATOR_REPO \
	  --set operator.image.tag=$$OPERATOR_TAG \
	  --set operator.etcdSnapshotEtcdctlImage.repository=$$ETCD_TOOLS_REPO \
	  --set operator.etcdSnapshotEtcdctlImage.tag=$$ETCD_TOOLS_TAG \
	  --set operator.etcdReplicas=$(KAFSCALE_DEMO_ETCD_REPLICAS) \
	  $$( [ "$(KAFSCALE_DEMO_ETCD_INMEM)" = "1" ] && echo "--set operator.etcdStorageMemory=true" ) \
	  --set console.enabled=$(KAFSCALE_DEMO_CONSOLE) \
	  --set console.image.repository=$$CONSOLE_REPO \
	  --set console.image.tag=$$CONSOLE_TAG \
	  --set console.auth.username=$(KAFSCALE_UI_USERNAME) \
	  --set console.auth.password=$(KAFSCALE_UI_PASSWORD) \
	  --set console.etcdEndpoints[0]=http://kafscale-etcd-client.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:2379 \
	  --set console.metrics.brokerMetricsURL=http://kafscale-broker.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:9093/metrics \
	  --set console.metrics.operatorMetricsURL=http://kafscale-operator-metrics.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:8080/metrics \
	  --set operator.etcdEndpoints[0]= \
	  $$( [ "$(KAFSCALE_DEMO_PROXY)" = "1" ] && echo "--set proxy.enabled=true --set proxy.image.repository=$$PROXY_REPO --set proxy.image.tag=$$PROXY_TAG --set proxy.etcdEndpoints[0]=http://kafscale-etcd-client.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:2379 --set proxy.advertisedHost=127.0.0.1 --set proxy.advertisedPort=39092 --set proxy.service.type=LoadBalancer --set proxy.service.port=9092" )
	@KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) bash -c 'set -euo pipefail; \
	  OPERATOR_DEPLOY=$$(kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get deployments -l app.kubernetes.io/component=operator -o jsonpath="{.items[0].metadata.name}"); \
	  if [ -z "$$OPERATOR_DEPLOY" ]; then \
	    echo "operator deployment not found"; \
	    exit 1; \
	  fi; \
	  kubectl -n $(KAFSCALE_DEMO_NAMESPACE) set env deployment/$$OPERATOR_DEPLOY \
	    BROKER_IMAGE=$(BROKER_IMAGE) \
	    KAFSCALE_OPERATOR_ETCD_ENDPOINTS= \
	    KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET=1 \
	    KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PROTECT_BUCKET=1 \
	    KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT=http://minio.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:9000; \
	  kubectl -n $(KAFSCALE_DEMO_NAMESPACE) rollout status deployment/$$OPERATOR_DEPLOY --timeout=120s; \
	'
	@KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) KAFSCALE_DEMO_NAMESPACE=$(KAFSCALE_DEMO_NAMESPACE) \
	  KAFSCALE_DEMO_BROKER_REPLICAS=$(KAFSCALE_DEMO_BROKER_REPLICAS) \
	  KAFSCALE_DEMO_PROXY=$(KAFSCALE_DEMO_PROXY) \
	  KAFSCALE_DEMO_ADVERTISED_HOST=$(KAFSCALE_DEMO_ADVERTISED_HOST) \
	  KAFSCALE_DEMO_ADVERTISED_PORT=$(KAFSCALE_DEMO_ADVERTISED_PORT) \
	  MINIO_REGION=$(MINIO_REGION) \
	  scripts/demo-platform.sh cluster
	@KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) KAFSCALE_DEMO_NAMESPACE=$(KAFSCALE_DEMO_NAMESPACE) \
	  scripts/demo-platform.sh wait

demo-platform: demo-platform-bootstrap ## Launch a full platform demo on kind (operator HA + managed etcd + console).
	@KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) bash -c 'set -euo pipefail; \
		start_pf() { \
		  local name="$$1"; shift; \
		  local log="$$1"; shift; \
		  while true; do \
		    kubectl -n $(KAFSCALE_DEMO_NAMESPACE) port-forward "$$@" >>"$$log" 2>&1 || true; \
		    echo "$$name port-forward exited; restarting..." >>"$$log"; \
		    sleep 1; \
		  done \
		}; \
		console_pf_pid=""; \
		if [ "$(KAFSCALE_DEMO_CONSOLE)" = "1" ]; then \
		  console_svc=$$(kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get svc -l app.kubernetes.io/component=console -o jsonpath="{.items[0].metadata.name}"); \
		  start_pf console /tmp/kafscale-demo-console.log svc/$$console_svc 8080:80 & \
		  console_pf_pid=$$!; \
		fi; \
		broker_addr="127.0.0.1:39092"; \
		if [ "$(KAFSCALE_DEMO_PROXY)" = "1" ]; then \
		  proxy_svc=$$(kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get svc -l app.kubernetes.io/component=proxy -o jsonpath="{.items[0].metadata.name}"); \
		  proxy_deploy=$$(kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get deployment -l app.kubernetes.io/component=proxy -o jsonpath="{.items[0].metadata.name}"); \
		  proxy_lb_ip=$$(kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get svc $$proxy_svc -o jsonpath="{.status.loadBalancer.ingress[0].ip}" 2>/dev/null || true); \
		  proxy_lb_host=$$(kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get svc $$proxy_svc -o jsonpath="{.status.loadBalancer.ingress[0].hostname}" 2>/dev/null || true); \
		  if [ -n "$$proxy_lb_ip" ] || [ -n "$$proxy_lb_host" ]; then \
		    proxy_advertised_host="$${proxy_lb_ip:-$$proxy_lb_host}"; \
		    proxy_advertised_port="9092"; \
		    if command -v nc >/dev/null 2>&1; then \
		      if ! nc -z -w 2 "$$proxy_advertised_host" "$$proxy_advertised_port" >/dev/null 2>&1; then \
		        proxy_advertised_host=""; \
		      fi; \
		    else \
		      proxy_advertised_host=""; \
		    fi; \
		    if [ -n "$$proxy_advertised_host" ]; then \
		      kubectl -n $(KAFSCALE_DEMO_NAMESPACE) set env deployment/$$proxy_deploy \
		        KAFSCALE_PROXY_ADVERTISED_HOST="$$proxy_advertised_host" \
		        KAFSCALE_PROXY_ADVERTISED_PORT="$$proxy_advertised_port" >/dev/null; \
		      kubectl -n $(KAFSCALE_DEMO_NAMESPACE) rollout status deployment/$$proxy_deploy --timeout=120s; \
		      broker_addr="$$proxy_advertised_host:$$proxy_advertised_port"; \
		    else \
		      kubectl -n $(KAFSCALE_DEMO_NAMESPACE) set env deployment/$$proxy_deploy \
		        KAFSCALE_PROXY_ADVERTISED_HOST="127.0.0.1" \
		        KAFSCALE_PROXY_ADVERTISED_PORT="39092" >/dev/null; \
		      kubectl -n $(KAFSCALE_DEMO_NAMESPACE) rollout status deployment/$$proxy_deploy --timeout=120s; \
		      start_pf proxy /tmp/kafscale-demo-proxy.log svc/$$proxy_svc 39092:9092 & \
		      broker_pf_pid=$$!; \
		    fi; \
		  else \
		    kubectl -n $(KAFSCALE_DEMO_NAMESPACE) set env deployment/$$proxy_deploy \
		      KAFSCALE_PROXY_ADVERTISED_HOST="127.0.0.1" \
		      KAFSCALE_PROXY_ADVERTISED_PORT="39092" >/dev/null; \
		    kubectl -n $(KAFSCALE_DEMO_NAMESPACE) rollout status deployment/$$proxy_deploy --timeout=120s; \
		    start_pf proxy /tmp/kafscale-demo-proxy.log svc/$$proxy_svc 39092:9092 & \
		    broker_pf_pid=$$!; \
		  fi; \
		else \
		  start_pf broker /tmp/kafscale-demo-broker.log svc/kafscale-broker 39092:9092 & \
		  broker_pf_pid=$$!; \
		fi; \
		trap "kill $${console_pf_pid:-} $${broker_pf_pid:-} 2>/dev/null || true" EXIT INT TERM; \
		if [ "$(KAFSCALE_DEMO_CONSOLE)" = "1" ]; then \
		  echo "Console available at http://127.0.0.1:8080/ui/ (logs: /tmp/kafscale-demo-console.log)"; \
		fi; \
		echo "Broker endpoint: $$broker_addr"; \
		echo "Streaming demo messages; press Ctrl+C to stop."; \
		KAFSCALE_DEMO_BROKER_ADDR=$$broker_addr \
		KAFSCALE_DEMO_TOPICS=demo-topic-1,demo-topic-2 \
		go run ./cmd/demo-workload; \
	'

iceberg-demo: KAFSCALE_DEMO_PROXY=0
iceberg-demo: KAFSCALE_DEMO_CONSOLE=0
iceberg-demo: KAFSCALE_DEMO_BROKER_REPLICAS=1
iceberg-demo: KAFSCALE_DEMO_ADVERTISED_HOST=kafscale-broker.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local
iceberg-demo: KAFSCALE_DEMO_ADVERTISED_PORT=9092
iceberg-demo: demo-platform-bootstrap ## Run the Iceberg processor demo on kind.
	$(MAKE) -C addons/processors/iceberg-processor docker-build IMAGE=$(ICEBERG_PROCESSOR_IMAGE) DOCKER_BUILD_ARGS="$(DOCKER_BUILD_ARGS) --build-arg GO_BUILD_FLAGS='$(ICEBERG_PROCESSOR_BUILD_FLAGS)'"
	KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) \
	KAFSCALE_DEMO_NAMESPACE=$(KAFSCALE_DEMO_NAMESPACE) \
	KAFSCALE_KIND_CLUSTER=$(KAFSCALE_KIND_CLUSTER) \
	ICEBERG_DEMO_NAMESPACE=$(ICEBERG_DEMO_NAMESPACE) \
	ICEBERG_REST_IMAGE=$(ICEBERG_REST_IMAGE) \
	ICEBERG_REST_PORT=$(ICEBERG_REST_PORT) \
	ICEBERG_WAREHOUSE_BUCKET=$(ICEBERG_WAREHOUSE_BUCKET) \
	ICEBERG_WAREHOUSE_PREFIX=$(ICEBERG_WAREHOUSE_PREFIX) \
	MINIO_REGION=$(MINIO_REGION) \
	MINIO_ROOT_USER=$(MINIO_ROOT_USER) \
	MINIO_ROOT_PASSWORD=$(MINIO_ROOT_PASSWORD) \
	ICEBERG_DEMO_TABLE_NAMESPACE=$(ICEBERG_DEMO_TABLE_NAMESPACE) \
	ICEBERG_DEMO_TABLE_NAME=$(ICEBERG_DEMO_TABLE_NAME) \
	ICEBERG_DEMO_TABLE=$(ICEBERG_DEMO_TABLE) \
	ICEBERG_DEMO_TOPIC=$(ICEBERG_DEMO_TOPIC) \
	ICEBERG_DEMO_RECORDS=$(ICEBERG_DEMO_RECORDS) \
	ICEBERG_DEMO_TIMEOUT_SEC=$(ICEBERG_DEMO_TIMEOUT_SEC) \
	ICEBERG_PROCESSOR_RELEASE=$(ICEBERG_PROCESSOR_RELEASE) \
	ICEBERG_PROCESSOR_IMAGE=$(ICEBERG_PROCESSOR_IMAGE) \
	ICEBERG_PROCESSOR_REST_DEBUG=$(ICEBERG_PROCESSOR_REST_DEBUG) \
	E2E_CLIENT_IMAGE=$(E2E_CLIENT_IMAGE) \
	bash scripts/iceberg-demo.sh

kafsql-demo: KAFSCALE_DEMO_PROXY=0
kafsql-demo: KAFSCALE_DEMO_CONSOLE=0
kafsql-demo: KAFSCALE_DEMO_BROKER_REPLICAS=1
kafsql-demo: KAFSCALE_DEMO_ADVERTISED_HOST=
kafsql-demo: demo-platform-bootstrap ## Run the KAFSQL processor e2e demo on kind.
	KUBECONFIG=$(KAFSCALE_KIND_KUBECONFIG) \
	KAFSCALE_DEMO_NAMESPACE=$(KAFSCALE_DEMO_NAMESPACE) \
	KAFSCALE_KIND_CLUSTER=$(KAFSCALE_KIND_CLUSTER) \
	KAFSQL_DEMO_NAMESPACE=$(KAFSQL_DEMO_NAMESPACE) \
	KAFSQL_DEMO_CLUSTER=$(KAFSQL_DEMO_CLUSTER) \
	KAFSQL_DEMO_TOPIC=$(KAFSQL_DEMO_TOPIC) \
	KAFSQL_DEMO_RECORDS=$(KAFSQL_DEMO_RECORDS) \
	KAFSQL_DEMO_TIMEOUT_SEC=$(KAFSQL_DEMO_TIMEOUT_SEC) \
	KAFSQL_PROCESSOR_RELEASE=$(KAFSQL_PROCESSOR_RELEASE) \
	SQL_PROCESSOR_IMAGE=$(SQL_PROCESSOR_IMAGE) \
	E2E_CLIENT_IMAGE=$(E2E_CLIENT_IMAGE) \
	MINIO_BUCKET=$(MINIO_BUCKET) \
	MINIO_REGION=$(MINIO_REGION) \
	MINIO_ROOT_USER=$(MINIO_ROOT_USER) \
	MINIO_ROOT_PASSWORD=$(MINIO_ROOT_PASSWORD) \
	bash scripts/kafsql-demo.sh

platform-demo: demo-platform ## Alias for demo-platform.

demo: release-broker-ports ensure-minio ## Launch the broker + console demo stack and open the UI (Ctrl-C to stop).
	KAFSCALE_E2E=1 \
	KAFSCALE_E2E_DEMO=1 \
	KAFSCALE_E2E_OPEN_UI=1 \
	KAFSCALE_UI_USERNAME=kafscaleadmin \
	KAFSCALE_UI_PASSWORD=kafscale \
	KAFSCALE_CONSOLE_BROKER_METRICS_URL=http://127.0.0.1:39093/metrics \
	KAFSCALE_CONSOLE_OPERATOR_METRICS_URL=http://127.0.0.1:8080/metrics \
	KAFSCALE_S3_BUCKET=$(MINIO_BUCKET) \
	KAFSCALE_S3_REGION=$(MINIO_REGION) \
	KAFSCALE_S3_NAMESPACE=default \
	KAFSCALE_S3_ENDPOINT=http://127.0.0.1:$(MINIO_PORT) \
	KAFSCALE_S3_PATH_STYLE=true \
	KAFSCALE_S3_ACCESS_KEY=$(MINIO_ROOT_USER) \
	KAFSCALE_S3_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	go test -count=1 -tags=e2e ./test/e2e -run TestDemoStack -v

tidy:
	go mod tidy

lint:
	golangci-lint run

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "%-20s %s\n", $$1, $$2}'
