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

.PHONY: proto build test tidy lint generate docker-build docker-build-e2e-client docker-clean ensure-minio start-minio stop-containers release-broker-ports test-produce-consume test-produce-consume-debug test-consumer-group test-ops-api test-mcp test-multi-segment-durability test-full test-operator demo demo-platform help

REGISTRY ?= ghcr.io/novatechflow
STAMP_DIR ?= .build
BROKER_IMAGE ?= $(REGISTRY)/kafscale-broker:dev
OPERATOR_IMAGE ?= $(REGISTRY)/kafscale-operator:dev
CONSOLE_IMAGE ?= $(REGISTRY)/kafscale-console:dev
MCP_IMAGE ?= $(REGISTRY)/kafscale-mcp:dev
E2E_CLIENT_IMAGE ?= $(REGISTRY)/kafscale-e2e-client:dev
MINIO_CONTAINER ?= kafscale-minio
MINIO_IMAGE ?= quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z
MINIO_PORT ?= 9000
MINIO_CONSOLE_PORT ?= 9001
MINIO_REGION ?= us-east-1
MINIO_ROOT_USER ?= minioadmin
MINIO_ROOT_PASSWORD ?= minioadmin
MINIO_BUCKET ?= kafscale
KAFSCALE_KIND_CLUSTER ?= kafscale-demo
KAFSCALE_DEMO_NAMESPACE ?= kafscale-demo
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

docker-build: docker-build-broker docker-build-operator docker-build-console docker-build-mcp docker-build-e2e-client ## Build all container images
	@mkdir -p $(STAMP_DIR)

DOCKER_BUILD_CMD := $(shell \
	if command -v docker >/dev/null 2>&1 && docker buildx version >/dev/null 2>&1; then \
		echo "docker buildx build --load"; \
	elif command -v docker-buildx >/dev/null 2>&1 && docker-buildx version >/dev/null 2>&1; then \
		echo "docker-buildx build --load"; \
	else \
		echo "DOCKER_BUILDKIT=1 docker build"; \
	fi)


BROKER_SRCS := $(shell find cmd/broker pkg go.mod go.sum)
docker-build-broker: $(STAMP_DIR)/broker.image ## Build broker container image
$(STAMP_DIR)/broker.image: $(BROKER_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) -t $(BROKER_IMAGE) -f deploy/docker/broker.Dockerfile .
	@touch $(STAMP_DIR)/broker.image

OPERATOR_SRCS := $(shell find cmd/operator pkg/operator api config go.mod go.sum)
docker-build-operator: $(STAMP_DIR)/operator.image ## Build operator container image
$(STAMP_DIR)/operator.image: $(OPERATOR_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) -t $(OPERATOR_IMAGE) -f deploy/docker/operator.Dockerfile .
	@touch $(STAMP_DIR)/operator.image

CONSOLE_SRCS := $(shell find cmd/console ui go.mod go.sum)
docker-build-console: $(STAMP_DIR)/console.image ## Build console container image
$(STAMP_DIR)/console.image: $(CONSOLE_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) -t $(CONSOLE_IMAGE) -f deploy/docker/console.Dockerfile .
	@touch $(STAMP_DIR)/console.image

MCP_SRCS := $(shell find cmd/mcp internal/mcpserver go.mod go.sum)
docker-build-mcp: $(STAMP_DIR)/mcp.image ## Build MCP container image
$(STAMP_DIR)/mcp.image: $(MCP_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) -t $(MCP_IMAGE) -f deploy/docker/mcp.Dockerfile .
	@touch $(STAMP_DIR)/mcp.image

E2E_CLIENT_SRCS := $(shell find cmd/e2e-client go.mod go.sum)
docker-build-e2e-client: $(STAMP_DIR)/e2e-client.image ## Build e2e client container image
$(STAMP_DIR)/e2e-client.image: $(E2E_CLIENT_SRCS)
	@mkdir -p $(STAMP_DIR)
	$(DOCKER_BUILD_CMD) -t $(E2E_CLIENT_IMAGE) -f deploy/docker/e2e-client.Dockerfile .
	@touch $(STAMP_DIR)/e2e-client.image

docker-clean: ## Remove local dev images and prune dangling Docker data
	@echo "WARNING: this resets Docker build caches (buildx/builder) and removes local images."
	@printf "Type YES to continue: "; read ans; [ "$$ans" = "YES" ] || { echo "aborted"; exit 1; }
	-docker image rm -f $(BROKER_IMAGE) $(OPERATOR_IMAGE) $(CONSOLE_IMAGE) $(MCP_IMAGE) $(E2E_CLIENT_IMAGE)
	-rm -rf $(STAMP_DIR)
	docker system prune --force --volumes
	docker buildx prune --force


stop-containers: ## Stop lingering e2e containers (MinIO + kind control planes) and free tcp ports
	-ids=$$(docker ps -q --filter "name=kafscale-minio"); if [ -n "$$ids" ]; then docker stop $$ids >/dev/null; fi
	-ids=$$(docker ps -q --filter "name=kafscale-e2e"); if [ -n "$$ids" ]; then docker stop $$ids >/dev/null; fi
	-$(MAKE) release-broker-ports

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
	go test -tags=e2e ./test/e2e -run TestFranzGoProduceConsume -v

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

test-operator: docker-build ## Run operator kind+helm snapshot e2e (requires kind/kubectl/helm).
	KAFSCALE_E2E=1 \
	KAFSCALE_E2E_KIND=1 \
	KAFSCALE_KIND_RECREATE=1 \
	go test -tags=e2e ./test/e2e -run TestOperatorEtcdSnapshotKindE2E -v

demo-platform: docker-build ## Launch a full platform demo on kind (operator HA + managed etcd + console).
	@command -v docker >/dev/null 2>&1 || { echo "docker is required"; exit 1; }
	@command -v kind >/dev/null 2>&1 || { echo "kind is required"; exit 1; }
	@command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required"; exit 1; }
	@command -v helm >/dev/null 2>&1 || { echo "helm is required"; exit 1; }
	@kind delete cluster --name $(KAFSCALE_KIND_CLUSTER) >/dev/null 2>&1 || true
	@if ! kind get clusters | grep -q '^$(KAFSCALE_KIND_CLUSTER)$$'; then kind create cluster --name $(KAFSCALE_KIND_CLUSTER); fi
	@kind load docker-image $(BROKER_IMAGE) --name $(KAFSCALE_KIND_CLUSTER)
	@kind load docker-image $(OPERATOR_IMAGE) --name $(KAFSCALE_KIND_CLUSTER)
	@kind load docker-image $(CONSOLE_IMAGE) --name $(KAFSCALE_KIND_CLUSTER)
	@kubectl create namespace $(KAFSCALE_DEMO_NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	@cat <<-EOF | kubectl apply -f -
	apiVersion: apps/v1
	kind: Deployment
	metadata:
	  name: minio
	  namespace: $(KAFSCALE_DEMO_NAMESPACE)
	spec:
	  replicas: 1
	  selector:
	    matchLabels:
	      app: minio
	  template:
	    metadata:
	      labels:
	        app: minio
	    spec:
	      containers:
	        - name: minio
	          image: $(MINIO_IMAGE)
	          args: ["server", "/data", "--console-address", ":9001"]
	          env:
	            - name: MINIO_ROOT_USER
	              value: $(MINIO_ROOT_USER)
	            - name: MINIO_ROOT_PASSWORD
	              value: $(MINIO_ROOT_PASSWORD)
	          ports:
	            - containerPort: 9000
	---
	apiVersion: v1
	kind: Service
	metadata:
	  name: minio
	  namespace: $(KAFSCALE_DEMO_NAMESPACE)
	spec:
	  selector:
	    app: minio
	  ports:
	    - name: api
	      port: 9000
	      targetPort: 9000
	EOF
	@kubectl -n $(KAFSCALE_DEMO_NAMESPACE) rollout status deployment/minio --timeout=120s
	@kubectl -n $(KAFSCALE_DEMO_NAMESPACE) create secret generic kafscale-s3-credentials \
	  --from-literal=KAFSCALE_S3_ACCESS_KEY=$(MINIO_ROOT_USER) \
	  --from-literal=KAFSCALE_S3_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	  --dry-run=client -o yaml | kubectl apply -f -
	@OPERATOR_REPO=$${OPERATOR_IMAGE%:*}; OPERATOR_TAG=$${OPERATOR_IMAGE##*:}; \
	CONSOLE_REPO=$${CONSOLE_IMAGE%:*}; CONSOLE_TAG=$${CONSOLE_IMAGE##*:}; \
	helm upgrade --install kafscale deploy/helm/kafscale \
	  --namespace $(KAFSCALE_DEMO_NAMESPACE) \
	  --create-namespace \
	  --set operator.replicaCount=2 \
	  --set operator.image.repository=$$OPERATOR_REPO \
	  --set operator.image.tag=$$OPERATOR_TAG \
	  --set console.image.repository=$$CONSOLE_REPO \
	  --set console.image.tag=$$CONSOLE_TAG \
	  --set operator.etcdEndpoints[0]=
	@OPERATOR_DEPLOY=$$(kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get deployments -l app.kubernetes.io/component=operator -o jsonpath='{.items[0].metadata.name}'); \
	kubectl -n $(KAFSCALE_DEMO_NAMESPACE) set env deployment/$$OPERATOR_DEPLOY \
	  BROKER_IMAGE=$(BROKER_IMAGE) \
	  KAFSCALE_OPERATOR_ETCD_ENDPOINTS= \
	  KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET=kafscale-snapshots \
	  KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET=1 \
	  KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PROTECT_BUCKET=1 \
	  KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT=http://minio.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:9000
	@kubectl -n $(KAFSCALE_DEMO_NAMESPACE) rollout status deployment/$$OPERATOR_DEPLOY --timeout=120s
	@cat <<-EOF | kubectl apply -f -
	apiVersion: kafscale.io/v1alpha1
	kind: KafscaleCluster
	metadata:
	  name: kafscale
	  namespace: $(KAFSCALE_DEMO_NAMESPACE)
	spec:
	  brokers:
	    replicas: 1
	  s3:
	    bucket: kafscale-snapshots
	    region: $(MINIO_REGION)
	    endpoint: http://minio.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:9000
	    credentialsSecretRef: kafscale-s3-credentials
	  etcd:
	    endpoints: []
	EOF
	@cat <<-EOF | kubectl apply -f -
	apiVersion: kafscale.io/v1alpha1
	kind: KafscaleTopic
	metadata:
	  name: demo-topic-1
	  namespace: $(KAFSCALE_DEMO_NAMESPACE)
	spec:
	  clusterRef: kafscale
	  partitions: 3
	---
	apiVersion: kafscale.io/v1alpha1
	kind: KafscaleTopic
	metadata:
	  name: demo-topic-2
	  namespace: $(KAFSCALE_DEMO_NAMESPACE)
	spec:
	  clusterRef: kafscale
	  partitions: 2
	EOF
	@kubectl -n $(KAFSCALE_DEMO_NAMESPACE) wait --for=condition=available deployment/kafscale-broker --timeout=180s
	@bash -c 'set -euo pipefail; \
		console_svc=$$(kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get svc -l app.kubernetes.io/component=console -o jsonpath="{.items[0].metadata.name}"); \
		kubectl -n $(KAFSCALE_DEMO_NAMESPACE) port-forward svc/$$console_svc 8080:80 >/tmp/kafscale-demo-console.log 2>&1 & \
		console_pid=$$!; \
		kubectl -n $(KAFSCALE_DEMO_NAMESPACE) port-forward svc/kafscale-broker 39092:9092 >/tmp/kafscale-demo-broker.log 2>&1 & \
		broker_pid=$$!; \
		trap "kill $$console_pid $$broker_pid" EXIT INT TERM; \
		echo "Console available at http://127.0.0.1:8080/ui/ (logs: /tmp/kafscale-demo-console.log)"; \
		echo "Streaming demo messages; press Ctrl+C to stop."; \
		KAFSCALE_DEMO_BROKER_ADDR=127.0.0.1:39092 \
		KAFSCALE_DEMO_TOPICS=demo-topic-1,demo-topic-2 \
		go run ./cmd/demo-workload; \
	'

demo: release-broker-ports ensure-minio ## Launch the broker + console demo stack and open the UI (Ctrl-C to stop).
	KAFSCALE_E2E=1 \
	KAFSCALE_E2E_DEMO=1 \
	KAFSCALE_E2E_OPEN_UI=1 \
	KAFSCALE_UI_USERNAME=kafscaleadmin \
	KAFSCALE_UI_PASSWORD=kafscale \
	KAFSCALE_CONSOLE_BROKER_METRICS_URL=http://127.0.0.1:39093/metrics \
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
