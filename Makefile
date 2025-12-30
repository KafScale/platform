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

.PHONY: proto build test tidy lint generate docker-build docker-build-e2e-client docker-clean ensure-minio start-minio stop-containers release-broker-ports test-produce-consume test-produce-consume-debug test-consumer-group test-ops-api test-mcp test-multi-segment-durability test-full test-operator demo demo-platform demo-platform-bootstrap iceberg-demo help

REGISTRY ?= ghcr.io/novatechflow
STAMP_DIR ?= .build
BROKER_IMAGE ?= $(REGISTRY)/kafscale-broker:dev
OPERATOR_IMAGE ?= $(REGISTRY)/kafscale-operator:dev
CONSOLE_IMAGE ?= $(REGISTRY)/kafscale-console:dev
MCP_IMAGE ?= $(REGISTRY)/kafscale-mcp:dev
E2E_CLIENT_IMAGE ?= $(REGISTRY)/kafscale-e2e-client:dev
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
ICEBERG_DEMO_NAMESPACE ?= $(KAFSCALE_DEMO_NAMESPACE)
ICEBERG_PROCESSOR_RELEASE ?= iceberg-processor-dev
ICEBERG_PROCESSOR_REST_DEBUG ?=
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

# this makefile is getting complicated; I added what is doing what here
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

ensure-minio: ## making sure the local MinIO helper container is running and reachable
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
	go test -tags=e2e ./test/e2e -run 'TestFranzGoProduceConsume|TestKafkaCliProduce' -v

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
	@kind delete cluster --name $(KAFSCALE_KIND_CLUSTER) >/dev/null 2>&1 || true
	@bash -c 'set -euo pipefail; if ! kind get clusters | grep -q "^$(KAFSCALE_KIND_CLUSTER)$$"; then kind create cluster --name $(KAFSCALE_KIND_CLUSTER) 2>&1 | LC_ALL=C tr -cd "\11\12\15\40-\176"; fi'
	@kind load docker-image $(BROKER_IMAGE) --name $(KAFSCALE_KIND_CLUSTER)
	@kind load docker-image $(OPERATOR_IMAGE) --name $(KAFSCALE_KIND_CLUSTER)
	@kind load docker-image $(CONSOLE_IMAGE) --name $(KAFSCALE_KIND_CLUSTER)
	@kubectl create namespace $(KAFSCALE_DEMO_NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	@bash -c $$'cat <<EOF | kubectl apply -f -\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: minio\n  namespace: $(KAFSCALE_DEMO_NAMESPACE)\nspec:\n  replicas: 1\n  selector:\n    matchLabels:\n      app: minio\n  template:\n    metadata:\n      labels:\n        app: minio\n    spec:\n      containers:\n        - name: minio\n          image: $(MINIO_IMAGE)\n          args: [\"server\", \"/data\", \"--console-address\", \":9001\"]\n          env:\n            - name: MINIO_ROOT_USER\n              value: $(MINIO_ROOT_USER)\n            - name: MINIO_ROOT_PASSWORD\n              value: $(MINIO_ROOT_PASSWORD)\n          ports:\n            - containerPort: 9000\n---\napiVersion: v1\nkind: Service\nmetadata:\n  name: minio\n  namespace: $(KAFSCALE_DEMO_NAMESPACE)\nspec:\n  selector:\n    app: minio\n  ports:\n    - name: api\n      port: 9000\n      targetPort: 9000\nEOF'
	@kubectl -n $(KAFSCALE_DEMO_NAMESPACE) rollout status deployment/minio --timeout=120s
	@kubectl -n $(KAFSCALE_DEMO_NAMESPACE) create secret generic kafscale-s3-credentials \
	  --from-literal=KAFSCALE_S3_ACCESS_KEY=$(MINIO_ROOT_USER) \
	  --from-literal=KAFSCALE_S3_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	  --dry-run=client -o yaml | kubectl apply -f -
	@OPERATOR_IMAGE="$(OPERATOR_IMAGE)"; CONSOLE_IMAGE="$(CONSOLE_IMAGE)"; \
	OPERATOR_REPO=$${OPERATOR_IMAGE%:*}; OPERATOR_TAG=$${OPERATOR_IMAGE##*:}; \
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
	@bash -c 'set -euo pipefail; \
		OPERATOR_DEPLOY=$$(kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get deployments -l app.kubernetes.io/component=operator -o name | head -n 1); \
		if [ -z "$$OPERATOR_DEPLOY" ]; then \
			echo "operator deployment not found in namespace $(KAFSCALE_DEMO_NAMESPACE)"; \
			exit 1; \
		fi; \
		kubectl -n $(KAFSCALE_DEMO_NAMESPACE) set env $$OPERATOR_DEPLOY \
		  BROKER_IMAGE=$(BROKER_IMAGE) \
		  KAFSCALE_OPERATOR_ETCD_ENDPOINTS= \
		  KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET=kafscale-snapshots \
		  KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET=1 \
		  KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PROTECT_BUCKET=1 \
		  KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT=http://minio.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:9000; \
		if ! kubectl -n $(KAFSCALE_DEMO_NAMESPACE) rollout status $$OPERATOR_DEPLOY --timeout=120s; then \
			kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get pods -l app.kubernetes.io/component=operator -o wide; \
			kubectl -n $(KAFSCALE_DEMO_NAMESPACE) describe $$OPERATOR_DEPLOY; \
			exit 1; \
		fi; \
	'
	@bash -c $$'cat <<EOF | kubectl apply -f -\napiVersion: kafscale.io/v1alpha1\nkind: KafscaleCluster\nmetadata:\n  name: kafscale\n  namespace: $(KAFSCALE_DEMO_NAMESPACE)\nspec:\n  brokers:\n    advertisedHost: 127.0.0.1\n    advertisedPort: 39092\n    replicas: 1\n  s3:\n    bucket: kafscale-snapshots\n    region: $(MINIO_REGION)\n    endpoint: http://minio.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:9000\n    credentialsSecretRef: kafscale-s3-credentials\n  etcd:\n    endpoints: []\nEOF'
	@bash -c $$'cat <<EOF | kubectl apply -f -\napiVersion: kafscale.io/v1alpha1\nkind: KafscaleTopic\nmetadata:\n  name: demo-topic-1\n  namespace: $(KAFSCALE_DEMO_NAMESPACE)\nspec:\n  clusterRef: kafscale\n  partitions: 3\n---\napiVersion: kafscale.io/v1alpha1\nkind: KafscaleTopic\nmetadata:\n  name: demo-topic-2\n  namespace: $(KAFSCALE_DEMO_NAMESPACE)\nspec:\n  clusterRef: kafscale\n  partitions: 2\nEOF'
	@bash -c 'set -euo pipefail; \
		deadline=$$(date +%s); deadline=$$((deadline + 120)); \
		while :; do \
			if kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get pods -l app=kafscale-broker -o name 2>/dev/null | grep -q "pod/"; then \
				break; \
			fi; \
			if [ "$$(date +%s)" -ge "$$deadline" ]; then \
				echo "broker pods not created in time"; \
				kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get deployments; \
				kubectl -n $(KAFSCALE_DEMO_NAMESPACE) get kafscalecluster kafscale -o yaml; \
				exit 1; \
			fi; \
			echo "waiting for broker pods..."; \
			sleep 5; \
		done; \
		kubectl -n $(KAFSCALE_DEMO_NAMESPACE) wait --for=condition=ready pod -l app=kafscale-broker --timeout=180s; \
	'

demo-platform: demo-platform-bootstrap ## Launch a full platform demo on kind (operator HA + managed etcd + console).
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

iceberg-demo: demo-platform-bootstrap ## Run the Iceberg processor demo on kind.
	@kubectl -n $(KAFSCALE_DEMO_NAMESPACE) patch kafscalecluster kafscale --type merge \
	  -p '{"spec":{"brokers":{"advertisedHost":"kafscale-broker.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local","advertisedPort":9092}}}'
	@kubectl -n $(KAFSCALE_DEMO_NAMESPACE) wait --for=condition=available deployment/kafscale-broker --timeout=180s
	@$(MAKE) -C addons/processors/iceberg-processor docker-build IMAGE=$(ICEBERG_PROCESSOR_IMAGE)
	@kind load docker-image $(ICEBERG_PROCESSOR_IMAGE) --name $(KAFSCALE_KIND_CLUSTER)
	@kind load docker-image $(E2E_CLIENT_IMAGE) --name $(KAFSCALE_KIND_CLUSTER)
	@kubectl create namespace $(ICEBERG_DEMO_NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	@kubectl -n $(ICEBERG_DEMO_NAMESPACE) create secret generic iceberg-s3-credentials \
	  --from-literal=AWS_ACCESS_KEY_ID=$(MINIO_ROOT_USER) \
	  --from-literal=AWS_SECRET_ACCESS_KEY=$(MINIO_ROOT_PASSWORD) \
	  --dry-run=client -o yaml | kubectl apply -f -
	@bash -c 'set -euo pipefail; \
		kubectl -n $(KAFSCALE_DEMO_NAMESPACE) exec deploy/minio -- sh -c "mkdir -p /data/$(ICEBERG_WAREHOUSE_BUCKET)/$(ICEBERG_WAREHOUSE_PREFIX)"; \
	'
	@bash -c $$'cat <<EOF | kubectl apply -f -\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: iceberg-rest\n  namespace: $(ICEBERG_DEMO_NAMESPACE)\nspec:\n  replicas: 1\n  selector:\n    matchLabels:\n      app: iceberg-rest\n  template:\n    metadata:\n      labels:\n        app: iceberg-rest\n    spec:\n      containers:\n        - name: iceberg-rest\n          image: $(ICEBERG_REST_IMAGE)\n          env:\n            - name: CATALOG_WAREHOUSE\n              value: s3://$(ICEBERG_WAREHOUSE_BUCKET)/$(ICEBERG_WAREHOUSE_PREFIX)\n            - name: CATALOG_IO__IMPL\n              value: org.apache.iceberg.aws.s3.S3FileIO\n            - name: CATALOG_S3_REGION\n              value: $(MINIO_REGION)\n            - name: AWS_REGION\n              value: $(MINIO_REGION)\n            - name: AWS_DEFAULT_REGION\n              value: $(MINIO_REGION)\n            - name: CATALOG_S3_ENDPOINT\n              value: http://minio.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:9000\n            - name: CATALOG_S3_PATH__STYLE__ACCESS\n              value: \"true\"\n            - name: CATALOG_S3_ACCESS__KEY__ID\n              valueFrom:\n                secretKeyRef:\n                  name: iceberg-s3-credentials\n                  key: AWS_ACCESS_KEY_ID\n            - name: CATALOG_S3_SECRET__ACCESS__KEY\n              valueFrom:\n                secretKeyRef:\n                  name: iceberg-s3-credentials\n                  key: AWS_SECRET_ACCESS_KEY\n          ports:\n            - containerPort: $(ICEBERG_REST_PORT)\n---\napiVersion: v1\nkind: Service\nmetadata:\n  name: iceberg-rest\n  namespace: $(ICEBERG_DEMO_NAMESPACE)\nspec:\n  selector:\n    app: iceberg-rest\n  ports:\n    - name: http\n      port: $(ICEBERG_REST_PORT)\n      targetPort: $(ICEBERG_REST_PORT)\nEOF'
	@bash -c 'set -euo pipefail; \
		if ! kubectl -n $(ICEBERG_DEMO_NAMESPACE) rollout status deployment/iceberg-rest --timeout=120s; then \
			kubectl -n $(ICEBERG_DEMO_NAMESPACE) get pods -l app=iceberg-rest -o wide; \
			pod=$$(kubectl -n $(ICEBERG_DEMO_NAMESPACE) get pods -l app=iceberg-rest -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || true); \
			if [ -n "$$pod" ]; then \
				kubectl -n $(ICEBERG_DEMO_NAMESPACE) logs "$$pod" --all-containers=true --tail=200; \
			fi; \
			kubectl -n $(ICEBERG_DEMO_NAMESPACE) describe deployment/iceberg-rest; \
			exit 1; \
		fi; \
	'
	@bash -c $$'set -euo pipefail; \
		rest_ip=$$(kubectl -n $(ICEBERG_DEMO_NAMESPACE) get svc iceberg-rest -o jsonpath="{.spec.clusterIP}"); \
		if [ -z "$$rest_ip" ]; then \
			echo "iceberg-rest service has no clusterIP"; \
			kubectl -n $(ICEBERG_DEMO_NAMESPACE) get svc iceberg-rest -o yaml; \
			exit 1; \
		fi; \
		rest_endpoint="http://$$rest_ip:$(ICEBERG_REST_PORT)"; \
		kubectl -n $(ICEBERG_DEMO_NAMESPACE) delete pod iceberg-rest-namespace --ignore-not-found=true >/dev/null; \
		kubectl -n $(ICEBERG_DEMO_NAMESPACE) run iceberg-rest-namespace --restart=Never \
		  --image=curlimages/curl:8.6.0 \
		  --env REST_ENDPOINT="$$rest_endpoint" \
		  --env ICEBERG_NAMESPACE="$(ICEBERG_DEMO_TABLE_NAMESPACE)" -- \
		  sh -c '\''set -e; \
		    for i in $$(seq 1 10); do \
		      resp=$$(curl -sS --connect-timeout 3 --max-time 10 -X POST -H "Content-Type: application/json" -d "{\"namespace\":[\"$${ICEBERG_NAMESPACE}\"]}" "$${REST_ENDPOINT}/v1/namespaces" -w "\n%{http_code}" || true); \
		      code=$$(printf "%s" "$$resp" | tail -n 1); \
		      body=$$(printf "%s" "$$resp" | sed "$$d"); \
		      if [ "$$code" = "200" ] || [ "$$code" = "409" ]; then \
		        echo "namespace create ok ($$code)"; \
		        exit 0; \
		      fi; \
		      echo "namespace create failed (attempt $$i): $$code"; \
		      if [ -n "$$body" ]; then echo "$$body"; fi; \
		      sleep 3; \
		    done; \
		    exit 1'\''; \
		for i in $$(seq 1 40); do \
			phase=$$(kubectl -n $(ICEBERG_DEMO_NAMESPACE) get pod iceberg-rest-namespace -o jsonpath="{.status.phase}" 2>/dev/null || true); \
			if [ "$$phase" = "Succeeded" ]; then \
				break; \
			fi; \
			if [ "$$phase" = "Failed" ]; then \
				kubectl -n $(ICEBERG_DEMO_NAMESPACE) logs pod/iceberg-rest-namespace --tail=200 || true; \
				kubectl -n $(ICEBERG_DEMO_NAMESPACE) describe pod/iceberg-rest-namespace || true; \
				exit 1; \
			fi; \
			sleep 3; \
		done; \
		if ! kubectl -n $(ICEBERG_DEMO_NAMESPACE) get pod iceberg-rest-namespace -o jsonpath="{.status.phase}" 2>/dev/null | grep -q Succeeded; then \
			kubectl -n $(ICEBERG_DEMO_NAMESPACE) logs pod/iceberg-rest-namespace --tail=200 || true; \
			kubectl -n $(ICEBERG_DEMO_NAMESPACE) describe pod/iceberg-rest-namespace || true; \
			exit 1; \
		fi; \
		kubectl -n $(ICEBERG_DEMO_NAMESPACE) logs pod/iceberg-rest-namespace --tail=200 || true; \
		kubectl -n $(ICEBERG_DEMO_NAMESPACE) delete pod iceberg-rest-namespace --ignore-not-found=true >/dev/null; \
	'
	@bash -c 'set -euo pipefail; \
		PROC_IMAGE="$(ICEBERG_PROCESSOR_IMAGE)"; \
		PROC_REPO=$${PROC_IMAGE%:*}; PROC_TAG=$${PROC_IMAGE##*:}; \
		if [ -z "$$PROC_REPO" ] || [ -z "$$PROC_TAG" ]; then \
			echo "invalid ICEBERG_PROCESSOR_IMAGE: $$PROC_IMAGE"; \
			exit 1; \
		fi; \
		helm upgrade --install $(ICEBERG_PROCESSOR_RELEASE) addons/processors/iceberg-processor/deploy/helm/iceberg-processor \
		  --namespace $(ICEBERG_DEMO_NAMESPACE) \
		  --set fullnameOverride=$(ICEBERG_PROCESSOR_RELEASE) \
		  --set image.repository=$$PROC_REPO \
		  --set image.tag=$$PROC_TAG \
		  --set s3.credentialsSecretRef=iceberg-s3-credentials \
		  --set config.s3.bucket=$(ICEBERG_WAREHOUSE_BUCKET) \
		  --set config.s3.namespace=$(KAFSCALE_DEMO_NAMESPACE) \
		  --set config.s3.region=$(MINIO_REGION) \
		  --set config.s3.endpoint=http://minio.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:9000 \
		  --set config.s3.path_style=true \
		  --set config.iceberg.catalog.uri=http://iceberg-rest.$(ICEBERG_DEMO_NAMESPACE).svc.cluster.local:$(ICEBERG_REST_PORT) \
		  --set config.iceberg.warehouse=s3://$(ICEBERG_WAREHOUSE_BUCKET)/$(ICEBERG_WAREHOUSE_PREFIX) \
		  --set-string config.schema.registry.base_url= \
		  --set config.offsets.backend=etcd \
		  --set config.etcd.endpoints[0]=http://kafscale-etcd-client.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:2379 \
		  --set env[0].name=ICEBERG_PROCESSOR_REST_DEBUG \
		  --set-string env[0].value=$(ICEBERG_PROCESSOR_REST_DEBUG) \
		  --set config.mappings[0].topic=$(ICEBERG_DEMO_TOPIC) \
		  --set config.mappings[0].table=$(ICEBERG_DEMO_TABLE) \
		  --set config.mappings[0].mode=append \
		  --set config.mappings[0].schema.source=none \
		  --set config.mappings[0].create_table_if_missing=true; \
	'
	@bash -c 'set -euo pipefail; \
		DEPLOY=$$(kubectl -n $(ICEBERG_DEMO_NAMESPACE) get deployments -l app.kubernetes.io/name=iceberg-processor,app.kubernetes.io/instance=$(ICEBERG_PROCESSOR_RELEASE) -o name | head -n 1); \
		if [ -z "$$DEPLOY" ]; then \
			echo "iceberg-processor deployment not found for release $(ICEBERG_PROCESSOR_RELEASE)"; \
			kubectl -n $(ICEBERG_DEMO_NAMESPACE) get deployments; \
			exit 1; \
		fi; \
		kubectl -n $(ICEBERG_DEMO_NAMESPACE) rollout status $$DEPLOY --timeout=180s; \
		echo "iceberg-processor startup logs:"; \
		kubectl -n $(ICEBERG_DEMO_NAMESPACE) logs $$DEPLOY --tail=120 || true; \
	'
	@kubectl -n $(ICEBERG_DEMO_NAMESPACE) delete job iceberg-demo-producer --ignore-not-found=true
	@bash -c $$'cat <<EOF | kubectl apply -f -\napiVersion: batch/v1\nkind: Job\nmetadata:\n  name: iceberg-demo-producer\n  namespace: $(ICEBERG_DEMO_NAMESPACE)\nspec:\n  ttlSecondsAfterFinished: 300\n  backoffLimit: 0\n  template:\n    spec:\n      restartPolicy: Never\n      containers:\n        - name: producer\n          image: $(E2E_CLIENT_IMAGE)\n          command:\n            - sh\n            - -c\n          args:\n            - |\n              set -e\n              KAFSCALE_E2E_MODE=probe /usr/local/bin/kafscale-e2e-client\n              KAFSCALE_E2E_MODE=produce /usr/local/bin/kafscale-e2e-client\n          env:\n            - name: KAFSCALE_E2E_ADDRS\n              value: kafscale-broker.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:9092\n            - name: KAFSCALE_E2E_BROKER_ADDR\n              value: kafscale-broker.$(KAFSCALE_DEMO_NAMESPACE).svc.cluster.local:9092\n            - name: KAFSCALE_E2E_TOPIC\n              value: $(ICEBERG_DEMO_TOPIC)\n            - name: KAFSCALE_E2E_COUNT\n              value: \"$(ICEBERG_DEMO_RECORDS)\"\n            - name: KAFSCALE_E2E_PROBE_RETRIES\n              value: \"20\"\n            - name: KAFSCALE_E2E_PROBE_SLEEP_MS\n              value: \"500\"\nEOF'
	@bash -c 'set -euo pipefail; \
		if ! kubectl -n $(ICEBERG_DEMO_NAMESPACE) wait --for=condition=complete job/iceberg-demo-producer --timeout=150s; then \
			kubectl -n $(ICEBERG_DEMO_NAMESPACE) get pods -l job-name=iceberg-demo-producer -o wide; \
			pod=$$(kubectl -n $(ICEBERG_DEMO_NAMESPACE) get pods -l job-name=iceberg-demo-producer -o jsonpath="{.items[0].metadata.name}" 2>/dev/null || true); \
			if [ -n "$$pod" ]; then \
				kubectl -n $(ICEBERG_DEMO_NAMESPACE) logs "$$pod" --all-containers=true --tail=200; \
			fi; \
			kubectl -n $(ICEBERG_DEMO_NAMESPACE) describe job/iceberg-demo-producer; \
			exit 1; \
		fi; \
	'
	@bash -c 'set -euo pipefail; \
		echo "post-producer S3 check (topic $(ICEBERG_DEMO_TOPIC)):"; \
		kubectl -n $(ICEBERG_DEMO_NAMESPACE) logs job/iceberg-demo-producer --tail=200 || true; \
		kubectl -n $(KAFSCALE_DEMO_NAMESPACE) exec deploy/minio -- sh -c "ls /data/$(ICEBERG_WAREHOUSE_BUCKET)/$(KAFSCALE_DEMO_NAMESPACE)/$(ICEBERG_DEMO_TOPIC) 2>/dev/null && ls /data/$(ICEBERG_WAREHOUSE_BUCKET)/$(KAFSCALE_DEMO_NAMESPACE)/$(ICEBERG_DEMO_TOPIC)/* 2>/dev/null | head -n 20" || true; \
	'
	@bash -c 'set -euo pipefail; \
		echo "waiting for segments + iceberg data files..."; \
		for i in $$(seq 1 30); do \
			seg=$$(kubectl -n $(KAFSCALE_DEMO_NAMESPACE) exec deploy/minio -- sh -c "ls /data/$(ICEBERG_WAREHOUSE_BUCKET)/$(KAFSCALE_DEMO_NAMESPACE)/$(ICEBERG_DEMO_TOPIC)/*/segment-*.kfs 2>/dev/null | head -n 1"); \
			data=$$(kubectl -n $(KAFSCALE_DEMO_NAMESPACE) exec deploy/minio -- sh -c "ls /data/$(ICEBERG_WAREHOUSE_BUCKET)/$(ICEBERG_WAREHOUSE_PREFIX)/$(ICEBERG_DEMO_TABLE_NAMESPACE)/$(ICEBERG_DEMO_TABLE_NAME)/data/*.parquet 2>/dev/null | head -n 1"); \
			if [ -n "$$seg" ] && [ -n "$$data" ]; then \
				break; \
			fi; \
			sleep 3; \
		done; \
		if [ -z "$$seg" ]; then \
			echo "no kafka segments found for topic $(ICEBERG_DEMO_TOPIC) under s3://$(ICEBERG_WAREHOUSE_BUCKET)/$(KAFSCALE_DEMO_NAMESPACE)"; \
			echo "bucket contents (top level):"; \
			kubectl -n $(KAFSCALE_DEMO_NAMESPACE) exec deploy/minio -- sh -c "ls /data/$(ICEBERG_WAREHOUSE_BUCKET) 2>/dev/null | head -n 50" || true; \
			echo "namespace contents:"; \
			kubectl -n $(KAFSCALE_DEMO_NAMESPACE) exec deploy/minio -- sh -c "ls /data/$(ICEBERG_WAREHOUSE_BUCKET)/$(KAFSCALE_DEMO_NAMESPACE) 2>/dev/null | head -n 50" || true; \
			echo "topic path listing:"; \
			kubectl -n $(KAFSCALE_DEMO_NAMESPACE) exec deploy/minio -- sh -c "ls -R /data/$(ICEBERG_WAREHOUSE_BUCKET)/$(KAFSCALE_DEMO_NAMESPACE)/$(ICEBERG_DEMO_TOPIC) 2>/dev/null | head -n 200" || true; \
			echo "segment files found anywhere in bucket:"; \
			kubectl -n $(KAFSCALE_DEMO_NAMESPACE) exec deploy/minio -- sh -c "ls /data/$(ICEBERG_WAREHOUSE_BUCKET)/*/*/*/segment-*.kfs 2>/dev/null | head -n 50" || true; \
			exit 1; \
		fi; \
		if [ -z "$$data" ]; then \
			echo "no iceberg parquet data files found"; \
			kubectl -n $(ICEBERG_DEMO_NAMESPACE) logs deployment/iceberg-rest --tail=200; \
			kubectl -n $(ICEBERG_DEMO_NAMESPACE) logs deployment/$(ICEBERG_PROCESSOR_RELEASE) --tail=200; \
			exit 1; \
		fi; \
	'
	@bash -c 'set -euo pipefail; \
		if ! kubectl -n $(KAFSCALE_DEMO_NAMESPACE) exec deploy/minio -- sh -c "ls /data/$(ICEBERG_WAREHOUSE_BUCKET) >/dev/null 2>&1"; then \
			echo "warehouse bucket missing: $(ICEBERG_WAREHOUSE_BUCKET)"; \
			exit 1; \
		fi; \
		if ! kubectl -n $(KAFSCALE_DEMO_NAMESPACE) exec deploy/minio -- sh -c "ls /data/$(ICEBERG_WAREHOUSE_BUCKET)/$(ICEBERG_WAREHOUSE_PREFIX) >/dev/null 2>&1"; then \
			echo "iceberg warehouse prefix missing: s3://$(ICEBERG_WAREHOUSE_BUCKET)/$(ICEBERG_WAREHOUSE_PREFIX)"; \
			kubectl -n $(ICEBERG_DEMO_NAMESPACE) logs deployment/iceberg-rest --tail=200; \
			kubectl -n $(ICEBERG_DEMO_NAMESPACE) logs deployment/$(ICEBERG_PROCESSOR_RELEASE) --tail=200; \
			exit 1; \
		fi; \
		echo "iceberg data files (count):"; \
		kubectl -n $(KAFSCALE_DEMO_NAMESPACE) exec deploy/minio -- sh -c "ls /data/$(ICEBERG_WAREHOUSE_BUCKET)/$(ICEBERG_WAREHOUSE_PREFIX)/$(ICEBERG_DEMO_TABLE_NAMESPACE)/$(ICEBERG_DEMO_TABLE_NAME)/data/*.parquet 2>/dev/null | wc -l"; \
		meta=$$(kubectl -n $(KAFSCALE_DEMO_NAMESPACE) exec deploy/minio -- sh -c "ls -1 /data/$(ICEBERG_WAREHOUSE_BUCKET)/$(ICEBERG_WAREHOUSE_PREFIX)/$(ICEBERG_DEMO_TABLE_NAMESPACE)/$(ICEBERG_DEMO_TABLE_NAME)/metadata/*.metadata.json 2>/dev/null | head -n 1"); \
		meta=$${meta%:}; \
		if [ -n "$$meta" ]; then \
			echo "iceberg metadata sample: $$meta"; \
		fi; \
	'
	@kubectl -n $(ICEBERG_DEMO_NAMESPACE) run iceberg-rest-curl --rm -i --restart=Never \
	  --image=alpine:3.19 -- \
	  sh -c 'set -e; \
	    apk add --no-cache curl jq >/dev/null; \
	    CURL="curl -fsS --max-time 10 --connect-timeout 5"; \
	    echo "iceberg catalog namespaces:"; \
	    $$CURL http://iceberg-rest.$(ICEBERG_DEMO_NAMESPACE).svc.cluster.local:$(ICEBERG_REST_PORT)/v1/namespaces | \
	      jq -r "[.namespaces[] | join(\".\")] | join(\", \")"; \
	    echo; \
	    echo "iceberg catalog tables (namespace $(ICEBERG_DEMO_TABLE_NAMESPACE)):"; \
	    $$CURL http://iceberg-rest.$(ICEBERG_DEMO_NAMESPACE).svc.cluster.local:$(ICEBERG_REST_PORT)/v1/namespaces/$(ICEBERG_DEMO_TABLE_NAMESPACE)/tables | \
	      jq -r "[.identifiers[].name] | join(\", \")"; \
	    echo; \
	    echo "iceberg catalog table (namespace $(ICEBERG_DEMO_TABLE_NAMESPACE)):"; \
	    table=$$($$CURL http://iceberg-rest.$(ICEBERG_DEMO_NAMESPACE).svc.cluster.local:$(ICEBERG_REST_PORT)/v1/namespaces/$(ICEBERG_DEMO_TABLE_NAMESPACE)/tables/$(ICEBERG_DEMO_TABLE_NAME)); \
	    echo "iceberg table schema (name, type):"; \
	    echo "$$table" | jq -r ".metadata.schemas[-1].fields[] | \"\\(.name)\\t\\(.type)\"" | awk '"'"'BEGIN{printf "%-24s %s\n","column","type"} {printf "%-24s %s\n",$$1,$$2}'"'"'; \
	    echo; \
	    echo "iceberg table summary:"; \
	    echo "$$table" | jq -r "\"data_files\\t\" + (.metadata.snapshots[-1].summary[\"total-data-files\"] // \"0\"), \
	      \"records\\t\" + (.metadata.snapshots[-1].summary[\"total-records\"] // \"0\"), \
	      \"data_path\\t\" + (.metadata.properties[\"write.data.path\"] // \"\"), \
	      \"metadata_path\\t\" + (.metadata.properties[\"write.metadata.path\"] // \"\"), \
	      \"snapshot_id\\t\" + (.metadata[\"current-snapshot-id\"]|tostring)" | \
	      awk '"'"'BEGIN{printf "%-14s %s\n","metric","value"} {printf "%-14s %s\n",$$1,$$2}'"'"'; \
	    echo; \
	    echo "iceberg table properties (selected):"; \
	    echo "$$table" | jq -r ".metadata.properties | to_entries[] | select(.key|test(\"^(write\\\\.|commit\\\\.|format-version)\")) | \"\\(.key)=\\(.value)\"" | sort; \
	    echo; \
	    echo "iceberg snapshot summary (latest):"; \
	    echo "$$table" | jq -r ".metadata.snapshots[-1].summary" | jq; \
	    echo; \
	    if echo "$$table" | jq -e ".metadata.snapshots[-1].summary[\"total-records\"]" >/dev/null; then \
	      expected=$(ICEBERG_DEMO_RECORDS); \
	      got=$$(echo "$$table" | jq -r ".metadata.snapshots[-1].summary[\"total-records\"]"); \
	      echo "total-records: $$got (expected $$expected)"; \
	    fi; \
	    echo; \
	    echo "iceberg table schema (json):"; \
	    echo "$$table" | jq -r ".metadata.schemas[-1]" | jq;'

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
