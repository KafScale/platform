.PHONY: proto build test tidy lint generate docker-build docker-build-broker docker-build-operator docker-build-console

REGISTRY ?= ghcr.io/novatechflow
BROKER_IMAGE ?= $(REGISTRY)/kafscale-broker:dev
OPERATOR_IMAGE ?= $(REGISTRY)/kafscale-operator:dev
CONSOLE_IMAGE ?= $(REGISTRY)/kafscale-console:dev

proto: ## Generate protobuf + gRPC stubs
	buf generate

generate: proto

build: ## Build all binaries
	go build ./...

test: ## Run unit tests
	go test ./...

docker-build: docker-build-broker docker-build-operator docker-build-console ## Build all container images

docker-build-broker: ## Build broker container image
	docker build -t $(BROKER_IMAGE) -f deploy/docker/broker.Dockerfile .

docker-build-operator: ## Build operator container image
	docker build -t $(OPERATOR_IMAGE) -f deploy/docker/operator.Dockerfile .

docker-build-console: ## Build console container image
	docker build -t $(CONSOLE_IMAGE) -f deploy/docker/console.Dockerfile .

docker-clean: ## Remove local dev images and prune dangling Docker data
	-docker image rm -f $(BROKER_IMAGE) $(OPERATOR_IMAGE) $(CONSOLE_IMAGE)
	docker system prune --force --volumes

stop-containers: ## Stop lingering e2e containers (MinIO + kind control planes)
	-ids=$$(docker ps -q --filter "name=kafscale-minio"); if [ -n "$$ids" ]; then docker stop $$ids; fi
	-ids=$$(docker ps -q --filter "name=kafscale-e2e"); if [ -n "$$ids" ]; then docker stop $$ids; fi

test-e2e: ## Run end-to-end tests (requires Docker; operator suite also needs kind/kubectl/helm). Run `make docker-build` first if code changed.
	KAFSCALE_E2E=1 go test -tags=e2e ./test/e2e -v

tidy:
	go mod tidy

lint:
	golangci-lint run

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "%-20s %s\n", $$1, $$2}'
