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

BROKER_SRCS := $(shell find cmd/broker pkg go.mod go.sum)
docker-build-broker: $(BROKER_SRCS) ## Build broker container image
	docker build -t $(BROKER_IMAGE) -f deploy/docker/broker.Dockerfile .

OPERATOR_SRCS := $(shell find cmd/operator pkg/operator api config go.mod go.sum)
docker-build-operator: $(OPERATOR_SRCS) ## Build operator container image
	docker build -t $(OPERATOR_IMAGE) -f deploy/docker/operator.Dockerfile .

CONSOLE_SRCS := $(shell find cmd/console ui go.mod go.sum)
docker-build-console: $(CONSOLE_SRCS) ## Build console container image
	docker build -t $(CONSOLE_IMAGE) -f deploy/docker/console.Dockerfile .

docker-clean: ## Remove local dev images and prune dangling Docker data
	-docker image rm -f $(BROKER_IMAGE) $(OPERATOR_IMAGE) $(CONSOLE_IMAGE)
	docker system prune --force --volumes

stop-containers: ## Stop lingering e2e containers (MinIO + kind control planes)
	-ids=$$(docker ps -q --filter "name=kafscale-minio"); if [ -n "$$ids" ]; then docker stop $$ids; fi
	-ids=$$(docker ps -q --filter "name=kafscale-e2e"); if [ -n "$$ids" ]; then docker stop $$ids; fi
	-pkill -f "cmd/broker"
	-pkill -f "cmd/operator"
	-pkill -f "cmd/console"

test-e2e: ## Run end-to-end tests (requires Docker; operator suite also needs kind/kubectl/helm). Run `make docker-build` first if code changed.
	KAFSCALE_E2E=1 go test -tags=e2e ./test/e2e -v

tidy:
	go mod tidy

lint:
	golangci-lint run

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "%-20s %s\n", $$1, $$2}'
