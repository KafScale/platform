SHELL := /bin/sh

BINARY := iceberg-processor
BUILD_DIR := bin
IMAGE ?= $(BINARY):dev

DOCKER_BUILD_CMD := $(shell \
	if command -v docker >/dev/null 2>&1 && docker buildx version >/dev/null 2>&1; then \
		echo "docker buildx build --load"; \
	elif command -v docker-buildx >/dev/null 2>&1 && docker-buildx version >/dev/null 2>&1; then \
		echo "docker-buildx build --load"; \
	else \
		echo "DOCKER_BUILDKIT=1 docker build"; \
	fi)

.PHONY: build test test-strict clean docker-build

build:
	mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/processor

test:
	go test ./...

test-strict:
	go vet ./...
	GOCACHE=$(PWD)/.gocache go test -race ./...

clean:
	rm -rf $(BUILD_DIR)

docker-build:
	$(DOCKER_BUILD_CMD) -t $(IMAGE) .
