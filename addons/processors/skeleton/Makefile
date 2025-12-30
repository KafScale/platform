SHELL := /bin/sh

BINARY := skeleton-processor
BUILD_DIR := bin

.PHONY: build test clean docker-build

build:
	mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/processor

test:
	go test ./...

clean:
	rm -rf $(BUILD_DIR)

docker-build:
	docker build -t $(BINARY):dev .
