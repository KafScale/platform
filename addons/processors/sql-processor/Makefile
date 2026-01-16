# Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

SHELL := /bin/sh

BINARY := sql-processor
BUILD_DIR := bin
IMAGE ?= $(BINARY):dev
DOCKER_BUILD_ARGS ?=

MINIO_CONTAINER ?= kafsql-minio
MINIO_IMAGE ?= quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z
MINIO_PORT ?= 9000
MINIO_CONSOLE_PORT ?= 9001
MINIO_ROOT_USER ?= minioadmin
MINIO_ROOT_PASSWORD ?= minioadmin
MINIO_BUCKET ?= kafsql-test
MIN_COVERAGE ?= 60

.PHONY: build test test-minio coverage clean docker-build ensure-minio start-minio stop-minio

build:
	mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/processor

test:
	go vet ./...
	go test -race ./...

test-minio: ensure-minio
	KAFSQL_MINIO_ENDPOINT=http://localhost:$(MINIO_PORT) \
	KAFSQL_MINIO_ACCESS_KEY=$(MINIO_ROOT_USER) \
	KAFSQL_MINIO_SECRET_KEY=$(MINIO_ROOT_PASSWORD) \
	KAFSQL_MINIO_BUCKET=$(MINIO_BUCKET) \
	go test -race ./...

coverage:
	sh ../../../hack/check_coverage.sh $(MIN_COVERAGE)

clean:
	rm -rf $(BUILD_DIR)

docker-build:
	docker build $(DOCKER_BUILD_ARGS) -t $(IMAGE) .

ensure-minio:
	@command -v docker >/dev/null 2>&1 || { echo "docker is required for MinIO tests"; exit 1; }
	@if docker ps --format '{{.Names}}' | grep -q '^$(MINIO_CONTAINER)$$'; then \
		if ! lsof -ti :$(MINIO_PORT) >/dev/null 2>&1; then \
			echo "MinIO container is running but port $(MINIO_PORT) is unavailable; restarting"; \
			docker rm -f $(MINIO_CONTAINER) >/dev/null; \
			$(MAKE) start-minio >/dev/null; \
		else \
			echo "MinIO container $(MINIO_CONTAINER) already running"; \
		fi; \
	else \
		if lsof -ti :$(MINIO_PORT) >/dev/null 2>&1; then \
			echo "MinIO port $(MINIO_PORT) already in use; using existing MinIO"; \
		else \
			$(MAKE) start-minio >/dev/null; \
		fi; \
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

stop-minio:
	-ids=$$(docker ps -q --filter "name=$(MINIO_CONTAINER)"); if [ -n "$$ids" ]; then docker stop $$ids >/dev/null; fi
