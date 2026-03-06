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

.PHONY: up down logs ps restart health test-upload test-download clean help

REGISTRY ?= 192.168.0.131:5100
TAG ?= dev

up: ## Start all services
	REGISTRY=$(REGISTRY) TAG=$(TAG) docker-compose up -d

down: ## Stop all services
	docker-compose down

logs: ## View logs (follow mode)
	docker-compose logs -f

ps: ## Show service status
	docker-compose ps

restart: ## Restart all services
	docker-compose restart

health: ## Check service health
	@echo "=== Health Checks ==="
	@echo -n "etcd:      "; curl -s http://localhost:2379/health | head -c 50 || echo "FAIL"
	@echo -n "minio:     "; curl -s http://localhost:9000/minio/health/live || echo "FAIL"
	@echo -n "lfs-proxy: "; curl -s http://localhost:9094/readyz || echo "FAIL"
	@echo -n "broker:    "; nc -z localhost 9092 && echo "OK" || echo "FAIL"

test-upload: ## Test LFS upload (creates 1KB test file)
	@echo "Uploading test file..."
	@dd if=/dev/urandom bs=1024 count=1 2>/dev/null | \
		curl -s -X POST http://localhost:8080/lfs/produce \
		-H "X-Kafka-Topic: test-uploads" \
		-H "Content-Type: application/octet-stream" \
		--data-binary @- | jq .

test-download: ## Test LFS download (requires KEY variable)
	@if [ -z "$(KEY)" ]; then echo "Usage: make test-download KEY=default/topic/lfs/..."; exit 1; fi
	@curl -s -X POST http://localhost:8080/lfs/download \
		-H "Content-Type: application/json" \
		-d '{"bucket":"kafscale","key":"$(KEY)","mode":"presign"}' | jq .

clean: ## Stop services and remove volumes
	docker-compose down -v

registry-check: ## Check local registry
	@echo "=== Registry Catalog ==="
	@curl -s http://$(REGISTRY)/v2/_catalog | jq .

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "%-15s %s\n", $$1, $$2}'
