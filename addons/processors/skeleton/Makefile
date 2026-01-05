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

SHELL := /bin/sh

BINARY := skeleton-processor
BUILD_DIR := bin
IMAGE ?= $(BINARY):dev
DOCKER_BUILD_ARGS ?=

.PHONY: build test clean docker-build

build:
	mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/processor

test:
	go test ./...

clean:
	rm -rf $(BUILD_DIR)

docker-build:
	docker build $(DOCKER_BUILD_ARGS) -t $(IMAGE) .
