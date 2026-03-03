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

.PHONY: install-sdk run run-demo run-small run-midsize run-large run-all help

SDK_DIR := ../../lfs-client-sdk/python
VENV := $(SDK_DIR)/.venv
PYTHON := $(VENV)/bin/python

install-sdk: ## Create venv and install the Python LFS SDK
	cd $(SDK_DIR) && python -m venv .venv && . .venv/bin/activate && pip install -e .

run: ## Run the E71 demo (all video sizes by default)
	$(PYTHON) demo.py

run-small: ## Run with small video only (1 MB)
	VIDEO_SIZES=small $(PYTHON) demo.py

run-midsize: ## Run with midsize video only (50 MB)
	VIDEO_SIZES=midsize $(PYTHON) demo.py

run-large: ## Run with large video only (200 MB)
	VIDEO_SIZES=large $(PYTHON) demo.py

run-all: ## Run with all video sizes (1 MB, 50 MB, 200 MB)
	VIDEO_SIZES=small,midsize,large $(PYTHON) demo.py

run-demo: install-sdk run ## Install SDK and run the demo

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "%-20s %s\n", $$1, $$2}'
