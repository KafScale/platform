#!/usr/bin/env bash
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

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/rc.sh"
sudo_cmd="${KSV_SUDO:-}"
$sudo_cmd docker rm -f kafscale-videos-apicurio >/dev/null 2>&1 || true
$sudo_cmd docker run -d --name kafscale-videos-apicurio -p 127.0.0.1:8085:8080 apicurio/apicurio-registry:3.0.6 >/dev/null
until curl -fs -o /dev/null http://127.0.0.1:8085/apis/ccompat/v7/subjects; do sleep 2; done
python3 demo/seed_apicurio.py
command -v kafscale-cli
