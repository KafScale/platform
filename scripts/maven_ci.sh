#!/usr/bin/env bash
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

# Retry only HTTP transfers, not Maven goals or test failures. The two prefixes
# support Resolver 1.x (Maven 3.9) and Resolver 2.x (newer Maven versions).
# https://maven.apache.org/resolver/configuration.html
set -euo pipefail
# Use the same repository identity on the host and in schema containers so a
# restored cache stays usable. Tests may supply a local HTTP fixture instead.
settings="${MAVEN_SETTINGS:-$(dirname "$0")/../.github/maven-settings.xml}"
options=(--settings "$settings" --batch-mode --no-transfer-progress --update-snapshots
  -Daether.connector.connectTimeout=10000
  -Daether.connector.requestTimeout=60000)
for prefix in aether.connector.http aether.transport.http; do
  options+=(
    "-D${prefix}.retryHandler.count=3"
    "-D${prefix}.retryHandler.interval=10000"
    "-D${prefix}.retryHandler.intervalMax=30000"
    "-D${prefix}.retryHandler.serviceUnavailable=429,502,503,504"
  )
done
options+=(-Daether.transport.http.connectTimeout=10000
  -Daether.transport.http.requestTimeout=60000)
exec mvn "${options[@]}" "$@"
