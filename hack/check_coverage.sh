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

MIN_COVERAGE="${1:-45}"

# Packages excluded from coverage: generated code, test utilities, demo tools,
# embed-only wrappers, e2e tests, CLI entry points, and addon/skeleton packages.
EXCLUDE=(
  "github.com/KafScale/platform/api/v1alpha1"
  "github.com/KafScale/platform/pkg/gen/"
  "github.com/KafScale/platform/internal/testutil"
  "github.com/KafScale/platform/ui"
  "github.com/KafScale/platform/cmd/"
  "github.com/KafScale/platform/test"
  "github.com/KafScale/platform/addons/"
)

# Build package list excluding non-testable packages.
PKGS=$(go list ./... | grep -v -F "$(printf '%s\n' "${EXCLUDE[@]}")")

go test -coverprofile=coverage.out $PKGS

total=$(go tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/,"",$3); print $3}')
if [ -z "$total" ]; then
  echo "coverage: failed to compute total coverage"
  exit 1
fi

awk -v total="$total" -v min="$MIN_COVERAGE" 'BEGIN { if (total + 0 < min) exit 1 }'
echo "coverage: ${total}% (min ${MIN_COVERAGE}%)"
