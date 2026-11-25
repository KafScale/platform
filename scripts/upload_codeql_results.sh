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

set -euo pipefail
: "${CODEQL_PATH:?CodeQL executable required}"
: "${GITHUB_TOKEN:?GitHub token required}"
: "${GITHUB_REPOSITORY:?GitHub repository required}"
: "${GITHUB_REF:?Analyzed ref required}"
: "${GITHUB_SHA:?Analyzed commit required}"
: "${GITHUB_WORKSPACE:?Checkout path required}"
results_dir="${1:?SARIF directory required}"
shopt -s nullglob
reports=("$results_dir"/*.sarif)
if [ "${#reports[@]}" -ne 1 ]; then
  echo "Expected one SARIF report for this language job, found ${#reports[@]}." >&2
  exit 1
fi
# Use the official CLI so each retry is an upload of the identical analysis,
# without rerunning queries or bypassing the Action's per-job category guard.
# Processing must succeed: do not pass --no-wait-for-processing.
for attempt in 1 2 3; do
  echo "Uploading and validating CodeQL results (attempt $attempt/3)."
  if "$CODEQL_PATH" github upload-results \
    --sarif="${reports[0]}" \
    --repository="$GITHUB_REPOSITORY" \
    --ref="$GITHUB_REF" \
    --commit="$GITHUB_SHA" \
    --checkout-path="$GITHUB_WORKSPACE" \
    --github-url="${GITHUB_SERVER_URL:-https://github.com}" \
    --wait-for-processing-timeout=180; then
    exit 0
  fi
  if [ "$attempt" -lt 3 ]; then
    sleep "$((attempt * 15))"
  fi
done
echo "CodeQL upload or processing failed after three attempts." >&2
exit 1
