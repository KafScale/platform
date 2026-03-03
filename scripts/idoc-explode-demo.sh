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

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUTPUT_DIR=${KAFSCALE_IDOC_OUTPUT_DIR:-"${ROOT_DIR}/.build/idoc-output"}
INPUT_XML=${KAFSCALE_IDOC_INPUT_XML:-"${ROOT_DIR}/examples/tasks/LFS/idoc-sample.xml"}

# Clean previous output
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

export KAFSCALE_IDOC_OUTPUT_DIR="${OUTPUT_DIR}"

echo "=== KafScale IDoc Exploder Demo ==="
echo ""
echo "Input:  ${INPUT_XML}"
echo "Output: ${OUTPUT_DIR}"
echo ""

"${ROOT_DIR}/bin/idoc-explode" -input "${INPUT_XML}"

echo ""
echo "--- Topic files produced ---"
echo ""

total_records=0
for f in "${OUTPUT_DIR}"/*.jsonl; do
  [ -f "$f" ] || continue
  topic=$(basename "$f" .jsonl)
  count=$(wc -l < "$f" | tr -d ' ')
  total_records=$((total_records + count))
  printf "  %-22s %3d records\n" "${topic}" "${count}"
done

echo ""
echo "Total: ${total_records} records across $(ls -1 "${OUTPUT_DIR}"/*.jsonl 2>/dev/null | wc -l | tr -d ' ') topics"
echo ""

# Show a preview of each topic
echo "--- Preview ---"
echo ""
for f in "${OUTPUT_DIR}"/*.jsonl; do
  [ -f "$f" ] || continue
  topic=$(basename "$f" .jsonl)
  echo "${topic}:"
  # Pretty-print first 2 records per topic
  head -2 "$f" | while IFS= read -r line; do
    if command -v python3 >/dev/null 2>&1; then
      echo "  $(echo "$line" | python3 -m json.tool --compact 2>/dev/null || echo "$line")"
    else
      echo "  ${line}"
    fi
  done
  count=$(wc -l < "$f" | tr -d ' ')
  if [ "$count" -gt 2 ]; then
    echo "  ... ($((count - 2)) more)"
  fi
  echo ""
done

echo "In production, each topic file maps to a Kafka topic."
echo "The LFS proxy would stream the original IDoc XML as a blob"
echo "and the exploder produces structured records per segment type."
