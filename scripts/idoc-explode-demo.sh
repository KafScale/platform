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

# MinIO / S3 config (passed from Makefile or env)
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_BUCKET="${MINIO_BUCKET:-kafscale}"
MINIO_REGION="${MINIO_REGION:-us-east-1}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
S3_ENDPOINT="http://127.0.0.1:${MINIO_PORT}"

# LFS object key (simulates what the LFS proxy would generate)
LFS_NAMESPACE="idoc-demo"
LFS_TOPIC="idoc-inbound"
LFS_S3_KEY="lfs/${LFS_NAMESPACE}/${LFS_TOPIC}/0/0-idoc-sample.xml"

# Clean previous output
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

echo "=== KafScale IDoc Exploder Demo (LFS Pipeline) ==="
echo ""
echo "This demo exercises the full LFS data flow:"
echo "  1. Upload IDoc XML to S3 (simulating LFS proxy blob upload)"
echo "  2. Create an LFS envelope (pointer record)"
echo "  3. Feed envelope to idoc-explode, which resolves the blob from S3"
echo "  4. Explode IDoc segments into topic-specific JSONL streams"
echo ""

# ---- Step 1: Upload IDoc XML to MinIO as an LFS blob ----
echo "--- Step 1: Upload IDoc XML to S3 ---"
echo ""
echo "  Endpoint: ${S3_ENDPOINT}"
echo "  Bucket:   ${MINIO_BUCKET}"
echo "  Key:      ${LFS_S3_KEY}"
echo ""

# Ensure bucket exists
if ! AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}" \
     AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}" \
     AWS_DEFAULT_REGION="${MINIO_REGION}" \
     AWS_EC2_METADATA_DISABLED=true \
     aws s3api head-bucket --bucket "${MINIO_BUCKET}" \
       --endpoint-url "${S3_ENDPOINT}" 2>/dev/null; then
  AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}" \
  AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}" \
  AWS_DEFAULT_REGION="${MINIO_REGION}" \
  AWS_EC2_METADATA_DISABLED=true \
  aws s3api create-bucket --bucket "${MINIO_BUCKET}" \
    --endpoint-url "${S3_ENDPOINT}" >/dev/null 2>&1
  echo "  Created bucket: ${MINIO_BUCKET}"
fi

# Upload the IDoc XML
AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}" \
AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}" \
AWS_DEFAULT_REGION="${MINIO_REGION}" \
AWS_EC2_METADATA_DISABLED=true \
aws s3 cp "${INPUT_XML}" "s3://${MINIO_BUCKET}/${LFS_S3_KEY}" \
  --endpoint-url "${S3_ENDPOINT}" \
  --content-type "application/xml" >/dev/null

BLOB_SIZE=$(wc -c < "${INPUT_XML}" | tr -d ' ')
BLOB_CHECKSUM=$(shasum -a 256 "${INPUT_XML}" | cut -d' ' -f1)

echo "  Uploaded: $(basename "${INPUT_XML}") (${BLOB_SIZE} bytes)"
echo "  SHA-256:  ${BLOB_CHECKSUM}"
echo ""

# ---- Step 2: Create LFS envelope ----
echo "--- Step 2: Create LFS envelope (pointer record) ---"
echo ""

ENVELOPE=$(cat <<ENVJSON
{"kfs_lfs":1,"bucket":"${MINIO_BUCKET}","key":"${LFS_S3_KEY}","content_type":"application/xml","size":${BLOB_SIZE},"sha256":"${BLOB_CHECKSUM}"}
ENVJSON
)

ENVELOPE_FILE="${OUTPUT_DIR}/lfs-envelope.jsonl"
echo "${ENVELOPE}" > "${ENVELOPE_FILE}"
echo "  ${ENVELOPE}" | python3 -m json.tool 2>/dev/null || echo "  ${ENVELOPE}"
echo ""
echo "  In production, this envelope is what Kafka consumers receive."
echo "  The original XML blob stays in S3."
echo ""

# ---- Step 3: Resolve blob from S3 and explode ----
echo "--- Step 3: Resolve LFS blob and explode IDoc segments ---"
echo ""

export KAFSCALE_IDOC_OUTPUT_DIR="${OUTPUT_DIR}"
export KAFSCALE_LFS_PROXY_S3_BUCKET="${MINIO_BUCKET}"
export KAFSCALE_LFS_PROXY_S3_REGION="${MINIO_REGION}"
export KAFSCALE_LFS_PROXY_S3_ENDPOINT="${S3_ENDPOINT}"
export KAFSCALE_LFS_PROXY_S3_ACCESS_KEY="${MINIO_ROOT_USER}"
export KAFSCALE_LFS_PROXY_S3_SECRET_KEY="${MINIO_ROOT_PASSWORD}"
export KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE=true

"${ROOT_DIR}/bin/idoc-explode" -input "${ENVELOPE_FILE}"

echo ""

# ---- Step 4: Show results ----
echo "--- Step 4: Topic streams (segment mapping) ---"
echo ""

# Show the mapping config
echo "  Segment routing:"
echo "    E1EDP01, E1EDP19     -> idoc-items     (order line items)"
echo "    E1EDKA1              -> idoc-partners   (business partners)"
echo "    E1STATS              -> idoc-status     (processing status)"
echo "    E1EDK03              -> idoc-dates      (dates/deadlines)"
echo "    EDI_DC40, E1EDK01, . -> idoc-segments   (all segments)"
echo "    (root)               -> idoc-headers    (IDoc metadata)"
echo ""

total_records=0
for f in "${OUTPUT_DIR}"/*.jsonl; do
  [ -f "$f" ] || continue
  topic=$(basename "$f" .jsonl)
  [ "$topic" = "lfs-envelope" ] && continue
  count=$(wc -l < "$f" | tr -d ' ')
  total_records=$((total_records + count))
  printf "  %-22s %3d records\n" "${topic}" "${count}"
done

topic_count=$(ls -1 "${OUTPUT_DIR}"/*.jsonl 2>/dev/null | grep -cv lfs-envelope || true)
echo ""
echo "  Total: ${total_records} records across ${topic_count} topics"
echo ""

# Show a preview of key topics
echo "--- Preview ---"
echo ""
for topic in idoc-headers idoc-items idoc-partners idoc-dates idoc-status; do
  f="${OUTPUT_DIR}/${topic}.jsonl"
  [ -f "$f" ] || continue
  count=$(wc -l < "$f" | tr -d ' ')
  echo "${topic} (${count}):"
  head -3 "$f" | while IFS= read -r line; do
    echo "  $(echo "$line" | python3 -m json.tool --compact 2>/dev/null || echo "$line")"
  done
  if [ "$count" -gt 3 ]; then
    echo "  ... ($((count - 3)) more)"
  fi
  echo ""
done

echo "=== Demo complete ==="
echo ""
echo "  IDoc XML blob:    s3://${MINIO_BUCKET}/${LFS_S3_KEY}"
echo "  LFS envelope:     ${ENVELOPE_FILE}"
echo "  Topic streams:    ${OUTPUT_DIR}/"
echo ""
echo "  In production, the LFS proxy handles step 1-2 automatically."
echo "  The explode processor consumes envelopes from Kafka and"
echo "  produces structured records to downstream topics."
