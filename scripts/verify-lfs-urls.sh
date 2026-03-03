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

# Verify LFS pointer records by downloading each object and comparing SHA256.
# Supports multiple fetch modes: mc (MinIO client), aws (AWS CLI), curl (public only).
#
# Environment variables:
#   LFS_VERIFY_ENDPOINT    - S3 endpoint (default: http://localhost:9000)
#   LFS_VERIFY_ACCESS_KEY  - S3 access key (default: minioadmin)
#   LFS_VERIFY_SECRET_KEY  - S3 secret key (default: minioadmin)
#   LFS_VERIFY_MODE        - Fetch mode: mc|aws|curl|auto (default: auto)
#
# Usage:
#   # Local with port-forward (recommended):
#   kubectl -n kafscale-demo port-forward svc/minio 9000:9000 &
#   ./verify-lfs-urls.sh records.txt
#
#   # With explicit config:
#   LFS_VERIFY_ENDPOINT=http://localhost:9000 ./verify-lfs-urls.sh records.txt
#
set -euo pipefail

ENDPOINT="${LFS_VERIFY_ENDPOINT:-http://localhost:9000}"
ACCESS_KEY="${LFS_VERIFY_ACCESS_KEY:-minioadmin}"
SECRET_KEY="${LFS_VERIFY_SECRET_KEY:-minioadmin}"
MODE="${LFS_VERIFY_MODE:-auto}"
INPUT="${1:-}"

if [[ -z "${INPUT}" ]]; then
  echo "Usage: $0 <records-file>" >&2
  echo "  or:  cat records.txt | $0 -" >&2
  echo "" >&2
  echo "Environment variables:" >&2
  echo "  LFS_VERIFY_ENDPOINT    S3 endpoint (default: http://localhost:9000)" >&2
  echo "  LFS_VERIFY_ACCESS_KEY  S3 access key (default: minioadmin)" >&2
  echo "  LFS_VERIFY_SECRET_KEY  S3 secret key (default: minioadmin)" >&2
  echo "  LFS_VERIFY_MODE        Fetch mode: mc|aws|curl|auto (default: auto)" >&2
  exit 1
fi

if [[ "${INPUT}" == "-" ]]; then
  RECORDS="$(cat)"
else
  RECORDS="$(cat "${INPUT}")"
fi

if [[ -z "${RECORDS}" ]]; then
  echo "No records provided." >&2
  exit 1
fi

# Warn about in-cluster endpoints
if [[ "${ENDPOINT}" == *".svc.cluster.local"* ]]; then
  echo "WARNING: In-cluster endpoint detected: ${ENDPOINT}" >&2
  echo "This won't work from outside the cluster. Run port-forward first:" >&2
  echo "  kubectl -n kafscale-demo port-forward svc/minio 9000:9000" >&2
  echo "Then use: LFS_VERIFY_ENDPOINT=http://localhost:9000" >&2
  echo "" >&2
fi

export LFS_VERIFY_ENDPOINT="${ENDPOINT}"
export LFS_VERIFY_ACCESS_KEY="${ACCESS_KEY}"
export LFS_VERIFY_SECRET_KEY="${SECRET_KEY}"
export LFS_VERIFY_MODE="${MODE}"

PYCODE=$(cat <<'PYCODE'
import json
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile

endpoint = os.environ.get("LFS_VERIFY_ENDPOINT", "http://localhost:9000")
access_key = os.environ.get("LFS_VERIFY_ACCESS_KEY", "minioadmin")
secret_key = os.environ.get("LFS_VERIFY_SECRET_KEY", "minioadmin")
mode = os.environ.get("LFS_VERIFY_MODE", "auto")

# Auto-detect fetch mode
if mode == "auto":
    if shutil.which("mc"):
        mode = "mc"
    elif shutil.which("aws"):
        mode = "aws"
    else:
        mode = "curl"
        print(f"NOTE: Using curl (no mc/aws found). Auth not supported - only public buckets.", file=sys.stderr)

# Setup mc alias if using mc mode
mc_alias_setup = False
if mode == "mc":
    try:
        subprocess.run(
            ["mc", "alias", "set", "lfsverify", endpoint, access_key, secret_key],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        mc_alias_setup = True
    except Exception as e:
        print(f"WARNING: Failed to setup mc alias: {e}", file=sys.stderr)
        mode = "curl"

records = sys.stdin.read().splitlines()
json_re = re.compile(r"\{.*\}")
rows = []

for line in records:
    m = json_re.search(line)
    if not m:
        continue
    try:
        data = json.loads(m.group(0))
    except Exception:
        continue
    key = data.get("key", "")
    bucket = data.get("bucket", "")
    expected = data.get("sha256", "")
    if not key or not bucket or not expected:
        continue
    url = f"{endpoint}/{bucket}/{key}"
    rows.append([key, expected, url, bucket])

if not rows:
    print("No pointer records found.")
    sys.exit(1)

def fetch_with_mc(bucket, key):
    """Fetch using MinIO client."""
    proc = subprocess.run(
        ["mc", "cat", f"lfsverify/{bucket}/{key}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        return None, proc.stderr.decode()[:100]
    return proc.stdout, None

def fetch_with_aws(bucket, key):
    """Fetch using AWS CLI."""
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = access_key
    env["AWS_SECRET_ACCESS_KEY"] = secret_key
    env["AWS_DEFAULT_REGION"] = "us-east-1"
    proc = subprocess.run(
        ["aws", "--endpoint-url", endpoint, "s3", "cp", f"s3://{bucket}/{key}", "-"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    if proc.returncode != 0:
        return None, proc.stderr.decode()[:100]
    return proc.stdout, None

def fetch_with_curl(url):
    """Fetch using curl (no auth)."""
    proc = subprocess.run(
        ["curl", "-fsSL", url],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        return None, proc.stderr.decode()[:100]
    return proc.stdout, None

results = []
for key, expected, url, bucket in rows:
    try:
        if mode == "mc":
            data, err = fetch_with_mc(bucket, key)
        elif mode == "aws":
            data, err = fetch_with_aws(bucket, key)
        else:
            data, err = fetch_with_curl(url)

        if data is None:
            actual = "error"
            status = "error"
            error_msg = err.strip().replace("\n", " ")[:50] if err else "fetch failed"
        else:
            actual = hashlib.sha256(data).hexdigest()
            status = "ok" if actual == expected else "mismatch"
            error_msg = ""
    except Exception as e:
        actual = "error"
        status = "error"
        error_msg = str(e)[:50]
    results.append([key, expected, actual, status, url, error_msg])

# Cleanup mc alias
if mc_alias_setup:
    subprocess.run(
        ["mc", "alias", "rm", "lfsverify"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

# Print summary
ok_count = sum(1 for r in results if r[3] == "ok")
mismatch_count = sum(1 for r in results if r[3] == "mismatch")
error_count = sum(1 for r in results if r[3] == "error")
print(f"Mode: {mode} | Endpoint: {endpoint}")
print(f"Results: {ok_count} ok, {mismatch_count} mismatch, {error_count} error")
print()

# Determine if we need error column
show_errors = any(r[5] for r in results)

if show_errors:
    headers = ["Key", "Expected", "Actual", "Status", "Error"]
    display_results = [[r[0], r[1], r[2], r[3], r[5]] for r in results]
else:
    headers = ["Key", "Expected", "Actual", "Status", "URL"]
    display_results = [[r[0], r[1], r[2], r[3], r[4]] for r in results]

cols = [headers] + display_results
widths = [max(len(str(c[i])) for c in cols) for i in range(len(headers))]

def border():
    return "+" + "+".join("-" * (w + 2) for w in widths) + "+"

def row(vals):
    return "| " + " | ".join(str(v).ljust(w) for v, w in zip(vals, widths)) + " |"

print(border())
print(row(headers))
print(border())
for r in display_results:
    print(row(r))
print(border())

sys.exit(0 if error_count == 0 and mismatch_count == 0 else 1)
PYCODE
)

printf '%s\n' "${RECORDS}" | python3 -c "${PYCODE}"