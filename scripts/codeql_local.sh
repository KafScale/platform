#!/usr/bin/env bash
# Copyright 2026 KafScale team.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v codeql >/dev/null 2>&1; then
  echo "ERROR: codeql CLI not found in PATH"
  echo "Install from: https://docs.github.com/en/code-security/codeql-cli/getting-started-with-the-codeql-cli"
  exit 1
fi

mkdir -p .tmp/codeql

echo "==> Ensuring CodeQL standard query packs are available"
codeql pack download codeql/go-queries codeql/javascript-queries codeql/actions-queries

# Allow callers to tune memory/parallelism while providing stable defaults for local runs.
# Keep JavaScript defaults lighter locally; callers can still raise them to match
# KafClaw-style settings via env overrides.
CODEQL_GO_RAM_MB="${CODEQL_GO_RAM_MB:-2048}"
CODEQL_GO_CGO_ENABLED="${CODEQL_GO_CGO_ENABLED:-0}"
CODEQL_JS_RAM_MB="${CODEQL_JS_RAM_MB:-2048}"
CODEQL_JS_THREADS="${CODEQL_JS_THREADS:-1}"
CODEQL_ACTIONS_RAM_MB="${CODEQL_ACTIONS_RAM_MB:-1024}"

# Match GitHub code-scanning defaults by default.
# Set CODEQL_QUERY_STRATEGY=security-and-quality to force explicit suites.
CODEQL_QUERY_STRATEGY="${CODEQL_QUERY_STRATEGY:-github}"

run_go() {
  echo "==> CodeQL (Go)"
  echo "    using --ram=${CODEQL_GO_RAM_MB}MB CGO_ENABLED=${CODEQL_GO_CGO_ENABLED}"
  rm -rf .tmp/codeql/go-db
  codeql database create .tmp/codeql/go-db \
    --language=go \
    --ram="$CODEQL_GO_RAM_MB" \
    --command="env CGO_ENABLED=${CODEQL_GO_CGO_ENABLED} go build -buildvcs=false -ldflags=-w ./..."

  if [[ "$CODEQL_QUERY_STRATEGY" == "security-and-quality" ]]; then
    codeql database analyze .tmp/codeql/go-db \
      codeql/go-queries:codeql-suites/go-security-and-quality.qls \
      --download \
      --ram="$CODEQL_GO_RAM_MB" \
      --format=sarifv2.1.0 \
      --sarif-category="/language:go" \
      --output .tmp/codeql/go.sarif
  else
    codeql database analyze .tmp/codeql/go-db \
      codeql/go-queries \
      --download \
      --ram="$CODEQL_GO_RAM_MB" \
      --format=sarifv2.1.0 \
      --sarif-category="/language:go" \
      --output .tmp/codeql/go.sarif
  fi
}

run_js() {
  echo "==> CodeQL (JavaScript/TypeScript)"
  echo "    using --ram=${CODEQL_JS_RAM_MB}MB --threads=${CODEQL_JS_THREADS}"
  rm -rf .tmp/codeql/js-db
  chmod +x scripts/codeql_js_build.sh
  codeql database create .tmp/codeql/js-db \
    --language=javascript-typescript \
    --ram="$CODEQL_JS_RAM_MB" \
    --command="./scripts/codeql_js_build.sh"

  if [[ "$CODEQL_QUERY_STRATEGY" == "security-and-quality" ]]; then
    codeql database analyze .tmp/codeql/js-db \
      codeql/javascript-queries:codeql-suites/javascript-security-and-quality.qls \
      --download \
      --ram="$CODEQL_JS_RAM_MB" \
      --threads="$CODEQL_JS_THREADS" \
      --format=sarifv2.1.0 \
      --sarif-category="/language:javascript-typescript" \
      --output .tmp/codeql/javascript.sarif
  else
    codeql database analyze .tmp/codeql/js-db \
      codeql/javascript-queries \
      --download \
      --ram="$CODEQL_JS_RAM_MB" \
      --threads="$CODEQL_JS_THREADS" \
      --format=sarifv2.1.0 \
      --sarif-category="/language:javascript-typescript" \
      --output .tmp/codeql/javascript.sarif
  fi
}

run_actions() {
  echo "==> CodeQL (Actions)"
  echo "    using --ram=${CODEQL_ACTIONS_RAM_MB}MB"
  rm -rf .tmp/codeql/actions-db
  codeql database create .tmp/codeql/actions-db \
    --language=actions \
    --build-mode=none \
    --ram="$CODEQL_ACTIONS_RAM_MB"

  codeql database analyze .tmp/codeql/actions-db \
    codeql/actions-queries \
    --download \
    --ram="$CODEQL_ACTIONS_RAM_MB" \
    --format=sarifv2.1.0 \
    --sarif-category="/language:actions" \
    --output .tmp/codeql/actions.sarif
}

run_go
run_js
run_actions

echo ""
echo "CodeQL local run complete."
echo "SARIF outputs:"
echo "  .tmp/codeql/go.sarif"
echo "  .tmp/codeql/javascript.sarif"
echo "  .tmp/codeql/actions.sarif"
