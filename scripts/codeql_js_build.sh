#!/usr/bin/env bash
# Copyright 2026 KafScale team.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_NODE_BIN="$ROOT_DIR/.tools/node/bin"
NPM_CMD="npm"

PATH_ENTRIES=()
if [ -x "$LOCAL_NODE_BIN/node" ]; then
  PATH_ENTRIES+=("$LOCAL_NODE_BIN")
fi
if [ -d /opt/homebrew/bin ]; then
  PATH_ENTRIES+=("/opt/homebrew/bin")
fi

if [ "${#PATH_ENTRIES[@]}" -gt 0 ]; then
  export PATH="$(IFS=:; echo "${PATH_ENTRIES[*]}"):$PATH"
fi

if [ -x "$LOCAL_NODE_BIN/npm" ]; then
  NPM_CMD="$LOCAL_NODE_BIN/npm"
fi

install_node_deps() {
  if [ -f package-lock.json ] || [ -f npm-shrinkwrap.json ]; then
    PATH="$LOCAL_NODE_BIN:$PATH" "$NPM_CMD" ci --include=dev
  else
    PATH="$LOCAL_NODE_BIN:$PATH" "$NPM_CMD" install --include=dev
  fi
}

if [ -f lfs-client-sdk/js/package.json ]; then
  (
    cd lfs-client-sdk/js
    install_node_deps
    PATH="$LOCAL_NODE_BIN:$PATH" "$NPM_CMD" run build
  )
fi

if [ -f lfs-client-sdk/js-browser/package.json ]; then
  (
    cd lfs-client-sdk/js-browser
    install_node_deps
    PATH="$LOCAL_NODE_BIN:$PATH" "$NPM_CMD" run build
  )
fi
