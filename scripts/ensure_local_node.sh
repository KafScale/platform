#!/usr/bin/env bash
# Copyright 2026 KafScale team.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

NODE_VERSION_FILE="${NODE_VERSION_FILE:-.nvmrc}"
TOOLS_DIR="${TOOLS_DIR:-$ROOT_DIR/.tools}"
NODE_LINK_DIR="${NODE_LINK_DIR:-$TOOLS_DIR/node}"

mkdir -p "$TOOLS_DIR"

if ! git check-ignore -q .tools 2>/dev/null; then
  echo "ERROR: .tools/ must be ignored in .gitignore before installing local Node." >&2
  exit 1
fi

if [[ ! -f "$NODE_VERSION_FILE" ]]; then
  echo "ERROR: missing $NODE_VERSION_FILE" >&2
  exit 1
fi

version_spec="$(tr -d '[:space:]' < "$NODE_VERSION_FILE")"
if [[ -z "$version_spec" ]]; then
  echo "ERROR: $NODE_VERSION_FILE is empty" >&2
  exit 1
fi

resolve_version() {
  local spec="$1"
  if [[ "$spec" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf '%s\n' "$spec"
    return 0
  fi
  if [[ "$spec" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    printf 'v%s\n' "$spec"
    return 0
  fi

  local prefix
  if [[ "$spec" =~ ^[0-9]+$ ]]; then
    prefix="v${spec}."
  elif [[ "$spec" =~ ^[0-9]+\.[0-9]+$ ]]; then
    prefix="v${spec}."
  else
    echo "ERROR: unsupported Node version spec '$spec' in $NODE_VERSION_FILE" >&2
    exit 1
  fi

  curl -fsSL https://nodejs.org/dist/index.json \
    | jq -r --arg prefix "$prefix" 'map(select(.version | startswith($prefix))) | .[0].version // empty'
}

platform="$(uname -s)"
arch="$(uname -m)"

case "$platform" in
  Darwin) platform="darwin" ;;
  Linux) platform="linux" ;;
  *)
    echo "ERROR: unsupported OS $(uname -s)" >&2
    exit 1
    ;;
esac

case "$arch" in
  arm64|aarch64) arch="arm64" ;;
  x86_64|amd64) arch="x64" ;;
  *)
    echo "ERROR: unsupported architecture $(uname -m)" >&2
    exit 1
    ;;
esac

resolved_version="$(resolve_version "$version_spec")"
if [[ -z "$resolved_version" ]]; then
  echo "ERROR: could not resolve Node version '$version_spec' from nodejs.org" >&2
  exit 1
fi

if [[ -x "$NODE_LINK_DIR/bin/node" ]] && [[ "$("$NODE_LINK_DIR/bin/node" --version)" == "$resolved_version" ]]; then
  echo "Local Node already installed: $resolved_version"
  exit 0
fi

archive_name="node-${resolved_version}-${platform}-${arch}.tar.gz"
archive_url="https://nodejs.org/dist/${resolved_version}/${archive_name}"
extract_dir="$TOOLS_DIR/node-${resolved_version}-${platform}-${arch}"
tmp_archive="$TOOLS_DIR/${archive_name}"

echo "Installing local Node ${resolved_version} into $NODE_LINK_DIR"
curl -fsSL "$archive_url" -o "$tmp_archive"
rm -rf "$extract_dir"
tar -xzf "$tmp_archive" -C "$TOOLS_DIR"
rm -f "$tmp_archive"
ln -sfn "$extract_dir" "$NODE_LINK_DIR"

echo "Installed local Node: $("$NODE_LINK_DIR/bin/node" --version)"
echo "Installed local npm: $(PATH="$NODE_LINK_DIR/bin:$PATH" "$NODE_LINK_DIR/bin/npm" --version)"
