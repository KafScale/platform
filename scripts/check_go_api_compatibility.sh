#!/usr/bin/env bash
# Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# Licensed under the Apache License, Version 2.0 (the "License").

# Compare exported root-module APIs with the published v1.6.3 baseline.
set -euo pipefail
root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
baseline="${1:-092291b78b8abf1e5bde6cafd92550e9726b46e0}"
module=github.com/KafScale/platform
scratch="$(mktemp -d "${TMPDIR:-/tmp}/kafscale-api.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/baseline" "$scratch/tools"
if [[ -n "${APIDIFF:-}" ]]; then
  api_diff="$APIDIFF"
else
  GOBIN="$scratch/tools" go install golang.org/x/exp/cmd/apidiff@v0.0.0-20251023183803-a4bb9ffd2546
  api_diff="$scratch/tools/apidiff"
fi
cd "$root_dir"
git archive "$baseline" | tar -x -C "$scratch/baseline"
(cd "$scratch/baseline" && "$api_diff" -m -w "$scratch/old.export" "$module")
"$api_diff" -m -w "$scratch/new.export" "$module"
"$api_diff" -m -incompatible "$scratch/old.export" "$scratch/new.export" > "$scratch/incompatible.txt"
cat "$scratch/incompatible.txt"
if [[ -s "$scratch/incompatible.txt" ]]; then
  echo "Incompatible exported API changes against $baseline" >&2
  exit 1
fi
go test ./pkg/broker -run '^TestPublished' -count=1
echo "No incompatible exported root-module API changes against $baseline"
