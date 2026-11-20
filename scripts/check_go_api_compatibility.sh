#!/usr/bin/env bash
# Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# Licensed under the Apache License, Version 2.0 (the "License").

# Compare exported root-module APIs with the published v1.6.3 baseline.
# v1.6.3 is module github.com/KafScale/platform. This tree is
# github.com/KafScale/platform/v2, a different module path, so the baseline
# checkout is rewritten onto /v2 before apidiff. The comparison is the
# exported symbols. Additions stay compatible. Do not tag v1.7.0 on /v2.
set -euo pipefail
root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
baseline="${1:-092291b78b8abf1e5bde6cafd92550e9726b46e0}"
module=github.com/KafScale/platform/v2
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
python3 - "$scratch/baseline" <<'PY'
import pathlib, re, sys
root = pathlib.Path(sys.argv[1])
nested = {p.parent for p in root.rglob("go.mod") if p.parent != root}
imp = re.compile(r'"github.com/KafScale/platform(?!/v2)')
for path in root.rglob("*"):
    if not path.is_file() or (path.suffix != ".go" and path.name != "go.mod"):
        continue
    if any(parent in nested for parent in path.parents):
        continue
    text = path.read_text()
    if path.name == "go.mod" and path.parent == root:
        lines = text.splitlines()
        out = ["module github.com/KafScale/platform/v2" if line == "module github.com/KafScale/platform" else line for line in lines]
        path.write_text("\n".join(out) + "\n")
        continue
    new = imp.sub('"github.com/KafScale/platform/v2', text)
    if new != text:
        path.write_text(new)
PY
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
