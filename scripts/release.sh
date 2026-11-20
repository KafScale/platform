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

usage() {
  cat <<'EOF'
Usage: scripts/release.sh -tag vX.Y.Z

Prepares a release branch, bumps versions, generates release notes from git
history, commits, and pushes the branch.
EOF
}

tag=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -tag)
      tag="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$tag" ]]; then
  echo "Missing -tag vX.Y.Z" >&2
  usage
  exit 1
fi

if [[ ! "$tag" =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
  echo "Tag must match vX.Y.Z, got: $tag" >&2
  exit 1
fi

major="${BASH_REMATCH[1]}"
minor="${BASH_REMATCH[2]}"
patch="${BASH_REMATCH[3]}"

if [[ "$minor" -eq 0 ]]; then
  echo "Minor version must be >= 1 to map chart version (0.(minor-1).patch)." >&2
  exit 1
fi

chart_version="0.$((minor - 1)).$patch"
addon_chart_version="0.1.$patch"
branch="prep-${tag}"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Working tree is dirty; commit or stash changes before running." >&2
  exit 1
fi

git checkout -b "$branch"

sed -i '' -E "s/^version: .*/version: ${chart_version}/" deploy/helm/kafscale/Chart.yaml
sed -i '' -E "s/^appVersion: .*/appVersion: \"${tag}\"/" deploy/helm/kafscale/Chart.yaml

while IFS= read -r chart; do
  sed -i '' -E "s/^version: .*/version: ${addon_chart_version}/" "$chart"
  sed -i '' -E "s/^appVersion: .*/appVersion: \"${tag}\"/" "$chart"
done < <(rg --files -g 'addons/processors/*/deploy/helm/*/Chart.yaml')

while IFS= read -r modfile; do
  sed -i '' -E "s|(github.com/KafScale/platform/v2) v[0-9]+\.[0-9]+\.[0-9]+|\\1 ${tag}|g" "$modfile"
done < <(rg --files -g 'addons/processors/**/go.mod')

release_file="docs/releases/${tag}.md"
if [[ -e "$release_file" ]]; then
  echo "Release notes already exist: $release_file" >&2
  exit 1
fi

prev_tag="$(git tag -l 'v*' | sort -V | tail -n 1)"
log_range="HEAD"
if [[ -n "$prev_tag" ]]; then
  log_range="${prev_tag}..HEAD"
fi

release_date="$(date +%F)"
commit_notes="$(git log --pretty=format:'- %s (%h)' ${log_range})"

cat > "$release_file" <<EOF
<!--
Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
This project is supported and financed by Scalytics, Inc. (www.scalytics.io).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ${tag}

Release date: ${release_date}

## Highlights

- TODO: Add high-level highlights for this release.

## Changes

${commit_notes}

## Bug fixes

- TODO: Summarize user-facing fixes.

## Maintenance

- TODO: Summarize dependency/tooling updates.

## Security fixes

- No known runtime vulnerabilities were fixed in this release.
EOF

git add deploy/helm/kafscale/Chart.yaml \
  addons/processors/iceberg-processor/deploy/helm/iceberg-processor/Chart.yaml \
  addons/processors/sql-processor/deploy/helm/sql-processor/Chart.yaml \
  addons/processors/skeleton/deploy/helm/skeleton-processor/Chart.yaml \
  addons/processors/iceberg-processor/go.mod \
  "$release_file"

git commit -m "release: prep ${tag}"
git push -u origin "$branch"
