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
cd "$(dirname "$0")/.."

tag="${RELEASE_TAG:?Set RELEASE_TAG to the published version tag}"
if [[ ! "$tag" =~ ^v[0-9][A-Za-z0-9._-]*$ ]]; then
  echo "Invalid release tag: $tag" >&2
  exit 1
fi
if [[ "$tag" == *-rc* || "$tag" == *-dev* ]]; then
  echo "skipping Homebrew formula PR for $tag"
  exit 0
fi
if [[ -z "${GH_TOKEN:-${GITHUB_TOKEN:-}}" ]]; then
  echo "GH_TOKEN or GITHUB_TOKEN is required to open the formula PR" >&2
  exit 1
fi

sums="${SHA256SUMS:-dist/release/SHA256SUMS}"
base="${DEFAULT_BRANCH:-main}"
version="${tag#v}"
branch="homebrew/kafscale-cli-${version}"

bash scripts/update_homebrew_formula.sh "$tag" "$sums" Formula/kafscale-cli.rb

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git checkout -B "$branch"
git add Formula/kafscale-cli.rb
if git diff --cached --quiet; then
  echo "formula already up to date for $tag"
  exit 0
fi
git commit -m "Update the Homebrew formula for kafscale-cli ${tag}"
gh auth setup-git
git push -u origin "HEAD:${branch}"
if gh pr view "$branch" --json url --jq .url >/dev/null 2>&1; then
  echo "pull request for $branch already exists"
  exit 0
fi
gh pr create --base "$base" --head "$branch" \
  --title "Update the Homebrew formula for kafscale-cli ${tag}" \
  --body "Regenerates Formula/kafscale-cli.rb from the ${tag} release assets. This is the in-repo Homebrew tap update, matching the inf-cli formula PR flow."
