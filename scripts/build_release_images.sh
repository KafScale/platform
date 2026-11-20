#!/usr/bin/env bash
# Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
publish="${PUBLISH_IMAGES:-false}"
case "$publish" in
  true)
    if [[ "${GITHUB_REF:-}" != refs/tags/v* || ! "${RELEASE_TAG:-}" =~ ^v[0-9][A-Za-z0-9._-]*$ ]]; then
      echo "Image publishing requires a version tag." >&2
      exit 1
    fi
    ;;
  false) ;;
  *) echo "PUBLISH_IMAGES must be true or false." >&2; exit 1 ;;
esac
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
python3 - "$work/override.json" <<'PYCONFIG'
import json, os, sys
catalog = json.load(open("deploy/docker/docker-bake.json"))
publish = os.environ.get("PUBLISH_IMAGES", "false") == "true"
overrides = {}
for name, image in catalog["target"].items():
    config = {}
    if publish:
        repository = image["tags"][0].rsplit(":", 1)[0]
        config["tags"] = [repository + ":" + os.environ["RELEASE_TAG"]]
        config["labels"] = {
            "org.opencontainers.image.version": os.environ["RELEASE_TAG"],
            "org.opencontainers.image.revision": os.environ.get("SOURCE_REVISION", ""),
            "org.opencontainers.image.source": "https://github.com/KafScale/platform",
        }
    else:
        # PRs compile every release image once on the native Linux runner.
        config["platforms"] = ["linux/amd64"]
        config["output"] = ["type=cacheonly"]
    if os.environ.get("GITHUB_ACTIONS") == "true":
        config["cache-from"] = ["type=gha,scope=" + name]
        config["cache-to"] = ["type=gha,mode=max,scope=" + name]
    overrides[name] = config
json.dump({"target": overrides}, open(sys.argv[1], "w"))
PYCONFIG
flags=()
if [ "$publish" = true ]; then flags+=(--push); fi
docker buildx bake --file deploy/docker/docker-bake.json --file "$work/override.json" "${flags[@]}" "$@"
