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
if [ "$#" -ne 2 ]; then
  echo "usage: $0 <persona> <poster-seconds>" >&2
  exit 2
fi
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"
dir="$repo/docs/media/videos/$1"
ffmpeg -v error -y -i "$dir/$1.webm" -an -c:v libx264 -preset slow -crf 23 -pix_fmt yuv420p -movflags +faststart -vf scale=960:540 "$dir/$1.mp4.tmp.mp4"
mv "$dir/$1.mp4.tmp.mp4" "$dir/$1.mp4"
ffmpeg -v error -y -ss "$2" -i "$dir/$1.mp4" -frames:v 1 -c:v libwebp -quality 85 "$dir/$1-poster.webp"
ls -l "$dir"
