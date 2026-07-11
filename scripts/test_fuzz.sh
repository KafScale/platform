#!/usr/bin/env bash
# Copyright 2026 KafScale team.
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

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FUZZ_TIME="${FUZZ_TIME:-30s}"

echo "==> fuzz: pkg/protocol/FuzzFrameRoundTrip"
go test -run=^$ -fuzz=FuzzFrameRoundTrip -fuzztime="${FUZZ_TIME}" ./pkg/protocol

echo ""
echo "Fuzz suite passed."
