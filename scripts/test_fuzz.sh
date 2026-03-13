#!/usr/bin/env bash
# Copyright 2026 KafScale team.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FUZZ_TIME="${FUZZ_TIME:-30s}"

echo "==> fuzz: pkg/protocol/FuzzFrameRoundTrip"
go test -run=^$ -fuzz=FuzzFrameRoundTrip -fuzztime="${FUZZ_TIME}" ./pkg/protocol

echo ""
echo "Fuzz suite passed."
