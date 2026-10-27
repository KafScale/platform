#!/usr/bin/env bash
# Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# Licensed under the Apache License, Version 2.0 (the "License").
set -euo pipefail
command -v helm >/dev/null 2>&1 || { echo 'Helm is required' >&2; exit 1; }
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
go test ./test/chart -run '^TestProxyMTLSChart$' -count=1
