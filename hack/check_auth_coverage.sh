#!/usr/bin/env bash
# Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# Licensed under the Apache License, Version 2.0 (the "License").
set -euo pipefail

# Include command packages: the general coverage gate excludes them.
profile="${1:-coverage-auth.out}"
if [[ $# -eq 0 ]]; then
  go test -race -coverprofile="$profile" ./pkg/authn ./pkg/acl ./pkg/broker ./cmd/proxy ./cmd/broker ./pkg/operator
fi
python3 - "$profile" <<'PY'
import sys

required = {
    'pkg/authn/forward.go',
    'pkg/authn/tls.go',
    'pkg/acl/acl.go',
    'pkg/broker/conn_context.go',
    'cmd/proxy/security.go',
    'cmd/proxy/metadata_errors.go',
    'cmd/proxy/handshake.go',
    'cmd/proxy/lfs_authorization.go',
    'cmd/proxy/lfs_http_authorization.go',
    'cmd/broker/identity.go',
    'pkg/operator/kafka_security.go',
    'pkg/operator/kafka_acl_rollout.go',
    'pkg/operator/secret_cache.go',
}
seen = set()
statements = covered = 0
for line in open(sys.argv[1]):
    fields = line.split()
    if len(fields) != 3:
        continue
    location, count, hits = fields
    path = location.split(':', 1)[0].removeprefix('github.com/KafScale/platform/')
    if path not in required:
        continue
    seen.add(path)
    count, hits = int(count), int(hits)
    statements += count
    covered += count if hits else 0
    if hits == 0:
        print(f'Uncovered security code: {location}', file=sys.stderr)
missing = required - seen
if missing:
    print(f'Missing security coverage: {sorted(missing)}', file=sys.stderr)
if missing or covered != statements:
    sys.exit(1)
print(f'Authentication coverage: 100% ({covered}/{statements} statements across {len(seen)} files)')
PY
