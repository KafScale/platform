<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

## Summary

- 

## Improvement Note

**Problem**
- FindCoordinator could return a broker ID that was missing from Metadata, causing Kafka clients to retry because the coordinator identity could not be reconciled with broker metadata.

**Solution**
- Select the coordinator broker from the metadata snapshot first, falling back to local broker info only if metadata is unavailable, and default broker ID to 0 to align with operator snapshots.

**How It Works**
- On FindCoordinator, the handler loads metadata and returns a broker ID that exists in the advertised broker list; if the local ID is missing, it returns the first metadata broker, and if metadata fails, it uses local broker info.

**Why Tests/Benchmarks Missed It**
- E2E Franz runs use in-memory metadata seeded from the same broker info, operator e2e paths use broker IDs derived from pod ordinals that match snapshots, and benchmarks use kcat produce/consume without consumer groups (no FindCoordinator).

## Testing

- [ ] `go test ./...`
- [ ] `make test-produce-consume` (broker changes)
- [ ] `make test-consumer-group` (group changes)

## Checklist

- [ ] Added/updated unit tests for new logic
- [ ] Added/updated e2e coverage for bug fixes
- [ ] Added license headers to new files
