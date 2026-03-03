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

# Blob Transformer Processor: Solution Design

This document describes a processor that declaratively transforms record fields, resolves LFS blobs, and calls external LLM/ML APIs using YAML configuration. No implementation changes are included here.

## Placement in the Architecture
- New processor service under `addons/processors/blob-transformer-processor/`.
- Uses the existing processor pattern: discovery → decode → resolve → transform → sink → checkpoint.
- Reuses `pkg/lfs` for envelope detection, S3 fetch, and checksum validation.

## High-Level Flow
```
+----------------------+     +--------------------+     +--------------------+
| KafScale Segments    | --> | Decoder (records)  | --> | LFS Resolver        |
+----------------------+     +--------------------+     +--------------------+
                                                           |
                                                           v
                                                     +-------------+
                                                     | Transformer |
                                                     |  (LLM/ML)   |
                                                     +-------------+
                                                           |
                                                           v
                                                     +-------------+
                                                     |   Sink      |
                                                     | (Kafka)     |
                                                     +-------------+
```

## Core Components
- **Discovery**: existing segment discovery (same as Iceberg processor).
- **Decoder**: existing `internal/decoder` to parse segments into records.
- **Resolver**: `pkg/lfs.Resolver` for envelope detection and S3 fetch.
- **Transformer Engine**: applies field extraction + operations from YAML.
- **LLM Client**: generic REST client with provider configs (timeout, retries, rate limits).
- **Sink**: Kafka output topic writer (initially), with optional error topic.
- **Checkpointing**: reuse existing lease/offset store for idempotency.

## Configuration Model (Draft)

### Top-level
```yaml
processor:
  poll_interval_seconds: 5

lfs:
  mode: resolve
  max_size: 104857600
  validate_checksum: true

providers:
  - name: openai
    base_url: https://api.openai.com/v1
    api_key_env: KAFSCALE_LLM_OPENAI_API_KEY
    timeout_seconds: 30
    max_in_flight: 8
    rate_limit_per_minute: 120

mappings:
  - topic: media-events
    output_topic: media-events.enriched
    error_topic: media-events.errors
    include_original: false
    store_lfs_metadata: true
    fields:
      - name: transcript
        input: value.json.payload.audio
        operations:
          - type: transcription
            provider: openai
            model: whisper-1
      - name: embedding
        input: value.json.payload.text
        operations:
          - type: embedding
            provider: openai
            model: text-embedding-3-large
```

### Field Selection
- `input` supports a simple, deterministic selector syntax:
  - `value` for raw bytes
  - `value.json.<path>` for JSON object fields
  - `key` and `headers.<name>` for metadata
- When `lfs.mode` is enabled, `value` is resolved before `value.json.*` selectors.

### LFS Modes
- `resolve`: always resolve LFS envelopes into full payloads.
- `reference`: do not resolve; allow metadata-only transforms.
- `skip`: drop LFS records entirely.
- `hybrid`: resolve only if payload size <= `max_size`.

### Operation Types (Initial Set)
- `embedding`: text input → vector output.
- `transcription`: audio input → text output.
- `image_analysis`: image input → labels/captions output.
- `prompt`: text input → text output (generic LLM call).

Each operation uses a provider and model. All provider secrets are sourced from env vars.

## Output Shape (JSON)
```json
{
  "meta": {
    "topic": "media-events",
    "partition": 3,
    "offset": 12841,
    "timestamp": 1738449150123
  },
  "derived": {
    "transcript": "...",
    "embedding": [0.01, 0.02, 0.03]
  },
  "lfs": {
    "content_type": "audio/mpeg",
    "blob_size": 1523432,
    "checksum": "...",
    "bucket": "kafscale-lfs",
    "key": "..."
  }
}
```

## Error Handling
- Resolve errors: emit to `error_topic` with original metadata and error code.
- LLM errors: retry with backoff, then emit to `error_topic`.
- Oversize payloads: emit error and skip transform for that record.
- Checksum mismatch: emit error, do not call LLM APIs.

## Idempotency and Ordering
- Output key derived from `{topic}:{partition}:{offset}:{operation}`.
- Checkpoint only after successful sink write for all operations in a batch.

## Rate Limiting and Backpressure
- Per-provider token bucket rate limiter.
- Max in-flight requests per worker.
- Batch records by topic to preserve ordering guarantees where required.

## Observability
- Metrics:
  - `processor_lfs_resolved_total`
  - `processor_lfs_resolved_bytes_total`
  - `processor_llm_requests_total{provider,operation,status}`
  - `processor_llm_latency_seconds{provider,operation}`
  - `processor_llm_rate_limited_total{provider}`
- Structured logs with operation name, provider, and record metadata.

## Security Notes
- All secrets must be env var backed; list defaults in `.env.example`.
- No public endpoints are required. If endpoints are added later, update `docs/lfs-proxy/OWASP-Hardening-Report.md`.
- LFS resolution uses existing S3 auth settings.

## Testing Strategy
- Unit tests for selector parsing and operation wiring.
- Resolver integration tests with MinIO.
- Provider mock tests for retry, timeout, and error codes.
- E2E pipeline: LFS proxy → Kafka → blob transformer → output topic.

## Tests Checklist (Draft)
- Config validation:
  - Invalid LFS mode rejected.
  - Missing required provider fields rejected.
  - Unsupported selector syntax rejected with clear message.
- Selector evaluation:
  - `value`, `key`, `headers.<name>` selectors.
  - `value.json.*` path traversal.
  - Missing path returns empty or error (explicit behavior).
- LFS resolution:
  - Envelope detection true/false.
  - Resolve mode replaces value.
  - Reference mode keeps envelope.
  - Skip mode drops records.
  - Hybrid mode respects `max_size`.
  - Checksum mismatch emits error and skips transform.
- Operation execution:
  - Embedding/transcription/image/prompt calls mapped to provider config.
  - Retry and backoff on transient errors.
  - Rate limit accounting and queueing behavior.
  - Timeout handling per provider.
- Output formatting:
  - Output schema contains expected `meta`, `derived`, `lfs`.
  - Output key uses `{topic}:{partition}:{offset}:{operation}`.
  - `include_original` behavior verified.
- Metrics:
  - Counters increment per operation.
  - Latency histograms observed.
  - Rate-limited calls tracked.
- E2E:
  - LFS proxy → Kafka → blob transformer → output topic.
  - Error topic receives failures with metadata.

## Config Spec (Draft)

### Top-Level
```yaml
processor:
  poll_interval_seconds: 5

discovery:
  mode: auto

offsets:
  backend: etcd
  lease_ttl_seconds: 30
  key_prefix: processors

etcd:
  endpoints: ["http://etcd.kafscale.svc.cluster.local:2379"]

s3:
  bucket: kafscale-data
  region: us-east-1
  endpoint: ""
  path_style: false

lfs:
  mode: resolve            # resolve | reference | skip | hybrid
  max_size: 104857600      # bytes
  validate_checksum: true

providers:
  - name: openai
    base_url: https://api.openai.com/v1
    api_key_env: KAFSCALE_LLM_OPENAI_API_KEY
    timeout_seconds: 30
    max_in_flight: 8
    rate_limit_per_minute: 120

mappings:
  - topic: media-events
    output_topic: media-events.enriched
    error_topic: media-events.errors
    include_original: false
    store_lfs_metadata: true
    fields:
      - name: transcript
        input: value.json.payload.audio
        operations:
          - type: transcription
            provider: openai
            model: whisper-1
```

### Field Selectors
- `value` → raw record value bytes.
- `value.json.<path>` → JSON extraction from (possibly resolved) value.
- `key` → record key bytes.
- `headers.<name>` → header value bytes.

### Mapping Fields
- `include_original`: include original `value` in output payload.
- `store_lfs_metadata`: include LFS metadata section in output.
- `fields[].name`: output field name in `derived`.
- `fields[].input`: selector string.
- `fields[].operations[]`:
  - `type`: `embedding` | `transcription` | `image_analysis` | `prompt`.
  - `provider`: reference to providers list.
  - `model`: provider-specific model identifier.

### Provider Fields
- `name`: unique key for provider selection.
- `base_url`: base API URL.
- `api_key_env`: env var name storing the API key.
- `timeout_seconds`: per-request timeout.
- `max_in_flight`: per-provider concurrency cap.
- `rate_limit_per_minute`: simple token-bucket rate limit.

## Rollout Plan
1. Config schema + validation only (no external calls).
2. LFS resolution + local transforms.
3. LLM integration with rate limits and retries.
4. Production hardening: metrics, dashboards, and E2E tests.
