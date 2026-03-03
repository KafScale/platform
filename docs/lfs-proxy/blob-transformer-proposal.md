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

# Blob Transformer Processor: Proposal

## Goal
Provide a standalone processor that declaratively transforms record fields, resolves LFS blobs when needed, and calls external LLM/ML APIs to produce derived outputs (embeddings, transcripts, labels) at scale.

## Problem
Teams want to enrich Kafka records containing large blobs (images, audio, PDFs, video) with ML-derived artifacts, but this should not couple with the Iceberg analytics processor or the LFS proxy. The enrichment workload is compute-heavy, often GPU-bound, and needs independent scaling, rate control, and cost governance.

## Scope
- New processor service under `addons/processors/`.
- Input: Kafka records decoded from KafScale segments (same discovery/decoder/checkpoint flow as other processors).
- Optional LFS resolution using `pkg/lfs` for record values or selected fields.
- Declarative YAML configuration for:
  - field extraction
  - blob resolution
  - transform operations (LLM/ML API calls)
  - output schema/topic
- Outputs to Kafka topics and/or object storage (future extension).

## Non-goals
- No changes to the LFS proxy protocol or envelope schema.
- No new OpenAPI endpoints in this phase.
- No in-cluster model serving in this phase (API-based only).

## Requirements
- All runtime properties must be overridable by environment variables.
- No hardcoded LFS configuration; reuse existing LFS env patterns.
- Declarative config must be validated at startup with clear errors.
- Idempotent output keyed by input topic/partition/offset + operation name.
- Bounded memory and concurrency; clear backpressure when LLM APIs throttle.

## Architecture Fit
- Aligns with the existing processor framework: discovery → decode → optional resolve → transform → sink → checkpoint.
- Keeps ML workloads isolated from Iceberg ingestion.
- Reuses `pkg/lfs` for envelope detection, S3 resolution, and checksum validation.

## Config Overview (Draft)
```yaml
mappings:
  - topic: media-events
    output_topic: media-events.enriched
    lfs:
      mode: resolve
      max_size: 104857600
      validate_checksum: true
    fields:
      - name: transcript
        input: value.json.body.audio
        operations:
          - type: transcription
            provider: openai
            model: whisper-1
      - name: embedding
        input: value.json.body.text
        operations:
          - type: embedding
            provider: openai
            model: text-embedding-3-large
```

## Output Model (Draft)
- Output record contains:
  - original metadata (topic, partition, offset, timestamp)
  - original fields (optional)
  - derived fields per operation
  - LFS metadata (optional)

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

## Security & Compliance
- All secrets provided via env vars or Kubernetes secrets.
- No credentials in logs; redact API keys.
- If any public endpoints are added later, update `docs/lfs-proxy/OWASP-Hardening-Report.md`.

## Observability
- Metrics per operation: total calls, latency, failures, retries, throttles.
- Per-topic resolution metrics for LFS payloads.

## Risks
- Cost spikes from LLM API usage.
- Model response variability if prompts are non-deterministic.
- Large payloads exceeding memory if not streamed or bounded.

## Proposed Milestones
1. Config schema + validation (no API calls).
2. LFS resolution integration and local transforms (no external calls).
3. LLM API integration with rate limiting and retries.
4. Output schema + metrics + tests.

## Open Questions
- Should output be a new topic only, or also support writing to Iceberg directly?
- Do we need a pluggable provider registry (OpenAI, Anthropic, Azure, custom REST)?
- What are the default guardrails for max payload size and concurrency?
