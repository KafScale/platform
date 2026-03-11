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

# KafScale LFS Java SDK

## Overview
This SDK provides Java helpers for producing LFS blobs via the LFS proxy HTTP endpoint and resolving LFS envelopes from Kafka.

## Retry/Backoff
- Retries are attempted for transient IO errors and HTTP 5xx responses.
- No retries are performed for HTTP 4xx responses.
- Default retries: 3 attempts total with linear backoff (200ms, 400ms, 600ms).

## Timeouts
- Connect timeout default: 10 seconds.
- Per-request timeout default: 5 minutes.
- Override via `new LfsProducer(endpoint, connectTimeout, requestTimeout)`.

## Error Surfacing
- HTTP failures throw `LfsHttpException` with status code, error code, request ID, and response body.
- `X-Request-ID` is generated if missing and returned in proxy responses for correlation.

## Example
```java
URI endpoint = URI.create("http://localhost:8080/lfs/produce");
LfsProducer producer = new LfsProducer(endpoint);
LfsEnvelope env = producer.produce("lfs-demo-topic", null, dataStream, Map.of(
    "content-type", "application/octet-stream",
    "LFS_BLOB", "true"
));
```

## Testing
```bash
mvn test
```

## Integration Tests
See `docs/integration-tests.md` for TestContainers setup and image overrides.
