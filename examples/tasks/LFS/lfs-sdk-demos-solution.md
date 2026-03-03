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

# LFS SDK Demo Plan (E70/E71)

## Goal
Provide two runnable examples that validate the **Java** and **Python** LFS SDKs against the same running LFS demo stack.

- **E70**: Java SDK demo
- **E71**: Python SDK demo

## Assumptions
- The `lfs-demo` stack (kind + broker + LFS proxy + MinIO) stays up after execution.
- Local host access is via `kubectl port-forward` or `make run-all` from the demo folder.

## Rebuild Requirements
- Each E70 run rebuilds the Java SDK and reloads the LFS proxy image.
- The demo stack stays running; only the proxy deployment is refreshed.


## Run Order (One Terminal Keeps Stack Alive)
1. Terminal A: bring the stack up once and keep it running.
   ```bash
   LFS_DEMO_CLEANUP=0 make lfs-demo
   ```
2. Terminal B: port-forward services for local SDKs.
   ```bash
   kubectl -n kafscale-demo port-forward svc/lfs-proxy 8080:8080
   kubectl -n kafscale-demo port-forward svc/kafscale-broker 9092:9092
   kubectl -n kafscale-demo port-forward svc/minio 9000:9000
   ```
3. Terminal C: run E70 (`make run` or `make run-all`) or E71.

## Demo Flow (Both E70/E71)
1. Produce a blob via LFS proxy HTTP (`/lfs/produce`).
2. Read pointer from Kafka topic (`lfs-demo-topic`).
3. Resolve blob from MinIO (S3-compatible).
4. Print payload size and envelope metadata.

## Environment Variables
- `LFS_HTTP_ENDPOINT` (default `http://localhost:8080/lfs/produce`)
- `KAFKA_BOOTSTRAP` (default `localhost:9092`)
- `LFS_TOPIC` (default `lfs-demo-topic`)
- `S3_BUCKET` (default `kafscale`)
- `S3_ENDPOINT` (default `http://localhost:9000`)
- `S3_REGION` (default `us-east-1`)
- `AWS_ACCESS_KEY_ID` (default `minioadmin`)
- `AWS_SECRET_ACCESS_KEY` (default `minioadmin`)

## Example Locations
- `examples/E70_java-lfs-sdk-demo`
- `examples/E71_python-lfs-sdk-demo`

## Acceptance Criteria
- E70 prints a produced envelope and resolves a payload from MinIO.
- E71 prints a produced envelope and resolves a payload from MinIO.
- No cluster teardown occurs after running the demos.

## Notes
- JS SDK demo is deferred to March 2026.
- These demos share the same `lfs-demo` cluster for fast iteration.
