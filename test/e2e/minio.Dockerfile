# syntax=docker/dockerfile:1.7
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

# Test-only fixture: RELEASE.2024-09-22T00-33-43Z, built from its source commit
# because the historical prebuilt MinIO images are no longer publicly pullable.
FROM golang:1.22.7-alpine@sha256:48eab5e3505d8c8b42a06fe5f1cf4c346c167cc6a89e772f31cb9e5c301dcf60 AS builder
RUN apk add --no-cache ca-certificates git
WORKDIR /src
RUN git init && git remote add origin https://github.com/minio/minio.git && \
    git fetch --depth 1 origin 03e996320ebb887112fb2a15c6f27936e5f124a0 && \
    git checkout --detach FETCH_HEAD
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -p 2 -trimpath -o /out/minio .

FROM alpine:3.19@sha256:6baf43584bcb78f2e5847d1de515f23499913ac9f12bdf834811a3145eb11ca1
RUN apk add --no-cache ca-certificates
COPY --from=builder /out/minio /usr/local/bin/minio
ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data"]
