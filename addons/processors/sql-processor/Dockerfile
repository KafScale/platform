# Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

ARG GO_VERSION=1.25.2
FROM golang:${GO_VERSION}-alpine@sha256:06cdd34bd531b810650e47762c01e025eb9b1c7eadd191553b91c9f2d549fae8 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download
COPY . ./
ARG GO_BUILD_FLAGS=
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build ${GO_BUILD_FLAGS} -o /out/sql-processor ./cmd/processor && \
    go build ${GO_BUILD_FLAGS} -o /out/kafsql-proxy ./cmd/proxy && \
    go build ${GO_BUILD_FLAGS} -o /out/kafsql-backfill ./cmd/backfill

FROM alpine:3.19
WORKDIR /app
COPY --from=build /out/sql-processor /app/sql-processor
COPY --from=build /out/kafsql-proxy /app/kafsql-proxy
COPY --from=build /out/kafsql-backfill /app/kafsql-backfill
USER 65532:65532
ENTRYPOINT ["/app/sql-processor"]
