# syntax=docker/dockerfile:1.5
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download
COPY . ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -o /out/iceberg-processor ./cmd/processor

FROM alpine:3.19
WORKDIR /app
COPY --from=build /out/iceberg-processor /app/iceberg-processor
USER 65532:65532
ENTRYPOINT ["/app/iceberg-processor"]
