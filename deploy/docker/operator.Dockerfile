# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.25.2
FROM golang:${GO_VERSION}-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src
RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
COPY third_party ./third_party
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w" -o /out/operator ./cmd/operator

FROM alpine:3.19
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 kafscale
USER 10001
WORKDIR /app

COPY --from=builder /out/operator /usr/local/bin/kafscale-operator

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/kafscale-operator"]
