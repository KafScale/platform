FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . ./
RUN go build -o /out/skeleton-processor ./cmd/processor

FROM alpine:3.19
WORKDIR /app
COPY --from=build /out/skeleton-processor /app/skeleton-processor
USER 65532:65532
ENTRYPOINT ["/app/skeleton-processor"]
