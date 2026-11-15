FROM gcr.io/etcd-development/etcd:v3.6.14@sha256:a491baeaa0cb0c9cd89c0062ac44ece53886e3e5bddad18d2daf36678ce665b6 AS etcd

FROM alpine:3.19@sha256:6baf43584bcb78f2e5847d1de515f23499913ac9f12bdf834811a3145eb11ca1
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 kafscale
USER 10001
COPY --from=etcd /usr/local/bin/etcdctl /usr/local/bin/etcdctl
COPY --from=etcd /usr/local/bin/etcdutl /usr/local/bin/etcdutl
