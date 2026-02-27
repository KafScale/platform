FROM gcr.io/etcd-development/etcd:v3.6.8 AS etcd

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=etcd /usr/local/bin/etcdctl /usr/local/bin/etcdctl
COPY --from=etcd /usr/local/bin/etcdutl /usr/local/bin/etcdutl
