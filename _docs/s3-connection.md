---
layout: doc
title: "Connect your S3"
permalink: /s3-connection/
nav_title: "Connect your S3"
nav_order: 3
---

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

# Connect your S3

Use an existing S3 bucket in the region you configure. KafScale needs access to
list the bucket and read, write and delete its objects. The backend must honor
conditional `PutObject` requests with `If-None-Match: *`; brokers verify this
before opening their Kafka listener. See [storage startup checks](/operations/#security--hardening)
and the startup gating notes in the operations guide.

For AWS S3, leave the custom endpoint unset and use the bucket's actual region.
For another S3-compatible service, set its reachable HTTP(S) endpoint and enable
path-style addressing where required. A container must be able to reach that
endpoint: `localhost` inside a container refers to that container.

## Kubernetes and Helm

The chart installs the operator. Configure the broker bucket on a
`KafscaleCluster` resource, not on the operator's image settings:

```yaml
apiVersion: kafscale.io/v1
kind: KafscaleCluster
metadata:
  name: demo
  namespace: kafscale
spec:
  brokers:
    replicas: 1
  s3:
    bucket: your-existing-bucket
    region: us-east-1
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints: []
```

The `kafscale-s3` Secret in the same namespace can contain `AWS_ACCESS_KEY_ID`
and `AWS_SECRET_ACCESS_KEY`, plus `AWS_SESSION_TOKEN` for temporary credentials.
[The Helm quick start](/quickstart/#2-create-an-s3-credentials-secret) shows how
to create it without putting credentials in a manifest or shell arguments.
[Credential precedence and rotation](/operations/#s3-credentials-secret) also
covers the alternative `KAFSCALE_S3_*` key names and workload credentials.

For an S3-compatible service, add `spec.s3.endpoint`, for example
`https://s3.example.net`. The operator enables path-style access when a custom
endpoint is configured. Keep `bucket`, `region` and `credentialsSecretRef` set.
Follow the [Helm guide](/quickstart/) to install the chart and create the cluster.

## Release Compose with your bucket

Download `docker-compose.yaml` and `SHA256SUMS` from a release that has assets
(see [CLI and release downloads](/cli-install/)). Verify the file:

```bash
grep '  docker-compose.yaml$' SHA256SUMS | shasum -a 256 -c -
```

On Linux, `sha256sum -c -` can replace `shasum -a 256 -c -`. This release stack
contains etcd, broker, proxy and console. It pulls released images, performs no
source builds, and does not contain MinIO. Configure it in your shell:

```bash
export KAFSCALE_S3_BUCKET=your-existing-bucket
export KAFSCALE_S3_REGION=us-east-1
printf 'S3 access key: '; read -r KAFSCALE_S3_ACCESS_KEY
printf 'S3 secret key: '; read -rs KAFSCALE_S3_SECRET_KEY; printf '\n'
printf 'Console password: '; read -rs KAFSCALE_UI_PASSWORD; printf '\n'
export KAFSCALE_S3_ACCESS_KEY KAFSCALE_S3_SECRET_KEY KAFSCALE_UI_PASSWORD
# For temporary credentials, also export KAFSCALE_S3_SESSION_TOKEN.
# For a custom S3 service:
# export KAFSCALE_S3_ENDPOINT=https://s3.example.net
# export KAFSCALE_S3_PATH_STYLE=true
docker compose up -d --wait
curl -fsS http://localhost:9094/readyz
```

Use `localhost:9093` for host Kafka clients and `http://localhost:3080` for the
console (`kafscaleadmin` and your chosen password). The host ports bind to
loopback; this Compose file is for access from that host. For a shared deployment
with authentication and TLS, follow [Kubernetes and Helm](/quickstart/).

## Native CLI

The installed CLI connects directly to S3 and etcd. Docker is not required.
For restore/prune, use the same S3 environment as above, plus:

```bash
export KAFSCALE_ETCD_ENDPOINTS=http://127.0.0.1:2379
export KAFSCALE_S3_NAMESPACE=default
./kafscale-cli prune # dry run; lists retired data without deleting it
```

Use the namespace of the data you want to manage. The release Compose stack
uses `default`; Kubernetes brokers use their `KafscaleCluster` namespace.
For Kubernetes, [CLI access from a laptop](/operations/#cli-access-from-a-laptop)
shows how to reach etcd and S3. [CLI configuration](/operations/#kafscale-cli)
lists all supported environment variables, including the AWS SDK credential
chain when no explicit key pair is configured. Schema import instead uses its
registry URL and authentication settings.

For a disposable local S3 test store, use the [development Compose stack](/local-quickstart/).
MinIO belongs to that test setup, not to the KafScale release artifacts.
