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

# Try KafScale locally

Start with Docker and Docker Compose v2.20 or later. The platform runs in
containers; no local Go toolchain, Kubernetes cluster or cloud account is needed.
Install a native [kcat client](https://github.com/edenhill/kcat#install) for the
produce/consume commands below. The [KafScale CLI](cli-install.md) is a separate
native tool for restore, prune and schema import.

## Start the development stack

```bash
git clone https://github.com/KafScale/platform.git
cd platform/deploy/docker-compose
docker compose up -d --wait
curl -fsS http://localhost:9094/readyz
```

This starts etcd, a local MinIO test store, the broker, proxy and console.
MinIO is built inside Docker on first use, which can take several minutes; no
MinIO binary or image is published as a KafScale release. Its credentials and
volumes are for local testing. Product images default to the published `v2.1.0`
tag; `TAG` selects another published release.

Open the console at [http://localhost:3080/ui/](http://localhost:3080/ui/). Sign in as
`kafscaleadmin` with password `kafscale`.

## Write and read your first message

Use the proxy's host endpoint, `localhost:9093`. The broker advertises its
container hostname for services inside the Compose network.

```bash
printf 'hello from KafScale\n' | kcat -P -b localhost:9093 -t quickstart
kcat -C -b localhost:9093 -t quickstart -o beginning -c 1 -e
```

`v2.1.0`, the first release with #28, supports default nontransactional
idempotent producers; see
[protocol support](protocol.md#idempotent-producers). Transactions remain unsupported.

To inspect or stop the stack:

```bash
docker compose ps
docker compose logs --tail=100 broker proxy
docker compose down
```

`down` preserves the local data volumes. `docker compose down -v` also deletes
the development stack's stored data.

## Connect your S3 or deploy Kubernetes

For an existing bucket, use [Connect your S3](s3-connection.md). The release
Compose file uses your S3 service directly and has no MinIO service or build
step. Native CLI archives run directly on your host; Docker is needed only if
you choose to run the platform with Compose.

For a managed cluster, continue with the [Kubernetes and Helm quick start](quickstart.md).
