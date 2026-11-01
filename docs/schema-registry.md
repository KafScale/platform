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

# Schema Registry

KafScale brokers store record values as bytes and do not read schemas. Clients that use the Confluent wire format (a zero byte, a four byte schema id, then the encoded value) need a schema registry to turn ids into schemas. There are two ways to provide one.

## Option 1: use an existing registry

Any registry that speaks the Confluent Schema Registry REST API works with KafScale, because the brokers never contact it. Point the serializer and deserializer at the registry URL as usual:

- Confluent Schema Registry (self managed)
- Confluent Cloud Schema Registry
- Karapace
- Apicurio Registry, through its Confluent compatible API

Data that is mirrored or copied from another Kafka cluster into KafScale keeps the schema ids in its frames. Consumers can read it as long as they use the registry that assigned those ids, or a registry that holds the same ids (see [Moving schemas into the built in registry](#moving-schemas-into-the-built-in-registry)).

Confluent Schema Registry and Karapace keep their own state in a Kafka topic (`_schemas`). Confluent Schema Registry 8.4.0 writes that topic with a transactional producer, and KafScale brokers do not support `InitProducerId`, so its `kafkastore.bootstrap.servers` must point at a Kafka cluster, not at KafScale.

## Option 2: run the built in registry

`kafscale-schema-registry` is a separate process that serves the Confluent Schema Registry REST API and keeps its state in the same etcd cluster as the brokers. It does not change broker or proxy behaviour. Every instance is stateless, so you can run several replicas behind one Service.

### What it supports

- Schema type: Avro only. `GET /schemas/types` returns `["AVRO"]`. Protobuf and JSON Schema are rejected with 42201.
- Compatibility levels: `NONE` and `BACKWARD` (the default). `BACKWARD` checks a new schema against the latest live version of the subject. Other levels are rejected with 42203.
- Modes: `READWRITE` (the default), `READONLY` and `IMPORT`.
- Schema references, resolved by subject and version, nested up to 32 levels.
- Soft and hard deletes of subjects and versions.

Endpoints:

| Method | Path | Query parameters |
|--------|------|------------------|
| GET | `/schemas/types` | |
| GET | `/schemas/ids/{id}` | |
| GET | `/schemas/ids/{id}/schema` | |
| GET | `/schemas/ids/{id}/versions` | `deleted` |
| GET | `/subjects` | `deleted` |
| GET | `/subjects/{subject}/versions` | `deleted` |
| GET | `/subjects/{subject}/versions/{version}` | `deleted` |
| GET | `/subjects/{subject}/versions/{version}/schema` | |
| POST | `/subjects/{subject}/versions` | |
| POST | `/subjects/{subject}` | `deleted` |
| DELETE | `/subjects/{subject}` | `permanent` |
| DELETE | `/subjects/{subject}/versions/{version}` | `permanent` |
| GET, PUT | `/config`, `/config/{subject}` | `defaultToGlobal` on GET |
| DELETE | `/config/{subject}` | |
| GET, PUT | `/mode`, `/mode/{subject}` | `defaultToGlobal` on GET, `force` on PUT |
| DELETE | `/mode/{subject}` | |
| POST | `/compatibility/subjects/{subject}/versions/{version}` | `verbose` |
| GET | `/healthz`, `/readyz`, `/metrics` | |

`{version}` accepts a number or `latest`. Errors use the Confluent body `{"error_code": ..., "message": ...}` and codes: 40401 subject not found, 40402 version not found, 40403 schema not found, 40404 subject soft deleted, 40405 subject not soft deleted, 40406 version soft deleted, 40407 version not soft deleted, 40408 subject compatibility not set, 40409 subject mode not set, 409 incompatible schema, 42201 invalid schema, 42202 invalid version, 42203 invalid compatibility level, 42204 invalid mode, 42205 operation not permitted, 42206 reference exists, 50001 store error, 50002 operation timed out.

### Ids and versions

Schema ids are global, unique and increase with every new schema. Ids can have gaps. Registering a schema that already exists anywhere in the registry returns its existing id. Registering the same schema again under the same subject returns the existing version. Two schemas are the same when their type, their JSON text after whitespace is removed, and their references match. The registry returns the text exactly as it was first registered.

Every write is one etcd transaction that compares the revisions of the keys it read, so concurrent requests on any number of replicas cannot give one id to two schemas, give one version to two ids, or lose a version. When a transaction loses a race it retries after a short random wait. When the request timeout runs out first, the request fails with 50002 and nothing is written.

### Deletes

A soft delete hides a version or a subject. A hard delete (`permanent=true`) needs a soft delete first and then removes the version or subject. Deleting a version or subject that another schema references fails with 42206.

A hard delete keeps the schema itself: `GET /schemas/ids/{id}` still returns it, and its id is never given to another schema. Registering the same schema again returns the same id. Removing schemas that no subject uses is a candidate for the `kafscale-cli prune` command and is not done today.

### Moving schemas into the built in registry

To keep the ids that existing data carries, import the schemas with their ids before any client registers new ones:

1. `PUT /mode` with `{"mode": "IMPORT"}`, or `PUT /mode/{subject}` for one subject. This is refused with 42205 while the registry, or that subject, already holds schemas, unless `force=true` is set.
2. For every subject version in the source registry, `POST /subjects/{subject}/versions` with `{"schema": ..., "schemaType": ..., "references": ..., "id": ..., "version": ...}`. The id must be free or already hold the same schema, and the version must be free or already hold the same id, otherwise the request fails with 42205. Compatibility is not checked in `IMPORT` mode.
3. `PUT /mode` with `{"mode": "READWRITE"}`. New schemas get ids above the highest imported id.

### Storage

All state lives in etcd under `/kafscale/schemas/`. Schema text larger than `KAFSCALE_SCHEMA_REGISTRY_INLINE_LIMIT_BYTES` (64 KiB by default) is written to S3 at `<namespace>/@schemas/sha256/<sha256 of the text>`, and etcd keeps only the key. Without `KAFSCALE_S3_BUCKET`, such schemas are rejected with 42201.

### Security

The registry has no authentication or authorization. Anyone who can reach its port can register and delete schemas. The Helm chart exposes it only through a ClusterIP Service. Restrict access with NetworkPolicy, and do not expose it outside the cluster.

### Running it

Environment variables are listed in [operations.md](operations.md#schema-registry). With Helm:

```yaml
schemaRegistry:
  enabled: true
  replicas: 2
  etcdEndpoints:
    - http://kafscale-etcd-client.kafscale.svc:2379
  s3:
    bucket: kafscale-data
    region: us-east-1
```

The chart runs the image `ghcr.io/kafscale/kafscale-schema-registry` with the chart `appVersion` tag unless `schemaRegistry.image.tag` is set. Readiness (`/readyz`) checks that etcd answers. Metrics on `/metrics`:

- `kafscale_schema_registry_requests_total{route,code}`
- `kafscale_schema_registry_txn_conflicts_total`

Client settings, using the in cluster Service `<fullname>-schema-registry` on port 8081, where `<fullname>` is the chart full name:

- Java: `schema.registry.url=http://<fullname>-schema-registry:8081`
- confluent-kafka-python: `SchemaRegistryClient({"url": "http://<fullname>-schema-registry:8081"})`
- franz-go: `sr.NewClient(sr.URLs("http://<fullname>-schema-registry:8081"))`
