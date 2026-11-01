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

Confluent Schema Registry and Karapace keep their own state in a Kafka topic (`_schemas`). Confluent Schema Registry 8.4.0 writes that topic with an idempotent producer that has no transactional id. That producer sends `InitProducerId` at startup, which KafScale does not support (see [Limits / Non-Goals](user-guide.md#limits--non-goals)), so its `kafkastore.bootstrap.servers` must point at a Kafka cluster, not at KafScale.

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
| GET | `/schemas/ids/{id}/subjects` | `deleted` |
| GET | `/schemas` | `subjectPrefix`, `deleted`, `latestOnly`, `offset`, `limit` |
| GET | `/contexts` | |
| GET | `/subjects` | `deleted` |
| GET | `/subjects/{subject}/versions` | `deleted` |
| GET | `/subjects/{subject}/versions/{version}` | `deleted` |
| GET | `/subjects/{subject}/versions/{version}/schema` | |
| GET | `/subjects/{subject}/versions/{version}/referencedby` | |
| POST | `/subjects/{subject}/versions` | `normalize` |
| POST | `/subjects/{subject}` | `deleted`, `normalize` |
| DELETE | `/subjects/{subject}` | `permanent` |
| DELETE | `/subjects/{subject}/versions/{version}` | `permanent` |
| GET, PUT | `/config`, `/config/{subject}` | `defaultToGlobal` on GET |
| DELETE | `/config`, `/config/{subject}` | |
| GET, PUT | `/mode`, `/mode/{subject}` | `defaultToGlobal` on GET, `force` on PUT |
| DELETE | `/mode/{subject}` | |
| POST | `/compatibility/subjects/{subject}/versions`, `/compatibility/subjects/{subject}/versions/{version}` | `verbose` |
| GET | `/healthz`, `/readyz`, `/metrics` | |

`{version}` accepts a number or `latest`. `/contexts` always returns `["."]`. Compatibility against a subject that does not exist returns `is_compatible: true` for `latest` or no version, and 40402 for a numbered version. `DELETE /config/{subject}` and `DELETE /mode/{subject}` return 40401 when the subject has no setting. `DELETE /config` returns the previous global level and resets it to `BACKWARD`. Errors use the Confluent body `{"error_code": ..., "message": ...}` and codes: 40401 subject not found, 40402 version not found, 40403 schema not found, 40404 subject soft deleted, 40405 subject not soft deleted, 40406 version soft deleted, 40407 version not soft deleted, 40408 subject compatibility not set, 40409 subject mode not set, 40901 incompatible schema (HTTP 409), 400 malformed request body, 42201 invalid schema, 42202 invalid version, 42203 invalid compatibility level, 42204 invalid mode, 42205 operation not permitted, 42206 reference exists, 50001 store error, 50002 operation timed out.

### Ids and versions

Schema ids are global, unique and increase with every new schema. Ids can have gaps. The registry stores each schema in the form Confluent Schema Registry stores it: the Avro schema printed by the Java Avro library, with whitespace removed, names written relative to the enclosing namespace, and default values and properties kept. Registering a schema whose stored form, type and references already exist anywhere in the registry returns the existing id. Under one subject, a schema that differs from an existing version only in the order of properties or aliases returns that version, the highest one if several match. With `normalize=true`, properties and aliases are sorted and `error` is stored as `record` before the id is chosen.

A new version number is one above the highest version the subject has, live or soft deleted. A version removed with a hard delete is no longer counted, so its number can be given again. Registering a schema that only exists as a soft deleted version of the subject creates a new version and removes the soft deleted one.

Every write is one etcd transaction that compares the revisions of the keys it read, so concurrent requests on any number of replicas cannot give one id to two schemas, give one version to two ids, or lose a version. When a transaction loses a race it retries after a short random wait. When the request timeout runs out first, the request fails with 50002 and nothing is written.

### Deletes

A soft delete hides a version or a subject. A hard delete (`permanent=true`) needs a soft delete first and then removes the version or subject. Deleting a version or subject that another schema references fails with 42206.

A hard delete keeps the schema itself: `GET /schemas/ids/{id}` still returns it, and its id is never given to another schema. Registering the same schema again returns the same id. Nothing removes schemas that no subject uses, and nothing removes their S3 objects.

### Moving schemas into the built in registry

To keep the ids that existing data carries, import the schemas with their ids before any client registers new ones:

1. `PUT /mode` with `{"mode": "IMPORT"}`, or `PUT /mode/{subject}` for one subject. This is refused with 42205 while the registry, or that subject, already holds schemas, unless `force=true` is set.
2. For every subject version in the source registry, `POST /subjects/{subject}/versions` with `{"schema": ..., "schemaType": ..., "references": ..., "id": ..., "version": ...}`. The id must be free or already hold the same schema, and the version must be free or already hold the same id, otherwise the request fails with 42205. Compatibility is not checked in `IMPORT` mode.
3. `PUT /mode` with `{"mode": "READWRITE"}`. New schemas get ids above the highest imported id.

### Storage

All state lives in etcd under `/kafscale/schemas/`. Schema text larger than `KAFSCALE_SCHEMA_REGISTRY_INLINE_LIMIT_BYTES` (64 KiB by default) is written to S3 at `<namespace>/@schemas/sha256/<sha256 of the text>`, and etcd keeps only the key. Without `KAFSCALE_S3_BUCKET`, such schemas are rejected with 42201. The object key is the hash of the text, so retries and identical schemas write the same object once per request. When a write uploads the text and then fails in etcd, the object stays in S3 without an etcd key pointing to it. Such objects are not removed today. To find them, list `<namespace>/@schemas/sha256/` and compare it with the `s3Key` values under `/kafscale/schemas/ids/`.

Each instance caches schemas by id. It drops the cache when etcd reports a lower revision or a lower id counter than it has seen. After restoring etcd from a backup, restart the registry pods anyway, so no instance serves a schema from before the restore.

### Differences from Confluent Schema Registry

Checked against Confluent Schema Registry 8.4.0:

- Responses have no `guid` and no `ts` fields.
- Error messages use different wording, and `verbose=true` returns plain messages.
- Only `NONE` and `BACKWARD` compatibility and only Avro are supported.
- A hard delete keeps the id. Confluent forgets an id once no subject uses it and gives the schema a new id when it is registered again.
- When one schema is stored under several ids by import, registering it in a new subject returns the first id. Confluent returns the latest.
- Named types defined inside referenced schemas are always known to the referencing schema.
- Doubles in defaults are printed with the shortest form that reads back to the same value. Java can print a longer form for some values.

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
