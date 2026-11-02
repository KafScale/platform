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
- Compatibility levels: `NONE`, `BACKWARD` (the default), `BACKWARD_TRANSITIVE`, `FORWARD`, `FORWARD_TRANSITIVE`, `FULL` and `FULL_TRANSITIVE`, as in Confluent Schema Registry 8.4.0. `BACKWARD` means the new schema can read data written with the old one, `FORWARD` means the old schema can read data written with the new one, and `FULL` means both. The plain levels check against the latest live version of the subject, the `_TRANSITIVE` levels against every live version. Soft deleted versions are not checked. `PUT /config` accepts the level in any case and returns it in upper case. Other values are rejected with 42203.
- Modes: `READWRITE` (the default), `READONLY` and `IMPORT`.
- Schema references, resolved by subject and version, nested up to 32 levels.
- Soft and hard deletes of subjects and versions.

Endpoints:

| Method | Path | Query parameters |
|--------|------|------------------|
| GET | `/schemas/types` | |
| GET | `/schemas/ids/{id}` | |
| GET | `/schemas/guids/{guid}` | |
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

`{version}` accepts a number or `latest`. Register, lookup, `GET /schemas/ids/{id}` and `GET /subjects/{subject}/versions/{version}` responses carry a `guid`, computed like Confluent Schema Registry 8.4.0: the MD5 of the stored schema text followed by the name, subject and version (four byte big endian) of each reference, written as a UUID. Clients that put the schema guid in the `__value_schema_id` or `__key_schema_id` record header, such as the `header_schema_id_serializer` in confluent-kafka-python and the Java `HeaderSchemaIdSerializer`, resolve it with `GET /schemas/guids/{guid}`. It returns `guid`, `schemaType`, `references`, `schema` and `deleted`, which is true when only soft deleted versions use the schema. An unknown guid returns 40403. Schema text above 1 MiB is rejected with 42209, and a request body above 2 MiB with 400. `/contexts` always returns `["."]`. `POST /compatibility/subjects/{subject}/versions` without a version checks the schema the way a register would under the subject's level. With a version or `latest`, it checks against that one version only, in the direction the level sets. Compatibility against a subject that does not exist returns `is_compatible: true` for `latest` or no version, and 40402 for a numbered version. `DELETE /config/{subject}` and `DELETE /mode/{subject}` return 40401 when the subject has no setting. `DELETE /config` returns the previous global level and resets it to `BACKWARD`. Errors use the Confluent body `{"error_code": ..., "message": ...}` and codes: 40401 subject not found, 40402 version not found, 40403 schema not found, 40404 subject soft deleted, 40405 subject not soft deleted, 40406 version soft deleted, 40407 version not soft deleted, 40408 subject compatibility not set, 40409 subject mode not set, 40901 incompatible schema (HTTP 409), 400 malformed request body, 42201 invalid schema, 42202 invalid version, 42203 invalid compatibility level, 42204 invalid mode, 42205 operation not permitted, 42206 reference exists, 42209 schema too large, 50001 store error, 50002 operation timed out.

### Ids and versions

Schema ids are global, unique and increase with every new schema. Ids can have gaps. The registry stores each schema in the form Confluent Schema Registry stores it: the Avro schema printed by the Java Avro library, with whitespace removed, names written relative to the enclosing namespace, and default values and properties kept. Registering a schema whose stored form, type and references already exist anywhere in the registry returns the existing id. Under one subject, a schema that differs from an existing version only in the order of properties or aliases returns that version, the highest one if several match. Doubles in default values are printed as Java's `Double.toString` prints them, for example `1.0E-5` and `4.9E-324`. With `normalize=true`, properties and aliases are sorted, `error` is stored as `record`, and an empty `namespace` counts as no namespace, so the type takes the enclosing namespace, before the id is chosen. A schema whose normalized form is not a valid schema is rejected with 42201.

A new version number is one above the highest version the subject has, live or soft deleted. A version removed with a hard delete is no longer counted, so its number can be given again. Registering a schema that only exists as a soft deleted version of the subject creates a new version and removes the soft deleted one.

Every write is one etcd transaction that compares the revisions of the keys it read, so concurrent requests on any number of replicas cannot give one id to two schemas, give one version to two ids, or lose a version. When a transaction loses a race it retries after a short random wait. When the request timeout runs out first, the request fails with 50002 and nothing is written.

### Deletes

A soft delete hides a version or a subject. A hard delete (`permanent=true`) needs a soft delete first and then removes the version or subject. Deleting a version or subject that another schema references fails with 42206.

A hard delete keeps the schema itself: `GET /schemas/ids/{id}` still returns it, and its id is never given to another schema. Registering the same schema again returns the same id. Nothing removes schemas that no subject uses, and nothing removes their S3 objects.

### Moving schemas into the built in registry

To keep the ids that existing data carries, import the schemas with their ids before any client registers new ones. `kafscale-cli schema import` does this from any registry with the Confluent REST API:

```
kafscale-cli schema import --from https://source-registry:8081 --to http://kafscale-schema-registry:8081 \
  [--auth user:password | --auth bearer:<token>] [--to-auth ...] [--subjects '<regex>'] [--dry-run] [--force]
```

It reads `/subjects?deleted=true` and every version of each subject, including soft deleted ones, and imports them in ascending id order, with every referenced subject version before the schemas that reference it. A referenced subject version that `--subjects` does not match is imported too, but its config and mode are not copied. Before the first write it switches the target to `IMPORT` mode, which needs `--force` when the target already holds schemas. Each version keeps its id and version number. Versions that are soft deleted in the source are soft deleted in the target. Afterwards it copies the global compatibility level and the compatibility level and mode of each subject that has its own. The global mode of the target stays `IMPORT`. `--auth` and `--to-auth` can also be set with `KAFSCALE_SCHEMA_IMPORT_AUTH` and `KAFSCALE_SCHEMA_REGISTRY_AUTH`, and `--to` with `KAFSCALE_SCHEMA_REGISTRY_URL`.

The import can be run again at any time, for example to copy schemas registered in the source since the last run or after a run stopped halfway. A version that the target already holds with the same id and the same schema is skipped; schemas are compared in their stored form, so formatting differences in the source do not count. When the target holds that subject version with a different id or schema, or holds the id with a different schema, the import reports both schemas as a conflict, imports the rest, prints the counts of imported, skipped and soft deleted versions, changed settings and conflicts, and exits with an error. Two source versions that carry the same id with different schemas are reported as a conflict too, also with `--dry-run`. Any other error, for example a 422 from the target, stops the run. The CLI then still prints the counts and, when the target is in `IMPORT` mode, says so; run the same command again to resume. KafScale has only the default schema context. Source subjects in another Confluent context, named `:.<context>:<subject>`, are not imported: the CLI names each one, counts them as skipped contexts and exits with an error, also with `--dry-run`. A subject named `:.:<subject>` is in the default context and is imported under that name. A subject whose mode in the target is not `IMPORT` is switched to `IMPORT` while its new versions are imported and set back afterwards. `--dry-run` prints what would change and writes nothing.

When the last run is done, switch the target to `READWRITE` with `PUT /mode` and `{"mode": "READWRITE"}`, and point clients at it. Hard deleted versions are not in the source anymore and are not copied.

Without the CLI, the same steps by hand:

1. `PUT /mode` with `{"mode": "IMPORT"}`, or `PUT /mode/{subject}` for one subject. This is refused with 42205 while the registry, or that subject, already holds schemas, unless `force=true` is set.
2. For every subject version in the source registry, `POST /subjects/{subject}/versions` with `{"schema": ..., "schemaType": ..., "references": ..., "id": ..., "version": ...}`. The id must be free or already hold the same schema, and the version must be free or already hold the same id, otherwise the request fails with 42205. Compatibility is not checked in `IMPORT` mode.
3. `PUT /mode` with `{"mode": "READWRITE"}`. New schemas get ids above the highest imported id.

In `READONLY` and `IMPORT` mode, registering a schema that the subject already holds without an id returns its existing id and version, as in Confluent Schema Registry. A new schema without an id is refused with 42205.

### Storage

All state lives in etcd under `/kafscale/schemas/`. Schema text larger than `KAFSCALE_SCHEMA_REGISTRY_INLINE_LIMIT_BYTES` (64 KiB by default) is written to S3 at `<namespace>/@schemas/sha256/<sha256 of the text>`, and etcd keeps only the key. Without `KAFSCALE_S3_BUCKET`, such schemas are rejected with 42201. The object key is the hash of the text, so retries and identical schemas write the same object once per request. When a write uploads the text and then fails in etcd, the object stays in S3 without an etcd key pointing to it. Such objects are not removed today. To find them, list `<namespace>/@schemas/sha256/` and compare it with the `s3Key` values under `/kafscale/schemas/ids/`.

Each instance caches schemas by id, up to 32 MiB of schema text, and drops entries when the cache is full. It drops the whole cache when etcd reports a lower revision, a different cluster id, a lower raft term or a lower id counter than it has seen.

With `KAFSCALE_S3_BUCKET` set, every id the registry gives out is also recorded in S3 as `<namespace>/@schemas/ids/<id, 20 digits>` holding the hash of the schema. The registry writes it after the request has passed validation, the compatibility check and the version checks, right before the etcd write, so a rejected register or import records nothing. The registry writes this object with `PutObject` and `If-None-Match: *`, so only one writer can create it. When the write returns 412 PreconditionFailed, the id is taken and the registry tries a higher id. When it returns 409 ConditionalRequestConflict, another write to the same key was in progress and this write did not happen; the registry retries the same id up to 3 times and then tries a higher id. These objects are never changed or removed by the registry, so an etcd restored from an older backup does not hand out an id that existing data already carries, and two instances that both see the same free id never record it for two schemas. When S3 cannot be written, registering a new schema fails with 50001 and nothing is written to etcd. Without `KAFSCALE_S3_BUCKET` the registry has only etcd, and after a restore it gives out again the ids issued after the backup was taken. After restoring etcd, restart the registry pods so no instance keeps state from before the restore. Restarting every registry pod after an etcd restore is required. Each pod caches schemas by id and clears its cache when etcd answers with a lower revision or raft term than the pod saw before the request started, or with a different cluster id. A restore does not reliably trigger any of these. `etcdutl snapshot restore` with the same cluster token and peer URLs, which is what the KafScale operator runs, keeps the cluster id and the member ids. The revision goes back to the one in the snapshot, but once other pods write, it climbs past what an idle pod saw, so that pod never sees a lower revision. The raft term starts again at 1 and rises with each election, so it is only lower when the old term was higher. A restore with `--bump-revision` and `--mark-compacted` raises the revision above anything a pod saw, so no pod resets at all, and every pod keeps serving schemas that the snapshot no longer has. Key values and their create and mod revisions come back exactly as they were in the snapshot, so no key can tell the old history from the new one. A pod that misses the restore keeps schemas it cached before it. With S3 configured, ids are never given out twice, so those cached schemas are still right. Without S3, an id given out again after the restore can be served from the cache with the schema it had before the restore.

At startup, with `KAFSCALE_S3_BUCKET` set, the registry writes the key `<namespace>/@schemas/probe/<random>` twice with `If-None-Match: *`, then reads it back, writes it with `If-Match` and a wrong ETag, writes it with `If-Match` and its ETag, and deletes it. If the second `If-None-Match` write or the wrong `If-Match` write does not fail with 412, the read returns no ETag, the matching write fails, or S3 cannot be reached, the registry exits with an error instead of serving, because a backend that ignores the conditions would let two instances record the same id for different schemas. The registry needs `s3:PutObject`, `s3:GetObject` and `s3:DeleteObject` on `<namespace>/@schemas/`. `DeleteObject` is used only to remove the probe key. When that delete fails, the registry logs a warning that names the key and keeps running, and the probe key stays in the bucket. Backends:

- AWS S3 and MinIO support the condition. MinIO was checked with the image built from `test/e2e/minio.Dockerfile`.
- Cloudflare R2 documents atomic `If-None-Match: *` on `PutObject`. Ceph RGW is reported to support it. Neither was tested here.
- Backblaze B2 does not support conditional writes, and the Google Cloud Storage XML API supports `If-None-Match` only on reads. The registry does not start against them with `KAFSCALE_S3_BUCKET` set.
- For any other provider in [supported.md](supported.md), the startup check decides.

A write can claim an id in S3 and then fail in etcd, for example when another instance registered first. That id stays recorded in S3 without a schema in etcd. New registrations skip it. When the etcd write fails because another write changed the state it read, the registry retries with the id it already recorded as long as no schema in etcd has that id and the id counter has not passed it. Within one instance, registrations of new schemas run one at a time, so concurrent registrations on one instance leave no such ids. A registration first checks without waiting whether the schema is already the latest version of the subject, and if it is, it returns that id and version, so these registrations and lookups are not held up by a slow write. A registration that waits for another write on the same instance and reaches its deadline fails with 50002. This limits new schemas to the rate one instance can write them one after another. With MinIO and a 3 node etcd on one machine, 1000 new schemas were registered at 180 per second with 1 writer, 250 with 16 and 261 with 64 writers. Registrations that race on different instances can leave one for each lost race, because ids stay increasing and a later id may already be given out. An import with an explicit id that S3 records for the same schema is accepted. An import with an explicit id that S3 records for a different schema is accepted when etcd has no schema with that id. The registry first reads the S3 object with its ETag. It then makes sure the id counter in etcd is at least that id, raising it in an etcd transaction that also checks that no schema has the id. After that, a register that recorded the id in S3 and has not written etcd yet can no longer write it, because its etcd write requires the counter it read, so it picks a new id. The registry then replaces the object with `PutObject` and `If-Match: <ETag>` and logs the id and key. When another write changed the object in between, the replace fails with 412 and the import is refused with 42205. When two imports of the same id with different schemas run at once, etcd accepts only one, and the one that wrote etcd rewrites the S3 object with its own hash if the other replaced it in between. No clock is used, so clock skew between pods and S3 does not matter. An id that data carries but etcd lost in a restore looks the same to S3, so before importing an id over an old object, check that no topic data uses that id with the schema whose hash the S3 object holds. To resolve a refused import by hand, check that no topic data uses that id with the schema whose hash the S3 object holds, for example in the source registry or in backups taken before the restore. If none does, delete the object named in the message and retry the import. If data does use it, import the schema under a different id.

### Differences from Confluent Schema Registry

Checked against Confluent Schema Registry 8.4.0:

- Responses have no `ts` field.
- `POST /subjects/{subject}/versions` with an empty body `{}` returns 42201. Confluent returns 42201 for a new subject and the latest version for an existing subject.
- The size limit for schema text is 1 MiB. Confluent limits the size of the stored record, which is slightly above 1 MiB of schema text.
- Error messages use different wording. An incompatible schema gets the same four details as in Confluent, the reason, `{oldSchemaVersion: n}`, `{oldSchema: '...'}` and `{validateFields: 'false', compatibility: '<level>'}`, but the reason is a plain sentence where Confluent returns an `errorType`, a `description` and `additionalInfo`.
- Only Avro is supported.
- A hard delete keeps the id. Confluent forgets an id once no subject uses it and gives the schema a new id when it is registered again.
- When one schema is stored under several ids by import, registering it in a new subject returns the first id. Confluent returns the latest.
- Named types defined inside referenced schemas are always known to the referencing schema.

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
