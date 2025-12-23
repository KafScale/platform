<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# Kafscale User Guide

Kafscale is a Kafka-compatible, S3-backed message transport system. It keeps brokers stateless, stores data in S3, and relies on Kubernetes for scheduling and scaling. This guide summarizes how to interact with the platform once it is deployed.

## Concepts

- **Topics / Partitions**: match upstream Kafka semantics. All Kafka client libraries continue to work.
- **Brokers**: stateless pods accepting Kafka protocol traffic on port 9092 and metrics + gRPC control on 9093.
- **Metadata**: stored in etcd, encoded via protobufs (`kafscale.metadata.*`).
- **Storage**: message segments live in S3 buckets; brokers only keep in-memory caches.
- **Operator**: Kubernetes controller that provisions brokers, topics, and wiring based on CRDs.

## S3 Configuration

1. **Create (or reference) a bucket**  
   - Name convention: `kafscale-{env}-{region}` (example: `kafscale-prod-us-east-1`).  
   - Bucket region should match the Kubernetes cluster region to avoid cross-region latency/cost.  
   - Enable versioning + SSE-KMS with a customer-managed key if your security posture requires it.

2. **Provision IAM credentials**  
   Grant `s3:ListBucket`, `GetObject`, `PutObject`, and `DeleteObject` on the bucket prefix. If the operator should manage buckets, include `CreateBucket` as well. Store the access key/secret (or assume-role info) in a Kubernetes `Secret`, ideally via Sealed Secrets/External Secrets so data at rest stays encrypted.

3. **Configure the KafscaleCluster CRD**  
   ```yaml
   apiVersion: kafscale.yourorg/v1alpha1
   kind: KafscaleCluster
   metadata:
     name: prod
   spec:
     s3:
       bucket: kafscale-prod-us-east-1
       region: us-east-1
       endpoint: ""          # optional custom endpoint
       kmsKeyArn: arn:aws:kms:us-east-1:1234:key/abcd
       forcePathStyle: false
       credentialsSecretRef:
         name: kafscale-s3-creds
         namespace: kafscale
   ```

   Optional for CRR: set `spec.s3.readBucket`, `spec.s3.readRegion`, and `spec.s3.readEndpoint` to point brokers at the replica bucket in their region. Brokers will attempt reads from the replica and fall back to the primary on misses.

4. **Mount credentials into brokers**  
   The operator projects the secret into broker pods and sets env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, etc.). Credential rotation is automatic—update the secret and the operator will restart brokers through the drain RPC.

5. **Validation**  
   The operator validates bucket access (list + put) during reconciliation. Check operator logs if the CRD status reports `S3ValidationFailed`.

6. **Broker overrides (optional)**  
   Brokers can read overrides from environment variables if you need different values per pod: `KAFSCALE_S3_BUCKET`, `KAFSCALE_S3_REGION`, `KAFSCALE_S3_ENDPOINT`, `KAFSCALE_S3_READ_BUCKET`, `KAFSCALE_S3_READ_REGION`, `KAFSCALE_S3_READ_ENDPOINT`, `KAFSCALE_S3_PATH_STYLE`, `KAFSCALE_S3_KMS_ARN`, `KAFSCALE_CACHE_BYTES`, `KAFSCALE_READAHEAD_SEGMENTS`, `KAFSCALE_SEGMENT_BYTES`, and `KAFSCALE_FLUSH_INTERVAL_MS`.

## Metadata Store (etcd)

Kafscale keeps topic/partition metadata and persisted offsets in etcd. The operator seeds the tree under `/kafscale/...` when you create clusters/topics. Brokers connect directly to etcd to fetch high watermarks and update offsets after flushes.

1. Provide etcd endpoints (usually the in-cluster control-plane or a dedicated StatefulSet) and configure the broker via env vars:
   ```bash
   export KAFSCALE_ETCD_ENDPOINTS="https://etcd-0:2379,https://etcd-1:2379"
   export KAFSCALE_ETCD_USERNAME="kafscale"
   export KAFSCALE_ETCD_PASSWORD="s3cr3t"
   ```
2. When these env vars are present, the broker uses the etcd-backed store; otherwise it falls back to the in-memory snapshot defined in the deployment manifest. The operator also writes the current cluster metadata as JSON under `/kafscale/metadata/snapshot`; brokers watch this key and pick up topic/partition changes automatically.
3. Credentials should be delivered via Kubernetes `Secret` objects and projected into the broker pods similar to the S3 credentials.

## Getting Started

1. **Deploy the Operator + Brokers**  
   The Helm chart under `deploy/helm/kafscale` installs the CRDs, operator, and a broker StatefulSet. (Helm packaging wired up once the codebase is ready.)

2. **Create a Topic**  
   Apply a `KafscaleTopic` custom resource (see `config/samples/`). The operator writes the protobuf topic config into etcd; brokers pick it up automatically.

3. **Produce / Consume**  
   Point any Kafka client at the broker service:
   ```bash
   kafka-console-producer --bootstrap-server kafscale-broker:9092 --topic orders
   kafka-console-consumer --bootstrap-server kafscale-broker:9092 --topic orders --from-beginning
   ```

4. **Monitoring**  
   - Metrics via Prometheus on port 9093 (`/metrics`)
   - Structured JSON logs from brokers/operators
   - Control-plane queries via the gRPC service defined in `proto/control/broker.proto`

5. **Scaling / Maintenance**  
   The operator uses Kubernetes HPA and the BrokerControl gRPC API to safely drain partitions before restarts. Users can request manual drains or flushes by invoking those RPCs (CLI tooling TBD).

## Limits / Non-Goals

- No embedded stream processing features—pair Kafscale with Flink, Wayang, Spark, etc.
- Transactions, idempotent producers, and log compaction are out of scope for the MVP.

For deeper architectural details or development guidance, read `kafscale-spec.md` and `docs/development.md`.
