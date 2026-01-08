<!--
Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# Storage Backend Compatibility

KafScale uses S3-compatible object storage as its source of truth. While designed for AWS S3, it works with any storage backend that implements the S3 API.

## Compatibility Matrix

| Provider | Compatibility | Notes |
|----------|--------------|-------|
| AWS S3 | ✅ Native | Full support, including IRSA |
| DigitalOcean Spaces | ✅ Native | Drop-in replacement |
| Cloudflare R2 | ✅ Native | Zero egress fees |
| Backblaze B2 | ✅ Native | S3-compatible API |
| Wasabi | ✅ Native | Flat pricing model |
| Linode Object Storage | ✅ Native | S3-compatible |
| Vultr Object Storage | ✅ Native | S3-compatible |
| MinIO | ✅ Native | Self-hosted, any infrastructure |
| Google Cloud Storage | ⚠️ Interop | Requires HMAC keys, XML API |
| Oracle Cloud | ⚠️ Interop | S3 Compatibility API |
| IBM Cloud Object Storage | ⚠️ Interop | S3-compatible API |
| Azure Blob Storage | ❌ Proxy | Requires MinIO Gateway |

**Legend:**
- ✅ Native: Standard S3 SDK works with endpoint change
- ⚠️ Interop: Works via compatibility layer, minor config differences
- ❌ Proxy: Requires additional infrastructure

---

## AWS S3

Native support. No `endpoint` configuration needed.

### KafScaleCluster

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: production
  namespace: kafscale
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-production
    region: us-east-1
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints: []
```

### Credentials Secret

```bash
kubectl -n kafscale create secret generic kafscale-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=AKIA... \
  --from-literal=AWS_SECRET_ACCESS_KEY=...
```

### IAM Role (EKS with IRSA)

For production on EKS, use IAM Roles for Service Accounts instead of static credentials:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kafscale-broker
  namespace: kafscale
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/kafscale-s3-role
```

Required IAM permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::kafscale-production",
        "arn:aws:s3:::kafscale-production/*"
      ]
    }
  ]
}
```

---

## DigitalOcean Spaces

Drop-in S3 replacement. Change endpoint and region.

### KafScaleCluster

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: production
  namespace: kafscale
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-production
    region: nyc3
    endpoint: https://nyc3.digitaloceanspaces.com
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints: []
```

### Credentials Secret

Generate Spaces access keys in the DigitalOcean console under API → Spaces Keys.

```bash
kubectl -n kafscale create secret generic kafscale-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=DO00... \
  --from-literal=AWS_SECRET_ACCESS_KEY=...
```

**Available regions:** `nyc3`, `sfo3`, `ams3`, `sgp1`, `fra1`

---

## Cloudflare R2

S3-compatible with zero egress fees. Ideal for high-read workloads.

### KafScaleCluster

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: production
  namespace: kafscale
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-production
    region: auto
    endpoint: https://<ACCOUNT_ID>.r2.cloudflarestorage.com
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints: []
```

### Credentials Secret

Generate R2 API tokens in Cloudflare dashboard under R2 → Manage R2 API Tokens.

```bash
kubectl -n kafscale create secret generic kafscale-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=... \
  --from-literal=AWS_SECRET_ACCESS_KEY=...
```

**Note:** Replace `<ACCOUNT_ID>` with your Cloudflare account ID.

---

## Backblaze B2

Cost-effective S3-compatible storage.

### KafScaleCluster

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: production
  namespace: kafscale
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-production
    region: us-west-004
    endpoint: https://s3.us-west-004.backblazeb2.com
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints: []
```

### Credentials Secret

Create application keys in B2 console with read/write access to your bucket.

```bash
kubectl -n kafscale create secret generic kafscale-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=<keyID> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<applicationKey>
```

**Endpoint format:** `https://s3.<region>.backblazeb2.com`

---

## Wasabi

Hot cloud storage with flat pricing and no egress fees.

### KafScaleCluster

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: production
  namespace: kafscale
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-production
    region: us-east-1
    endpoint: https://s3.us-east-1.wasabisys.com
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints: []
```

### Credentials Secret

```bash
kubectl -n kafscale create secret generic kafscale-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=... \
  --from-literal=AWS_SECRET_ACCESS_KEY=...
```

**Available regions:** `us-east-1`, `us-east-2`, `us-west-1`, `eu-central-1`, `eu-west-1`, `ap-northeast-1`, `ap-northeast-2`

---

## Google Cloud Storage

GCS provides S3 interoperability via its XML API. Requires HMAC keys.

### Setup

1. Enable interoperability in GCS console: Storage → Settings → Interoperability
2. Create HMAC keys for a service account

### KafScaleCluster

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: production
  namespace: kafscale
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-production
    region: auto
    endpoint: https://storage.googleapis.com
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints: []
```

### Credentials Secret

Use HMAC keys (not JSON service account keys):

```bash
kubectl -n kafscale create secret generic kafscale-s3 \
  --from-literal=AWS_ACCESS_KEY_ID=GOOG... \
  --from-literal=AWS_SECRET_ACCESS_KEY=...
```

### Limitations

- Some S3 features may behave differently (e.g., versioning, lifecycle policies)
- Path-style URLs required (GCS doesn't support virtual-hosted style for interop)
- Multipart upload semantics may vary slightly

---

## Azure Blob Storage

Azure Blob Storage is **not** S3-compatible. Use MinIO as a gateway proxy.

### Architecture

```
KafScale Brokers → MinIO Gateway → Azure Blob Storage
```

### Deploy MinIO Gateway

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio-azure-gateway
  namespace: kafscale
spec:
  replicas: 2
  selector:
    matchLabels:
      app: minio-gateway
  template:
    metadata:
      labels:
        app: minio-gateway
    spec:
      containers:
      - name: minio
        image: minio/minio:latest
        args:
          - gateway
          - azure
        env:
          - name: MINIO_ROOT_USER
            valueFrom:
              secretKeyRef:
                name: minio-gateway-creds
                key: accessKey
          - name: MINIO_ROOT_PASSWORD
            valueFrom:
              secretKeyRef:
                name: minio-gateway-creds
                key: secretKey
          - name: AZURE_STORAGE_ACCOUNT
            valueFrom:
              secretKeyRef:
                name: azure-storage-creds
                key: accountName
          - name: AZURE_STORAGE_KEY
            valueFrom:
              secretKeyRef:
                name: azure-storage-creds
                key: accountKey
        ports:
          - containerPort: 9000
---
apiVersion: v1
kind: Service
metadata:
  name: minio-gateway
  namespace: kafscale
spec:
  selector:
    app: minio-gateway
  ports:
    - port: 9000
      targetPort: 9000
```

### KafScaleCluster

Point KafScale at the MinIO gateway:

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: production
  namespace: kafscale
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-production
    endpoint: http://minio-gateway.kafscale.svc:9000
    credentialsSecretRef: minio-gateway-creds
  etcd:
    endpoints: []
```

### Secrets

```bash
# MinIO gateway credentials (what KafScale uses)
kubectl -n kafscale create secret generic minio-gateway-creds \
  --from-literal=accessKey=kafscale-access \
  --from-literal=secretKey=kafscale-secret-key

# Azure storage credentials (what MinIO uses)
kubectl -n kafscale create secret generic azure-storage-creds \
  --from-literal=accountName=<storage-account-name> \
  --from-literal=accountKey=<storage-account-key>
```

### Trade-offs

- **Added latency:** Extra network hop through gateway
- **Operational overhead:** Another component to manage and monitor
- **Single point of failure:** Gateway needs HA configuration
- **Cost:** Compute for gateway instances

Consider native S3-compatible providers if Azure isn't a hard requirement.

---

## MinIO (Self-Hosted)

Run your own S3-compatible storage on any infrastructure—on-prem, edge, or any cloud.

### Docker Compose (Development)

The default `docker-compose.yml` includes MinIO:

```bash
docker-compose up -d
```

MinIO runs on port 9000 with default credentials `minioadmin:minioadmin`.

### KafScaleCluster (Kubernetes)

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafScaleCluster
metadata:
  name: demo
  namespace: kafscale
spec:
  brokers:
    replicas: 3
  s3:
    bucket: kafscale-data
    endpoint: http://minio.minio-system.svc:9000
    credentialsSecretRef: kafscale-s3
  etcd:
    endpoints: []
```

### Production MinIO

For production, deploy MinIO in distributed mode with erasure coding:

```bash
helm repo add minio https://charts.min.io/
helm install minio minio/minio \
  --namespace minio-system --create-namespace \
  --set mode=distributed \
  --set replicas=4 \
  --set persistence.size=100Gi
```

---

## Common Considerations

### Path Style vs Virtual Hosted Style

Some S3-compatible backends only support path-style URLs. If you encounter bucket resolution issues, ensure your SDK is configured for path-style access.

### TLS/SSL

For production, always use HTTPS endpoints. Self-signed certificates may require additional CA configuration in the broker pods.

### Regional Latency

Place your storage in the same region as your Kubernetes cluster. KafScale's ~500ms latency target assumes low-latency storage access.

### Bucket Lifecycle

KafScale manages segment files in `.kfs` format. Configure bucket lifecycle policies carefully—avoid auto-deletion rules that could remove active segments.

### Testing Your Backend

Before deploying KafScale, verify S3 compatibility:

```bash
# Using AWS CLI with custom endpoint
aws s3 ls --endpoint-url https://your-endpoint.com

# Create test bucket
aws s3 mb s3://kafscale-test --endpoint-url https://your-endpoint.com

# Upload test object
echo "test" | aws s3 cp - s3://kafscale-test/test.txt --endpoint-url https://your-endpoint.com

# Verify
aws s3 ls s3://kafscale-test/ --endpoint-url https://your-endpoint.com
```

---

## Tested Configurations

| Backend | Version Tested | Status |
|---------|---------------|--------|
| AWS S3 | - | ✅ Production |
| MinIO | 2024-01-xx | ✅ Production |
| DigitalOcean Spaces | - | ✅ Tested |
| Cloudflare R2 | - | ✅ Tested |

*Other backends listed should work but may not be continuously tested. Community reports welcome.*
