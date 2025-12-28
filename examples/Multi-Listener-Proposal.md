# Multi-Listener Support Proposal

## 1. Problem Statement

Currently, the `KafscaleCluster` CRD only supports a single listener configuration via simpler fields:
```yaml
spec:
  brokers:
    advertisedHost: kafscale-broker
    advertisedPort: 9092
```

This limitation prevents:
1.  Separating internal cluster traffic (Pod-to-Pod) from external traffic (Client-to-Cluster).
2.  Supporting different security protocols (PLAINTEXT vs SSL/SASL) on different ports.
3.  Connecting to the broker from outside Kubernetes (e.g., local development or external consumers) while maintaining stable internal addressing.

## 2. Proposed Solution

We propose extending the `KafscaleCluster` CRD to replace the single `advertisedHost` and `advertisedPort` fields with a flexible `listeners` list. This aligns with standard Kafka configuration patterns (`listeners`, `advertised.listeners`, `listener.security.protocol.map`).

### 2.1 CRD Changes

**Deprecate**:
- `spec.brokers.advertisedHost`
- `spec.brokers.advertisedPort`

**Introduce**:
- `spec.brokers.listeners`: A list of listener configurations.

#### Schema Definition (Draft)

```yaml
spec:
  brokers:
    listeners:
      - name: <string>           # Name of the listener (e.g., INTERNAL, EXTERNAL)
        port: <int>              # The container port to bind to
        type: <enum>             # internal | external | nodeport | loadbalancer
        advertisedHost: <string> # Optional: Explicit advertised hostname
        advertisedPort: <int>    # Optional: Explicit advertised port (if different from bind port)
        securityProtocol: <string> # Future: PLAINTEXT, SSL, SASL_PLAINTEXT, etc. (Default: PLAINTEXT)
```

### 2.2 Configuration Examples

#### Scenario A: Hybrid Internal & External Access (The goal)

This setup allows internal apps to talk via the K8s Service DNS (stable, internal) and external apps (e.g., developers, external data pipelines) to talk via NodePort or LoadBalancer.

```yaml
apiVersion: kafscale.io/v1alpha1
kind: KafscaleCluster
metadata:
  name: kafscale-hybrid
spec:
  brokers:
    replicas: 3
    listeners:
      # Listener for In-Cluster Traffic
      - name: INTERNAL
        port: 9092
        type: internal
        # securityProtocol: PLAINTEXT (default)

      # Listener for External Traffic (e.g., NodePort)
      - name: EXTERNAL
        port: 39092
        type: nodeport
        advertisedHost: k8s-node-ip.example.com # Or dynamically resolved
        # securityProtocol: PLAINTEXT
```

**Resulting Kafka Config (Internal Mapping):**
```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:39092
advertised.listeners=INTERNAL://kafscale-broker.namespace.svc:9092,EXTERNAL://k8s-node-ip.example.com:39092
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
```

### 2.3 Implementation Details

1.  **Service Generation**:
    *   `type: internal` listeners create/update the standard ClusterIP Headless Service.
    *   `type: nodeport` listeners create/update a NodePort Service.
    *   `type: loadbalancer` listeners create/update a LoadBalancer Service.

2.  **Advertised Address Resolution**:
    *   **Internal**: Defaults to `<service-name>.<namespace>.svc`.
    *   **External**: The operator might need to determine the Node IP or LoadBalancer IP to populate `advertised.listeners` correctly in the `server.properties` or environment variables injected into the Pod.

## 3. Future Extensions (Security)

By using the `name` field as a key, we can easily map security settings in a future `security` section of the CRD.

```yaml
spec:
  security:
    listeners:
      EXTERNAL:
        protocol: SSL
        sslSecretRef: my-tls-secret
      INTERNAL:
        protocol: PLAINTEXT
```

## 4. Migration Path

1.  Make `listeners` optional initially.
2.  If `listeners` is missing but `advertisedHost/Port` exists, auto-generate a single `listeners` entry (backward compatibility).
3.  If `listeners` is present, ignore legacy fields.
