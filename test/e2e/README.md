# Kafscale E2E Harness

These tests spin up a full cluster (via [kind](https://kind.sigs.k8s.io)), install the local Helm chart, and hit the console API over a port-forward. They are opt-in because they require Docker/kind/helm on the host and take several minutes.

## Prerequisites

1. Docker daemon (Colima, Docker Desktop, etc.)
2. `kind`, `kubectl`, and `helm` binaries on your `$PATH`
3. Internet access to pull the Bitnami `etcd` chart (the harness installs a single-node etcd for the operator)

## Running

```bash
KAFSCALE_E2E=1 go test -tags=e2e ./test/e2e -v
```

Optional environment variables:

- `KAFSCALE_KIND_CLUSTER`: reuse an existing kind cluster without creating/deleting one.

The harness installs everything into the `kafscale-e2e` namespace and removes it after the test (unless you reused a cluster).
