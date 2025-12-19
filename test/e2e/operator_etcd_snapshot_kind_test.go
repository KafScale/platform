//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	kindEnvEnable = "KAFSCALE_E2E_KIND"
	kindNamespace = "kafscale-e2e"
)

func TestOperatorEtcdSnapshotKindE2E(t *testing.T) {
	if !parseBoolEnv("KAFSCALE_E2E") || !parseBoolEnv(kindEnvEnable) {
		t.Skipf("set KAFSCALE_E2E=1 and %s=1 to run kind integration test", kindEnvEnable)
	}

	requireBinaries(t, "docker", "kind", "kubectl", "helm")

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	clusterName := envOrDefault("KAFSCALE_KIND_CLUSTER", "kafscale-e2e")
	created := false
	if os.Getenv("KAFSCALE_KIND_CLUSTER") == "" {
		if parseBoolEnv("KAFSCALE_KIND_RECREATE") && kindClusterExists(ctx, clusterName) {
			_ = execCommand(ctx, "kind", "delete", "cluster", "--name", clusterName)
		}
		if !kindClusterExists(ctx, clusterName) {
			runCmdGetOutput(t, ctx, "kind", "create", "cluster", "--name", clusterName)
			created = true
		}
	}
	t.Cleanup(func() {
		if created {
			_ = execCommand(ctx, "kind", "delete", "cluster", "--name", clusterName)
		}
	})

	ensureNamespace(t, ctx, kindNamespace)
	applyMinio(t, ctx, kindNamespace)
	waitForRollout(t, ctx, kindNamespace, "deployment/minio", 2*time.Minute)

	brokerImage := envOrDefault("KAFSCALE_BROKER_IMAGE", "ghcr.io/novatechflow/kafscale-broker:dev")
	operatorImage := envOrDefault("KAFSCALE_OPERATOR_IMAGE", "ghcr.io/novatechflow/kafscale-operator:dev")
	consoleImage := envOrDefault("KAFSCALE_CONSOLE_IMAGE", "ghcr.io/novatechflow/kafscale-console:dev")

	requireImage(t, ctx, brokerImage)
	requireImage(t, ctx, operatorImage)
	requireImage(t, ctx, consoleImage)

	loadImage(t, ctx, clusterName, brokerImage)
	loadImage(t, ctx, clusterName, operatorImage)
	loadImage(t, ctx, clusterName, consoleImage)

	chartPath := filepath.Join(repoRoot(t), "deploy", "helm", "kafscale")
	operatorRepo, operatorTag := splitImage(operatorImage)
	consoleRepo, consoleTag := splitImage(consoleImage)

	runCmdGetOutput(t, ctx, "helm", "upgrade", "--install", "kafscale", chartPath,
		"--namespace", kindNamespace,
		"--create-namespace",
		"--set", fmt.Sprintf("operator.image.repository=%s", operatorRepo),
		"--set", fmt.Sprintf("operator.image.tag=%s", operatorTag),
		"--set", fmt.Sprintf("console.image.repository=%s", consoleRepo),
		"--set", fmt.Sprintf("console.image.tag=%s", consoleTag),
		"--set", "operator.etcdEndpoints[0]=",
	)

	operatorDeployment := getComponentDeployment(t, ctx, kindNamespace, "operator")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "set", "env", "deployment/"+operatorDeployment,
		"BROKER_IMAGE="+brokerImage,
		"KAFSCALE_OPERATOR_ETCD_ENDPOINTS=",
		"KAFSCALE_OPERATOR_ETCD_SNAPSHOT_BUCKET=kafscale-snapshots",
		"KAFSCALE_OPERATOR_ETCD_SNAPSHOT_CREATE_BUCKET=1",
		"KAFSCALE_OPERATOR_ETCD_SNAPSHOT_PROTECT_BUCKET=1",
		"KAFSCALE_OPERATOR_ETCD_SNAPSHOT_S3_ENDPOINT=http://minio."+kindNamespace+".svc.cluster.local:9000",
	)
	waitForRollout(t, ctx, kindNamespace, "deployment/"+operatorDeployment, 2*time.Minute)

	applyS3Secret(t, ctx, kindNamespace)
	applyClusterManifest(t, ctx, kindNamespace)

	if err := waitForResource(t, ctx, kindNamespace, "cronjob", "kafscale-etcd-snapshot", 2*time.Minute); err != nil {
		dumpKindDebug(t, ctx, kindNamespace, operatorDeployment)
		t.Fatalf("timeout waiting for cronjob/kafscale-etcd-snapshot: %v", err)
	}
	waitForCondition(t, ctx, kindNamespace, "kafscalecluster/kafscale", "EtcdSnapshotAccess", "True", 2*time.Minute)

	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "delete", "job", "etcd-snapshot-manual", "--ignore-not-found=true")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "create", "job", "etcd-snapshot-manual", "--from=cronjob/kafscale-etcd-snapshot")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "wait", "--for=condition=complete", "job/etcd-snapshot-manual", "--timeout=180s")

	listing := runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "run", "s3-list", "--rm", "-i", "--restart=Never",
		"--image=amazon/aws-cli:2.15.0",
		"--env", "AWS_ACCESS_KEY_ID=minioadmin",
		"--env", "AWS_SECRET_ACCESS_KEY=minioadmin",
		"--env", "AWS_DEFAULT_REGION=us-east-1",
		"--env", "AWS_EC2_METADATA_DISABLED=true",
		"--command", "--", "sh", "-c",
		"aws --endpoint-url http://minio."+kindNamespace+".svc.cluster.local:9000 s3api list-objects-v2 --bucket kafscale-snapshots --prefix etcd-snapshots/ --query 'Contents[].Key' --output text")
	t.Logf("minio snapshot objects:\n%s", string(listing))
	if !bytes.Contains(listing, []byte(".db")) {
		t.Fatalf("expected snapshot .db object in minio, got:\n%s", string(listing))
	}
	if !bytes.Contains(listing, []byte(".sha256")) {
		t.Fatalf("expected snapshot .db.sha256 object in minio, got:\n%s", string(listing))
	}

	t.Log("testing operator HA: deleting leader pod and waiting for rollout")
	operatorPod := getPodByLabel(t, ctx, kindNamespace, "app.kubernetes.io/component=operator")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "delete", "pod", operatorPod)
	waitForRollout(t, ctx, kindNamespace, "deployment/"+operatorDeployment, 2*time.Minute)

	t.Log("testing etcd HA: deleting one member and waiting for ready")
	etcdPod := getPodByLabel(t, ctx, kindNamespace, "app=kafscale-etcd")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "delete", "pod", etcdPod)
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "wait", "--for=condition=ready", "pod", "-l", "app=kafscale-etcd", "--timeout=180s")

	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "delete", "job", "etcd-snapshot-manual", "--ignore-not-found=true")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "create", "job", "etcd-snapshot-manual", "--from=cronjob/kafscale-etcd-snapshot")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "wait", "--for=condition=complete", "job/etcd-snapshot-manual", "--timeout=180s")

	postFailoverListing := runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "run", "s3-list", "--rm", "-i", "--restart=Never",
		"--image=amazon/aws-cli:2.15.0",
		"--env", "AWS_ACCESS_KEY_ID=minioadmin",
		"--env", "AWS_SECRET_ACCESS_KEY=minioadmin",
		"--env", "AWS_DEFAULT_REGION=us-east-1",
		"--env", "AWS_EC2_METADATA_DISABLED=true",
		"--command", "--", "sh", "-c",
		"aws --endpoint-url http://minio."+kindNamespace+".svc.cluster.local:9000 s3api list-objects-v2 --bucket kafscale-snapshots --prefix etcd-snapshots/ --query 'Contents[].Key' --output text")
	t.Logf("minio snapshot objects after failover:\n%s", string(postFailoverListing))
	if !bytes.Contains(postFailoverListing, []byte(".db")) {
		t.Fatalf("expected snapshot .db object in minio after failover, got:\n%s", string(postFailoverListing))
	}
	if !bytes.Contains(postFailoverListing, []byte(".sha256")) {
		t.Fatalf("expected snapshot .db.sha256 object in minio after failover, got:\n%s", string(postFailoverListing))
	}
}

func requireBinaries(t *testing.T, names ...string) {
	t.Helper()
	for _, name := range names {
		if _, err := exec.LookPath(name); err != nil {
			t.Fatalf("%s not found in PATH", name)
		}
	}
}

func execCommand(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func ensureNamespace(t *testing.T, ctx context.Context, namespace string) {
	t.Helper()
	manifest := runCmdWithOutput(t, ctx, "kubectl", "create", "namespace", namespace, "--dry-run=client", "-o", "yaml")
	runCmdWithInput(t, ctx, "kubectl", string(manifest), "apply", "-f", "-")
}

func applyMinio(t *testing.T, ctx context.Context, namespace string) {
	t.Helper()
	manifest := fmt.Sprintf(`
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
        - name: minio
          image: quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z
          args: ["server", "/data", "--console-address", ":9001"]
          env:
            - name: MINIO_ROOT_USER
              value: minioadmin
            - name: MINIO_ROOT_PASSWORD
              value: minioadmin
          ports:
            - containerPort: 9000
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: %s
spec:
  selector:
    app: minio
  ports:
    - name: api
      port: 9000
      targetPort: 9000
`, namespace, namespace)
	runCmdWithInput(t, ctx, "kubectl", manifest, "apply", "-f", "-")
}

func applyS3Secret(t *testing.T, ctx context.Context, namespace string) {
	t.Helper()
	manifest := fmt.Sprintf(`
apiVersion: v1
kind: Secret
metadata:
  name: kafscale-s3-credentials
  namespace: %s
type: Opaque
stringData:
  KAFSCALE_S3_ACCESS_KEY: minioadmin
  KAFSCALE_S3_SECRET_KEY: minioadmin
`, namespace)
	runCmdWithInput(t, ctx, "kubectl", manifest, "apply", "-f", "-")
}

func applyClusterManifest(t *testing.T, ctx context.Context, namespace string) {
	t.Helper()
	manifest := fmt.Sprintf(`
apiVersion: kafscale.io/v1alpha1
kind: KafscaleCluster
metadata:
  name: kafscale
  namespace: %s
spec:
  brokers:
    replicas: 1
  s3:
    bucket: kafscale-snapshots
    region: us-east-1
    endpoint: http://minio.%s.svc.cluster.local:9000
    credentialsSecretRef: kafscale-s3-credentials
  etcd:
    endpoints: []
`, namespace, namespace)
	runCmdWithInput(t, ctx, "kubectl", manifest, "apply", "-f", "-")
}

func runCmdWithInput(t *testing.T, ctx context.Context, name, input string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = strings.NewReader(input)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("command %s %s failed: %v\n%s", name, strings.Join(args, " "), err, buf.String())
	}
}

func waitForRollout(t *testing.T, ctx context.Context, namespace, resource string, timeout time.Duration) {
	t.Helper()
	runCmdGetOutput(t, ctx, "kubectl", "-n", namespace, "rollout", "status", resource, fmt.Sprintf("--timeout=%s", timeout))
}

func waitForResource(t *testing.T, ctx context.Context, namespace, resource, name string, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		out := runCmdWithOutput(t, ctx, "kubectl", "-n", namespace, "get", resource, name, "-o", "name", "--ignore-not-found")
		if strings.TrimSpace(string(out)) != "" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s", timeout)
		}
		time.Sleep(2 * time.Second)
	}
}

func runCmdWithOutput(t *testing.T, ctx context.Context, name string, args ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(ctx, name, args...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	_ = cmd.Run()
	return buf.Bytes()
}

func waitForCondition(t *testing.T, ctx context.Context, namespace, resource, condition, status string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	jsonPath := fmt.Sprintf("{.status.conditions[?(@.type==\"%s\")].status}", condition)
	for {
		out := runCmdGetOutput(t, ctx, "kubectl", "-n", namespace, "get", resource, "-o", "jsonpath="+jsonPath)
		if strings.TrimSpace(string(out)) == status {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s condition %s=%s (got %s)", resource, condition, status, strings.TrimSpace(string(out)))
		}
		time.Sleep(2 * time.Second)
	}
}

func splitImage(image string) (string, string) {
	parts := strings.Split(image, ":")
	if len(parts) < 2 {
		return image, "latest"
	}
	return strings.Join(parts[:len(parts)-1], ":"), parts[len(parts)-1]
}

func requireImage(t *testing.T, ctx context.Context, image string) {
	t.Helper()
	if err := execCommand(ctx, "docker", "image", "inspect", image); err != nil {
		t.Fatalf("docker image %s not found; run `make docker-build` or set KAFSCALE_*_IMAGE envs", image)
	}
}

func loadImage(t *testing.T, ctx context.Context, clusterName, image string) {
	t.Helper()
	runCmdGetOutput(t, ctx, "kind", "load", "docker-image", image, "--name", clusterName)
}

func kindClusterExists(ctx context.Context, name string) bool {
	cmd := exec.CommandContext(ctx, "kind", "get", "clusters")
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) == name {
			return true
		}
	}
	return false
}

func getComponentDeployment(t *testing.T, ctx context.Context, namespace, component string) string {
	t.Helper()
	out := runCmdGetOutput(t, ctx, "kubectl", "-n", namespace, "get", "deployments",
		"-l", "app.kubernetes.io/component="+component,
		"-o", "jsonpath={.items[0].metadata.name}",
	)
	name := strings.TrimSpace(string(out))
	if name == "" {
		t.Fatalf("unable to resolve %s deployment", component)
	}
	return name
}

func getPodByLabel(t *testing.T, ctx context.Context, namespace, selector string) string {
	t.Helper()
	out := runCmdGetOutput(t, ctx, "kubectl", "-n", namespace, "get", "pods",
		"-l", selector,
		"-o", "jsonpath={.items[0].metadata.name}",
	)
	name := strings.TrimSpace(string(out))
	if name == "" {
		t.Fatalf("unable to resolve pod for %s", selector)
	}
	return name
}

func dumpKindDebug(t *testing.T, ctx context.Context, namespace, operatorDeployment string) {
	t.Helper()
	t.Log("dumping operator debug state")
	clusterOut := runCmdWithOutput(t, ctx, "kubectl", "-n", namespace, "get", "kafscalecluster", "kafscale", "-o", "yaml")
	t.Logf("kafscalecluster:\n%s", string(clusterOut))
	cronOut := runCmdWithOutput(t, ctx, "kubectl", "-n", namespace, "get", "cronjob", "-o", "wide")
	t.Logf("cronjobs:\n%s", string(cronOut))
	deployOut := runCmdWithOutput(t, ctx, "kubectl", "-n", namespace, "get", "deployment", operatorDeployment, "-o", "yaml")
	t.Logf("operator deployment:\n%s", string(deployOut))
	logsOut := runCmdWithOutput(t, ctx, "kubectl", "-n", namespace, "logs", "deployment/"+operatorDeployment, "--tail=200")
	t.Logf("operator logs:\n%s", string(logsOut))
	eventsOut := runCmdWithOutput(t, ctx, "kubectl", "-n", namespace, "get", "events", "--sort-by=.lastTimestamp")
	t.Logf("events:\n%s", string(eventsOut))
}
