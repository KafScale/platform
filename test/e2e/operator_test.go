//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	enableEnv      = "KAFSCALE_E2E"
	namespace      = "kafscale-e2e"
	releaseName    = "kafscale-e2e"
	consoleService = releaseName + "-kafscale-console"
	operatorDeploy = releaseName + "-kafscale-operator"
	consoleDeploy  = releaseName + "-kafscale-console"
)

func TestOperatorAndConsoleDeploy(t *testing.T) {
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ensureBinary(t, "kind")
	ensureBinary(t, "kubectl")
	ensureBinary(t, "helm")

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	clusterName, cleanupKind := ensureKindCluster(t, ctx)
	if cleanupKind != nil {
		t.Cleanup(func() { cleanupKind(t, context.Background()) })
	}

	runCmd(t, ctx, "kubectl", "config", "use-context", fmt.Sprintf("kind-%s", clusterName))

	runCmdIgnoreErr(t, ctx, "kubectl", "delete", "ns", namespace, "--ignore-not-found")
	runCmd(t, ctx, "kubectl", "create", "namespace", namespace)

	runCmd(t, ctx, "helm", "repo", "add", "bitnami", "https://charts.bitnami.com/bitnami", "--force-update")

	runCmd(t, ctx,
		"helm", "upgrade", "--install", "kafscale-etcd", "bitnami/etcd",
		"--namespace", namespace,
		"--set", "auth.rbac.enabled=false",
		"--set", "replicaCount=1",
		"--wait",
		"--timeout", "5m",
		"--create-namespace",
	)

	chartPath := filepath.Join("deploy", "helm", "kafscale")

	runCmd(t, ctx,
		"helm", "upgrade", "--install", releaseName, chartPath,
		"--namespace", namespace,
		"--set", "operator.etcdEndpoints[0]=http://kafscale-etcd:2379",
		"--set", "operator.image.pullPolicy=IfNotPresent",
		"--set", "console.image.pullPolicy=IfNotPresent",
		"--wait",
		"--timeout", "10m",
	)

	waitForRollout(t, ctx, operatorDeploy)
	waitForRollout(t, ctx, consoleDeploy)

	checkConsoleStatus(t, ctx)
}

func ensureBinary(t *testing.T, name string) {
    if _, err := exec.LookPath(name); err != nil {
        t.Skipf("%s not found in PATH", name)
    }
}

func ensureKindCluster(t *testing.T, ctx context.Context) (string, func(*testing.T, context.Context)) {
	if name := os.Getenv("KAFSCALE_KIND_CLUSTER"); name != "" {
		return name, nil
	}
	name := fmt.Sprintf("kafscale-e2e-%d", time.Now().Unix())
	runCmd(t, ctx, "kind", "create", "cluster", "--name", name, "--wait", "60s")
	cleanup := func(t *testing.T, ctx context.Context) {
		runCmdIgnoreErr(t, ctx, "kind", "delete", "cluster", "--name", name)
	}
	return name, cleanup
}

func waitForRollout(t *testing.T, ctx context.Context, deployment string) {
	runCmd(t, ctx, "kubectl", "-n", namespace, "rollout", "status", "--timeout=300s", fmt.Sprintf("deployment/%s", deployment))
}

func checkConsoleStatus(t *testing.T, ctx context.Context) {
	localPort := "18080"
	pfCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := exec.CommandContext(pfCtx, "kubectl", "-n", namespace, "port-forward", fmt.Sprintf("svc/%s", consoleService), localPort+":80")
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Start(); err != nil {
		t.Fatalf("port-forward start failed: %v (%s)", err, buf.String())
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})

	time.Sleep(5 * time.Second)

	url := fmt.Sprintf("http://127.0.0.1:%s/ui/api/status", localPort)
	reqCtx, reqCancel := context.WithTimeout(ctx, 15*time.Second)
	defer reqCancel()
	req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s failed: %v (port-forward logs: %s)", url, err, buf.String())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code from console: %d", resp.StatusCode)
	}
}

func runCmd(t *testing.T, ctx context.Context, name string, args ...string) {
	cmd := exec.CommandContext(ctx, name, args...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("command %s %s failed: %v\n%s", name, strings.Join(args, " "), err, buf.String())
	}
}

func runCmdIgnoreErr(t *testing.T, ctx context.Context, name string, args ...string) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Run()
}
