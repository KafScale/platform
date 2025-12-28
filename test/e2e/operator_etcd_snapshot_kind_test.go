// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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
	e2eClientImage := envOrDefault("KAFSCALE_E2E_CLIENT_IMAGE", "ghcr.io/novatechflow/kafscale-e2e-client:dev")

	requireImage(t, ctx, brokerImage)
	requireImage(t, ctx, operatorImage)
	requireImage(t, ctx, consoleImage)
	requireImage(t, ctx, e2eClientImage)
	kafkaCliImage := envOrDefault("KAFSCALE_KAFKA_CLI_IMAGE", "confluentinc/cp-kafka:7.6.0")
	requireImageOrPull(t, ctx, kafkaCliImage)

	loadImage(t, ctx, clusterName, brokerImage)
	loadImage(t, ctx, clusterName, operatorImage)
	loadImage(t, ctx, clusterName, consoleImage)
	loadImage(t, ctx, clusterName, e2eClientImage)

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
	if err := waitForResource(t, ctx, kindNamespace, "statefulset", "kafscale-etcd", 2*time.Minute); err != nil {
		dumpKindDebug(t, ctx, kindNamespace, operatorDeployment)
		t.Fatalf("timeout waiting for statefulset/kafscale-etcd: %v", err)
	}
	waitForReadyPods(t, ctx, kindNamespace, "app=kafscale-etcd,cluster=kafscale", 2*time.Minute)
	waitForServiceEndpoints(t, ctx, kindNamespace, "kafscale-etcd-client", 2*time.Minute)
	t.Log("waiting for etcd client port to accept connections")
	if err := runPortCheckPod(t, ctx, kindNamespace, e2eClientImage, "kafscale-etcd-client."+kindNamespace+".svc.cluster.local", []int{2379}); err != nil {
		t.Fatalf("etcd client port sanity: %v", err)
	}

	if err := waitForResource(t, ctx, kindNamespace, "cronjob", "kafscale-etcd-snapshot", 2*time.Minute); err != nil {
		dumpKindDebug(t, ctx, kindNamespace, operatorDeployment)
		t.Fatalf("timeout waiting for cronjob/kafscale-etcd-snapshot: %v", err)
	}
	waitForCondition(t, ctx, kindNamespace, "kafscalecluster/kafscale", "EtcdSnapshotAccess", "True", 2*time.Minute)

	waitForReadyPods(t, ctx, kindNamespace, "app=kafscale-broker", 2*time.Minute)
	waitForServiceEndpoints(t, ctx, kindNamespace, "kafscale-broker", 2*time.Minute)
	t.Log("testing broker port sanity (9092/9093 reachable via service DNS)")
	if err := runPortCheckPod(t, ctx, kindNamespace, e2eClientImage, "kafscale-broker."+kindNamespace+".svc.cluster.local", []int{9092, 9093}); err != nil {
		t.Fatalf("broker port sanity: %v", err)
	}
	t.Log("testing kafka CLI producer (idempotence disabled)")
	cliTopic := fmt.Sprintf("cli-%08x", rand.Uint32())
	if err := runKafkaCliProducer(t, ctx, kindNamespace, kafkaCliImage, "kafscale-broker."+kindNamespace+".svc.cluster.local:9092", cliTopic, "cli-smoke"); err != nil {
		t.Fatalf("kafka CLI producer failed: %v", err)
	}

	externalPort := findFreePort(t)
	t.Logf("testing external access via port-forward (advertised 127.0.0.1:%d)", externalPort)
	patchClusterAdvertisedEndpoint(t, ctx, kindNamespace, "kafscale", "127.0.0.1", externalPort)
	waitForRollout(t, ctx, kindNamespace, "deployment/kafscale-broker", 2*time.Minute)
	waitForReadyPods(t, ctx, kindNamespace, "app=kafscale-broker,cluster=kafscale", 2*time.Minute)
	waitForServiceEndpoints(t, ctx, kindNamespace, "kafscale-broker", 2*time.Minute)
	t.Log("waiting for broker service ports after advertised endpoint update")
	if err := runPortCheckPod(t, ctx, kindNamespace, e2eClientImage, "kafscale-broker."+kindNamespace+".svc.cluster.local", []int{9092, 9093}); err != nil {
		t.Fatalf("broker port sanity after update: %v", err)
	}
	portForwardCtx, portForwardCancel := context.WithCancel(ctx)
	portForward := startPortForward(t, portForwardCtx, kindNamespace, "svc/kafscale-broker", externalPort, 9092)
	t.Cleanup(func() {
		portForwardCancel()
		_ = portForward.Wait()
	})
	waitForLocalPort(t, fmt.Sprintf("127.0.0.1:%d", externalPort), 5*time.Second)
	t.Log("external access: running host e2e client")
	if err := runHostE2EClient(t, ctx, fmt.Sprintf("127.0.0.1:%d", externalPort), fmt.Sprintf("external-%08x", rand.Uint32()), 3); err != nil {
		dumpKindDebug(t, ctx, kindNamespace, operatorDeployment)
		t.Fatalf("external access check failed: %v", err)
	}
	t.Log("external access: host e2e client finished")
	internalHost := fmt.Sprintf("kafscale-broker.%s.svc.cluster.local", kindNamespace)
	patchClusterAdvertisedEndpoint(t, ctx, kindNamespace, "kafscale", internalHost, 9092)
	waitForRollout(t, ctx, kindNamespace, "deployment/kafscale-broker", 2*time.Minute)
	waitForReadyPods(t, ctx, kindNamespace, "app=kafscale-broker,cluster=kafscale", 2*time.Minute)
	waitForServiceEndpoints(t, ctx, kindNamespace, "kafscale-broker", 2*time.Minute)

	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "delete", "job", "etcd-snapshot-manual", "--ignore-not-found=true")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "create", "job", "etcd-snapshot-manual", "--from=cronjob/kafscale-etcd-snapshot")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "wait", "--for=condition=complete", "job/etcd-snapshot-manual", "--timeout=180s")

	listing := listSnapshotObjects(t, ctx, kindNamespace)
	t.Logf("minio snapshot objects:\n%s", string(listing))
	if !bytes.Contains(listing, []byte(".db")) {
		t.Fatalf("expected snapshot .db object in minio, got:\n%s", string(listing))
	}
	if !bytes.Contains(listing, []byte(".sha256")) {
		t.Fatalf("expected snapshot .db.sha256 object in minio, got:\n%s", string(listing))
	}

	t.Log("testing broker restart durability: produce before restart, consume after restart")
	brokerAddr := fmt.Sprintf("kafscale-broker.%s.svc.cluster.local:9092", kindNamespace)
	topic := fmt.Sprintf("restart-%08x", rand.Uint32())
	messageCount := 5
	if err := runE2EClient(t, ctx, kindNamespace, e2eClientImage, "produce", brokerAddr, topic, messageCount, 40); err != nil {
		t.Fatalf("produce before restart: %v", err)
	}
	time.Sleep(600 * time.Millisecond)
	if err := runE2EClient(t, ctx, kindNamespace, e2eClientImage, "produce", brokerAddr, topic, 1, 40); err != nil {
		t.Fatalf("produce flush trigger: %v", err)
	}
	messageCount++
	time.Sleep(300 * time.Millisecond)

	brokerPod := getPodByLabel(t, ctx, kindNamespace, "app=kafscale-broker")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "delete", "pod", brokerPod)
	waitForRollout(t, ctx, kindNamespace, "deployment/kafscale-broker", 2*time.Minute)
	waitForReadyPods(t, ctx, kindNamespace, "app=kafscale-etcd,job-name!=etcd-snapshot-manual", 2*time.Minute)

	time.Sleep(1 * time.Second)
	if err := runE2EClient(t, ctx, kindNamespace, e2eClientImage, "consume", brokerAddr, topic, messageCount, 60); err != nil {
		t.Fatalf("consume after restart: %v", err)
	}

	t.Log("testing S3 outage handling: stop MinIO, assert produce fails, recover on restart")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "scale", "deployment/minio", "--replicas=0")
	waitForPodDeletion(t, ctx, kindNamespace, "app=minio", 90*time.Second)
	outageTopic := fmt.Sprintf("s3-outage-%08x", rand.Uint32())
	if err := runE2EClientExpectFailure(t, ctx, kindNamespace, e2eClientImage, "produce", brokerAddr, outageTopic, 1, 30); err != nil {
		t.Fatalf("expected produce to fail during S3 outage: %v", err)
	}
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "scale", "deployment/minio", "--replicas=1")
	waitForRollout(t, ctx, kindNamespace, "deployment/minio", 2*time.Minute)
	waitForReadyPods(t, ctx, kindNamespace, "app=minio", 2*time.Minute)
	touchClusterAnnotation(t, ctx, kindNamespace, "kafscale")
	waitForCondition(t, ctx, kindNamespace, "kafscalecluster/kafscale", "EtcdSnapshotAccess", "True", 2*time.Minute)
	if err := runE2EClient(t, ctx, kindNamespace, e2eClientImage, "produce", brokerAddr, outageTopic, 1, 60); err != nil {
		t.Fatalf("expected produce to recover after S3 outage: %v", err)
	}

	t.Log("testing operator HA: deleting leader pod and waiting for rollout")
	operatorPod := getPodByLabel(t, ctx, kindNamespace, "app.kubernetes.io/component=operator")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "delete", "pod", operatorPod)
	waitForRollout(t, ctx, kindNamespace, "deployment/"+operatorDeployment, 2*time.Minute)

	operatorPod = getPodByLabel(t, ctx, kindNamespace, "app.kubernetes.io/component=operator")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "delete", "pod", operatorPod)
	waitForRollout(t, ctx, kindNamespace, "deployment/"+operatorDeployment, 2*time.Minute)
	waitForCondition(t, ctx, kindNamespace, "kafscalecluster/kafscale", "EtcdSnapshotAccess", "True", 2*time.Minute)

	t.Log("testing etcd HA: deleting one member and waiting for ready")
	etcdPod := getPodByLabel(t, ctx, kindNamespace, "app=kafscale-etcd")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "delete", "pod", etcdPod)
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "wait", "--for=condition=ready", "pod", "-l", "app=kafscale-etcd", "--timeout=180s")

	t.Log("testing etcd member loss: snapshots still run after member delete")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "delete", "job", "etcd-snapshot-manual", "--ignore-not-found=true")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "create", "job", "etcd-snapshot-manual", "--from=cronjob/kafscale-etcd-snapshot")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "wait", "--for=condition=complete", "job/etcd-snapshot-manual", "--timeout=180s")

	t.Log("testing snapshot status conditions: force failure and confirm EtcdSnapshotAccess=False")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "scale", "deployment/minio", "--replicas=0")
	waitForPodDeletion(t, ctx, kindNamespace, "app=minio", 90*time.Second)
	touchClusterAnnotation(t, ctx, kindNamespace, "kafscale")
	waitForCondition(t, ctx, kindNamespace, "kafscalecluster/kafscale", "EtcdSnapshotAccess", "False", 2*time.Minute)

	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "scale", "deployment/minio", "--replicas=1")
	waitForRollout(t, ctx, kindNamespace, "deployment/minio", 2*time.Minute)
	waitForReadyPods(t, ctx, kindNamespace, "app=minio", 2*time.Minute)
	touchClusterAnnotation(t, ctx, kindNamespace, "kafscale")
	waitForCondition(t, ctx, kindNamespace, "kafscalecluster/kafscale", "EtcdSnapshotAccess", "True", 2*time.Minute)

	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "delete", "job", "etcd-snapshot-manual", "--ignore-not-found=true")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "create", "job", "etcd-snapshot-manual", "--from=cronjob/kafscale-etcd-snapshot")
	runCmdGetOutput(t, ctx, "kubectl", "-n", kindNamespace, "wait", "--for=condition=complete", "job/etcd-snapshot-manual", "--timeout=180s")

	postFailoverListing := listSnapshotObjects(t, ctx, kindNamespace)
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

func waitForReadyPods(t *testing.T, ctx context.Context, namespace, selector string, timeout time.Duration) {
	t.Helper()
	runCmdGetOutput(t, ctx, "kubectl", "-n", namespace, "wait", "--for=condition=ready", "pod", "-l", selector, "--timeout="+timeout.String())
}

func listSnapshotObjects(t *testing.T, ctx context.Context, namespace string) []byte {
	t.Helper()
	podName := fmt.Sprintf("s3-list-%d", time.Now().UnixNano())
	if err := execCommand(ctx, "kubectl", "-n", namespace, "run", podName, "--restart=Never",
		"--image=amazon/aws-cli:2.15.0",
		"--env", "AWS_ACCESS_KEY_ID=minioadmin",
		"--env", "AWS_SECRET_ACCESS_KEY=minioadmin",
		"--env", "AWS_DEFAULT_REGION=us-east-1",
		"--env", "AWS_EC2_METADATA_DISABLED=true",
		"--command", "--", "sh", "-c",
		"aws --endpoint-url http://minio."+namespace+".svc.cluster.local:9000 s3api list-objects-v2 --bucket kafscale-snapshots --prefix etcd-snapshots/ --query 'Contents[].Key' --output text"); err != nil {
		t.Fatalf("start s3 list pod: %v", err)
	}
	runCmdGetOutput(t, ctx, "kubectl", "-n", namespace, "wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/"+podName, "--timeout=60s")
	listing := runCmdGetOutput(t, ctx, "kubectl", "-n", namespace, "logs", "pod/"+podName)
	runCmdGetOutput(t, ctx, "kubectl", "-n", namespace, "delete", "pod", podName, "--ignore-not-found=true")
	return listing
}

func runE2EClient(t *testing.T, ctx context.Context, namespace, image, mode, brokerAddr, topic string, count int, timeoutSec int) error {
	t.Helper()
	podName := fmt.Sprintf("kafscale-e2e-%s-%d", mode, time.Now().UnixNano())
	args := []string{
		"-n", namespace,
		"run", podName,
		"--restart=Never",
		"--image", image,
		"--env", "KAFSCALE_E2E_MODE=" + mode,
		"--env", "KAFSCALE_E2E_BROKER_ADDR=" + brokerAddr,
		"--env", "KAFSCALE_E2E_TOPIC=" + topic,
		"--env", fmt.Sprintf("KAFSCALE_E2E_COUNT=%d", count),
		"--env", fmt.Sprintf("KAFSCALE_E2E_TIMEOUT_SEC=%d", timeoutSec),
	}
	if err := execCommand(ctx, "kubectl", args...); err != nil {
		return fmt.Errorf("start e2e client pod: %w", err)
	}
	if err := waitForPodPhase(ctx, namespace, podName, "Succeeded", 90*time.Second); err != nil {
		logs := runCmdWithOutput(t, ctx, "kubectl", "-n", namespace, "logs", "pod/"+podName)
		_ = execCommand(ctx, "kubectl", "-n", namespace, "delete", "pod", podName, "--ignore-not-found=true")
		return fmt.Errorf("e2e client pod failed: %w\nlogs:\n%s", err, string(logs))
	}
	_ = execCommand(ctx, "kubectl", "-n", namespace, "delete", "pod", podName, "--ignore-not-found=true")
	return nil
}

func runE2EClientExpectFailure(t *testing.T, ctx context.Context, namespace, image, mode, brokerAddr, topic string, count int, timeoutSec int) error {
	t.Helper()
	podName := fmt.Sprintf("kafscale-e2e-%s-%d", mode, time.Now().UnixNano())
	args := []string{
		"-n", namespace,
		"run", podName,
		"--restart=Never",
		"--image", image,
		"--env", "KAFSCALE_E2E_MODE=" + mode,
		"--env", "KAFSCALE_E2E_BROKER_ADDR=" + brokerAddr,
		"--env", "KAFSCALE_E2E_TOPIC=" + topic,
		"--env", fmt.Sprintf("KAFSCALE_E2E_COUNT=%d", count),
		"--env", fmt.Sprintf("KAFSCALE_E2E_TIMEOUT_SEC=%d", timeoutSec),
	}
	if err := execCommand(ctx, "kubectl", args...); err != nil {
		return fmt.Errorf("start e2e client pod: %w", err)
	}
	if err := waitForPodPhase(ctx, namespace, podName, "Failed", 90*time.Second); err != nil {
		logs := runCmdWithOutput(t, ctx, "kubectl", "-n", namespace, "logs", "pod/"+podName)
		_ = execCommand(ctx, "kubectl", "-n", namespace, "delete", "pod", podName, "--ignore-not-found=true")
		return fmt.Errorf("expected failure but pod did not fail: %w\nlogs:\n%s", err, string(logs))
	}
	_ = execCommand(ctx, "kubectl", "-n", namespace, "delete", "pod", podName, "--ignore-not-found=true")
	return nil
}

func waitForPodPhase(ctx context.Context, namespace, podName, phase string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := exec.CommandContext(ctx, "kubectl", "-n", namespace, "get", "pod", podName, "-o", "jsonpath={.status.phase}").Output()
		if err == nil && strings.TrimSpace(string(out)) == phase {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for pod %s phase %s", podName, phase)
}

func waitForServiceEndpoints(t *testing.T, ctx context.Context, namespace, service string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out := runCmdWithOutput(t, ctx, "kubectl", "-n", namespace, "get", "endpoints", service, "-o", "jsonpath={.subsets[*].addresses[*].ip}")
		if strings.TrimSpace(string(out)) != "" {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for service endpoints for %s", service)
}

func touchClusterAnnotation(t *testing.T, ctx context.Context, namespace, name string) {
	t.Helper()
	key := "kafscale.io/reconcile-at"
	value := fmt.Sprintf("%d", time.Now().UnixNano())
	runCmdGetOutput(t, ctx, "kubectl", "-n", namespace, "annotate", "kafscalecluster/"+name, key+"="+value, "--overwrite")
}

func waitForPodDeletion(t *testing.T, ctx context.Context, namespace, selector string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out := runCmdWithOutput(t, ctx, "kubectl", "-n", namespace, "get", "pods", "-l", selector, "-o", "name")
		if strings.TrimSpace(string(out)) == "" {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for pods with selector %s to be deleted", selector)
}
func runPortCheckPod(t *testing.T, ctx context.Context, namespace, image, host string, ports []int) error {
	t.Helper()
	podName := fmt.Sprintf("kafscale-portcheck-%d", time.Now().UnixNano())
	var addrs []string
	for _, port := range ports {
		addrs = append(addrs, fmt.Sprintf("%s:%d", host, port))
	}
	if err := execCommand(ctx, "kubectl", "-n", namespace, "run", podName, "--restart=Never",
		"--image", image,
		"--env", "KAFSCALE_E2E_MODE=probe",
		"--env", "KAFSCALE_E2E_ADDRS="+strings.Join(addrs, ","),
		"--env", "KAFSCALE_E2E_PROBE_RETRIES=30",
		"--env", "KAFSCALE_E2E_PROBE_SLEEP_MS=500"); err != nil {
		return fmt.Errorf("start port check pod: %w", err)
	}
	if err := execCommand(ctx, "kubectl", "-n", namespace, "wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/"+podName, "--timeout=60s"); err != nil {
		logs := runCmdWithOutput(t, ctx, "kubectl", "-n", namespace, "logs", "pod/"+podName)
		_ = execCommand(ctx, "kubectl", "-n", namespace, "delete", "pod", podName, "--ignore-not-found=true")
		return fmt.Errorf("port check failed: %w\nlogs:\n%s", err, string(logs))
	}
	_ = execCommand(ctx, "kubectl", "-n", namespace, "delete", "pod", podName, "--ignore-not-found=true")
	return nil
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

func patchClusterAdvertisedEndpoint(t *testing.T, ctx context.Context, namespace, name, host string, port int) {
	t.Helper()
	patch := fmt.Sprintf(`{"spec":{"brokers":{"advertisedHost":"%s","advertisedPort":%d}}}`, host, port)
	runCmdGetOutput(t, ctx, "kubectl", "-n", namespace, "patch", "kafscalecluster/"+name, "--type=merge", "-p", patch)
}

func startPortForward(t *testing.T, ctx context.Context, namespace, target string, localPort, remotePort int) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(ctx, "kubectl", "-n", namespace, "port-forward", target, fmt.Sprintf("%d:%d", localPort, remotePort))
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start port-forward: %v", err)
	}
	return cmd
}

func waitForLocalPort(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for local port %s", addr)
}

func findFreePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func runHostE2EClient(t *testing.T, ctx context.Context, brokerAddr, topic string, count int) error {
	t.Helper()
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return fmt.Errorf("create host client: %w", err)
	}
	defer client.Close()
	for i := 0; i < count; i++ {
		msg := fmt.Sprintf("external-%d", i)
		if err := client.ProduceSync(runCtx, &kgo.Record{Topic: topic, Value: []byte(msg)}).FirstErr(); err != nil {
			if runCtx.Err() != nil {
				return fmt.Errorf("produce timed out: %w", runCtx.Err())
			}
			return fmt.Errorf("produce failed: %w", err)
		}
	}
	return nil
}

func runKafkaCliProducer(t *testing.T, ctx context.Context, namespace, image, brokerAddr, topic, message string) error {
	t.Helper()
	podName := fmt.Sprintf("kafscale-kafka-cli-%d", time.Now().UnixNano())
	cliCmd := envOrDefault("KAFSCALE_KAFKA_CLI_CMD", "/usr/bin/kafka-console-producer")
	script := fmt.Sprintf("printf '%%s\\n' %q | %s --bootstrap-server %s --topic %s --producer-property enable.idempotence=false", message, cliCmd, brokerAddr, topic)
	if err := execCommand(ctx, "kubectl", "-n", namespace, "run", podName, "--restart=Never",
		"--image", image,
		"--command", "--", "sh", "-c", script); err != nil {
		return fmt.Errorf("start kafka cli pod: %w", err)
	}
	if err := waitForPodPhase(ctx, namespace, podName, "Succeeded", 60*time.Second); err != nil {
		logs := runCmdWithOutput(t, ctx, "kubectl", "-n", namespace, "logs", "pod/"+podName)
		_ = execCommand(ctx, "kubectl", "-n", namespace, "delete", "pod", podName, "--ignore-not-found=true")
		return fmt.Errorf("kafka cli pod failed: %w\nlogs:\n%s", err, string(logs))
	}
	_ = execCommand(ctx, "kubectl", "-n", namespace, "delete", "pod", podName, "--ignore-not-found=true")
	return nil
}

func runCmdWithInput(t *testing.T, ctx context.Context, name, input string, args ...string) {
	t.Helper()
	if name == "kubectl" {
		for _, arg := range args {
			if arg == "--validate=false" || strings.HasPrefix(arg, "--validate=") {
				goto run
			}
		}
		for _, arg := range args {
			if arg == "apply" {
				args = append(args, "--validate=false")
				break
			}
		}
	}
run:
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

func requireImageOrPull(t *testing.T, ctx context.Context, image string) {
	t.Helper()
	if err := execCommand(ctx, "docker", "image", "inspect", image); err == nil {
		return
	}
	t.Logf("docker image %s not found; pulling...", image)
	if err := execCommand(ctx, "docker", "pull", image); err != nil {
		t.Fatalf("docker image %s not found and pull failed", image)
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
