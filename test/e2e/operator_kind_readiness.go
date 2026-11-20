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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/KafScale/platform/v2/pkg/metadata"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

type kindCommand func(context.Context, string, ...string) ([]byte, error)

func readEtcdSnapshot(ctx context.Context, run kindCommand, namespace, cluster, pod string) ([]byte, error) {
	endpoint := fmt.Sprintf("http://%s-etcd-client.%s.svc.cluster.local:2379", cluster, namespace)
	return run(ctx, "kubectl", "-n", namespace, "exec", pod, "-c", "etcd", "--",
		"etcdctl", "--endpoints="+endpoint, "get", "/kafscale/metadata/snapshot", "--print-value-only")
}

func checkSingleBrokerEndpoint(data []byte, host string, port int32) error {
	var snapshot metadata.ClusterMetadata
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode metadata snapshot: %w", err)
	}
	if len(snapshot.Brokers) != 1 || snapshot.Brokers[0].Host != host || snapshot.Brokers[0].Port != port {
		return fmt.Errorf("metadata does not advertise exactly one broker at %s:%d", host, port)
	}
	return nil
}

func waitForSingleBrokerEndpoint(ctx context.Context, read func() []byte, host string, port int32, timeout time.Duration) error {
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(context.Context) (bool, error) {
		lastErr = checkSingleBrokerEndpoint(read(), host, port)
		return lastErr == nil, nil
	})
	if err != nil {
		return fmt.Errorf("wait for advertised broker: %w (last check: %v)", err, lastErr)
	}
	return nil
}

// checkBrokerReplicas requires the requested count and completed rollout, not
// merely readyReplicas == spec.replicas at the previous replica count.
func checkBrokerReplicas(ctx context.Context, run kindCommand, namespace, cluster string, replicas int32) error {
	out, err := run(ctx, "kubectl", "-n", namespace, "get", "statefulset", cluster+"-broker", "-o", "json", "--request-timeout=5s")
	if err != nil {
		return fmt.Errorf("get broker StatefulSet: %w: %s", err, out)
	}
	var sts appsv1.StatefulSet
	if err := json.Unmarshal(out, &sts); err != nil {
		return fmt.Errorf("decode broker StatefulSet: %w", err)
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != replicas || sts.Status.ObservedGeneration < sts.Generation ||
		sts.Status.Replicas != replicas || sts.Status.ReadyReplicas != replicas || sts.Status.UpdatedReplicas != replicas ||
		sts.Status.CurrentRevision != sts.Status.UpdateRevision {
		return fmt.Errorf("broker StatefulSet has not converged to %d updated, ready replicas", replicas)
	}
	out, err = run(ctx, "kubectl", "-n", namespace, "get", "pods", "-l", "app=kafscale-broker,cluster="+cluster, "-o", "json", "--request-timeout=5s")
	if err != nil {
		return fmt.Errorf("list broker pods: %w: %s", err, out)
	}
	var pods corev1.PodList
	if err := json.Unmarshal(out, &pods); err != nil {
		return fmt.Errorf("decode broker pods: %w", err)
	}
	if len(pods.Items) != int(replicas) {
		return fmt.Errorf("found %d broker pods, want %d including terminating pods", len(pods.Items), replicas)
	}
	for _, pod := range pods.Items {
		ready := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		if pod.DeletionTimestamp != nil || !ready {
			return fmt.Errorf("broker pod %s is terminating or not Ready", pod.Name)
		}
	}
	return nil
}

func waitForBrokerReplicas(ctx context.Context, run kindCommand, namespace, cluster string, replicas int32, timeout time.Duration) error {
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		lastErr = checkBrokerReplicas(ctx, run, namespace, cluster, replicas)
		return lastErr == nil, nil
	})
	if err != nil {
		return fmt.Errorf("wait for broker scale: %w (last check: %v)", err, lastErr)
	}
	return nil
}

// checkOperatorBackends checks every expected etcd member rather than one
// Service backend, then each broker's application readiness rather than TCP.
func checkOperatorBackends(ctx context.Context, run kindCommand, namespace, cluster string, etcdReplicas int) error {
	if etcdReplicas < 1 {
		return fmt.Errorf("no expected etcd members")
	}
	etcd := cluster + "-etcd"
	endpoints := make([]string, etcdReplicas)
	for i := range endpoints {
		endpoints[i] = fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2379", etcd, i, etcd, namespace)
	}
	out, err := run(ctx, "kubectl", "-n", namespace, "exec", "statefulset/"+etcd, "--", "etcdctl",
		"--endpoints="+strings.Join(endpoints, ","), "--dial-timeout=3s", "--command-timeout=5s", "endpoint", "health")
	if err != nil {
		return fmt.Errorf("etcd member health: %w: %s", err, out)
	}
	out, err = run(ctx, "kubectl", "-n", namespace, "get", "pods", "-l", "app=kafscale-broker,cluster="+cluster,
		"-o", "jsonpath={.items[*].metadata.name}", "--request-timeout=5s")
	if err != nil {
		return fmt.Errorf("list brokers: %w: %s", err, out)
	}
	pods := strings.Fields(string(out))
	if len(pods) == 0 {
		return fmt.Errorf("no broker pods found")
	}
	for _, pod := range pods {
		path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:9093/proxy/readyz", namespace, pod)
		if out, err := run(ctx, "kubectl", "get", "--raw="+path, "--request-timeout=5s"); err != nil {
			return fmt.Errorf("broker %s readiness: %w: %s", pod, err, out)
		}
	}
	return nil
}

func waitForOperatorBackends(ctx context.Context, run kindCommand, namespace, cluster string, etcdReplicas int, timeout time.Duration) error {
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		lastErr = checkOperatorBackends(ctx, run, namespace, cluster, etcdReplicas)
		return lastErr == nil, nil
	})
	if err != nil {
		return fmt.Errorf("wait for operator backends: %w (last check: %v)", err, lastErr)
	}
	return nil
}

func waitForStatefulSetPodReady(ctx context.Context, run kindCommand, namespace, pod string, timeout, interval time.Duration) error {
	last := "not checked"
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		out, err := run(ctx, "kubectl", "-n", namespace, "get", "pod", pod,
			"-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`, "--request-timeout=10s")
		if err != nil {
			last = fmt.Sprintf("%v: %s", err, strings.TrimSpace(string(out)))
			return false, nil
		}
		status := strings.TrimSpace(string(out))
		if status == "" {
			status = "unknown"
		}
		last = "Ready=" + status
		return status == "True", nil
	})
	if err != nil {
		return fmt.Errorf("pod/%s not Ready within %s: %w (last check: %s)", pod, timeout, err, last)
	}
	return nil
}

// The startup smoke test may retry once after rechecking dependency health.
// Other produce checks, including outage assertions, must keep their failures.
func produceAfterReadiness(ctx context.Context, ready, produce func(context.Context) error) error {
	if err := ready(ctx); err != nil {
		return err
	}
	if err := produce(ctx); err == nil {
		return nil
	}
	if err := ready(ctx); err != nil {
		return err
	}
	if err := produce(ctx); err != nil {
		return fmt.Errorf("produce failed after one retry: %w", err)
	}
	return nil
}
