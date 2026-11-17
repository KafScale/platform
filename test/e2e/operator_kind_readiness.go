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
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

type kindCommand func(context.Context, string, ...string) ([]byte, error)

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
