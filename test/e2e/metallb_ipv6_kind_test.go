// Copyright 2026, Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"os"
	"strings"
	"testing"
	"time"
)

func TestDemoPlatformMetallbIPv6(t *testing.T) {
	if !parseBoolEnv("KAFSCALE_E2E") || !parseBoolEnv("KAFSCALE_E2E_KIND") || !parseBoolEnv("KAFSCALE_E2E_KIND_IPV6") {
		t.Skip("set KAFSCALE_E2E=1, KAFSCALE_E2E_KIND=1, and KAFSCALE_E2E_KIND_IPV6=1 to run IPv6 MetalLB e2e")
	}

	requireBinaries(t, "docker", "kind", "kubectl")

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	clusterName := strings.TrimSpace(os.Getenv("KAFSCALE_KIND_CLUSTER"))
	if clusterName == "" {
		clusterName = "kafscale-e2e-ipv6"
	}

	if parseBoolEnv("KAFSCALE_KIND_RECREATE") && kindClusterExists(ctx, clusterName) {
		_ = execCommand(ctx, "kind", "delete", "cluster", "--name", clusterName)
	}

	created := false
	if !kindClusterExists(ctx, clusterName) {
		configPath := writeKindIPv6Config(t)
		runCmdGetOutput(t, ctx, "kind", "create", "cluster", "--name", clusterName, "--config", configPath, "--wait", "180s")
		created = true
	}
	t.Cleanup(func() {
		if created {
			_ = execCommand(context.Background(), "kind", "delete", "cluster", "--name", clusterName)
		}
	})

	ensureKindKubeconfig(t, ctx, clusterName)
	waitForKubeAPI(t, ctx, 2*time.Minute)
	waitForNodesReady(t, ctx, 2*time.Minute)

	subnet := strings.TrimSpace(string(runCmdGetOutput(t, ctx, "docker", "network", "inspect", "kind", "--format", "{{(index .IPAM.Config 0).Subnet}}")))
	if subnet == "" || !strings.Contains(subnet, ":") {
		t.Skipf("kind network is not IPv6 (subnet=%q)", subnet)
	}

	repo := repoRoot(t)
	cmd := "cd " + repo + " && scripts/demo-platform.sh metallb"
	if err := execCommand(ctx, "bash", "-lc", cmd); err != nil {
		t.Fatalf("apply metallb config: %v", err)
	}

	address := strings.TrimSpace(string(runCmdGetOutput(t, ctx, "kubectl", "-n", "metallb-system", "get", "ipaddresspool", "kind-pool", "-o", "jsonpath={.spec.addresses[0]}")))
	if address != subnet {
		t.Fatalf("expected MetalLB pool %q, got %q", subnet, address)
	}
}

func writeKindIPv6Config(t *testing.T) string {
	t.Helper()
	tmp, err := os.CreateTemp("", "kafscale-kind-ipv6-*.yaml")
	if err != nil {
		t.Fatalf("create kind ipv6 config: %v", err)
	}
	config := "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnetworking:\n  ipFamily: ipv6\n"
	if _, err := tmp.WriteString(config); err != nil {
		_ = tmp.Close()
		t.Fatalf("write kind ipv6 config: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close kind ipv6 config: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(tmp.Name()) })
	return tmp.Name()
}
