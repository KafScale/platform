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
	"os"
	"strings"
	"testing"
)

const kindKubeProxyModeEnv = "KAFSCALE_KIND_KUBE_PROXY_MODE"

func kindKubeProxyMode() (string, error) {
	mode := strings.TrimSpace(os.Getenv(kindKubeProxyModeEnv))
	switch mode {
	case "", "nftables":
		return mode, nil
	default:
		return "", fmt.Errorf("%s=%q is not supported; set nftables or leave it unset", kindKubeProxyModeEnv, mode)
	}
}

func kindClusterConfig(mode string) string {
	if mode == "" {
		return ""
	}
	return "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnetworking:\n  kubeProxyMode: " + mode + "\n"
}

func kindCreateClusterArgs(clusterName, mode, configPath string) []string {
	args := []string{"create", "cluster", "--name", clusterName, "--wait", "180s"}
	if mode != "" {
		args = append(args, "--config", configPath)
	}
	return args
}

func createKindCluster(t *testing.T, ctx context.Context, clusterName string) {
	t.Helper()
	mode, err := kindKubeProxyMode()
	if err != nil {
		t.Fatal(err)
	}
	args := kindCreateClusterArgs(clusterName, mode, "")
	if mode != "" {
		f, err := os.CreateTemp("", "kafscale-kind-*.yaml")
		if err != nil {
			t.Fatalf("create kind config: %v", err)
		}
		path := f.Name()
		defer os.Remove(path)
		if _, err := f.WriteString(kindClusterConfig(mode)); err != nil {
			_ = f.Close()
			t.Fatalf("write kind config: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close kind config: %v", err)
		}
		args = kindCreateClusterArgs(clusterName, mode, path)
	}
	runCmdGetOutput(t, ctx, "kind", args...)
}
