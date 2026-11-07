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
	"strings"
	"testing"
)

func TestKindKubeProxyModeDefault(t *testing.T) {
	t.Setenv(kindKubeProxyModeEnv, "")
	mode, err := kindKubeProxyMode()
	if err != nil {
		t.Fatalf("empty %s: %v", kindKubeProxyModeEnv, err)
	}
	if mode != "" {
		t.Fatalf("empty %s: got %q, want empty", kindKubeProxyModeEnv, mode)
	}
	if cfg := kindClusterConfig(mode); cfg != "" {
		t.Fatalf("default mode wrote a kind config: %q", cfg)
	}
	args := kindCreateClusterArgs("kafscale-e2e", mode, "/tmp/unused.yaml")
	for i, arg := range args {
		if arg == "--config" {
			t.Fatalf("default mode passed --config %v", args[i:])
		}
	}
}

func TestKindKubeProxyModeNftables(t *testing.T) {
	t.Setenv(kindKubeProxyModeEnv, " nftables ")
	mode, err := kindKubeProxyMode()
	if err != nil {
		t.Fatalf("nftables %s: %v", kindKubeProxyModeEnv, err)
	}
	if mode != "nftables" {
		t.Fatalf("nftables %s: got %q", kindKubeProxyModeEnv, mode)
	}
	cfg := kindClusterConfig(mode)
	for _, want := range []string{
		"kind: Cluster",
		"apiVersion: kind.x-k8s.io/v1alpha4",
		"kubeProxyMode: nftables",
	} {
		if !strings.Contains(cfg, want) {
			t.Fatalf("nftables kind config missing %q:\n%s", want, cfg)
		}
	}
	args := kindCreateClusterArgs("kafscale-e2e", mode, "/tmp/kind-nftables.yaml")
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--config /tmp/kind-nftables.yaml") {
		t.Fatalf("nftables create args missing --config: %v", args)
	}
}

func TestKindKubeProxyModeInvalid(t *testing.T) {
	for _, value := range []string{"iptables", "ipvs", "none", "foo"} {
		t.Setenv(kindKubeProxyModeEnv, value)
		_, err := kindKubeProxyMode()
		if err == nil {
			t.Fatalf("accepted unsupported %s=%q", kindKubeProxyModeEnv, value)
		}
		msg := err.Error()
		if !strings.Contains(msg, kindKubeProxyModeEnv) || !strings.Contains(msg, value) || !strings.Contains(msg, "not supported") {
			t.Fatalf("unclear error for %s=%q: %v", kindKubeProxyModeEnv, value, err)
		}
	}
}
