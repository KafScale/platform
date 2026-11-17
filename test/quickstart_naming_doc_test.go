// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestQuickstartUsesInstalledAPIGroup(t *testing.T) {
	text := readRepoDoc(t, "docs/quickstart.md")
	if strings.Contains(text, "kafscale.novatechflow.io") {
		t.Fatal("docs/quickstart.md still uses api group kafscale.novatechflow.io; the chart CRDs use kafscale.io")
	}
	if !strings.Contains(text, "apiVersion: kafscale.io/v1alpha1") {
		t.Fatal("docs/quickstart.md has no apiVersion: kafscale.io/v1alpha1 example")
	}
}

func TestQuickstartTopicDeleteAndClientAccess(t *testing.T) {
	text := readRepoDoc(t, "docs/quickstart.md")
	for _, want := range []string{
		"does not delete the Kafka topic",
		"delete that resource first",
		"DeleteTopics",
		"/ui/api/status/topics/",
		"spec.brokers.advertisedHost",
		"spec.brokers.replicas",
		"proxy.advertisedHost",
		"edenhill/kcat:1.7.1",
		"kcat-produce",
		"-t orders",
		"kcat-consume",
		"next operator reconcile publishes the topic again",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("docs/quickstart.md does not contain %q", want)
		}
	}
}

func TestImageTagKeysDocumented(t *testing.T) {
	keys := chartUseLatestKeys(t)
	if len(keys) == 0 {
		t.Fatal("no useLatest keys found in deploy/helm/kafscale/templates")
	}
	ops := readRepoDoc(t, "docs/operations.md")
	quick := readRepoDoc(t, "docs/quickstart.md")
	for _, key := range keys {
		if !strings.Contains(ops, "`"+key+"`") {
			t.Errorf("docs/operations.md does not document image key %s", key)
		}
	}
	if strings.Contains(ops, "lfsDemos.e72Browser.image") {
		t.Error("docs/operations.md documents lfsDemos.e72Browser.image, which no chart template reads")
	}
	if !strings.Contains(quick, "operations.md#installing-with-helm") || !strings.Contains(strings.ToLower(quick), "appversion") {
		t.Error("docs/quickstart.md does not point at operations.md#installing-with-helm or mention appVersion")
	}
}

func TestServiceNamePatternDocumented(t *testing.T) {
	for _, doc := range []string{
		"docs/operations.md",
		"docs/quickstart.md",
		"docs/schema-registry.md",
		"deploy/helm/kafscale/README.md",
	} {
		text := readRepoDoc(t, doc)
		if !strings.Contains(text, "<cluster>-etcd-client") {
			t.Errorf("%s does not document the <cluster>-etcd-client Service name", doc)
		}
	}
	for _, doc := range []string{
		"docs/operations.md",
		"docs/quickstart.md",
		"deploy/helm/kafscale/README.md",
	} {
		text := readRepoDoc(t, doc)
		if !strings.Contains(text, "<cluster>-broker") {
			t.Errorf("%s does not document the <cluster>-broker Service name", doc)
		}
	}

	ops := readRepoDoc(t, "docs/operations.md")
	if !strings.Contains(ops, "demo-etcd-client.kafscale.svc.cluster.local:2379") {
		t.Error("docs/operations.md has no demo-etcd-client example that matches the quickstart cluster name")
	}
	if !strings.Contains(ops, "demo-broker.kafscale.svc.cluster.local:9093") && !strings.Contains(ops, "demo-broker.kafscale.svc.cluster.local:9092") {
		t.Error("docs/operations.md has no demo-broker example that matches the quickstart cluster name")
	}

	sr := readRepoDoc(t, "docs/schema-registry.md")
	for _, want := range []string{"credentialsSecret: kafscale-s3", "namespace: kafscale"} {
		if !strings.Contains(sr, want) {
			t.Errorf("docs/schema-registry.md Helm example is missing %q", want)
		}
	}
}

func chartUseLatestKeys(t *testing.T) []string {
	t.Helper()
	root := filepath.Join(repoRoot(t), "deploy/helm/kafscale/templates")
	re := regexp.MustCompile(`\.Values\.([\w.]+)\.useLatest`)
	seen := map[string]struct{}{}
	var keys []string
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(root, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range re.FindAllStringSubmatch(string(body), -1) {
			key := match[1]
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	return keys
}
