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
	"strings"
	"testing"
)

func TestBrokerHPAScalingDocumented(t *testing.T) {
	read := func(rel string) string {
		t.Helper()
		body, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	ops := read("docs/operations.md")
	for _, want := range []string{
		"### Scaling brokers",
		"the operator keeps the replica count the HPA set",
		"four times that minimum",
		"to 0 stops all brokers",
		"KAFSCALE_PARTITION_REBALANCE_INTERVAL_MS",
		"at most one broker per 60 seconds",
		"loses no acknowledged write",
		"-n kafscale patch kafscalecluster",
	} {
		if !strings.Contains(ops, want) {
			t.Errorf("docs/operations.md is missing %q", want)
		}
	}
	for _, stale := range []string{"sets the StatefulSet replicas to the new value", "to safely drain partitions before restarts"} {
		for _, doc := range []string{"docs/operations.md", "docs/user-guide.md"} {
			if strings.Contains(read(doc), stale) {
				t.Errorf("%s still says %q", doc, stale)
			}
		}
	}
	guide := read("docs/user-guide.md")
	for _, want := range []string{"<cluster>-broker", "keeps the replica count the HPA sets", "operations.md#scaling-brokers"} {
		if !strings.Contains(guide, want) {
			t.Errorf("docs/user-guide.md is missing %q", want)
		}
	}
}
