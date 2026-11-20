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

func TestConsoleSnapshotPanelReplicaBehaviorDocumented(t *testing.T) {
	root := repoRoot(t)
	read := func(parts ...string) string {
		body, err := os.ReadFile(filepath.Join(append([]string{root}, parts...)...))
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	var row string
	for _, line := range strings.Split(read("docs", "operations.md"), "\n") {
		if strings.HasPrefix(line, "| `operator.replicaCount` |") {
			row = line
		}
	}
	var note string
	for _, line := range strings.Split(read("docs", "releases", "v2.0.0.md"), "\n") {
		if strings.Contains(line, "KAFSCALE_CONSOLE_OPERATOR_METRICS_URL") && strings.Contains(line, "(#358)") {
			note = line
		}
	}
	if row == "" || note == "" {
		t.Fatalf("missing operator.replicaCount row (%t) or #358 note (%t)", row != "", note != "")
	}
	const marker = "Only the leader exports"
	i := strings.Index(row, marker)
	j := strings.Index(note, marker)
	if i < 0 || j < 0 {
		t.Fatalf("replica behavior text missing: row=%t note=%t", i >= 0, j >= 0)
	}
	rowText := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(row[i:]), "|"))
	noteText := note[j:]
	if k := strings.Index(noteText, " (#358)"); k >= 0 {
		noteText = noteText[:k]
	}
	noteText = strings.TrimSuffix(noteText, ".")
	rowText = strings.TrimSuffix(rowText, ".")
	if rowText != noteText {
		t.Fatalf("operations.md and v2.0.0.md describe the panel differently:\n%s\n%s", rowText, noteText)
	}
	for _, want := range []string{
		"headless operator metrics Service",
		"scrapes every ready pod directly",
		"up to 3 minutes",
		"adds the time since that read to the snapshot age",
		"`kafscale_operator_etcd_snapshot_threshold_seconds`",
		"Before the first leader read, and 3 minutes after the last one, the panel shows n/a",
		"`KAFSCALE_OPERATOR_ETCD_SNAPSHOT_SKIP_PREFLIGHT=true`",
		"does not manage",
		"access FAIL",
	} {
		if !strings.Contains(rowText, want) {
			t.Errorf("replica behavior text does not mention %q", want)
		}
	}
	if strings.Contains(rowText, "Service balances console scrapes") {
		t.Error("snapshot discovery must not rely on Service load balancing")
	}
	release := read("docs", "releases", "v2.0.0.md")
	for _, warning := range []string{"2.0 console operator metrics discovery", "Upgrade action:", "KAFSCALE_CONSOLE_OPERATOR_METRICS_DISCOVERY_URL", "older console images"} {
		if !strings.Contains(release, warning) {
			t.Errorf("release notes missing %q", warning)
		}
	}
	if !strings.Contains(read("docs", "metrics.md"), "| `kafscale_operator_etcd_snapshot_threshold_seconds` |") {
		t.Error("docs/metrics.md does not list kafscale_operator_etcd_snapshot_threshold_seconds")
	}
}
