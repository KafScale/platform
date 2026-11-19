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

	"github.com/KafScale/platform/pkg/metadata"
)

func TestConsoleLoginDocumented(t *testing.T) {
	text := readOpsDoc(t, "docs/operations.md")
	for _, want := range []string{
		"POST /ui/api/auth/login",
		`"username"`,
		`"password"`,
		"kafscale_ui_session",
		"kafscale-console",
		"8080:80",
		"invalid credentials",
		"return `429`",
		"`503`. A wrong password",
		"console.replicaCount",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("docs/operations.md does not document console login %q", want)
		}
	}
}

func TestPartitionLeaseDocsMatchCode(t *testing.T) {
	text := readOpsDoc(t, "docs/operations.md")
	prefix := metadata.PartitionLeasePrefix()
	if prefix != "/kafscale/partition-leases" {
		t.Fatalf("unexpected PartitionLeasePrefix %q", prefix)
	}
	exact := prefix + "/<topic>/<partition>"
	if !strings.Contains(text, exact) {
		t.Errorf("docs/operations.md does not document the exact lease key %s", exact)
	}
	if strings.Contains(text, "a delay, not a leak") {
		t.Error("docs/operations.md still calls leftover lease keys a delay, not a leak")
	}
	if !strings.Contains(text, "demo-etcd-0") || !strings.Contains(text, "-c etcd") {
		t.Error("docs/operations.md does not show kubectl exec into demo-etcd-0 -c etcd for etcdctl")
	}
}

func TestEtcdSnapshotJobDocumented(t *testing.T) {
	text := readOpsDoc(t, "docs/operations.md")
	for _, want := range []string{
		"<cluster>-etcd-snapshot",
		"create job --from=cronjob/",
		"--timeout=5m",
		"logs job/snapshot-now",
		"describe job/snapshot-now",
		"Use a new Job name for each manual snapshot",
		"etcd-snapshots/",
		"EtcdSnapshot",
		"SnapshotNotManaged",
		"only when etcd is operator-managed",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("docs/operations.md does not contain %q", want)
		}
	}
}

func TestEtcdSnapshotStatusDistinguishesManualJobs(t *testing.T) {
	text := readOpsDoc(t, "docs/operations.md")
	for _, want := range []string{
		"Verify the manual Job's `Complete` condition and its logs",
		"A manual Job created with `--from=cronjob` does not update the CronJob's `status.lastSuccessfulTime`",
		"it does not refresh the cluster's `EtcdSnapshot` condition",
		"`EtcdSnapshot` tracks scheduled snapshots through the CronJob's `status.lastSuccessfulTime`",
		"reconciliation delays can postpone the condition update",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("docs/operations.md does not explain snapshot status: missing %q", want)
		}
	}
}

func readOpsDoc(t *testing.T, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
