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
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/KafScale/platform/pkg/metadata"
)

func TestConsoleLoginDocumented(t *testing.T) {
	text := readOpsDoc(t, "docs/operations.md")
	for _, want := range []string{
		"POST /ui/api/auth/login",
		"read -rsp 'Console password: '",
		"--data-binary @-",
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

func TestEtcdSnapshotCommandsExecute(t *testing.T) {
	text := readOpsDoc(t, "docs/operations.md")
	script := opsBashExample(t, text, "create job --from=cronjob/")
	describe := regexp.MustCompile("`(kubectl[^`]+describe job/snapshot-now)`").FindStringSubmatch(text)
	if len(describe) != 2 {
		t.Fatal("missing inline snapshot describe command")
	}
	capture := filepath.Join(t.TempDir(), "kubectl-args")
	t.Setenv("OPS_KUBECTL_ARGS", capture)
	cmd := exec.Command("bash", "-ec", `kubectl() { printf '%s\0' "$@" >> "$OPS_KUBECTL_ARGS"; }
`+script+"\n"+describe[1])
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("snapshot commands failed: %v\n%s", err, output)
	}
	want := []string{
		"-n", "<namespace>", "create", "job", "--from=cronjob/<cluster>-etcd-snapshot", "snapshot-now",
		"-n", "<namespace>", "wait", "--for=condition=complete", "--timeout=5m", "job/snapshot-now",
		"-n", "<namespace>", "logs", "job/snapshot-now",
		"-n", "<namespace>", "describe", "job/snapshot-now",
	}
	if got := opsCommandArgs(t, capture); !slices.Equal(got, want) {
		t.Fatalf("kubectl arguments = %q, want %q", got, want)
	}
}

func TestConsoleLoginExampleKeepsPasswordOffCommandLine(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to execute the documented console login example")
	}
	script := opsBashExample(t, readOpsDoc(t, "docs/operations.md"), "curl -c cookies.txt")
	dir := t.TempDir()
	for _, name := range []string{"CURL_ARGS", "CURL_BODY", "JQ_ARGS"} {
		t.Setenv("OPS_"+name, filepath.Join(dir, name))
	}
	const username = `  qa "user"\team  `
	const password = `  secret "quotes" \ slashes $(false) spaces  `
	cmd := exec.Command("bash", "-euc", `set -o pipefail
curl() { printf '%s\0' "$@" > "$OPS_CURL_ARGS"; cat > "$OPS_CURL_BODY"; }
jq() { printf '%s\0' "$@" > "$OPS_JQ_ARGS"; command jq "$@"; }
`+script+`
test -z "${CONSOLE_PASSWORD+x}"
`)
	cmd.Stdin = strings.NewReader(username + "\n" + password + "\n")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("console login example failed: %v\n%s", err, output)
	} else if strings.Contains(string(output), password) {
		t.Fatal("console password appeared in command output")
	}
	curlArgs := opsCommandArgs(t, filepath.Join(dir, "CURL_ARGS"))
	want := []string{"-c", "cookies.txt", "-X", "POST", "http://127.0.0.1:8080/ui/api/auth/login", "-H", "Content-Type: application/json", "--data-binary", "@-"}
	if !slices.Equal(curlArgs, want) {
		t.Fatalf("curl arguments = %q, want %q", curlArgs, want)
	}
	for _, arg := range append(curlArgs, opsCommandArgs(t, filepath.Join(dir, "JQ_ARGS"))...) {
		if strings.Contains(arg, password) {
			t.Fatal("console password was passed as a process argument")
		}
	}
	body, err := os.ReadFile(filepath.Join(dir, "CURL_BODY"))
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("login body is not valid JSON: %v", err)
	}
	if payload.Username != username || payload.Password != password {
		t.Fatal("login body did not preserve the entered credentials")
	}
}

func opsBashExample(t *testing.T, text, marker string) string {
	t.Helper()
	for _, block := range strings.Split(text, "```bash\n")[1:] {
		script, _, ok := strings.Cut(block, "```")
		if ok && strings.Contains(script, marker) {
			return script
		}
	}
	t.Fatalf("missing Bash example containing %q", marker)
	return ""
}

func opsCommandArgs(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(data), "\x00"), "\x00")
}

func readOpsDoc(t *testing.T, rel string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
