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

package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestSnapshotDownloadRenderedScript(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal(err)
	}
	timeout, err := exec.LookPath("timeout")
	if err != nil {
		t.Fatal(err)
	}
	objects := func(prefix string, newestFirst bool) []map[string]string {
		result := []map[string]string{{"Key": prefix + "a.db", "LastModified": "2026-11-01T00:00:00Z"}}
		for i := 0; i < 1001; i++ {
			result = append(result, map[string]string{"Key": fmt.Sprintf("%shealth/%04d.txt", prefix, i), "LastModified": "2026-11-07T00:00:00Z"})
		}
		result = append(result, map[string]string{"Key": prefix + "z.db", "LastModified": "2026-11-02T00:00:00Z"})
		if newestFirst {
			result[0]["LastModified"] = "2026-11-03T00:00:00Z"
		}
		return result
	}
	tests := []struct {
		name, prefix, want, raw     string
		objects                     []map[string]string
		missingPython, brokenPython bool
		wantError                   string
		failures, exit, calls       int
	}{
		{name: "newest_on_last_page", prefix: "snap", objects: objects("snap/", false), want: "snap/z.db", calls: 1},
		{name: "newest_on_first_page", prefix: "snap", objects: objects("snap/", true), want: "snap/a.db", calls: 1},
		{name: "health_only_last_page", prefix: "snap", objects: objects("snap/", true)[:1002], want: "snap/a.db", calls: 1},
		{name: "partial_stdout_nonzero", prefix: "snap", objects: objects("snap/", false), raw: `"snap/partial.db"`, failures: 1, exit: 1, want: "snap/z.db", calls: 2},
		{name: "partial_stdout_timeout", prefix: "snap", objects: objects("snap/", false), raw: `"snap/partial.db"`, failures: 1, exit: 124, want: "snap/z.db", calls: 2},
		{name: "persistent_partial_failure", prefix: "snap", raw: `"snap/partial.db"`, failures: 15, exit: 1, calls: 15},
		{name: "multiple_keys", prefix: "snap", raw: "snap/a.db\nsnap/b.db", calls: 15, wantError: "could not decode snapshot listing"},
		{name: "multiple_json_keys", prefix: "snap", raw: `["snap/a.db","snap/b.db"]`, calls: 15, wantError: "could not decode snapshot listing"},
		{name: "invalid_suffix", prefix: "snap", raw: `"snap/a.db.sha256"`, calls: 15},
		{name: "missing_python", prefix: "snap", missingPython: true, wantError: "python is required to decode etcd snapshot listings"},
		{name: "broken_python", prefix: "snap", brokenPython: true, calls: 15, wantError: "could not decode snapshot listing"},
		{name: "malformed_successful_listing", prefix: "snap", raw: `{broken`, calls: 15, wantError: "could not decode snapshot listing"},
		{name: "invalid_then_empty", prefix: "snap", raw: `{broken`, failures: 1, calls: 15},
		{name: "invalid_then_snapshot", prefix: "snap", raw: `{broken`, failures: 1, objects: objects("snap/", false), want: "snap/z.db", calls: 2},
		{name: "empty_bucket", prefix: "snap", calls: 15},
		{name: "no_snapshots", prefix: "snap", objects: []map[string]string{{"Key": "snap/health/check.txt", "LastModified": "2026-11-07"}}, calls: 15},
		{name: "literal_metacharacters", prefix: "s[nap].*+?$^(x)|'`", objects: objects("s[nap].*+?$^(x)|'`/", false), want: "s[nap].*+?$^(x)|'`/z.db", calls: 1},
		{name: "key_whitespace_unicode", prefix: "snap", objects: []map[string]string{{"Key": "snap/sub dir/quote\"tab\tline\né.db", "LastModified": "2026-11-07"}}, want: "snap/sub dir/quote\"tab\tline\né.db", calls: 1},
		{name: "empty_prefix", objects: objects("", false), want: "z.db", calls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(operatorEtcdEndpointsEnv, "")
			cluster := testCluster("snapshot-script", nil)
			scheme := testScheme(t)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
			if _, err := EnsureEtcd(context.Background(), c, scheme, cluster); err != nil {
				t.Fatal(err)
			}
			sts := &appsv1.StatefulSet{}
			assertFound(t, c, sts, cluster.Namespace, cluster.Name+"-etcd")
			script := ""
			for _, ic := range sts.Spec.Template.Spec.InitContainers {
				if ic.Name == "snapshot-download" {
					script = ic.Command[2]
				}
			}
			if script == "" {
				t.Fatal("snapshot-download script missing")
			}
			dir := t.TempDir()
			script = strings.ReplaceAll(script, "/var/lib/etcd", filepath.Join(dir, "data"))
			script = strings.ReplaceAll(script, "/snapshots/etcd-snapshot.db", filepath.Join(dir, "snapshot.db"))
			fixture := map[string]interface{}{"objects": tt.objects, "raw": tt.raw, "failures": tt.failures, "exit": tt.exit, "prefix": tt.prefix}
			data, err := json.Marshal(fixture)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(dir, "fixture.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(dir, "aws"), []byte("#!"+python+"\n"+snapshotAWSStub), 0700); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(dir, "sleep"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
				t.Fatal(err)
			}
			if tt.brokenPython {
				if err = os.WriteFile(filepath.Join(dir, "python"), []byte("#!/bin/sh\nexit 7\n"), 0700); err != nil {
					t.Fatal(err)
				}
			} else if !tt.missingPython {
				if err = os.Symlink(python, filepath.Join(dir, "python")); err != nil {
					t.Fatal(err)
				}
			}
			if err = os.Symlink(timeout, filepath.Join(dir, "timeout")); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir)
			t.Setenv("SNAPSHOT_STUB_DIR", dir)
			t.Setenv("SNAPSHOT_BUCKET", "backups")
			t.Setenv("SNAPSHOT_PREFIX", tt.prefix)
			t.Setenv("AWS_MAX_ATTEMPTS", "1")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, bash, "-c", script).CombinedOutput()
			if tt.wantError != "" {
				if err == nil {
					t.Fatalf("invalid decoder/listing returned success\n%s", out)
				}
				if !strings.Contains(string(out), tt.wantError) {
					t.Fatalf("missing error %q\n%s", tt.wantError, out)
				}
				if strings.Contains(string(out), "no snapshot found in s3://backups/snap\n") {
					t.Fatalf("invalid decoder/listing treated as empty bucket\n%s", out)
				}
			} else if err != nil {
				t.Fatalf("rendered script: %v\n%s", err, out)
			}
			copied, err := os.ReadFile(filepath.Join(dir, "copied.json"))
			if tt.want == "" {
				if !os.IsNotExist(err) {
					t.Fatalf("unexpected copy %s: %v\n%s", copied, err, out)
				}
			} else {
				if err != nil {
					t.Fatalf("missing copy: %v\n%s", err, out)
				}
				var args []string
				if err = json.Unmarshal(copied, &args); err != nil {
					t.Fatal(err)
				}
				if len(args) != 2 || args[0] != "s3://backups/"+tt.want || args[1] != filepath.Join(dir, "snapshot.db") {
					t.Fatalf("copy args %q, want key %q", args, tt.want)
				}
			}
			count, err := os.ReadFile(filepath.Join(dir, "calls"))
			if tt.calls == 0 && os.IsNotExist(err) {
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(count) != fmt.Sprint(tt.calls) {
				t.Fatalf("listing calls %s, want %d\n%s", count, tt.calls, out)
			}
		})
	}
}

const snapshotAWSStub = `import json, os, sys
from pathlib import Path
root = Path(os.environ["SNAPSHOT_STUB_DIR"])
fixture = json.loads((root / "fixture.json").read_text())
args = sys.argv[1:]
def option(name):
    return args[args.index(name) + 1]
assert option("--cli-connect-timeout") == "2"
assert option("--cli-read-timeout") == "5"
assert os.environ["AWS_MAX_ATTEMPTS"] == "1"
if "cp" in args:
    (root / "copied.json").write_text(json.dumps(args[args.index("cp")+1:]))
    sys.exit(0)
assert "list-objects-v2" in args
assert option("--bucket") == "backups"
prefix = fixture["prefix"] + ("/" if fixture["prefix"] else "")
assert option("--prefix") == prefix
counter = root / "calls"
count = int(counter.read_text()) + 1 if counter.exists() else 1
counter.write_text(str(count))
if count <= fixture["failures"]:
    print(fixture["raw"] if option("--output") == "json" else prefix + "partial.db")
    sys.exit(fixture["exit"])
if fixture["raw"] and not fixture["failures"]:
    print(fixture["raw"])
    sys.exit(0)
objects = [o for o in (fixture["objects"] or []) if o["Key"].startswith(prefix)]
query = option("--query")
def select(items):
    if query == "Contents[].Key":
        return [o["Key"] for o in items]
    assert "ends_with(Key, " + chr(96) + ".db" + chr(96) + ")" in query
    assert "sort_by(" in query and "LastModified" in query and "[-1].Key" in query
    snapshots = [o for o in items if o["Key"].endswith(".db")]
    return sorted(snapshots, key=lambda o:o["LastModified"])[-1]["Key"] if snapshots else None
if option("--output") == "json":
    assert "--no-paginate" not in args and "--max-items" not in args
    print(json.dumps(select(objects)))
else:
    for page in [objects[i:i+1000] for i in range(0, len(objects), 1000)] or [[]]:
        value = select(page)
        print("\t".join(value) if isinstance(value,list) else value if value is not None else "None")
`
