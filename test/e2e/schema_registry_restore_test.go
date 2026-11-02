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
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const restoreRevisionBump = "1000000000"

type restoreEtcd struct {
	t        *testing.T
	dir      string
	client   string
	peer     string
	cluster  string
	snapshot string
	cmd      *exec.Cmd
}

func (e *restoreEtcd) start(dataDir string) {
	e.t.Helper()
	cmd := exec.Command(filepath.Join(e.dir, "etcd"),
		"--name", "n1", "--data-dir", dataDir,
		"--listen-client-urls", e.client, "--advertise-client-urls", e.client,
		"--listen-peer-urls", e.peer, "--initial-advertise-peer-urls", e.peer,
		"--initial-cluster", e.cluster, "--initial-cluster-token", "restore-e2e")
	configureProcessGroup(cmd)
	logs := mustLogFile(e.t, "etcd-restore.log")
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		e.t.Fatal(err)
	}
	e.cmd = cmd
	e.t.Cleanup(e.stop)
}

func (e *restoreEtcd) stop() {
	if e.cmd == nil {
		return
	}
	_ = e.cmd.Process.Signal(syscall.SIGTERM)
	_ = e.cmd.Wait()
	e.cmd = nil
}

func (e *restoreEtcd) run(name string, args ...string) {
	e.t.Helper()
	out, err := exec.Command(filepath.Join(e.dir, name), args...).CombinedOutput()
	if err != nil {
		e.t.Fatalf("%s %v: %v\n%s", name, args, err, out)
	}
}

func (e *restoreEtcd) clusterID(cli *clientv3.Client) uint64 {
	e.t.Helper()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := cli.Get(ctx, "/kafscale/schemas/counter")
		cancel()
		if err == nil {
			return resp.Header.ClusterId
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("etcd not ready: %v", err)
		}
	}
}

func registerRestoreSchema(t *testing.T, url, subject, name string) int64 {
	t.Helper()
	var out struct {
		ID int64 `json:"id"`
	}
	schema := `{"type":"record","name":"` + name + `","fields":[{"name":"f","type":"string"}]}`
	srCall(t, http.MethodPost, url+"/subjects/"+subject+"/versions", map[string]string{"schema": schema}, &out)
	return out.ID
}

func servedSchema(t *testing.T, url string, id int64) string {
	t.Helper()
	var out struct {
		Schema string `json:"schema"`
	}
	srCall(t, http.MethodGet, url+"/schemas/ids/"+strconv.FormatInt(id, 10), nil, &out)
	return out.Schema
}

func TestSchemaRegistryEtcdRestoreE2E(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E") != "1" {
		t.Skip("set KAFSCALE_E2E=1")
	}
	etcdDir := os.Getenv("KAFSCALE_E2E_ETCD_DIR")
	if etcdDir == "" {
		t.Skip("set KAFSCALE_E2E_ETCD_DIR to a directory with etcd, etcdctl and etcdutl 3.6")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	work := t.TempDir()
	peer := "http://127.0.0.1:" + pickFreePort(t)
	e := &restoreEtcd{
		t:        t,
		dir:      etcdDir,
		client:   "http://127.0.0.1:" + pickFreePort(t),
		peer:     peer,
		cluster:  "n1=" + peer,
		snapshot: filepath.Join(work, "snapshot.db"),
	}
	e.start(filepath.Join(work, "before"))
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{e.client}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	clusterBefore := e.clusterID(cli)

	h := &srHarness{t: t, ctx: ctx, work: work, endpoints: []string{e.client}, s3Env: []string{"KAFSCALE_S3_BUCKET="}}
	h.bin = filepath.Join(work, "kafscale-schema-registry")
	h.run(nil, "go", "build", "-o", h.bin, filepath.Join(repoRoot(t), "cmd", "schema-registry"))
	idleAddr := "127.0.0.1:" + pickFreePort(t)
	idle := h.startRegistry(idleAddr)
	defer func() { _ = idle.Process.Kill(); _ = idle.Wait() }()
	idleURL := "http://" + idleAddr
	srCall(t, http.MethodPut, idleURL+"/config", map[string]string{"compatibility": "NONE"}, &map[string]any{})

	if id := registerRestoreSchema(t, idleURL, "kept", "Kept"); id != 1 {
		t.Fatalf("first id %d, want 1", id)
	}
	e.run("etcdctl", "--endpoints", e.client, "snapshot", "save", e.snapshot)
	lost := registerRestoreSchema(t, idleURL, "lost", "Lost")
	lostText := servedSchema(t, idleURL, lost)

	e.stop()
	restored := filepath.Join(work, "restored")
	e.run("etcdutl", "snapshot", "restore", e.snapshot,
		"--data-dir", restored, "--name", "n1",
		"--initial-cluster", e.cluster, "--initial-cluster-token", "restore-e2e",
		"--initial-advertise-peer-urls", peer,
		"--bump-revision", restoreRevisionBump, "--mark-compacted")
	e.start(restored)
	if got := e.clusterID(cli); got != clusterBefore {
		t.Fatalf("cluster id changed from %x to %x; this test needs the same cluster id", clusterBefore, got)
	}

	otherAddr := "127.0.0.1:" + pickFreePort(t)
	other := h.startRegistry(otherAddr)
	defer func() { _ = other.Process.Kill(); _ = other.Wait() }()
	newID := registerRestoreSchema(t, "http://"+otherAddr, "after", "After")
	if newID != lost {
		t.Fatalf("id after restore %d, want the lost id %d to be given out again without S3", newID, lost)
	}
	newText := servedSchema(t, "http://"+otherAddr, newID)

	var got string
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		if got = servedSchema(t, idleURL, newID); got == newText {
			t.Logf("idle registry serves id %d from the restored etcd", newID)
			return
		}
	}
	if got == lostText {
		t.Fatalf("idle registry still serves id %d with the schema from before the restore: %s", newID, got)
	}
	t.Fatalf("idle registry serves id %d as %s, want %s", newID, got, newText)
}

func (e *restoreEtcd) restore(dataDir string) {
	e.t.Helper()
	e.run("etcdutl", "snapshot", "restore", e.snapshot,
		"--data-dir", dataDir, "--name", "n1",
		"--initial-cluster", e.cluster, "--initial-cluster-token", "restore-e2e",
		"--initial-advertise-peer-urls", e.peer,
		"--bump-revision", restoreRevisionBump, "--mark-compacted")
}

func startRegistryUnready(t *testing.T, h *srHarness, addr string) (*exec.Cmd, string) {
	t.Helper()
	cmd := exec.CommandContext(h.ctx, h.bin)
	configureProcessGroup(cmd)
	cmd.Env = append(os.Environ(),
		"KAFSCALE_SCHEMA_REGISTRY_ADDR="+addr,
		"KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS="+strings.Join(h.endpoints, ","))
	cmd.Env = append(cmd.Env, h.s3Env...)
	logs := mustLogFile(t, "schema-registry-unready.log").(*os.File)
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd, logs.Name()
}

func waitForLogCount(t *testing.T, path, text string, n int, timeout time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		data, _ := os.ReadFile(path)
		if strings.Count(string(data), text) >= n {
			return
		}
	}
	t.Fatalf("%q did not appear %d times in %s", text, n, path)
}

func TestSchemaRegistryEtcdRestoreWhileWatchStartsE2E(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E") != "1" {
		t.Skip("set KAFSCALE_E2E=1")
	}
	etcdDir := os.Getenv("KAFSCALE_E2E_ETCD_DIR")
	if etcdDir == "" {
		t.Skip("set KAFSCALE_E2E_ETCD_DIR to a directory with etcd, etcdctl and etcdutl 3.6")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	work := t.TempDir()
	peer := "http://127.0.0.1:" + pickFreePort(t)
	e := &restoreEtcd{
		t:        t,
		dir:      etcdDir,
		client:   "http://127.0.0.1:" + pickFreePort(t),
		peer:     peer,
		cluster:  "n1=" + peer,
		snapshot: filepath.Join(work, "snapshot.db"),
	}
	before := filepath.Join(work, "before")
	e.start(before)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{e.client}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	e.clusterID(cli)

	h := &srHarness{t: t, ctx: ctx, work: work, endpoints: []string{e.client}, s3Env: []string{"KAFSCALE_S3_BUCKET="}}
	h.bin = filepath.Join(work, "kafscale-schema-registry")
	h.run(nil, "go", "build", "-o", h.bin, filepath.Join(repoRoot(t), "cmd", "schema-registry"))
	writerAddr := "127.0.0.1:" + pickFreePort(t)
	writer := h.startRegistry(writerAddr)
	writerURL := "http://" + writerAddr
	srCall(t, http.MethodPut, writerURL+"/config", map[string]string{"compatibility": "NONE"}, &map[string]any{})
	if id := registerRestoreSchema(t, writerURL, "kept", "Kept"); id != 1 {
		t.Fatalf("first id %d, want 1", id)
	}
	e.run("etcdctl", "--endpoints", e.client, "snapshot", "save", e.snapshot)
	lost := registerRestoreSchema(t, writerURL, "lost", "Lost")
	_ = writer.Process.Kill()
	_ = writer.Wait()
	e.stop()

	idleAddr := "127.0.0.1:" + pickFreePort(t)
	idle, idleLog := startRegistryUnready(t, h, idleAddr)
	defer func() { _ = idle.Process.Kill(); _ = idle.Wait() }()
	idleURL := "http://" + idleAddr
	waitForLogCount(t, idleLog, "read schema id counter from etcd", 1, time.Minute)

	e.start(before)
	e.clusterID(cli)
	lostText := servedSchema(t, idleURL, lost)
	e.stop()
	e.restore(filepath.Join(work, "restored"))
	e.start(filepath.Join(work, "restored"))
	e.clusterID(cli)

	otherAddr := "127.0.0.1:" + pickFreePort(t)
	other := h.startRegistry(otherAddr)
	defer func() { _ = other.Process.Kill(); _ = other.Wait() }()
	newID := registerRestoreSchema(t, "http://"+otherAddr, "after", "After")
	if newID != lost {
		t.Fatalf("id after restore %d, want the lost id %d to be given out again without S3", newID, lost)
	}
	newText := servedSchema(t, "http://"+otherAddr, newID)

	var got string
	for deadline := time.Now().Add(40 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		if got = servedSchema(t, idleURL, newID); got == newText {
			t.Logf("idle registry serves id %d from the restored etcd", newID)
			return
		}
	}
	if got == lostText {
		t.Fatalf("idle registry still serves id %d with the schema from before the restore: %s", newID, got)
	}
	t.Fatalf("idle registry serves id %d as %s, want %s", newID, got, newText)
}
