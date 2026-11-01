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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/storage"
	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
)

const (
	srUserSchema     = `{"type":"record","name":"User","namespace":"e2e","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`
	srConfluentImage = "confluentinc/cp-schema-registry:8.4.0"
	srMavenImage     = "maven:3.9-eclipse-temurin-21"
	srKafkaImage     = "apache/kafka:4.3.2"
	srRecords        = 5
)

type srUser struct {
	Name string `avro:"name"`
	Age  int    `avro:"age"`
}

type srHarness struct {
	t         *testing.T
	ctx       context.Context
	bin       string
	endpoints []string
	docker    []string
	python    string
	javaDir   string
	m2        string
	work      string
	s3Env     []string
	after     func(url string)
}

func TestSchemaRegistryE2E(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E") != "1" {
		t.Skip("set KAFSCALE_E2E=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	h := &srHarness{t: t, ctx: ctx, work: t.TempDir(), s3Env: []string{"KAFSCALE_S3_BUCKET="}}
	h.checkTools()
	h.runScenarios()
}

func TestSchemaRegistryMinIOE2E(t *testing.T) {
	if os.Getenv("KAFSCALE_E2E") != "1" {
		t.Skip("set KAFSCALE_E2E=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	h := &srHarness{t: t, ctx: ctx, work: t.TempDir()}
	h.checkTools()
	s3cfg := h.startMinIO()
	h.s3Env = []string{
		"KAFSCALE_S3_BUCKET=" + s3cfg.Bucket,
		"KAFSCALE_S3_REGION=" + s3cfg.Region,
		"KAFSCALE_S3_ENDPOINT=" + s3cfg.Endpoint,
		"KAFSCALE_S3_ACCESS_KEY=" + s3cfg.AccessKeyID,
		"KAFSCALE_S3_SECRET_KEY=" + s3cfg.SecretAccessKey,
		"KAFSCALE_S3_NAMESPACE=sr-e2e",
		"KAFSCALE_SCHEMA_REGISTRY_INLINE_LIMIT_BYTES=64",
	}
	h.after = func(url string) { h.checkBucket(s3cfg, url) }
	h.runScenarios()
}

func (h *srHarness) startMinIO() storage.S3Config {
	t := h.t
	image := os.Getenv("KAFSCALE_E2E_MINIO_IMAGE")
	if image == "" {
		image = "kafscale-minio:sr-e2e"
		h.run(nil, h.docker[0], append(h.docker[1:], "build", "-q", "-t", image, "-f", filepath.Join(repoRoot(t), "test", "e2e", "minio.Dockerfile"), repoRoot(t))...)
	}
	addr := freeAddr(t)
	name := "kafscale-sr-minio-" + strings.ReplaceAll(strings.ReplaceAll(addr, ":", "-"), ".", "-")
	h.container(t, name, "-e", "MINIO_ROOT_USER=minioadmin", "-e", "MINIO_ROOT_PASSWORD=minioadmin", image, "server", "/data", "--address", addr, "--console-address", freeAddr(t))
	cfg := storage.S3Config{Bucket: "kafscale-sr-e2e", Region: "us-east-1", Endpoint: "http://" + addr, ForcePathStyle: true, AccessKeyID: "minioadmin", SecretAccessKey: "minioadmin"}
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(500 * time.Millisecond) {
		cli, err := storage.NewS3Client(h.ctx, cfg)
		if err == nil {
			if err = cli.EnsureBucket(h.ctx); err == nil {
				return cfg
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("MinIO on %s not ready: %v", addr, err)
		}
	}
}

func (h *srHarness) checkBucket(cfg storage.S3Config, url string) {
	t := h.t
	cli, err := storage.NewS3Client(h.ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	list := func(prefix string) []string {
		objs, err := cli.ListSegments(h.ctx, prefix)
		if err != nil {
			t.Fatal(err)
		}
		var keys []string
		for _, o := range objs {
			keys = append(keys, o.Key)
		}
		return keys
	}
	if probes := list("sr-e2e/@schemas/probe/"); len(probes) != 0 {
		t.Fatalf("probe objects left behind: %v", probes)
	}
	if texts := list("sr-e2e/@schemas/sha256/"); len(texts) == 0 {
		t.Fatal("no schema text stored in S3 with a 64 byte inline limit")
	}
	markers := list("sr-e2e/@schemas/ids/")
	if len(markers) == 0 {
		t.Fatal("no schema id markers in S3")
	}
	var schemas []struct {
		ID int64 `json:"id"`
	}
	if code := srCall(t, http.MethodGet, url+"/schemas?deleted=true", nil, &schemas); code != http.StatusOK || len(schemas) == 0 {
		t.Fatalf("GET /schemas: HTTP %d, %d schemas", code, len(schemas))
	}
	have := map[string]bool{}
	for _, key := range markers {
		have[key] = true
	}
	served := map[int64]bool{}
	for _, sc := range schemas {
		if key := fmt.Sprintf("sr-e2e/@schemas/ids/%020d", sc.ID); !have[key] && !served[sc.ID] {
			t.Fatalf("schema id %d has no marker %s", sc.ID, key)
		}
		served[sc.ID] = true
	}
	t.Logf("MinIO bucket: %d schema ids, %d id markers, %d markers without a schema, schema text objects present, no probe objects", len(served), len(markers), len(markers)-len(served))
}

func (h *srHarness) runScenarios() {
	t := h.t
	ctx := h.ctx
	parent := t
	etcd, endpoints := startEmbeddedEtcd(t)
	defer etcd.Close()
	h.endpoints = endpoints
	brokerAddr := freeAddr(t)
	brokerCmd, brokerLogs := startBrokerWithEtcd(t, ctx, brokerAddr, freeAddr(t), freeAddr(t), endpoints)
	defer stopBroker(t, brokerCmd)
	waitForBroker(t, brokerLogs, brokerAddr)
	h.bin = filepath.Join(h.work, "kafscale-schema-registry")
	h.run(nil, "go", "build", "-o", h.bin, filepath.Join(repoRoot(t), "cmd", "schema-registry"))
	addrA, addrB := freeAddr(t), freeAddr(t)
	regA := h.startRegistry(addrA)
	regB := h.startRegistry(addrB)
	defer func() { stopBroker(t, regB) }()
	urlA, urlB := "http://"+addrA, "http://"+addrB

	t.Run("confluent-data-migrates", func(t *testing.T) {
		h.t = t
		defer func() { h.t = parent }()
		h.confluentMigration(t, brokerAddr, urlA, urlB)
	})
	t.Run("instance-killed-mid-run", func(t *testing.T) {
		h.t = t
		defer func() { h.t = parent }()
		regA = h.killMidRun(t, regA, addrA, urlA, urlB)
	})
	defer func() { stopBroker(t, regA) }()
	t.Run("clients-interoperate", func(t *testing.T) {
		h.t = t
		defer func() { h.t = parent }()
		h.interop(t, brokerAddr, urlA, urlB)
	})
	if h.after != nil && !parent.Failed() {
		h.after(urlA)
	}
}

func (h *srHarness) checkTools() {
	t := h.t
	h.docker = strings.Fields(os.Getenv("KAFSCALE_E2E_DOCKER"))
	if len(h.docker) == 0 {
		h.docker = []string{"docker"}
	}
	h.python = os.Getenv("KAFSCALE_SR_PYTHON")
	if h.python == "" {
		h.python = "python3"
	}
	if err := exec.Command(h.python, "-c", "import confluent_kafka").Run(); err != nil {
		t.Skipf("confluent_kafka is not importable with %s (set KAFSCALE_SR_PYTHON): %v", h.python, err)
	}
	if out := strings.TrimSpace(h.run(nil, h.python, "-c", "import confluent_kafka; print(confluent_kafka.version()[0] if isinstance(confluent_kafka.version(), tuple) else confluent_kafka.version())")); out != "2.16.1" {
		t.Fatalf("confluent-kafka 2.16.1 is required, found %s", out)
	}
	h.m2 = os.Getenv("KAFSCALE_E2E_M2")
	if h.m2 == "" {
		h.m2 = filepath.Join(os.TempDir(), "kafscale-e2e-m2")
	}
	if err := os.MkdirAll(h.m2, 0o755); err != nil {
		t.Fatal(err)
	}
	h.javaDir = filepath.Join(h.work, "java")
	h.run(nil, "cp", "-R", filepath.Join(repoRoot(t), "test", "e2e", "testdata", "schemaregistry", "java"), h.javaDir)
	h.run(nil, h.docker[0], append(h.docker[1:], "pull", "-q", srConfluentImage)...)
	h.run(nil, h.docker[0], append(h.docker[1:], "pull", "-q", srMavenImage)...)
	h.run(nil, h.docker[0], append(h.docker[1:], "pull", "-q", srKafkaImage)...)
	h.maven("package")
	h.dataPath("")
}

func (h *srHarness) run(stdin io.Reader, name string, args ...string) string {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = stdin
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		h.t.Fatalf("%s %s: %v\n%s\n%s", name, strings.Join(args, " "), err, stdout.String(), stderr.String())
	}
	return stdout.String()
}

func (h *srHarness) maven(goals ...string) string {
	h.t.Helper()
	args := append(h.docker[1:], "run", "--rm", "--network", "host",
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-e", "HOME=/tmp", "-e", "MAVEN_CONFIG=/tmp/.m2",
		"-v", h.m2+":/m2", "-v", h.javaDir+":/src", "-w", "/src",
		srMavenImage, "mvn", "-q", "-B", "-Dmaven.repo.local=/m2")
	return h.run(nil, h.docker[0], append(args, goals...)...)
}

func (h *srHarness) java(mode, url, topic, file string, n int) string {
	h.t.Helper()
	execArgs := fmt.Sprintf("%s %s %s /src/data/%s %d", mode, url, topic, file, n)
	return h.maven("exec:java", "-Dexec.args="+execArgs)
}

func (h *srHarness) dataPath(file string) string {
	dir := filepath.Join(h.javaDir, "data")
	if err := os.MkdirAll(dir, 0o777); err != nil {
		h.t.Fatal(err)
	}
	return filepath.Join(dir, file)
}

func (h *srHarness) py(mode, url, topic, file string, n int) string {
	h.t.Helper()
	script := filepath.Join(repoRoot(h.t), "test", "e2e", "testdata", "schemaregistry", "client.py")
	return h.run(nil, h.python, script, mode, url, topic, h.dataPath(file), fmt.Sprint(n))
}

func (h *srHarness) startRegistry(addr string) *exec.Cmd {
	t := h.t
	t.Helper()
	cmd := exec.CommandContext(h.ctx, h.bin)
	configureProcessGroup(cmd)
	cmd.Env = append(os.Environ(),
		"KAFSCALE_SCHEMA_REGISTRY_ADDR="+addr,
		"KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS="+strings.Join(h.endpoints, ","))
	cmd.Env = append(cmd.Env, h.s3Env...)
	logs := mustLogFile(t, "schema-registry-"+strings.ReplaceAll(addr, ":", "-")+".log")
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return cmd
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("schema registry on %s not ready: %v", addr, err)
		}
	}
}

func srCall(t *testing.T, method, url string, body any, out any) int {
	t.Helper()
	var r io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil && resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(data, out); err != nil {
			t.Fatalf("%s %s: %v: %s", method, url, err, data)
		}
	}
	if resp.StatusCode != http.StatusOK && out != nil {
		t.Fatalf("%s %s: %d %s", method, url, resp.StatusCode, data)
	}
	return resp.StatusCode
}

func (h *srHarness) confluentMigration(t *testing.T, brokerAddr, urlA, urlB string) {
	suffix := fmt.Sprint(time.Now().UnixNano())
	kafkaAddr, controllerAddr, cpAddr := freeAddr(t), freeAddr(t), freeAddr(t)
	h.container(t, "kafscale-e2e-kafka-"+suffix,
		"-e", "KAFKA_NODE_ID=1",
		"-e", "KAFKA_PROCESS_ROLES=broker,controller",
		"-e", "KAFKA_LISTENERS=PLAINTEXT://"+kafkaAddr+",CONTROLLER://"+controllerAddr,
		"-e", "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://"+kafkaAddr,
		"-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
		"-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
		"-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@"+controllerAddr,
		"-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		"-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
		"-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
		srKafkaImage)
	h.container(t, "kafscale-e2e-cp-schema-registry-"+suffix,
		"-e", "SCHEMA_REGISTRY_HOST_NAME=127.0.0.1",
		"-e", "SCHEMA_REGISTRY_LISTENERS=http://"+cpAddr,
		"-e", "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://"+kafkaAddr,
		srConfluentImage)
	cpURL := "http://" + cpAddr
	for deadline := time.Now().Add(3 * time.Minute); ; time.Sleep(time.Second) {
		resp, err := http.Get(cpURL + "/subjects")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("cp-schema-registry not ready: %v", err)
		}
	}
	h.java("serialize", cpURL, "legacy", "legacy.hex", srRecords)
	var subjects []string
	srCall(t, http.MethodGet, cpURL+"/subjects", nil, &subjects)
	if len(subjects) != 1 || subjects[0] != "legacy-value" {
		t.Fatalf("confluent subjects %v", subjects)
	}
	var source struct {
		ID      int    `json:"id"`
		Version int    `json:"version"`
		Schema  string `json:"schema"`
	}
	srCall(t, http.MethodGet, cpURL+"/subjects/legacy-value/versions/1", nil, &source)
	var mode map[string]any
	srCall(t, http.MethodPut, urlA+"/mode/legacy-value", map[string]string{"mode": "IMPORT"}, &mode)
	var reg map[string]any
	srCall(t, http.MethodPost, urlA+"/subjects/legacy-value/versions", map[string]any{"schema": source.Schema, "id": source.ID, "version": source.Version}, &reg)
	srCall(t, http.MethodPut, urlA+"/mode/legacy-value", map[string]string{"mode": "READWRITE"}, &mode)

	frames := readFrames(t, h.dataPath("legacy.hex"))
	for _, f := range frames {
		if got := int(binary.BigEndian.Uint32(f[1:5])); f[0] != 0 || got != source.ID {
			t.Fatalf("frame carries id %d, confluent assigned %d", got, source.ID)
		}
	}
	mirrored := mirror(t, h.ctx, brokerAddr, "legacy", frames)
	writeFrames(t, h.dataPath("legacy-mirrored.hex"), mirrored)
	want := expectedUsers("java")
	if got := h.java("deserialize", urlB, "legacy", "legacy-mirrored.hex", 0); got != want {
		t.Fatalf("java decode through KafScale registry:\n%s\nwant:\n%s", got, want)
	}
	if got := h.py("deserialize", urlA, "legacy", "legacy-mirrored.hex", 0); got != want {
		t.Fatalf("python decode through KafScale registry:\n%s\nwant:\n%s", got, want)
	}
	if got := decodeFranz(t, urlB, mirrored); got != want {
		t.Fatalf("franz-go decode through KafScale registry:\n%s\nwant:\n%s", got, want)
	}
	var next struct {
		ID int `json:"id"`
	}
	srCall(t, http.MethodPost, urlB+"/subjects/after-import-value/versions", map[string]string{"schema": `{"type":"record","name":"After","fields":[{"name":"a","type":"long"}]}`}, &next)
	if next.ID <= source.ID {
		t.Fatalf("new id %d must be above imported id %d", next.ID, source.ID)
	}
}

func (h *srHarness) container(t *testing.T, name string, args ...string) {
	t.Helper()
	h.run(nil, h.docker[0], append(append(h.docker[1:], "run", "-d", "--name", name, "--network", "host"), args...)...)
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := exec.Command(h.docker[0], append(h.docker[1:], "logs", "--tail", "40", name)...).CombinedOutput()
			t.Logf("%s logs:\n%s", name, logs)
		}
		_ = exec.Command(h.docker[0], append(h.docker[1:], "rm", "-f", name)...).Run()
	})
}

func (h *srHarness) killMidRun(t *testing.T, regA *exec.Cmd, addrA, urlA, urlB string) *exec.Cmd {
	clientA, err := sr.NewClient(sr.URLs(urlA))
	if err != nil {
		t.Fatal(err)
	}
	clientB, err := sr.NewClient(sr.URLs(urlB))
	if err != nil {
		t.Fatal(err)
	}
	const subjects, goroutines, perGoroutine = 4, 16, 20
	for i := range subjects {
		res := clientB.SetCompatibility(h.ctx, sr.SetCompatibility{Level: sr.CompatNone}, fmt.Sprintf("load-%d", i))
		if len(res) != 1 || res[0].Err != nil {
			t.Fatalf("set compatibility: %+v", res)
		}
	}
	type claim struct {
		subject string
		schema  string
		id      int
		version int
	}
	var mu sync.Mutex
	var claims []claim
	var failedOnA int
	var wg sync.WaitGroup
	killed := make(chan struct{})
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := clientB
			if g%2 == 0 {
				client = clientA
			}
			for i := range perGoroutine {
				subject := fmt.Sprintf("load-%d", (g+i)%subjects)
				schema := fmt.Sprintf(`{"type":"record","name":"Load%d_%d","fields":[{"name":"f","type":"string"}]}`, g, i)
				ctx, cancel := context.WithTimeout(h.ctx, 15*time.Second)
				ss, err := client.CreateSchema(ctx, subject, sr.Schema{Schema: schema})
				cancel()
				mu.Lock()
				if err != nil {
					if client == clientB {
						t.Errorf("register through surviving instance: %v", err)
					}
					failedOnA++
				} else {
					claims = append(claims, claim{subject, schema, ss.ID, ss.Version})
				}
				mu.Unlock()
				if i == perGoroutine/2 && g == 0 {
					close(killed)
				}
			}
		}()
	}
	<-killed
	if err := syscall.Kill(-regA.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = regA.Wait()
	wg.Wait()
	if t.Failed() {
		return h.startRegistry(addrA)
	}
	t.Logf("%d registrations succeeded, %d failed against the killed instance", len(claims), failedOnA)
	if failedOnA == 0 {
		t.Fatalf("no registration hit the killed instance")
	}
	owner := map[int]string{}
	for _, c := range claims {
		if prev, ok := owner[c.id]; ok && prev != c.schema {
			t.Fatalf("id %d given to two schemas", c.id)
		}
		owner[c.id] = c.schema
	}
	regA = h.startRegistry(addrA)
	for _, c := range claims {
		for _, client := range []*sr.Client{clientA, clientB} {
			s, err := client.SchemaByID(h.ctx, c.id)
			if err != nil {
				t.Fatal(err)
			}
			if s.Schema != c.schema {
				t.Fatalf("id %d holds %s, caller registered %s", c.id, s.Schema, c.schema)
			}
		}
	}
	for i := range subjects {
		subject := fmt.Sprintf("load-%d", i)
		all, err := clientA.Schemas(h.ctx, subject)
		if err != nil {
			t.Fatal(err)
		}
		prev := 0
		for n, s := range all {
			if s.Version != n+1 || s.ID <= prev {
				t.Fatalf("subject %s versions out of order at %+v", subject, s)
			}
			prev = s.ID
		}
	}
	return regA
}

func (h *srHarness) interop(t *testing.T, brokerAddr, urlA, urlB string) {
	h.py("serialize", urlA, "users", "python.hex", srRecords)
	h.java("serialize", urlB, "users", "java.hex", srRecords)
	pyFrames := readFrames(t, h.dataPath("python.hex"))
	javaFrames := readFrames(t, h.dataPath("java.hex"))
	if !bytes.Equal(pyFrames[0][:5], javaFrames[0][:5]) {
		t.Fatalf("python and java registered the same schema under different ids: %x %x", pyFrames[0][:5], javaFrames[0][:5])
	}
	pyMirrored := mirror(t, h.ctx, brokerAddr, "users-python", pyFrames)
	javaMirrored := mirror(t, h.ctx, brokerAddr, "users-java", javaFrames)
	writeFrames(t, h.dataPath("python-mirrored.hex"), pyMirrored)
	writeFrames(t, h.dataPath("java-mirrored.hex"), javaMirrored)
	if got, want := h.java("deserialize", urlA, "users", "python-mirrored.hex", 0), expectedUsers("python"); got != want {
		t.Fatalf("java decode of python data:\n%s\nwant:\n%s", got, want)
	}
	if got, want := h.py("deserialize", urlB, "users", "java-mirrored.hex", 0), expectedUsers("java"); got != want {
		t.Fatalf("python decode of java data:\n%s\nwant:\n%s", got, want)
	}
	if got, want := decodeFranz(t, urlA, javaMirrored), expectedUsers("java"); got != want {
		t.Fatalf("franz-go decode of java data:\n%s\nwant:\n%s", got, want)
	}
	client, err := sr.NewClient(sr.URLs(urlB))
	if err != nil {
		t.Fatal(err)
	}
	ss, err := client.CreateSchema(h.ctx, "users-value", sr.Schema{Schema: srUserSchema})
	if err != nil {
		t.Fatal(err)
	}
	if ss.ID != int(binary.BigEndian.Uint32(pyFrames[0][1:5])) || ss.Version != 1 {
		t.Fatalf("franz-go register of the same schema got id %d version %d", ss.ID, ss.Version)
	}
	schema, err := avro.Parse(srUserSchema)
	if err != nil {
		t.Fatal(err)
	}
	var serde sr.Serde
	serde.Register(ss.ID, srUser{}, sr.EncodeFn(func(v any) ([]byte, error) { return avro.Marshal(schema, v) }))
	var goFrames [][]byte
	for i := range srRecords {
		b, err := serde.Encode(srUser{Name: fmt.Sprintf("go-%d", i), Age: i})
		if err != nil {
			t.Fatal(err)
		}
		goFrames = append(goFrames, b)
	}
	writeFrames(t, h.dataPath("go.hex"), mirror(t, h.ctx, brokerAddr, "users-go", goFrames))
	if got, want := h.java("deserialize", urlB, "users", "go.hex", 0), expectedUsers("go"); got != want {
		t.Fatalf("java decode of franz-go data:\n%s\nwant:\n%s", got, want)
	}
	if got, want := h.py("deserialize", urlA, "users", "go.hex", 0), expectedUsers("go"); got != want {
		t.Fatalf("python decode of franz-go data:\n%s\nwant:\n%s", got, want)
	}
	h.guidHeaders(t, urlA, urlB)
}

func (h *srHarness) guidHeaders(t *testing.T, urlA, urlB string) {
	h.py("guid-serialize", urlA, "guids", "python-guid.txt", srRecords)
	h.java("guid-serialize", urlB, "guids", "java-guid.txt", srRecords)
	var latest struct {
		GUID   string `json:"guid"`
		Schema string `json:"schema"`
	}
	srCall(t, http.MethodGet, urlB+"/subjects/guids-value/versions/latest", nil, &latest)
	guid, err := hex.DecodeString(strings.ReplaceAll(latest.GUID, "-", ""))
	if err != nil || len(guid) != 16 {
		t.Fatalf("guid %q: %v", latest.GUID, err)
	}
	for _, file := range []string{"python-guid.txt", "java-guid.txt"} {
		data, err := os.ReadFile(h.dataPath(file))
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			header, err := hex.DecodeString(strings.Fields(line)[0])
			if err != nil || len(header) != 17 || header[0] != 1 || !bytes.Equal(header[1:], guid) {
				t.Fatalf("%s header %x, want 01%x", file, header, guid)
			}
		}
	}
	var byGUID struct {
		Schema string `json:"schema"`
	}
	srCall(t, http.MethodGet, urlA+"/schemas/guids/"+latest.GUID, nil, &byGUID)
	if byGUID.Schema != latest.Schema {
		t.Fatalf("schema by guid %q, want %q", byGUID.Schema, latest.Schema)
	}
	if got, want := h.java("guid-deserialize", urlA, "guids", "python-guid.txt", 0), expectedUsers("python"); got != want {
		t.Fatalf("java decode of python guid header data:\n%s\nwant:\n%s", got, want)
	}
	if got, want := h.py("guid-deserialize", urlB, "guids", "java-guid.txt", 0), expectedUsers("java"); got != want {
		t.Fatalf("python decode of java guid header data:\n%s\nwant:\n%s", got, want)
	}
}

func expectedUsers(prefix string) string {
	var b strings.Builder
	for i := range srRecords {
		fmt.Fprintf(&b, "%s-%d,%d\n", prefix, i, i)
	}
	return b.String()
}

func readFrames(t *testing.T, path string) [][]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out [][]byte
	for _, line := range strings.Fields(string(data)) {
		b, err := hex.DecodeString(line)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, b)
	}
	if len(out) != srRecords {
		t.Fatalf("%s has %d frames", path, len(out))
	}
	return out
}

func writeFrames(t *testing.T, path string, frames [][]byte) {
	t.Helper()
	var b strings.Builder
	for _, f := range frames {
		b.WriteString(hex.EncodeToString(f) + "\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mirror(t *testing.T, ctx context.Context, brokerAddr, topic string, frames [][]byte) [][]byte {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr), kgo.AllowAutoTopicCreation(), kgo.DefaultProduceTopic(topic),
		kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, f := range frames {
		if err := client.ProduceSync(ctx, &kgo.Record{Value: f}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	var out [][]byte
	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for len(out) < len(frames) {
		fetches := client.PollFetches(pollCtx)
		if err := pollCtx.Err(); err != nil {
			t.Fatalf("consumed %d of %d records from %s: %v", len(out), len(frames), topic, err)
		}
		fetches.EachError(func(_ string, _ int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) { out = append(out, r.Value) })
	}
	return out
}

func decodeFranz(t *testing.T, url string, frames [][]byte) string {
	t.Helper()
	client, err := sr.NewClient(sr.URLs(url))
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	var serde sr.Serde
	known := map[int]bool{}
	for _, f := range frames {
		id := int(binary.BigEndian.Uint32(f[1:5]))
		if !known[id] {
			known[id] = true
			s, err := client.SchemaByID(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			schema, err := avro.Parse(s.Schema)
			if err != nil {
				t.Fatal(err)
			}
			serde.Register(id, srUser{}, sr.DecodeFn(func(b []byte, v any) error { return avro.Unmarshal(schema, b, v) }))
		}
		var u srUser
		if err := serde.Decode(f, &u); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "%s,%d\n", u.Name, u.Age)
	}
	return b.String()
}
