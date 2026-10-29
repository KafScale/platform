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

package imagerefs

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
)

var (
	deadMinioImage = regexp.MustCompile(`quay\.io/minio/|docker\.io/minio/|bitnami/minio|(^|[^\w./-])minio/(minio|mc)([^\w.-]|$)`)
	helmChartRef   = regexp.MustCompile(`\bhelm\s+(?:install|upgrade)\b(?:\s+--?[\w-]+)*\s+\S+\s+minio/minio\b`)
)

func isDeadMinioRef(line string) bool {
	return deadMinioImage.MatchString(helmChartRef.ReplaceAllString(line, ""))
}

func TestIsDeadMinioRef(t *testing.T) {
	cases := []struct {
		line string
		want bool
	}{
		{"MINIO_IMAGE ?= quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z", true},
		{"    image: minio/minio:latest", true},
		{"    image: minio/mc:latest", true},
		{"docker pull minio/minio", true},
		{"FROM minio/minio", true},
		{"FROM docker.io/minio/minio", true},
		{"  image: bitnami/minio:2024", true},
		{`"minio/minio"`, true},
		{"helm install minio minio/minio \\", false},
		{"helm upgrade --install minio minio/minio", false},
		{"helm upgrade -i minio minio/minio", false},
		{"helm install x minio/minio --set image.repository=quay.io/minio/minio", true},
		{"RUN git remote add origin https://github.com/minio/minio.git", false},
		{"github.com/minio/minio-go/v7 v7.0.0", false},
		{"MINIO_IMAGE ?= kafscale-minio:local", false},
	}
	for _, c := range cases {
		if got := isDeadMinioRef(c.line); got != c.want {
			t.Errorf("isDeadMinioRef(%q) = %v, want %v", c.line, got, c.want)
		}
	}
}

func TestNoUnpullableMinioImages(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("git", "-C", root, "ls-files", "-z").Output()
	if err != nil {
		t.Fatalf("git ls-files: %v", err)
	}
	files := bytes.Split(bytes.TrimRight(out, "\x00"), []byte{0})
	if len(files) == 0 {
		t.Fatal("git ls-files returned no files")
	}
	for _, name := range files {
		if string(name) == "test/imagerefs/imagerefs_test.go" {
			continue
		}
		path := filepath.Join(root, string(name))
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("read %s: %v", name, err)
		}
		scanner := bufio.NewScanner(bytes.NewReader(data))
		scanner.Buffer(make([]byte, 0, 64*1024), len(data)+1)
		line := 0
		for scanner.Scan() {
			line++
			if isDeadMinioRef(scanner.Text()) {
				t.Errorf("%s:%d references a MinIO image that is no longer publicly pullable; use kafscale-minio:local built from test/e2e/minio.Dockerfile", name, line)
			}
		}
		if err := scanner.Err(); err != nil {
			t.Fatalf("scan %s: %v", name, err)
		}
	}
}

func TestMakeBuildsMinioOnlyForDefaultImage(t *testing.T) {
	if _, err := exec.LookPath("make"); err != nil {
		t.Skip("make not found on PATH")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		target string
		image  string
		build  bool
	}{
		{"start-minio", "", true},
		{"test-operator", "", true},
		{"demo-platform-bootstrap", "", true},
		{"start-minio", "example.com/minio:custom", false},
		{"test-operator", "example.com/minio:custom", false},
		{"demo-platform-bootstrap", "example.com/minio:custom", false},
	}
	for _, c := range cases {
		args := []string{"-C", root, "--dry-run", "--no-print-directory", c.target}
		if c.image != "" {
			args = append(args, "MINIO_IMAGE="+c.image)
		}
		out, err := exec.Command("make", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("make %s: %v\n%s", c.target, err, out)
		}
		built := bytes.Contains(out, []byte("-f test/e2e/minio.Dockerfile"))
		if built != c.build {
			t.Errorf("make %s MINIO_IMAGE=%q: builds MinIO = %v, want %v", c.target, c.image, built, c.build)
		}
	}
}
