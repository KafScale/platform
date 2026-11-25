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
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"debug/elf"
	"debug/macho"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// Run against actual build outputs in the native-release CI job, rather than
// rebuilding four platforms during every unit-test invocation.
func TestReleaseAssets(t *testing.T) {
	dir := os.Getenv("KAFSCALE_RELEASE_ASSET_DIR")
	if dir == "" {
		t.Skip("set KAFSCALE_RELEASE_ASSET_DIR to built release assets")
	}
	tag := os.Getenv("RELEASE_TAG")
	if tag == "" {
		t.Fatal("RELEASE_TAG required")
	}
	expected := map[string]bool{"docker-compose.yaml": true}
	for _, osName := range []string{"linux", "darwin"} {
		for _, arch := range []string{"amd64", "arm64"} {
			name := fmt.Sprintf("kafscale-cli_%s_%s_%s.tar.gz", tag, osName, arch)
			expected[name] = true
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			gz, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			reader := tar.NewReader(gz)
			entries := map[string]bool{}
			for {
				entry, err := reader.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if entries[entry.Name] || (entry.Name != "kafscale-cli" && entry.Name != "LICENSE" && entry.Name != "NOTICE") || entry.Typeflag != tar.TypeReg {
					t.Fatalf("unexpected archive entry: %+v", entry)
				}
				entries[entry.Name] = true
				body, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				if len(body) == 0 {
					t.Fatalf("empty %s", entry.Name)
				}
				if entry.Name != "kafscale-cli" {
					continue
				}
				if entry.Mode&0111 == 0 {
					t.Fatal("CLI is not executable")
				}
				if osName == "linux" {
					binary, err := elf.NewFile(bytes.NewReader(body))
					if err != nil {
						t.Fatal(err)
					}
					want := elf.EM_X86_64
					if arch == "arm64" {
						want = elf.EM_AARCH64
					}
					if binary.Machine != want {
						t.Fatalf("%s has machine %s", name, binary.Machine)
					}
					for _, program := range binary.Progs {
						if program.Type == elf.PT_INTERP {
							t.Fatal("Linux CLI requires a dynamic loader")
						}
					}
					binary.Close()
				} else {
					binary, err := macho.NewFile(bytes.NewReader(body))
					if err != nil {
						t.Fatal(err)
					}
					want := macho.CpuAmd64
					if arch == "arm64" {
						want = macho.CpuArm64
					}
					if binary.Cpu != want {
						t.Fatalf("%s has CPU %s", name, binary.Cpu)
					}
					binary.Close()
				}
				if osName == runtime.GOOS && arch == runtime.GOARCH {
					executable := filepath.Join(t.TempDir(), "kafscale-cli")
					if err := os.WriteFile(executable, body, 0755); err != nil {
						t.Fatal(err)
					}
					output, err := exec.Command(executable, "--version").CombinedOutput()
					if err != nil || strings.TrimSpace(string(output)) != "kafscale-cli "+tag {
						t.Fatalf("version: %s %v", output, err)
					}
					output, err = exec.Command(executable, "--help").CombinedOutput()
					if err != nil || !strings.Contains(string(output), "restore") {
						t.Fatalf("help: %s %v", output, err)
					}
				}
			}
			gz.Close()
			if len(entries) != 3 {
				t.Fatalf("missing archive files in %s: %v", name, entries)
			}
		}
	}
	sums, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(sums)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || !expected[fields[1]] {
			t.Fatalf("unexpected checksum row %q", line)
		}
		body, err := os.ReadFile(filepath.Join(dir, fields[1]))
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(body)) != fields[0] {
			t.Fatalf("checksum mismatch for %s", fields[1])
		}
		delete(expected, fields[1])
	}
	if len(expected) != 0 {
		t.Fatalf("missing checksums: %v", expected)
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 6 {
		t.Fatalf("unexpected release contents: %v %v", files, err)
	}
	compose, err := os.ReadFile(filepath.Join(dir, "docker-compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Services map[string]struct {
			Image       string
			Build       any
			Environment map[string]string
		}
	}
	if err := yaml.Unmarshal(compose, &config); err != nil {
		t.Fatal(err)
	}
	if len(config.Services) != 4 {
		t.Fatalf("release services: %+v", config.Services)
	}
	for _, name := range []string{"etcd", "broker", "proxy", "console"} {
		service, ok := config.Services[name]
		if !ok || service.Build != nil {
			t.Fatalf("%s must be a pullable service", name)
		}
		if name != "etcd" && !strings.HasSuffix(service.Image, ":"+tag) {
			t.Fatalf("%s is not pinned to %s", name, tag)
		}
	}
	if !strings.Contains(config.Services["broker"].Environment["KAFSCALE_S3_BUCKET"], ":?") {
		t.Fatal("external bucket must be required")
	}
}
