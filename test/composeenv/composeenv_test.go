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

package composeenv

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	serviceLine = regexp.MustCompile(`^  ([a-z0-9-]+):\s*$`)
	envKey      = regexp.MustCompile(`^\s+-?\s*(KAFSCALE_[A-Z0-9_]+)[:=]`)
	composeTag  = regexp.MustCompile(`\$\{TAG:-([^}]+)\}`)
	makeTag     = regexp.MustCompile(`(?m)^TAG \?= (\S+)$`)
	readmeTag   = regexp.MustCompile("TAG=(v[0-9][^ \n`]*)|`(v[0-9][^`]*)` images|at tag `(v[0-9][^`]*)`|kafscale-broker:(v[0-9][^ \n`]*)")
)

var serviceCmd = map[string]string{
	"broker":  "./cmd/broker",
	"proxy":   "./cmd/proxy",
	"console": "./cmd/console",
}

func repoRoot(t *testing.T) string {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func serviceEnvKeys(compose string) map[string][]string {
	keys := map[string][]string{}
	service := ""
	for _, line := range strings.Split(compose, "\n") {
		if m := serviceLine.FindStringSubmatch(line); m != nil {
			service = m[1]
			continue
		}
		if m := envKey.FindStringSubmatch(line); m != nil && service != "" {
			keys[service] = append(keys[service], m[1])
		}
	}
	return keys
}

func unreadKeys(keys map[string][]string, sources map[string]string) []string {
	var missing []string
	for service, list := range keys {
		code, ok := sources[service]
		for _, k := range list {
			if !ok || !strings.Contains(code, `"`+k+`"`) {
				missing = append(missing, service+": "+k)
			}
		}
	}
	return missing
}

func serviceSource(t *testing.T, root, pkg string) string {
	cmd := exec.Command("go", "list", "-deps", "-f", `{{if .Module}}{{if eq .Module.Path "github.com/KafScale/platform"}}{{range .GoFiles}}{{$.Dir}}/{{.}}{{"\n"}}{{end}}{{end}}{{end}}`, pkg)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %s: %v", pkg, err)
	}
	var src strings.Builder
	for _, file := range strings.Fields(string(out)) {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		src.Write(b)
	}
	return src.String()
}

func TestComposeEnvKeysAreReadByTheirService(t *testing.T) {
	root := repoRoot(t)
	compose, err := os.ReadFile(filepath.Join(root, "deploy/docker-compose/docker-compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	keys := serviceEnvKeys(string(compose))
	for service := range serviceCmd {
		if len(keys[service]) == 0 {
			t.Fatalf("no KAFSCALE_ env keys found for %s", service)
		}
	}
	sources := map[string]string{}
	for service, pkg := range serviceCmd {
		sources[service] = serviceSource(t, root, pkg)
	}
	for service := range keys {
		if _, ok := serviceCmd[service]; !ok {
			t.Errorf("service %s sets KAFSCALE_ env keys but has no source mapping", service)
		}
	}
	for _, m := range unreadKeys(keys, sources) {
		t.Errorf("docker-compose.yaml sets %s but that service does not read it", m)
	}
}

func TestKeyOnWrongServiceFails(t *testing.T) {
	compose := "services:\n  broker:\n    environment:\n      - KAFSCALE_PROXY_ADDR=:9092\n  proxy:\n    environment:\n      KAFSCALE_PROXY_ADDR: \":9092\"\n"
	sources := map[string]string{
		"broker": `os.Getenv("KAFSCALE_BROKER_HOST")`,
		"proxy":  `os.Getenv("KAFSCALE_PROXY_ADDR")`,
	}
	got := unreadKeys(serviceEnvKeys(compose), sources)
	if len(got) != 1 || got[0] != "broker: KAFSCALE_PROXY_ADDR" {
		t.Fatalf("unreadKeys = %v, want [broker: KAFSCALE_PROXY_ADDR]", got)
	}
}

func TestDefaultTagMatchesEverywhere(t *testing.T) {
	root := repoRoot(t)
	files := map[string]*regexp.Regexp{
		"deploy/docker-compose/docker-compose.yaml": composeTag,
		"deploy/docker-compose/Makefile":            makeTag,
		"deploy/docker-compose/README.md":           readmeTag,
	}
	tags := map[string][]string{}
	for file, re := range files {
		b, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			t.Fatal(err)
		}
		matches := re.FindAllStringSubmatch(string(b), -1)
		if len(matches) == 0 {
			t.Fatalf("no default tag found in %s", file)
		}
		for _, m := range matches {
			for _, g := range m[1:] {
				if g != "" {
					tags[g] = append(tags[g], file)
				}
			}
		}
	}
	if len(tags) != 1 {
		t.Fatalf("default TAG differs between files: %v", tags)
	}
}
