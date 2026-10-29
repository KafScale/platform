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
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var envKey = regexp.MustCompile(`(?m)^\s+(KAFSCALE_[A-Z0-9_]+):`)

func TestComposeEnvKeysAreRead(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	compose, err := os.ReadFile(filepath.Join(root, "deploy/docker-compose/docker-compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	keys := envKey.FindAllStringSubmatch(string(compose), -1)
	if len(keys) == 0 {
		t.Fatal("no KAFSCALE_ env keys found in docker-compose.yaml")
	}
	var src strings.Builder
	for _, dir := range []string{"cmd", "internal", "pkg", "addons"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			src.Write(b)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	code := src.String()
	for _, k := range keys {
		if !strings.Contains(code, `"`+k[1]+`"`) {
			t.Errorf("docker-compose.yaml sets %s but no Go code reads it", k[1])
		}
	}
}
