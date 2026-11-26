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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func TestCLIInstallAndCredentialsDocumented(t *testing.T) {
	ops := readRepoDoc(t, "docs/operations.md")
	for _, want := range []string{
		"Install a [native CLI release archive](cli-install.md) for Linux or macOS, amd64",
		"The binary runs directly on your host, without Docker or Go.",
		"go build -o bin/kafscale-cli ./cmd/kafscale-cli",
		"### Installing kafscale-cli",
		"It defaults to `KAFSCALE_NAMESPACE`, then `KAFSCALE_S3_NAMESPACE`, then `default`.",
		"- S3 credentials are `KAFSCALE_S3_ACCESS_KEY` and `KAFSCALE_S3_SECRET_KEY`, optionally `KAFSCALE_S3_SESSION_TOKEN`. If either is unset, the AWS SDK default credential chain is used: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`; the shared config and credentials files with `AWS_PROFILE`; web identity with `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` (EKS IRSA); and ECS or EC2 instance role credentials. Without credentials, restore fails at its first S3 call, listing the source segments, with an error that starts `list objects <prefix>:` and includes `get credentials`, and creates no target.",
		"- Restore fails when there are no segments for the selected partitions under `<namespace>/<topic>/`, or `<namespace>/<topic>@<generation>/` for a re-created topic, or when no record in those segments is at or before `--to`. The command exits with a non-zero status and names the prefix it searched. If the first listing already shows no segments, the target topic is not created; otherwise the target is rolled back. Set `--namespace`, `KAFSCALE_NAMESPACE` or `KAFSCALE_S3_NAMESPACE` to the cluster namespace (`KAFSCALE_NAMESPACE` wins over `KAFSCALE_S3_NAMESPACE`).",
		"- `kafscale-cli prune` needs the same S3 and etcd environment as restore. A dry run needs them too, because it calls CheckBucket, lists etcd and lists S3. It talks to etcd and S3 directly and does not go through the brokers, so access to prune is controlled by the etcd and S3 credentials. `KAFSCALE_ALLOW_ADMIN_APIS` and Kafka ACLs do not apply.",
		"- If `kafscale-cli restore` creates the target topic and a later step fails (`UpdateTopicConfig`, `TopicGeneration` or the copy), restore rolls back by retiring that target. That writes a `/kafscale/deleted-topics` record with no objects, or only those copied before the failure, which `kafscale-cli prune` lists. If the rollback itself fails, restore prints `warning: failed to roll back target topic <name>: ...` to stderr, the target stays, and no deleted-topics record is written. `prune --topic <target> --force` removes the record, and every other deleted incarnation of that name.",
		"Every environment variable `restore` reads is listed in [kafscale-cli](#kafscale-cli).",
		"Every environment variable `prune` reads is listed in [kafscale-cli](#kafscale-cli):",
		"### kafscale-cli",
		"AWS_SESSION_TOKEN",
		"AWS_PROFILE",
	} {
		if !strings.Contains(ops, want) {
			t.Errorf("docs/operations.md is missing exact sentence:\n%s", want)
		}
	}

	sr := readRepoDoc(t, "docs/schema-registry.md")
	for _, want := range []string{
		"go build -o bin/kafscale-cli ./cmd/kafscale-cli",
		"operations.md#installing-kafscale-cli",
		"`--auth` and `--to-auth` values are visible to other local users in the process list, so prefer the env vars.",
		"Every environment variable `schema import` reads is listed in [kafscale-cli](operations.md#kafscale-cli).",
		"read -rs KAFSCALE_SCHEMA_IMPORT_AUTH",
		"export KAFSCALE_SCHEMA_IMPORT_AUTH",
		"read -rs KAFSCALE_SCHEMA_REGISTRY_AUTH",
		"export KAFSCALE_SCHEMA_REGISTRY_AUTH",
		"kafscale-cli schema import --from https://source-registry:8081 --to http://kafscale-schema-registry:8081",
		"Optional flags are `--subjects '<regex>'`, `--dry-run` and `--force`.",
	} {
		if !strings.Contains(sr, want) {
			t.Errorf("docs/schema-registry.md is missing exact sentence:\n%s", want)
		}
	}
}

func TestCLIEnvIndexDocumentsEveryCLIEnv(t *testing.T) {
	ops := readRepoDoc(t, "docs/operations.md")
	section := cliEnvIndexSection(t, ops)
	root := filepath.Join(repoRoot(t), "cmd", "kafscale-cli")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	seen := map[string]struct{}{}
	ident := regexp.MustCompile(`(?:KAFSCALE|AWS)_[A-Z0-9_]+`)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(root, entry.Name())
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			for _, name := range ident.FindAllString(lit.Value, -1) {
				seen[name] = struct{}{}
			}
			return true
		})
	}
	if len(seen) == 0 {
		t.Fatal("no KAFSCALE_* or AWS_* string literals found in cmd/kafscale-cli")
	}
	var missing []string
	for name := range seen {
		if !strings.Contains(section, name) {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("docs/operations.md ### kafscale-cli is missing %s", strings.Join(missing, ", "))
	}
}

func cliEnvIndexSection(t *testing.T, ops string) string {
	t.Helper()
	start := strings.Index(ops, "### kafscale-cli\n")
	if start < 0 {
		t.Fatal("docs/operations.md has no ### kafscale-cli section")
	}
	rest := ops[start+len("### kafscale-cli\n"):]
	if end := strings.Index(rest, "\n### "); end >= 0 {
		return rest[:end]
	}
	return rest
}

func cliInstallDownloadBlock(t *testing.T) string {
	t.Helper()
	doc := readRepoDoc(t, "docs/cli-install.md")
	for _, block := range regexp.MustCompile("(?s)```bash\n(.*?)```").FindAllStringSubmatch(doc, -1) {
		if strings.Contains(block[1], "releases/download") {
			return block[1]
		}
	}
	t.Fatal("docs/cli-install.md has no release download block")
	return ""
}

func TestCLIInstallDownloadBlockIsPastable(t *testing.T) {
	block := cliInstallDownloadBlock(t)
	if strings.Contains(block, "X.Y.Z") {
		t.Fatal("download block still contains a version placeholder")
	}
	for _, want := range []string{`VERSION="${VERSION:-v2.1.0}"`, "uname -s", "uname -m", "sha256sum -c -", "shasum -a 256 -c -", `&& tar -xzf "$ARCHIVE"`} {
		if !strings.Contains(block, want) {
			t.Errorf("download block is missing %q", want)
		}
	}
	syntax := exec.Command("bash", "-n")
	syntax.Stdin = strings.NewReader(block)
	if out, err := syntax.CombinedOutput(); err != nil {
		t.Fatalf("download block is not valid bash: %v %s", err, out)
	}
}

func TestCLIInstallDownloadBlockAgainstReleaseAssets(t *testing.T) {
	assets := os.Getenv("KAFSCALE_RELEASE_ASSET_DIR")
	if assets == "" {
		t.Skip("set KAFSCALE_RELEASE_ASSET_DIR to built release assets")
	}
	tag := os.Getenv("RELEASE_TAG")
	if tag == "" {
		t.Fatal("RELEASE_TAG required")
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skipf("no CLI archive for %s", runtime.GOOS)
	}
	bin := t.TempDir()
	curl := "#!/bin/sh\nfor arg in \"$@\"; do url=\"$arg\"; done\ncase \"$url\" in\n https://github.com/KafScale/platform/releases/download/" + tag + "/*) cp \"$ASSETS/${url##*/}\" . ;;\n *) echo \"unexpected URL $url\" >&2; exit 22 ;;\nesac\n"
	if err := os.WriteFile(filepath.Join(bin, "curl"), []byte(curl), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "-e")
	cmd.Dir = t.TempDir()
	cmd.Stdin = strings.NewReader(cliInstallDownloadBlock(t))
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "ASSETS="+assets, "VERSION="+tag)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("download block failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), ": OK") || !strings.Contains(string(out), "kafscale-cli "+tag) {
		t.Fatalf("download block did not verify and run the CLI:\n%s", out)
	}
}
