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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestDocumentedS3SecretCommandsKeepKeysOutOfArgv(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required")
	}
	for _, tc := range []struct {
		doc, marker string
		rotation    bool
	}{
		{"docs/quickstart.md", "credentials_dir=$(mktemp -d)", false},
		{"docs/operations.md", "secret_name='kafscale-s3'", true},
		{"docs/schema-registry.md", "secret_name='kafscale-s3'", true},
	} {
		for _, fail := range []bool{false, true} {
			t.Run(tc.doc+"/failure="+fmt.Sprint(fail), func(t *testing.T) {
				script := opsBashExample(t, readRepoDoc(t, tc.doc), tc.marker)
				capture := t.TempDir()
				t.Setenv("DOC_CAPTURE", capture)
				t.Setenv("DOC_FAIL", fmt.Sprint(fail))
				prefix := `mktemp() { local dir; dir=$(command mktemp "$@") || return; printf '%s' "$dir" > "$DOC_CAPTURE/temp-path"; printf '%s\n' "$dir"; }
record_files() {
  for key in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY; do
    cp -p "$credentials_dir/$key" "$DOC_CAPTURE/$key"
  done
}
jq() { printf '%s\0' "$@" >> "$DOC_CAPTURE/jq-args"; record_files; command jq "$@"; }
kubectl() {
  printf '%s\0' "$@" >> "$DOC_CAPTURE/kubectl-args"
  if [[ " $* " == *" create secret "* ]]; then record_files; fi
  if [[ " $* " == *" patch secret "* ]]; then cat > "$DOC_CAPTURE/patch.json"; fi
  if [[ "$DOC_FAIL" == true ]]; then return 9; fi
}
`
				access, secret := `  public "identifier"\path  `, `  private "secret"\path $not_expanded  `
				cmd := exec.Command("bash", "-euc", prefix+script)
				cmd.Stdin = strings.NewReader(access + "\n" + secret + "\n")
				output, err := cmd.CombinedOutput()
				if (err != nil) != fail {
					t.Fatalf("command failure=%v want %v: %s", err, fail, output)
				}
				for key, want := range map[string]string{"AWS_ACCESS_KEY_ID": access, "AWS_SECRET_ACCESS_KEY": secret} {
					body, err := os.ReadFile(filepath.Join(capture, key))
					if err != nil {
						t.Fatal(err)
					}
					if string(body) != want {
						t.Fatalf("%s changed during input", key)
					}
					info, err := os.Stat(filepath.Join(capture, key))
					if err != nil || info.Mode().Perm() != 0o600 {
						t.Fatalf("%s permissions: %v %v", key, info, err)
					}
				}
				temporary, err := os.ReadFile(filepath.Join(capture, "temp-path"))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(string(temporary)); !os.IsNotExist(err) {
					t.Fatalf("credential temp directory not removed: %v", err)
				}
				for _, name := range []string{"kubectl-args", "jq-args"} {
					args, _ := os.ReadFile(filepath.Join(capture, name))
					if strings.Contains(string(args), access) || strings.Contains(string(args), secret) {
						t.Fatal("credential in process arguments")
					}
				}
				if strings.Contains(string(output), secret) {
					t.Fatal("secret echoed")
				}
				if tc.rotation {
					body, err := os.ReadFile(filepath.Join(capture, "patch.json"))
					if err != nil {
						t.Fatal(err)
					}
					var patch struct{ Data map[string]string }
					if err := json.Unmarshal(body, &patch); err != nil {
						t.Fatal(err)
					}
					if len(patch.Data) != 2 {
						t.Fatalf("rotation replaced unrelated keys: %v", patch.Data)
					}
					for key, want := range map[string]string{"AWS_ACCESS_KEY_ID": access, "AWS_SECRET_ACCESS_KEY": secret} {
						value, err := base64.StdEncoding.DecodeString(patch.Data[key])
						if err != nil || string(value) != want {
							t.Fatalf("invalid patch key %s", key)
						}
					}
					args := opsCommandArgs(t, filepath.Join(capture, "kubectl-args"))
					if !slices.Contains(args, "--type=merge") || !slices.Contains(args, "--patch-file=/dev/stdin") {
						t.Fatalf("rotation must merge via stdin: %v", args)
					}
				}
			})
		}
	}
}

func TestDocumentedMCPSecretUsesStdin(t *testing.T) {
	script := opsBashExample(t, readRepoDoc(t, "docs/ops-mcp.md"), "--from-file=mcp-token=/dev/stdin")
	for _, token := range []string{`private-token-"quotes"-\slash`, ""} {
		t.Run("empty="+fmt.Sprint(token == ""), func(t *testing.T) {
			capture := t.TempDir()
			t.Setenv("DOC_CAPTURE", capture)
			prefix := `kubectl() { printf '%s\0' "$@" > "$DOC_CAPTURE/args"; cat > "$DOC_CAPTURE/token"; }
`
			cmd := exec.Command("bash", "-euc", prefix+script)
			cmd.Stdin = strings.NewReader(token + "\n")
			output, err := cmd.CombinedOutput()
			if token == "" {
				if err == nil {
					t.Fatal("empty token accepted")
				}
				if _, err := os.Stat(filepath.Join(capture, "token")); !os.IsNotExist(err) {
					t.Fatal("empty token reached kubectl")
				}
				return
			}
			if err != nil {
				t.Fatalf("token command: %v %s", err, output)
			}
			body, err := os.ReadFile(filepath.Join(capture, "token"))
			if err != nil || string(body) != token {
				t.Fatal("token changed")
			}
			args := opsCommandArgs(t, filepath.Join(capture, "args"))
			if slices.Contains(args, token) || strings.Contains(string(output), token) {
				t.Fatal("token exposed")
			}
			if !slices.Contains(args, "my-mcp-token") || !slices.Contains(args, "--from-file=mcp-token=/dev/stdin") {
				t.Fatalf("wrong Secret reference: %v", args)
			}
		})
	}
}

func TestQuickstartClientAndUpgradeCommands(t *testing.T) {
	text := readRepoDoc(t, "docs/quickstart.md")
	capture := t.TempDir()
	t.Setenv("DOC_CAPTURE", capture)
	prefix := `kubectl() {
  printf '%s\0' "$@" >> "$DOC_CAPTURE/args"
  if [[ " $* " == *" exec -i "* ]]; then cat > "$DOC_CAPTURE/input"; fi
}
helm() { printf '%s\0' "$@" > "$DOC_CAPTURE/helm"; }
`
	client := opsBashExample(t, text, "run kcat-client")
	upgrade := opsBashExample(t, text, "--reuse-values")
	if output, err := exec.Command("bash", "-euc", prefix+client+"\n"+upgrade).CombinedOutput(); err != nil {
		t.Fatalf("documented client/upgrade: %v %s", err, output)
	}
	args := opsCommandArgs(t, filepath.Join(capture, "args"))
	if slices.Contains(args, "--rm") || slices.Contains(args, "-it") || !slices.Contains(args, "exec") || !slices.Contains(args, "--for=condition=Ready") {
		t.Fatalf("client lifecycle arguments: %v", args)
	}
	body, err := os.ReadFile(filepath.Join(capture, "input"))
	if err != nil || string(body) != "hello\n" {
		t.Fatal("producer input changed")
	}
	helm := opsCommandArgs(t, filepath.Join(capture, "helm"))
	if !slices.Contains(helm, "--reuse-values") {
		t.Fatal("upgrade loses install values")
	}
	for _, arg := range helm {
		if strings.HasPrefix(arg, "operator.etcdEndpoints=") {
			t.Fatal("upgrade resets external etcd")
		}
	}
	for _, want := range []string{"operations.md#scaling-brokers", "metrics server", "resources"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing HPA requirement %s", want)
		}
	}
}

func TestSchemaImportPolicyAndCLIConnectivityDocumented(t *testing.T) {
	schema := readRepoDoc(t, "docs/schema-registry.md")
	if strings.Contains(schema, "bucket: kafscale-data") {
		t.Fatal("schema example uses a bucket absent from quickstart")
	}
	for _, want := range []string{"bucket: kafscale-demo", "Apicurio import", "including `NONE`", "/config/{subject}", `--data '{"compatibility":"BACKWARD"}'`} {
		if !strings.Contains(schema, want) {
			t.Fatalf("missing import guidance %s", want)
		}
	}
	ops := readRepoDoc(t, "docs/operations.md")
	for _, want := range []string{"svc/demo-etcd-client 2379:2379", "svc/minio 9000:9000", "read -rs KAFSCALE_S3_SECRET_KEY", "#cli-access-from-a-laptop"} {
		if !strings.Contains(ops, want) {
			t.Fatalf("missing CLI guidance %s", want)
		}
	}
}
