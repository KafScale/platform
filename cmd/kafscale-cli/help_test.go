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

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestCommandHelpPrintsFlags(t *testing.T) {
	tests := []struct {
		args   []string
		want   []string
		unwant []string
	}{
		{
			args: []string{"--help"},
			want: []string{
				"[--namespace <ns>]",
				"[--target-namespace <ns>]",
				"[--partitions <list>]",
				"[--to-auth user:password]",
				"KAFSCALE_NAMESPACE",
				"KAFSCALE_S3_NAMESPACE",
				"KAFSCALE_S3_ACCESS_KEY",
				"AWS_ACCESS_KEY_ID",
				"AWS_SESSION_TOKEN",
				"AWS_PROFILE",
				"KAFSCALE_S3_KMS_ARN",
				"KAFSCALE_S3_CONCURRENCY (default 16)",
				"KAFSCALE_SCHEMA_REGISTRY_URL",
				"KAFSCALE_SCHEMA_IMPORT_AUTH",
				"KAFSCALE_SCHEMA_REGISTRY_AUTH",
				"If either is unset",
				"restore --namespace defaults to KAFSCALE_NAMESPACE, then KAFSCALE_S3_NAMESPACE, then default",
				"--auth and --to-auth values are visible to other local users in the process list, so prefer the env vars.",
			},
			unwant: []string{"Build it from this repository"},
		},
		{
			args: []string{"restore", "--help"},
			want: []string{
				restoreUsage,
				"  -namespace string",
				"  -target-namespace string",
				"  -partitions string",
				"  -topic string",
				"  -target-topic string",
				"  -to string",
				"KAFSCALE_S3_NAMESPACE",
			},
		},
		{
			args: []string{"prune", "--help"},
			want: []string{pruneUsage, "  -topic string", "  -older-than string", "  -force"},
		},
		{
			args: []string{"schema", "import", "--help"},
			want: []string{schemaImportUsage, "  -from string", "  -to string", "  -auth string", "  -to-auth string", "  -subjects string", "  -dry-run", "  -force"},
		},
		{
			args: []string{"schema", "--help"},
			want: []string{schemaImportUsage, "--to-auth user:password", "--force"},
		},
		{
			args: []string{"schema", "-h"},
			want: []string{schemaImportUsage},
		},
		{
			args: []string{"schema", "help"},
			want: []string{schemaImportUsage},
		},
	}
	for _, tc := range tests {
		var stdout, stderr bytes.Buffer
		err := run(context.Background(), tc.args, &stdout, &stderr)
		if err != nil {
			t.Errorf("%v returned %v", tc.args, err)
			continue
		}
		if stderr.Len() != 0 {
			t.Errorf("%v wrote to stderr: %s", tc.args, stderr.String())
		}
		out := stdout.String()
		if strings.Contains(out, "flag: help requested") {
			t.Errorf("%v still prints %q", tc.args, "flag: help requested")
		}
		for _, want := range tc.want {
			if !strings.Contains(out, want) {
				t.Errorf("%v output does not contain %q:\n%s", tc.args, want, out)
			}
		}
		for _, unwant := range tc.unwant {
			if strings.Contains(out, unwant) {
				t.Errorf("%v output still contains %q:\n%s", tc.args, unwant, out)
			}
		}
		var usage string
		var flagLine string
		switch {
		case len(tc.args) >= 1 && tc.args[0] == "restore":
			usage, flagLine = restoreUsage, "  -topic string"
		case len(tc.args) >= 1 && tc.args[0] == "prune":
			usage, flagLine = pruneUsage, "  -topic string"
		case len(tc.args) >= 2 && tc.args[0] == "schema" && tc.args[1] == "import":
			usage, flagLine = schemaImportUsage, "  -from string"
		}
		if usage != "" {
			usageIdx := strings.Index(out, usage)
			flagIdx := strings.Index(out, flagLine)
			if usageIdx < 0 || flagIdx < 0 || usageIdx > flagIdx {
				t.Errorf("%v did not print usage before the flag list:\n%s", tc.args, out)
			}
		}
	}
}

func TestSchemaImportHelpHidesAuthEnv(t *testing.T) {
	const importSentinel = "SENTINEL_IMPORT_AUTH_VALUE"
	const registrySentinel = "SENTINEL_REGISTRY_AUTH_VALUE"
	t.Setenv("KAFSCALE_SCHEMA_IMPORT_AUTH", importSentinel)
	t.Setenv("KAFSCALE_SCHEMA_REGISTRY_AUTH", registrySentinel)
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"schema", "import", "--help"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("schema import --help returned %v", err)
	}
	combined := stdout.String() + stderr.String()
	for _, sentinel := range []string{importSentinel, registrySentinel} {
		if strings.Contains(combined, sentinel) {
			t.Errorf("schema import --help leaked %q:\nstdout=%s\nstderr=%s", sentinel, stdout.String(), stderr.String())
		}
	}
}
