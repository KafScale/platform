// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package operator

import (
	"os/exec"
	"strings"
	"testing"
)

func TestSnapshotCredentialPreludeMatchesGoSelection(t *testing.T) {
	const nbsp = "\u00a0"
	const emSpace = "\u2003"
	cases := []struct {
		name string
		env  map[string]string
	}{
		{name: "kafscale pair padded with no-break spaces", env: map[string]string{s3AccessKeyEnv: nbsp + "ks-key" + nbsp, s3SecretKeyEnv: nbsp + "ks-secret" + nbsp, s3SessionTokenEnv: nbsp + "ks-token" + nbsp}},
		{name: "no-break space kafscale keys next to an aws pair", env: map[string]string{s3AccessKeyEnv: nbsp, s3SecretKeyEnv: nbsp, awsAccessKeyEnv: "aws-key", awsSecretKeyEnv: "aws-secret", awsSessionTokenEnv: "aws-token"}},
		{name: "aws pair padded with em spaces", env: map[string]string{awsAccessKeyEnv: emSpace + "aws-key" + emSpace, awsSecretKeyEnv: emSpace + "aws-secret", awsSessionTokenEnv: emSpace}},
		{name: "ascii padded kafscale pair", env: map[string]string{s3AccessKeyEnv: " \t\v\fks-key\r\n", s3SecretKeyEnv: "\tks-secret ", s3SessionTokenEnv: " \f", awsAccessKeyEnv: "aws-key", awsSecretKeyEnv: "aws-secret", awsSessionTokenEnv: "aws-token"}},
		{name: "ascii blank kafscale pair next to a padded aws pair", env: map[string]string{s3AccessKeyEnv: " \v", s3SecretKeyEnv: "\f\t", awsAccessKeyEnv: "\naws-key ", awsSecretKeyEnv: " aws-secret\r", awsSessionTokenEnv: "\vaws-token\f"}},
	}
	shells := 0
	for _, shell := range []string{"bash", "dash"} {
		path, err := exec.LookPath(shell)
		if err != nil {
			t.Logf("%s not found, skipping it", shell)
			continue
		}
		shells++
		for _, tc := range cases {
			t.Run(shell+"/"+tc.name, func(t *testing.T) {
				access, secret, token := selectSnapshotS3Credentials(func(key string) (string, bool) {
					val, ok := tc.env[key]
					return val, ok
				})
				want := strings.Join([]string{access, secret, token}, "|")
				script := snapshotCredentialsSelectScript + "printf '%s|%s|%s' \"${AWS_ACCESS_KEY_ID-}\" \"${AWS_SECRET_ACCESS_KEY-}\" \"${AWS_SESSION_TOKEN-}\"\n"
				cmd := exec.Command(path, "-c", script)
				cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8", "LC_ALL=C.UTF-8"}
				for k, v := range tc.env {
					cmd.Env = append(cmd.Env, k+"="+v)
				}
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("prelude failed: %v\n%s", err, out)
				}
				if got := string(out); got != want {
					t.Fatalf("prelude resolved %q, Go resolved %q", got, want)
				}
			})
		}
	}
	if shells == 0 {
		t.Skip("neither bash nor dash is available")
	}
}
