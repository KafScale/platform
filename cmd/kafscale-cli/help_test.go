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
	cases := []struct {
		args []string
		want []string
	}{
		{
			args: []string{"--help"},
			want: []string{"--namespace", "--target-namespace", "--partitions", "KAFSCALE_NAMESPACE", "KAFSCALE_S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"},
		},
		{
			args: []string{"restore", "--help"},
			want: []string{"-namespace", "-target-namespace", "-partitions", "-topic", "-target-topic", "-to"},
		},
		{
			args: []string{"prune", "--help"},
			want: []string{"-topic", "-older-than", "-force"},
		},
		{
			args: []string{"schema", "import", "--help"},
			want: []string{"-from", "-to", "-auth", "-subjects", "-dry-run", "-force"},
		},
	}
	for _, tc := range cases {
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
	}
}
