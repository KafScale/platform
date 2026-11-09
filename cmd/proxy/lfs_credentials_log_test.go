// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"log/slog"
	"strings"
	"testing"
)

func TestInitLFSModuleLogsS3CredentialSource(t *testing.T) {
	cases := []struct {
		name       string
		accessKey  string
		secretKey  string
		wantSource string
	}{
		{name: "static keys", accessKey: "test-access", secretKey: "test-secret", wantSource: "static keys"},
		{name: "access key only", accessKey: "test-access", wantSource: "default AWS credential chain"},
		{name: "no keys", wantSource: "default AWS credential chain"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KAFSCALE_LFS_PROXY_S3_BUCKET", "bucket")
			t.Setenv("KAFSCALE_LFS_PROXY_S3_REGION", "us-east-1")
			t.Setenv("KAFSCALE_LFS_PROXY_S3_ENDPOINT", "http://127.0.0.1:1")
			t.Setenv("KAFSCALE_LFS_PROXY_S3_ACCESS_KEY", tc.accessKey)
			t.Setenv("KAFSCALE_LFS_PROXY_S3_SECRET_KEY", tc.secretKey)
			t.Setenv("KAFSCALE_LFS_TRACKER_ENABLED", "false")
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if _, err := initLFSModule(ctx, logger); err != nil {
				t.Fatalf("initLFSModule: %v", err)
			}
			var line string
			for _, l := range strings.Split(logs.String(), "\n") {
				if strings.Contains(l, "lfs s3 credentials") {
					line = l
				}
			}
			if !strings.Contains(line, tc.wantSource) {
				t.Fatalf("credential source log = %q, want %q in:\n%s", line, tc.wantSource, logs.String())
			}
			if strings.Contains(logs.String(), "test-access") || strings.Contains(logs.String(), "test-secret") {
				t.Fatalf("log contains credential values:\n%s", logs.String())
			}
		})
	}
}
