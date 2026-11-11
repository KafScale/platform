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
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestInitLFSModuleLogsS3CredentialSource(t *testing.T) {
	cases := []struct {
		name             string
		accessKey        string
		secretKey        string
		wantSource       string
		sessionToken     string
		wantTokenField   bool
		wantSessionToken bool
	}{
		{name: "permanent static keys", accessKey: "test-access", secretKey: "test-secret", wantSource: "static keys", wantTokenField: true},
		{name: "blank static token", accessKey: "test-access", secretKey: "test-secret", sessionToken: "  ", wantSource: "static keys", wantTokenField: true},
		{name: "temporary static keys", accessKey: "test-access", secretKey: "test-secret", sessionToken: "test-session-token", wantSource: "static keys", wantTokenField: true, wantSessionToken: true},
		{name: "access key only with unused token", accessKey: "test-access", sessionToken: "test-session-token", wantSource: "default AWS credential chain"},
		{name: "no keys", wantSource: "default AWS credential chain"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KAFSCALE_LFS_PROXY_S3_BUCKET", "bucket")
			t.Setenv("KAFSCALE_LFS_PROXY_S3_REGION", "us-east-1")
			t.Setenv("KAFSCALE_LFS_PROXY_S3_ENDPOINT", "http://127.0.0.1:1")
			t.Setenv("KAFSCALE_LFS_PROXY_S3_ACCESS_KEY", tc.accessKey)
			t.Setenv("KAFSCALE_LFS_PROXY_S3_SECRET_KEY", tc.secretKey)
			t.Setenv("KAFSCALE_LFS_PROXY_S3_SESSION_TOKEN", tc.sessionToken)
			t.Setenv("AWS_SESSION_TOKEN", "test-chain-session-token")
			t.Setenv("KAFSCALE_LFS_TRACKER_ENABLED", "false")
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if _, err := initLFSModule(ctx, logger); err != nil {
				t.Fatalf("initLFSModule: %v", err)
			}
			var record map[string]any
			for _, line := range strings.Split(logs.String(), "\n") {
				var candidate map[string]any
				if json.Unmarshal([]byte(line), &candidate) == nil && candidate["msg"] == "lfs s3 credentials" {
					record = candidate
				}
			}
			source, _ := record["source"].(string)
			if !strings.Contains(source, tc.wantSource) {
				t.Fatalf("credential source = %q, want %q", source, tc.wantSource)
			}
			token, found := record["session_token"]
			if found != tc.wantTokenField {
				t.Fatalf("session_token presence = %v, want %v: %s", found, tc.wantTokenField, logs.String())
			}
			if found {
				value, ok := token.(bool)
				if !ok || value != tc.wantSessionToken {
					t.Fatalf("session_token = %v, want boolean %v", token, tc.wantSessionToken)
				}
			}
			for _, value := range []string{"test-access", "test-secret", "test-session-token", "test-chain-session-token"} {
				if strings.Contains(logs.String(), value) {
					t.Fatalf("log contains credential value: %s", logs.String())
				}
			}
		})
	}
}
