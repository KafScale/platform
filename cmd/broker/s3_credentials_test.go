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
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/KafScale/platform/pkg/storage"
)

func TestBuildS3ConfigsFromEnvTrimsCredentials(t *testing.T) {
	t.Setenv("KAFSCALE_USE_MEMORY_S3", "")
	for _, key := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
		t.Setenv(key, "")
	}
	t.Setenv("KAFSCALE_S3_ACCESS_KEY", " access\n")
	t.Setenv("KAFSCALE_S3_SECRET_KEY", "\tsecret ")
	t.Setenv("KAFSCALE_S3_SESSION_TOKEN", " token ")
	writeCfg, readCfg, _, credsProvided, _ := buildS3ConfigsFromEnv()
	if !credsProvided {
		t.Fatalf("expected credsProvided true")
	}
	for _, cfg := range []storage.S3Config{writeCfg, readCfg} {
		if cfg.AccessKeyID != "access" || cfg.SecretAccessKey != "secret" || cfg.SessionToken != "token" {
			t.Fatalf("credentials = %q/%q/%q, want trimmed values", cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
		}
	}

	t.Setenv("KAFSCALE_S3_ACCESS_KEY", "  ")
	t.Setenv("KAFSCALE_S3_SESSION_TOKEN", "\t")
	writeCfg, _, _, credsProvided, _ = buildS3ConfigsFromEnv()
	if credsProvided || writeCfg.AccessKeyID != "" || writeCfg.SessionToken != "" {
		t.Fatalf("whitespace-only access key counted as set: credsProvided=%v cfg=%q/%q", credsProvided, writeCfg.AccessKeyID, writeCfg.SessionToken)
	}
}

func TestBrokerS3ClientIgnoresWhitespaceKafscaleKeys(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		env                          map[string]string
		wantKey, wantToken           string
		wantStaticCredentialProvided bool
	}{
		{
			name:    "whitespace kafscale pair uses the padded aws pair",
			env:     map[string]string{"KAFSCALE_S3_ACCESS_KEY": "  ", "KAFSCALE_S3_SECRET_KEY": "\t", "KAFSCALE_S3_SESSION_TOKEN": " ", "AWS_ACCESS_KEY_ID": " aws-key\n", "AWS_SECRET_ACCESS_KEY": "aws-secret ", "AWS_SESSION_TOKEN": " aws-token "},
			wantKey: "aws-key", wantToken: "aws-token",
		},
		{
			name:    "whitespace aws session token is no token",
			env:     map[string]string{"KAFSCALE_S3_ACCESS_KEY": "", "KAFSCALE_S3_SECRET_KEY": "", "AWS_ACCESS_KEY_ID": "aws-key", "AWS_SECRET_ACCESS_KEY": "aws-secret", "AWS_SESSION_TOKEN": "  "},
			wantKey: "aws-key",
		},
		{
			name:    "padded kafscale pair",
			env:     map[string]string{"KAFSCALE_S3_ACCESS_KEY": " ks-key ", "KAFSCALE_S3_SECRET_KEY": " ks-secret", "KAFSCALE_S3_SESSION_TOKEN": "  ", "AWS_ACCESS_KEY_ID": "aws-key", "AWS_SECRET_ACCESS_KEY": "aws-secret", "AWS_SESSION_TOKEN": "aws-token"},
			wantKey: "ks-key", wantStaticCredentialProvided: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan http.Header, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case requests <- r.Header.Clone():
				default:
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			t.Setenv("KAFSCALE_USE_MEMORY_S3", "")
			t.Setenv("KAFSCALE_S3_BUCKET", "broker-creds")
			t.Setenv("KAFSCALE_S3_REGION", "us-east-1")
			t.Setenv("KAFSCALE_S3_ENDPOINT", server.URL)
			t.Setenv("KAFSCALE_S3_READ_BUCKET", "")
			t.Setenv("KAFSCALE_S3_READ_REGION", "")
			t.Setenv("KAFSCALE_S3_READ_ENDPOINT", "")
			t.Setenv("AWS_CONFIG_FILE", os.DevNull)
			t.Setenv("AWS_SHARED_CREDENTIALS_FILE", os.DevNull)
			t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			writeCfg, _, _, credsProvided, _ := buildS3ConfigsFromEnv()
			if credsProvided != tc.wantStaticCredentialProvided {
				t.Fatalf("credsProvided = %v, want %v", credsProvided, tc.wantStaticCredentialProvided)
			}
			client, err := storage.NewS3Client(context.Background(), writeCfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.EnsureBucket(context.Background()); err != nil {
				t.Fatal(err)
			}
			headers := <-requests
			if got := headers.Get("X-Amz-Security-Token"); got != tc.wantToken {
				t.Fatalf("signed request token = %q, want %q", got, tc.wantToken)
			}
			if auth := headers.Get("Authorization"); !strings.Contains(auth, "Credential="+tc.wantKey+"/") {
				t.Fatalf("request signed with %q, want key %q", auth, tc.wantKey)
			}
		})
	}
}
