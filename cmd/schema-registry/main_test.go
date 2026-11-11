// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/schemaregistry"
)

func env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestLoadSettingsDefaults(t *testing.T) {
	s, err := loadSettings(env(map[string]string{"KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS": " a:2379, ,b:2379 "}))
	if err != nil {
		t.Fatal(err)
	}
	if s.addr != ":8081" || s.namespace != "default" || s.timeout != 10*time.Second || s.inlineLimit != schemaregistry.DefaultInlineLimit || s.s3 != nil {
		t.Fatalf("defaults %+v", s)
	}
	if strings.Join(s.endpoints, ",") != "a:2379,b:2379" {
		t.Fatalf("endpoints %v", s.endpoints)
	}
}

func TestLoadSettingsOverrides(t *testing.T) {
	s, err := loadSettings(env(map[string]string{
		"KAFSCALE_SCHEMA_REGISTRY_ADDR":               ":9000",
		"KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS":     "e:2379",
		"KAFSCALE_SCHEMA_REGISTRY_ETCD_USERNAME":      "u",
		"KAFSCALE_SCHEMA_REGISTRY_ETCD_PASSWORD":      "p",
		"KAFSCALE_SCHEMA_REGISTRY_REQUEST_TIMEOUT":    "3s",
		"KAFSCALE_SCHEMA_REGISTRY_INLINE_LIMIT_BYTES": "100",
		"KAFSCALE_S3_NAMESPACE":                       "ns",
		"KAFSCALE_S3_BUCKET":                          "bucket",
		"KAFSCALE_S3_REGION":                          "eu-west-1",
		"KAFSCALE_S3_ENDPOINT":                        "http://minio:9000",
		"KAFSCALE_S3_PATH_STYLE":                      "false",
		"KAFSCALE_S3_ACCESS_KEY":                      "ak",
		"KAFSCALE_S3_SECRET_KEY":                      "sk",
		"KAFSCALE_S3_SESSION_TOKEN":                   "st",
		"KAFSCALE_S3_KMS_ARN":                         "kms",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if s.addr != ":9000" || s.username != "u" || s.password != "p" || s.timeout != 3*time.Second || s.inlineLimit != 100 || s.namespace != "ns" {
		t.Fatalf("settings %+v", s)
	}
	c := s.s3
	if c == nil || c.Bucket != "bucket" || c.Region != "eu-west-1" || c.Endpoint != "http://minio:9000" || c.ForcePathStyle || c.AccessKeyID != "ak" || c.SecretAccessKey != "sk" || c.SessionToken != "st" || c.KMSKeyARN != "kms" {
		t.Fatalf("s3 %+v", c)
	}
	s, err = loadSettings(env(map[string]string{"KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS": "e:2379", "KAFSCALE_S3_BUCKET": "b", "KAFSCALE_S3_ENDPOINT": "http://minio:9000"}))
	if err != nil || !s.s3.ForcePathStyle {
		t.Fatalf("path style follows the endpoint: %+v %v", s.s3, err)
	}
}

func TestLoadSettingsTrimsS3Keys(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		access, secret, token string
		want                  [3]string
	}{
		{name: "valid keys", access: "ak", secret: "sk", token: "st", want: [3]string{"ak", "sk", "st"}},
		{name: "padded keys", access: " ak\n", secret: "\tsk ", token: " st\n", want: [3]string{"ak", "sk", "st"}},
		{name: "whitespace keys count as unset", access: "  ", secret: "\t", token: " ", want: [3]string{"", "", ""}},
		{name: "whitespace session token is no token", access: "ak", secret: "sk", token: "  ", want: [3]string{"ak", "sk", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := loadSettings(env(map[string]string{
				"KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS": "e:2379",
				"KAFSCALE_S3_BUCKET":                      "bucket",
				"KAFSCALE_S3_ACCESS_KEY":                  tc.access,
				"KAFSCALE_S3_SECRET_KEY":                  tc.secret,
				"KAFSCALE_S3_SESSION_TOKEN":               tc.token,
			}))
			if err != nil {
				t.Fatal(err)
			}
			if got := [3]string{s.s3.AccessKeyID, s.s3.SecretAccessKey, s.s3.SessionToken}; got != tc.want {
				t.Fatalf("s3 keys = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestLoadSettingsErrors(t *testing.T) {
	for name, values := range map[string]map[string]string{
		"no endpoints": {},
		"bad timeout":  {"KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS": "e", "KAFSCALE_SCHEMA_REGISTRY_REQUEST_TIMEOUT": "soon"},
		"zero timeout": {"KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS": "e", "KAFSCALE_SCHEMA_REGISTRY_REQUEST_TIMEOUT": "0s"},
		"bad limit":    {"KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS": "e", "KAFSCALE_SCHEMA_REGISTRY_INLINE_LIMIT_BYTES": "-1"},
		"bad style":    {"KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS": "e", "KAFSCALE_S3_BUCKET": "b", "KAFSCALE_S3_PATH_STYLE": "maybe"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadSettings(env(values)); err == nil {
				t.Fatal("expected an error")
			}
			if err := run(context.Background(), env(values)); err == nil {
				t.Fatal("run must fail on the same settings")
			}
		})
	}
}

func TestNamespaceWarning(t *testing.T) {
	env := map[string]string{"KAFSCALE_SCHEMA_REGISTRY_ETCD_ENDPOINTS": "e:2379", "KAFSCALE_S3_BUCKET": "b", "KAFSCALE_S3_NAMESPACE": "  "}
	s, err := loadSettings(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if msg := namespaceWarning(s); !strings.Contains(msg, `"default"`) || !strings.Contains(msg, "bucket b") {
		t.Fatalf("warning %q", msg)
	}
	env["KAFSCALE_S3_NAMESPACE"] = "prod"
	if s, _ = loadSettings(func(k string) string { return env[k] }); namespaceWarning(s) != "" {
		t.Fatal("warning for a set namespace")
	}
	delete(env, "KAFSCALE_S3_BUCKET")
	env["KAFSCALE_S3_NAMESPACE"] = ""
	if s, _ = loadSettings(func(k string) string { return env[k] }); namespaceWarning(s) != "" {
		t.Fatal("warning without S3")
	}
}
