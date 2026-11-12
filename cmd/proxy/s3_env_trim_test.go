// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
package main

import (
	"os"
	"testing"
)

func TestNormalizeAWSCredentialEnvTrimsOnlyASCIIWhitespace(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "\u00a0aws-key\u00a0")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "\v aws-secret\f\r\n")
	t.Setenv("AWS_SESSION_TOKEN", "\u2003")
	normalizeAWSCredentialEnv()
	if got := os.Getenv("AWS_ACCESS_KEY_ID"); got != "\u00a0aws-key\u00a0" {
		t.Fatalf("AWS_ACCESS_KEY_ID = %q, want no-break spaces kept", got)
	}
	if got := os.Getenv("AWS_SECRET_ACCESS_KEY"); got != "aws-secret" {
		t.Fatalf("AWS_SECRET_ACCESS_KEY = %q, want ASCII whitespace trimmed", got)
	}
	if got, ok := os.LookupEnv("AWS_SESSION_TOKEN"); !ok || got != "\u2003" {
		t.Fatalf("AWS_SESSION_TOKEN = %q (set %v), want it kept", got, ok)
	}

	t.Setenv("AWS_SESSION_TOKEN", " \t\v\f\r\n")
	normalizeAWSCredentialEnv()
	if _, ok := os.LookupEnv("AWS_SESSION_TOKEN"); ok {
		t.Fatalf("ASCII whitespace AWS_SESSION_TOKEN should be unset")
	}
}
