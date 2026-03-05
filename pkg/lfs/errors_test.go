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

package lfs

import (
	"errors"
	"strings"
	"testing"
)

func TestLfsErrorWithOp(t *testing.T) {
	err := &LfsError{Op: "upload", Err: errors.New("connection refused")}
	got := err.Error()
	if !strings.Contains(got, "upload") {
		t.Fatalf("expected 'upload' in error, got: %s", got)
	}
	if !strings.Contains(got, "connection refused") {
		t.Fatalf("expected 'connection refused' in error, got: %s", got)
	}
}

func TestLfsErrorWithoutOp(t *testing.T) {
	err := &LfsError{Err: errors.New("some failure")}
	got := err.Error()
	if !strings.Contains(got, "lfs error") {
		t.Fatalf("expected 'lfs error' in output, got: %s", got)
	}
	if !strings.Contains(got, "some failure") {
		t.Fatalf("expected 'some failure' in output, got: %s", got)
	}
}

func TestLfsErrorNilReceiver(t *testing.T) {
	var err *LfsError
	got := err.Error()
	if got != "lfs error" {
		t.Fatalf("expected 'lfs error', got: %s", got)
	}
}

func TestLfsErrorUnwrap(t *testing.T) {
	inner := errors.New("inner error")
	err := &LfsError{Op: "test", Err: inner}
	if !errors.Is(err, inner) {
		t.Fatal("Unwrap should return inner error")
	}
}

func TestLfsErrorUnwrapNil(t *testing.T) {
	var err *LfsError
	if err.Unwrap() != nil {
		t.Fatal("nil receiver Unwrap should return nil")
	}
}

func TestChecksumErrorMessage(t *testing.T) {
	err := &ChecksumError{Expected: "abc", Actual: "def"}
	got := err.Error()
	if !strings.Contains(got, "abc") || !strings.Contains(got, "def") {
		t.Fatalf("expected both checksums in error, got: %s", got)
	}
	if !strings.Contains(got, "mismatch") {
		t.Fatalf("expected 'mismatch' in error, got: %s", got)
	}
}
