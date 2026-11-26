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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/pkg/broker"
	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/KafScale/platform/v2/pkg/storage"
)

type startupProbeS3 struct {
	*storage.MemoryS3Client
	put    func(context.Context, string, []byte) error
	remove func(context.Context, string) error
}

func (s *startupProbeS3) UploadSegment(ctx context.Context, key string, body []byte) error {
	if s.put != nil {
		return s.put(ctx, key, body)
	}
	return s.MemoryS3Client.UploadSegment(ctx, key, body)
}

func (s *startupProbeS3) DeleteSegment(ctx context.Context, key string) error {
	if s.remove != nil {
		return s.remove(ctx, key)
	}
	return s.MemoryS3Client.DeleteSegment(ctx, key)
}

func TestStartupChecksConditionalS3(t *testing.T) {
	for _, tc := range []struct {
		name            string
		secondErr       error
		ignoreCondition bool
		wantMessage     string
	}{
		{name: "honors-condition"},
		{name: "wrapped-precondition", secondErr: fmt.Errorf("PUT: %w", storage.ErrPreconditionFailed)},
		{name: "ignores-condition", ignoreCondition: true, wantMessage: "ignores If-None-Match"},
		{name: "rejects-second-write", secondErr: errors.New("unsupported header"), wantMessage: "want 412 PreconditionFailed"},
		{name: "conditional-conflict-is-not-proof", secondErr: storage.ErrConditionalConflict, wantMessage: "want 412 PreconditionFailed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s3 := &startupProbeS3{MemoryS3Client: storage.NewMemoryS3Client()}
			calls := 0
			var firstKey string
			s3.put = func(ctx context.Context, key string, body []byte) error {
				calls++
				if calls == 1 {
					firstKey = key
					return s3.MemoryS3Client.UploadSegment(ctx, key, body)
				}
				if calls != 2 || key != firstKey {
					t.Fatalf("probe must create the same key twice: calls=%d first=%s current=%s", calls, firstKey, key)
				}
				if tc.ignoreCondition {
					return s3.UploadIndex(ctx, key, body)
				}
				if tc.secondErr != nil {
					return tc.secondErr
				}
				return s3.MemoryS3Client.UploadSegment(ctx, key, body)
			}
			h := newHandler(metadata.NewInMemoryStore(defaultMetadata()), s3, protocol.MetadataBroker{}, testLogger())
			t.Cleanup(h.coordinator.Stop)
			err := h.runStartupChecks(context.Background())
			if tc.wantMessage == "" {
				if err != nil {
					t.Fatal(err)
				}
				if snap := h.s3Health.Snapshot(); snap.State != broker.S3StateHealthy || snap.ErrorRate != 0 {
					t.Fatalf("expected 412 poisoned startup health: %+v", snap)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("startup error=%v, want %q", err, tc.wantMessage)
			}
			if tc.secondErr != nil && tc.wantMessage != "" && !errors.Is(err, tc.secondErr) {
				t.Fatalf("startup lost underlying error: %v", err)
			}
			if calls != 2 {
				t.Fatalf("PUT calls=%d, want 2", calls)
			}
			if objects, err := s3.ListSegments(context.Background(), ""); err != nil || len(objects) != 0 {
				t.Fatalf("startup leaked probe objects: %+v, %v", objects, err)
			}
		})
	}
}

func TestStartupProbeCleansUpAfterCanceledPut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s3 := &startupProbeS3{MemoryS3Client: storage.NewMemoryS3Client()}
	calls, deletes := 0, 0
	s3.put = func(ctx context.Context, key string, body []byte) error {
		calls++
		if err := s3.MemoryS3Client.UploadSegment(ctx, key, body); err != nil {
			t.Fatal(err)
		}
		cancel() // The PUT completed but its response was lost.
		return context.Canceled
	}
	s3.remove = func(cleanupCtx context.Context, key string) error {
		deletes++
		if cleanupCtx.Err() != nil {
			t.Fatalf("cleanup inherited cancellation: %v", cleanupCtx.Err())
		}
		if _, ok := cleanupCtx.Deadline(); !ok {
			t.Fatal("cleanup has no deadline")
		}
		return s3.MemoryS3Client.DeleteSegment(cleanupCtx, key)
	}
	h := newHandler(metadata.NewInMemoryStore(defaultMetadata()), s3, protocol.MetadataBroker{}, testLogger())
	t.Cleanup(h.coordinator.Stop)
	if err := h.verifyS3(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if calls != 1 || deletes != 1 {
		t.Fatalf("PUTs=%d DELETEs=%d, want 1 each", calls, deletes)
	}
	if objects, err := s3.ListSegments(context.Background(), ""); err != nil || len(objects) != 0 {
		t.Fatalf("canceled PUT leaked a probe: %+v, %v", objects, err)
	}
}

func TestStartupProbeRejectedInitialCondition(t *testing.T) {
	rejected := errors.New("If-None-Match is unsupported")
	s3 := &startupProbeS3{MemoryS3Client: storage.NewMemoryS3Client()}
	s3.put = func(context.Context, string, []byte) error { return rejected }
	h := newHandler(metadata.NewInMemoryStore(defaultMetadata()), s3, protocol.MetadataBroker{}, testLogger())
	t.Cleanup(h.coordinator.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := h.verifyS3(ctx)
	if !errors.Is(err, rejected) || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "PutObject with If-None-Match: *") {
		t.Fatalf("initial conditional PUT rejection lost its diagnostic: %v", err)
	}
}

func TestStartupProbeCleanupFailure(t *testing.T) {
	for _, cleanupErr := range []error{storage.ErrNotFound, errors.New("delete denied")} {
		t.Run(cleanupErr.Error(), func(t *testing.T) {
			s3 := &startupProbeS3{MemoryS3Client: storage.NewMemoryS3Client()}
			deletes := 0
			s3.remove = func(context.Context, string) error { deletes++; return cleanupErr }
			h := newHandler(metadata.NewInMemoryStore(defaultMetadata()), s3, protocol.MetadataBroker{}, testLogger())
			t.Cleanup(h.coordinator.Stop)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := h.verifyS3(ctx); err != nil {
				t.Fatal(err)
			}
			if deletes != 1 {
				t.Fatalf("DELETE calls=%d, want 1", deletes)
			}
		})
	}
}

func TestStartupProbeAWSConditionalPUT(t *testing.T) {
	for _, secondStatus := range []int{http.StatusPreconditionFailed, http.StatusOK, http.StatusBadRequest} {
		t.Run(http.StatusText(secondStatus), func(t *testing.T) {
			var mu sync.Mutex
			puts, deletes := 0, 0
			var key string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodPut:
					puts++
					if r.Header.Get("If-None-Match") != "*" {
						t.Errorf("conditional header=%q", r.Header.Get("If-None-Match"))
					}
					if puts == 1 {
						key = r.URL.Path
						w.WriteHeader(http.StatusOK)
						return
					}
					if puts != 2 || r.URL.Path != key {
						t.Errorf("second PUT must use the same key: puts=%d path=%s first=%s", puts, r.URL.Path, key)
					}
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(secondStatus)
					switch secondStatus {
					case http.StatusPreconditionFailed:
						_, _ = fmt.Fprint(w, "<Error><Code>PreconditionFailed</Code></Error>")
					case http.StatusBadRequest:
						_, _ = fmt.Fprint(w, "<Error><Code>InvalidRequest</Code><Message>unsupported condition</Message></Error>")
					}
				case http.MethodDelete:
					deletes++
					if r.URL.Path != key {
						t.Errorf("deleted %s, want %s", r.URL.Path, key)
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected S3 request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s3, err := storage.NewS3Client(ctx, storage.S3Config{
				Bucket: "startup-test", Region: "us-east-1", Endpoint: server.URL,
				ForcePathStyle: true, AccessKeyID: "test", SecretAccessKey: "test",
			})
			if err != nil {
				t.Fatal(err)
			}
			h := newHandler(metadata.NewInMemoryStore(defaultMetadata()), s3, protocol.MetadataBroker{}, testLogger())
			t.Cleanup(h.coordinator.Stop)
			err = h.runStartupChecks(ctx)
			if (err == nil) != (secondStatus == http.StatusPreconditionFailed) {
				t.Fatalf("second PUT status=%d, startup error=%v", secondStatus, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if puts != 2 || deletes != 1 {
				t.Fatalf("PUTs=%d DELETEs=%d, want 2 and 1", puts, deletes)
			}
		})
	}
}

func TestStartupProbeBackendRejectsConditionFailsFast(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		code   string
	}{
		{name: "501-not-implemented", status: http.StatusNotImplemented, code: "NotImplemented"},
		{name: "501-no-code", status: http.StatusNotImplemented},
		{name: "400-invalid-argument", status: http.StatusBadRequest, code: "InvalidArgument"},
		{name: "400-invalid-request", status: http.StatusBadRequest, code: "InvalidRequest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KAFSCALE_STARTUP_TIMEOUT_SEC", "10")
			var mu sync.Mutex
			puts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.Method {
				case http.MethodPut:
					puts++
					if r.Header.Get("If-None-Match") != "*" {
						t.Errorf("conditional header=%q", r.Header.Get("If-None-Match"))
					}
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(tc.status)
					if tc.status == http.StatusBadRequest {
						_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>invalid argument</Message></Error>", tc.code)
					} else if tc.code != "" {
						_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>If-None-Match is not supported</Message></Error>", tc.code)
					}
				case http.MethodDelete:
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected S3 request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			s3, err := storage.NewS3Client(context.Background(), storage.S3Config{
				Bucket: "startup-test", Region: "us-east-1", Endpoint: server.URL,
				ForcePathStyle: true, AccessKeyID: "test", SecretAccessKey: "test",
			})
			if err != nil {
				t.Fatal(err)
			}
			h := newHandler(metadata.NewInMemoryStore(defaultMetadata()), s3, protocol.MetadataBroker{}, testLogger())
			t.Cleanup(h.coordinator.Stop)
			start := time.Now()
			err = h.runStartupChecks(context.Background())
			elapsed := time.Since(start)
			wantMessage := "backend rejects If-None-Match: *"
			if tc.status == http.StatusBadRequest {
				wantMessage = "backend rejected the conditional PutObject (If-None-Match: *)"
			}
			if err == nil || !strings.Contains(err.Error(), wantMessage) || !strings.Contains(err.Error(), "cannot be fenced") {
				t.Fatalf("startup error=%v, want %q", err, wantMessage)
			}
			if tc.status == http.StatusBadRequest {
				msg := err.Error()
				if strings.Contains(msg, "backend rejects If-None-Match") {
					t.Fatalf("400 startup error blamed If-None-Match support: %v", err)
				}
				if strings.Contains(msg, "not supported") || strings.Contains(msg, "\n") {
					t.Fatalf("400 startup error still leaked unsupported wording or a joined error: %q", msg)
				}
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("startup waited for the timeout instead of failing fast after %s: %v", elapsed, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if puts != 1 {
				t.Fatalf("PUTs=%d, want 1 with no retry", puts)
			}
		})
	}
}
