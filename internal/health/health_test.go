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

package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/internal/testutil"
	"github.com/KafScale/platform/v2/pkg/metadata"
)

type probeStore struct {
	metadata.Store
	check func(context.Context) error
}

func (s probeStore) CheckReadiness(ctx context.Context) error { return s.check(ctx) }

type legacyStore struct {
	metadata.Store
	err error
}

func (s legacyStore) Metadata(context.Context, []string) (*metadata.ClusterMetadata, error) {
	return nil, s.err
}

func probe(mux http.Handler, ctx context.Context, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx))
	return rec
}

func TestReadinessAndLiveness(t *testing.T) {
	failure := errors.New("private backend details")
	for _, tc := range []struct {
		name  string
		store metadata.Store
		ready int
	}{
		{"no-store", nil, http.StatusOK},
		{"memory", metadata.NewInMemoryStore(metadata.ClusterMetadata{}), http.StatusOK},
		{"legacy-success", legacyStore{}, http.StatusOK},
		{"legacy-failure", legacyStore{err: failure}, http.StatusServiceUnavailable},
		{"live-failure", probeStore{check: func(context.Context) error { return failure }}, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mux := http.NewServeMux()
			Register(mux, tc.store)
			if rec := probe(mux, context.Background(), "/readyz"); rec.Code != tc.ready || strings.Contains(rec.Body.String(), failure.Error()) {
				t.Fatalf("readiness=%d body=%s", rec.Code, rec.Body.String())
			}
			if rec := probe(mux, context.Background(), "/healthz"); rec.Code != http.StatusOK {
				t.Fatalf("liveness=%d", rec.Code)
			}
		})
	}
}

func TestReadinessRecoversAndBoundsItsCheck(t *testing.T) {
	unavailable := true
	checks := 0
	mux := http.NewServeMux()
	Register(mux, probeStore{check: func(ctx context.Context) error {
		checks++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > readinessTimeout {
			t.Fatal("readiness check has no bounded deadline")
		}
		if unavailable {
			return errors.New("offline")
		}
		return nil
	}})
	if rec := probe(mux, context.Background(), "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatal(rec.Code)
	}
	unavailable = false
	if rec := probe(mux, context.Background(), "/readyz"); rec.Code != http.StatusOK {
		t.Fatal(rec.Code)
	}
	if checks != 2 {
		t.Fatalf("cached readiness instead of checking again: %d", checks)
	}
}

func TestReadinessHonorsRequestCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	mux := http.NewServeMux()
	Register(mux, probeStore{check: func(checkCtx context.Context) error { <-checkCtx.Done(); return checkCtx.Err() }})
	if rec := probe(mux, ctx, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatal(rec.Code)
	}
}

func TestReadinessUsesLiveEtcdDespiteCachedMetadata(t *testing.T) {
	ctx := context.Background()
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	mux := http.NewServeMux()
	Register(mux, store)
	if rec := probe(mux, ctx, "/readyz"); rec.Code != http.StatusOK {
		t.Fatal(rec.Code, rec.Body.String())
	}
	// Cached Metadata still succeeds after losing the live client.
	if err := store.EtcdClient().Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Metadata(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if rec := probe(mux, ctx, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatal(rec.Code)
	}
	if rec := probe(mux, ctx, "/healthz"); rec.Code != http.StatusOK {
		t.Fatal(rec.Code)
	}
}

func TestReadinessRejectsUnresolvableEtcd(t *testing.T) {
	initCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	store, err := metadata.NewEtcdStore(initCtx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: []string{"http://unreachable.invalid:2379"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if _, err := store.Metadata(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	Register(mux, store)
	if rec := probe(mux, context.Background(), "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatal(rec.Code)
	}
}
