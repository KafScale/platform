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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/broker"
	"github.com/KafScale/platform/pkg/metadata"
)

type readinessProbeStore struct {
	metadata.Store
	check func(context.Context) error
}

func (s readinessProbeStore) CheckReadiness(ctx context.Context) error { return s.check(ctx) }

func brokerReadiness(h *handler, ctx context.Context) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.readinessHandler(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil).WithContext(ctx))
	return rec
}

func TestBrokerReadinessDependencies(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		s3Failures, s3Successes int
		metadataError           bool
		want                    int
		state                   string
	}{
		{name: "healthy", want: 200, state: "healthy"},
		{name: "S3 degraded remains ready", s3Failures: 1, s3Successes: 1, want: 200, state: "degraded"},
		{name: "S3 unavailable", s3Failures: 1, want: 503, state: "unavailable"},
		{name: "metadata unavailable", metadataError: true, want: 503, state: "metadata_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checks := 0
			store := readinessProbeStore{check: func(ctx context.Context) error {
				checks++
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > time.Second {
					t.Fatal("unbounded metadata check")
				}
				if tc.metadataError {
					return errors.New("private etcd details")
				}
				return nil
			}}
			monitor := broker.NewS3HealthMonitor(broker.S3HealthConfig{})
			for range tc.s3Failures {
				monitor.RecordOperation("upload", time.Millisecond, errors.New("offline"))
			}
			for range tc.s3Successes {
				monitor.RecordOperation("upload", time.Millisecond, nil)
			}
			rec := brokerReadiness(&handler{store: store, s3Health: monitor}, context.Background())
			if rec.Code != tc.want || !strings.Contains(rec.Body.String(), "state="+tc.state) || strings.Contains(rec.Body.String(), "private") {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
				t.Fatal(got)
			}
			if tc.state == "unavailable" && checks != 0 {
				t.Fatal("metadata checked despite S3 outage")
			}
		})
	}
}

func TestBrokerReadinessRecoveryAndCancellation(t *testing.T) {
	failure := errors.New("offline")
	store := readinessProbeStore{check: func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return failure
	}}
	h := &handler{store: store, s3Health: broker.NewS3HealthMonitor(broker.S3HealthConfig{})}
	if rec := brokerReadiness(h, context.Background()); rec.Code != 503 {
		t.Fatal(rec.Code)
	}
	failure = nil
	if rec := brokerReadiness(h, context.Background()); rec.Code != 200 {
		t.Fatal(rec.Code)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if rec := brokerReadiness(h, ctx); rec.Code != 503 {
		t.Fatal(rec.Code)
	}
	h.store = metadata.NewInMemoryStore(metadata.ClusterMetadata{})
	if rec := brokerReadiness(h, context.Background()); rec.Code != 200 {
		t.Fatal(rec.Code)
	}
}

func TestBrokerReadinessRejectsCachedMetadataDuringEtcdLoss(t *testing.T) {
	store, err := metadata.NewEtcdStore(context.Background(), metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: testutil.StartEmbeddedEtcd(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	h := &handler{store: store, s3Health: broker.NewS3HealthMonitor(broker.S3HealthConfig{})}
	if rec := brokerReadiness(h, context.Background()); rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	if err := store.EtcdClient().Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Metadata(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if rec := brokerReadiness(h, context.Background()); rec.Code != 503 {
		t.Fatal(rec.Code, rec.Body.String())
	}
}
