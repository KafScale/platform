// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package server

import (
	"context"
	"errors"
	"testing"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/discovery"
)

type staticGenerations struct {
	generations map[string]int64
	err         error
}

func (s staticGenerations) Generations(ctx context.Context) (map[string]int64, error) {
	return s.generations, s.err
}

func withGenerations(srv *Server, source generationSource) *Server {
	srv.generations = source
	srv.generationsInit = true
	return srv
}

func TestSelectReadsCurrentGenerationFromEtcd(t *testing.T) {
	cases := []struct {
		name        string
		generations map[string]int64
		want        []string
	}{
		{"never deleted", map[string]int64{}, []string{"0"}},
		{"re-created", map[string]int64{"orders": 1}, []string{"1"}},
		{"deleted", map[string]int64{"orders": 3}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := selectValues(t, withGenerations(generationTestServer(), staticGenerations{generations: tc.generations}))
			if len(values) != len(tc.want) || (len(values) == 1 && values[0] != tc.want[0]) {
				t.Fatalf("got %v, want %v", values, tc.want)
			}
		})
	}
}

func TestListCurrentSegmentsErrors(t *testing.T) {
	srv := withGenerations(generationTestServer(), staticGenerations{err: errors.New("etcd down")})
	if _, err := srv.listCurrentSegments(context.Background(), srv.lister); err == nil {
		t.Fatal("expected generation error")
	}
	srv = New(config.Config{}, nil)
	srv.generationsInit = true
	srv.generationsErr = errors.New("dial failed")
	if _, err := srv.listCurrentSegments(context.Background(), &mockLister{}); err == nil {
		t.Fatal("expected source error")
	}
	if _, err := srv.listCurrentSegments(context.Background(), &errLister{}); err == nil {
		t.Fatal("expected list error")
	}
	if got := storageGeneration(discovery.SegmentRef{Topic: "orders", StorageTopic: "orders@x"}); got != 0 {
		t.Fatalf("generation %d", got)
	}
}

type errLister struct{}

func (errLister) ListCompleted(ctx context.Context) ([]discovery.SegmentRef, error) {
	return nil, errors.New("list failed")
}

func TestGetGenerationsUsesEtcdOnlyWhenConfigured(t *testing.T) {
	srv := New(config.Config{}, nil)
	if source, err := srv.getGenerations(); source != nil || err != nil {
		t.Fatalf("expected no source, got %v %v", source, err)
	}
	srv = New(config.Config{Metadata: config.MetaConfig{Etcd: config.EtcdConfig{Endpoints: []string{"127.0.0.1:1"}}}}, nil)
	source, err := srv.getGenerations()
	if err != nil || source == nil {
		t.Fatalf("expected etcd source, got %v %v", source, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.Generations(ctx); err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if again, _ := srv.getGenerations(); again != source {
		t.Fatal("expected cached source")
	}
}
