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

package discovery

import (
	"context"
	"testing"
	"time"
)

type stubLister struct {
	segments []SegmentRef
	calls    int
}

func (s *stubLister) ListCompleted(ctx context.Context) ([]SegmentRef, error) {
	s.calls++
	return s.segments, nil
}

func TestCachedListerUsesCache(t *testing.T) {
	inner := &stubLister{
		segments: []SegmentRef{{Topic: "orders", Partition: 0, BaseOffset: 0}},
	}
	cached := newCachedLister(inner, time.Minute, 100)

	if _, err := cached.ListCompleted(context.Background()); err != nil {
		t.Fatalf("list: %v", err)
	}
	if _, err := cached.ListCompleted(context.Background()); err != nil {
		t.Fatalf("list: %v", err)
	}
	if inner.calls != 1 {
		t.Fatalf("expected 1 call, got %d", inner.calls)
	}
}

func TestCachedListerSkipsCacheOnOverflow(t *testing.T) {
	inner := &stubLister{
		segments: []SegmentRef{
			{Topic: "orders", Partition: 0, BaseOffset: 0},
			{Topic: "orders", Partition: 0, BaseOffset: 100},
		},
	}
	cached := newCachedLister(inner, time.Minute, 1)

	if _, err := cached.ListCompleted(context.Background()); err != nil {
		t.Fatalf("list: %v", err)
	}
	if _, err := cached.ListCompleted(context.Background()); err != nil {
		t.Fatalf("list: %v", err)
	}
	if inner.calls != 2 {
		t.Fatalf("expected 2 calls, got %d", inner.calls)
	}
}
