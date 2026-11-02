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

package schemaregistry

import (
	"context"
	"testing"

	"github.com/KafScale/platform/pkg/storage"
)

type resetOnDownload struct {
	*storage.MemoryS3Client
	fn func()
}

func (s *resetOnDownload) DownloadSegment(ctx context.Context, key string, rng *storage.ByteRange) ([]byte, error) {
	if s.fn != nil {
		f := s.fn
		s.fn = nil
		f()
	}
	return s.MemoryS3Client.DownloadSegment(ctx, key, rng)
}

func TestResetDuringS3DownloadIsNotCached(t *testing.T) {
	s3 := &resetOnDownload{MemoryS3Client: storage.NewMemoryS3Client()}
	h := newHarness(t, nil, Config{S3: s3, Namespace: "prod", InlineLimit: 16})
	h.register("s", schema(userV1))
	r := h.reg
	forceReset(r)
	r.mu.Lock()
	r.counter = 5
	r.mu.Unlock()
	s3.fn = func() { forceReset(r) }
	if s, err := r.SchemaByID(context.Background(), 1); err != nil || s == nil {
		t.Fatalf("schema %v err %v", s, err)
	}
	if cached(r, 1) {
		t.Fatalf("schema read across a reset during the S3 download was cached")
	}
}
