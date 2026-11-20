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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/pkg/metadata"
	"github.com/KafScale/platform/v2/pkg/storage"
)

func TestFetchRestoresMisleadingSidecar(t *testing.T) {
	for version := int16(4); version <= 13; version++ {
		for _, cached := range []bool{false, true} {
			for _, wrongPosition := range []bool{false, true} {
				t.Run(fmt.Sprintf("v%d/cached=%v/position=%v", version, cached, wrongPosition), func(t *testing.T) {
					ctx := context.Background()
					store := metadata.NewInMemoryStore(defaultMetadata())
					if err := store.UpdateOffsets(ctx, "orders", 0, 7); err != nil {
						t.Fatal(err)
					}
					s3 := storage.NewMemoryS3Client()
					h := newHandler(store, s3, defaultMetadata().Brokers[0], testLogger())
					t.Cleanup(h.coordinator.Stop)
					h.logConfig.CacheEnabled = cached
					name, err := h.topicStorageName(ctx, "orders")
					if err != nil {
						t.Fatal(err)
					}
					a, err := storage.NewRecordBatchFromBytes(testBatchBytes(5, 1, 2))
					if err != nil {
						t.Fatal(err)
					}
					b, err := storage.NewRecordBatchFromBytes(testBatchBytes(7, 0, 1))
					if err != nil {
						t.Fatal(err)
					}
					artifact, err := storage.BuildSegment(storage.SegmentWriterConfig{IndexIntervalMessages: 1}, []storage.RecordBatch{a, b}, time.Unix(1, 0))
					if err != nil {
						t.Fatal(err)
					}
					prefix := path.Join(h.s3Namespace, name, "0")
					key := path.Join(prefix, fmt.Sprintf("segment-%020d.kfs", artifact.BaseOffset))
					if err := s3.UploadSegment(ctx, key, artifact.SegmentBytes); err != nil {
						t.Fatal(err)
					}
					index := storage.NewIndexBuilder(1)
					first, second := artifact.RelativeIndex[0], *artifact.RelativeIndex[1]
					if wrongPosition {
						second.Position++
					} else {
						second.Offset--
					}
					index.MaybeAdd(first.Offset, first.Position, 1)
					index.MaybeAdd(second.Offset, second.Position, 1)
					sidecar, err := index.BuildBytes()
					if err != nil {
						t.Fatal(err)
					}
					indexKey := path.Join(prefix, fmt.Sprintf("segment-%020d.index", artifact.BaseOffset))
					if err := s3.UploadIndex(ctx, indexKey, sidecar); err != nil {
						t.Fatal(err)
					}
					if cached {
						h.cache.SetSegment(path.Join(h.s3Namespace, name), 0, 5, artifact.SegmentBytes)
					}
					for _, offset := range []int64{5, 6, 7} {
						part := fetchAtOffset(t, h, version, offset)
						want := int64(5)
						if offset == 7 {
							want = 7
						}
						if part.ErrorCode != 0 || part.HighWatermark != 8 || len(part.RecordBatches) < 8 || int64(binary.BigEndian.Uint64(part.RecordBatches[:8])) != want {
							t.Fatalf("Fetch %d skipped its containing batch: %+v", offset, part)
						}
					}
					unchanged, err := s3.DownloadIndex(ctx, indexKey)
					if err != nil || !bytes.Equal(unchanged, sidecar) {
						t.Fatal("Fetch rewrote the winning sidecar")
					}
				})
			}
		}
	}
}
