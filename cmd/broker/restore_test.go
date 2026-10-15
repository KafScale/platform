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
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type restoreCountingS3 struct {
	*storage.MemoryS3Client
	lists atomic.Int64
}

func (s *restoreCountingS3) ListSegments(ctx context.Context, prefix string) ([]storage.S3Object, error) {
	s.lists.Add(1)
	return s.MemoryS3Client.ListSegments(ctx, prefix)
}

func TestPartitionRestoreWithRecreatedMetadata(t *testing.T) {
	const count = 64
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s3 := &restoreCountingS3{MemoryS3Client: storage.NewMemoryS3Client()}
	writer := storage.NewPartitionLog("default", "orders", 0, 0, s3, nil, storage.PartitionLogConfig{
		Buffer:  storage.WriteBufferConfig{MaxBytes: 1},
		Segment: storage.SegmentWriterConfig{IndexIntervalMessages: 1},
	}, nil, nil, nil)
	for i := 0; i < count; i++ {
		batch, err := storage.NewRecordBatchFromBytes(testBatchBytes(0, 0, 1))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.AppendBatch(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	store := metadata.NewInMemoryStore(defaultMetadata())
	h := newHandler(store, s3, protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}, testLogger())
	h.s3Namespace = "default"
	before, err := store.NextOffset(ctx, "orders", 0)
	if err != nil || before != 0 {
		t.Fatalf("fresh metadata: offset=%d err=%v", before, err)
	}
	var wg sync.WaitGroup
	results := make(chan *storage.PartitionLog, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			log, err := h.getPartitionLog(ctx, "orders", 0)
			if err != nil {
				errs <- err
				return
			}
			results <- log
		})
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var first *storage.PartitionLog
	for log := range results {
		if first == nil {
			first = log
		} else if first != log {
			t.Fatal("concurrent callers received different logs")
		}
	}
	if s3.lists.Load() != 1 {
		t.Fatalf("restore listings=%d, want 1", s3.lists.Load())
	}
	after, err := store.NextOffset(ctx, "orders", 0)
	if err != nil || after != count {
		t.Fatalf("recovered metadata: offset=%d err=%v", after, err)
	}
	for i := 0; i < count; i++ {
		resp, err := h.handleFetch(ctx, &protocol.RequestHeader{CorrelationID: int32(i), APIVersion: 11}, &kmsg.FetchRequest{
			MaxWaitMillis: 1,
			Topics:        []kmsg.FetchRequestTopic{{Topic: "orders", Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0, FetchOffset: int64(i), PartitionMaxBytes: 1 << 20}}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		decoded := decodeKmsgResponse(t, 11, resp, kmsg.NewPtrFetchResponse)
		if len(decoded.Topics) != 1 || len(decoded.Topics[0].Partitions) != 1 {
			t.Fatal("missing restored partition")
		}
		p := decoded.Topics[0].Partitions[0]
		if p.ErrorCode != 0 || p.HighWatermark != count || len(p.RecordBatches) < 8 {
			t.Fatalf("fetch offset %d: %+v", i, p)
		}
		if got := int64(binary.BigEndian.Uint64(p.RecordBatches[:8])); got != int64(i) {
			t.Fatalf("fetch offset %d returned batch at %d", i, got)
		}
	}
	resp, err := h.handleProduce(ctx, &protocol.RequestHeader{CorrelationID: 100}, &kmsg.ProduceRequest{
		Acks: -1, TimeoutMillis: 1000,
		Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: testBatchBytes(0, 0, 1)}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded := decodeKmsgResponse(t, 0, resp, kmsg.NewPtrProduceResponse)
	if len(decoded.Topics) != 1 || len(decoded.Topics[0].Partitions) != 1 {
		t.Fatal("missing produce result")
	}
	p := decoded.Topics[0].Partitions[0]
	if p.ErrorCode != 0 || p.BaseOffset != count {
		t.Fatalf("produce after restore: %+v", p)
	}
	after, err = store.NextOffset(ctx, "orders", 0)
	if err != nil || after != count+1 {
		t.Fatalf("offset after produce=%d err=%v", after, err)
	}
}
