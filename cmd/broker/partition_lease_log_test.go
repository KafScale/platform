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
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Exercise the same A -> B -> A movement as an expired lease after a broker pause.
func TestPartitionLogLeaseHandoff(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := metadata.NewEtcdStore(ctx, defaultMetadata(), metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s3 := storage.NewMemoryS3Client()
	a := newHandler(store, s3, protocol.MetadataBroker{NodeID: 0}, testLogger())
	b := newHandler(store, s3, protocol.MetadataBroker{NodeID: 1}, testLogger())
	defer a.coordinator.Stop()
	defer b.coordinator.Stop()
	defer a.leaseManager.ReleaseAll()
	defer b.leaseManager.ReleaseAll()
	produce := func(h *handler, want int64) {
		t.Helper()
		raw, err := h.handleProduce(ctx, &protocol.RequestHeader{APIVersion: 3}, &kmsg.ProduceRequest{Acks: -1, TimeoutMillis: 1000, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: testBatchBytes(0, 0, 1)}}}}})
		if err != nil {
			t.Fatal(err)
		}
		p := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
		if p.ErrorCode != 0 || p.BaseOffset != want {
			t.Fatalf("produce: error=%d offset=%d, want %d", p.ErrorCode, p.BaseOffset, want)
		}
	}
	revoke := func(h *handler) {
		t.Helper()
		key := fmt.Sprintf("%s/orders/0", metadata.PartitionLeasePrefix())
		got, err := store.EtcdClient().Get(ctx, key)
		if err != nil || len(got.Kvs) != 1 {
			t.Fatalf("lease: %+v, %v", got, err)
		}
		if _, err := store.EtcdClient().Revoke(ctx, clientv3.LeaseID(got.Kvs[0].Lease)); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for h.leaseManager.Owns("orders", 0) {
			if time.Now().After(deadline) {
				t.Fatal("revoked lease still owned")
			}
			time.Sleep(time.Millisecond)
		}
	}
	produce(a, 0)
	old, err := a.getPartitionLog(ctx, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	revoke(a)
	produce(b, 1)
	original, err := s3.DownloadSegment(ctx, "default/orders/0/segment-00000000000000000001.kfs", nil)
	if err != nil {
		t.Fatal(err)
	}
	// A non-owner must redirect fetches rather than return a stale empty tail.
	raw, err := a.handleFetch(ctx, &protocol.RequestHeader{APIVersion: 11}, &kmsg.FetchRequest{Topics: []kmsg.FetchRequestTopic{{Topic: "orders", Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0, FetchOffset: 1, PartitionMaxBytes: 1 << 20}}}}})
	if err != nil {
		t.Fatal(err)
	}
	fetch := decodeKmsgResponse(t, 11, raw, kmsg.NewPtrFetchResponse).Topics[0].Partitions[0]
	if fetch.ErrorCode != protocol.NOT_LEADER_OR_FOLLOWER || len(fetch.RecordBatches) != 0 {
		t.Errorf("non-owner fetch: %+v", fetch)
	}
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr("orders")}}
	raw, err = a.Handle(ctx, &protocol.RequestHeader{APIVersion: 1}, req)
	if err != nil {
		t.Fatal(err)
	}
	meta := decodeKmsgResponse(t, 1, raw, kmsg.NewPtrMetadataResponse)
	if meta.Topics[0].Partitions[0].Leader != 1 {
		t.Errorf("metadata leader=%d, want 1", meta.Topics[0].Partitions[0].Leader)
	}
	oldToken := b.leaseManager.OwnershipToken("orders", 0)
	revoke(b)
	produce(a, 2)
	if err := store.UpdateOffsetsForLease(ctx, "orders", 0, 0, "orders", oldToken); err == nil {
		t.Fatal("stale callback regressed offsets")
	}
	current, err := a.getPartitionLog(ctx, "orders", 0)
	if err != nil || current == old {
		t.Fatalf("reused stale log: %v", err)
	}
	for i := int64(0); i < 3; i++ {
		if data, err := current.Read(ctx, i, 1<<20); err != nil || len(data) == 0 {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	preserved, err := s3.DownloadSegment(ctx, "default/orders/0/segment-00000000000000000001.kfs", nil)
	if err != nil || !bytes.Equal(original, preserved) {
		t.Fatalf("acked segment changed: %v", err)
	}
	if next, err := store.NextOffset(ctx, "orders", 0); err != nil || next != 3 {
		t.Fatalf("next offset=%d: %v", next, err)
	}
	// In-flight callers retaining a handle from the first acquisition stay fenced.
	batch, _ := storage.NewRecordBatchFromBytes(testBatchBytes(0, 0, 1))
	if _, err := old.AppendBatches(ctx, []storage.RecordBatch{batch}, true); err == nil {
		t.Fatal("old handle appended after reacquisition")
	}
	// An initialization paused in S3 must not be shared with a later acquisition.
	a.leaseManager.Release("orders", 0)
	blocked := &blockedLeaseRestoreS3{MemoryS3Client: s3, started: make(chan struct{}), release: make(chan struct{})}
	var unblock sync.Once
	defer unblock.Do(func() { close(blocked.release) })
	a.s3 = blocked
	firstInit := make(chan error, 1)
	go func() { _, err := a.getPartitionLog(ctx, "orders", 0); firstInit <- err }()
	select {
	case <-blocked.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	revoke(a)
	produce(b, 3)
	revoke(b)
	secondInit := make(chan *storage.PartitionLog, 1)
	initErr := make(chan error, 1)
	go func() {
		log, err := a.getPartitionLog(ctx, "orders", 0)
		if err != nil {
			initErr <- err
		} else {
			secondInit <- log
		}
	}()
	var fresh *storage.PartitionLog
	select {
	case fresh = <-secondInit:
	case err := <-initErr:
		t.Fatal(err)
	case <-time.After(3 * time.Second):
		t.Fatal("new lease shared a restore from the old lease")
	}
	produce(a, 4)
	unblock.Do(func() { close(blocked.release) })
	select {
	case err := <-firstInit:
		if err == nil {
			t.Fatal("old restore published after reacquisition")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if got, err := a.getPartitionLog(ctx, "orders", 0); err != nil || got != fresh {
		t.Fatalf("old restore replaced current log: %v", err)
	}

}

// Only the first restore blocks, allowing a new acquisition to initialize independently.
type blockedLeaseRestoreS3 struct {
	*storage.MemoryS3Client
	started chan struct{}
	release chan struct{}
	blocked atomic.Bool
}

func (s *blockedLeaseRestoreS3) ListSegments(ctx context.Context, prefix string) ([]storage.S3Object, error) {
	if s.blocked.CompareAndSwap(false, true) {
		close(s.started)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.MemoryS3Client.ListSegments(ctx, prefix)
}
