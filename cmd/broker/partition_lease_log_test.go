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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/broker"
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
	// Earliest-offset discovery must redirect too; UNKNOWN_SERVER_ERROR is not
	// a leader retry and can terminate a consumer during the same handoff.
	if p := requestListOffset(t, a, 4, "orders", 0, -2); p.ErrorCode != protocol.NOT_LEADER_OR_FOLLOWER {
		t.Errorf("non-owner earliest offset: %+v", p)
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

// Losing the authoritative lease just before offset publication must reject Produce,
// even when the keepalive/session monitor has not noticed the change yet.
type replaceLeaseOnIndexS3 struct {
	*storage.MemoryS3Client
	client  *clientv3.Client
	replace atomic.Bool
}

func (s *replaceLeaseOnIndexS3) UploadIndex(ctx context.Context, key string, body []byte) error {
	if err := s.MemoryS3Client.UploadIndex(ctx, key, body); err != nil {
		return err
	}
	if s.replace.CompareAndSwap(false, true) {
		_, err := s.client.Put(ctx, metadata.PartitionLeasePrefix()+"/orders/0", "replacement")
		return err
	}
	return nil
}

func TestProduceRejectsLostLeaseAtOffsetCommit(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := metadata.NewEtcdStore(ctx, defaultMetadata(), metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s3 := &replaceLeaseOnIndexS3{MemoryS3Client: storage.NewMemoryS3Client(), client: store.EtcdClient()}
	h := newHandler(store, s3, protocol.MetadataBroker{NodeID: 0}, testLogger())
	defer h.coordinator.Stop()
	defer h.leaseManager.ReleaseAll()
	raw, err := h.handleProduce(ctx, &protocol.RequestHeader{APIVersion: 3}, &kmsg.ProduceRequest{Acks: -1, TimeoutMillis: 1000, Topics: []kmsg.ProduceRequestTopic{{Topic: "orders", Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: 0, Records: testBatchBytes(0, 0, 1)}}}}})
	if err != nil {
		t.Fatal(err)
	}
	response := decodeKmsgResponse(t, 3, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions[0]
	if response.ErrorCode != protocol.NOT_LEADER_OR_FOLLOWER {
		t.Fatalf("rejected lease commit was acknowledged: %+v", response)
	}
	if next, err := store.NextOffset(ctx, "orders", 0); err != nil || next != 0 {
		t.Fatalf("offset=%d err=%v", next, err)
	}
	h.logMu.RLock()
	cached := h.logs["orders"][0]
	h.logMu.RUnlock()
	if cached != nil {
		t.Fatal("log retained after failed offset commit")
	}
}

type lostProbeResponseS3 struct {
	*storage.MemoryS3Client
	attempts atomic.Int32
}

func (s *lostProbeResponseS3) UploadSegment(ctx context.Context, key string, body []byte) error {
	if err := s.MemoryS3Client.UploadSegment(ctx, key, body); err != nil {
		return err
	}
	if s.attempts.Add(1) == 1 {
		return errors.New("probe response lost")
	}
	return nil
}
func TestStartupProbeRecoversFromLostUploadResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s3 := &lostProbeResponseS3{MemoryS3Client: storage.NewMemoryS3Client()}
	h := newHandler(metadata.NewInMemoryStore(defaultMetadata()), s3, protocol.MetadataBroker{}, testLogger())
	defer h.coordinator.Stop()
	if err := h.verifyS3(ctx); err != nil {
		t.Fatalf("immutable probe collision prevented startup: %v", err)
	}
	if got := s3.attempts.Load(); got != 2 {
		t.Fatalf("successful probe PUTs=%d", got)
	}
	if objects, err := s3.ListSegments(ctx, "__health/"); err != nil || len(objects) != 0 {
		t.Fatalf("lost response left probe objects behind: %+v, %v", objects, err)
	}
}

// Some S3 implementations return 409 for competing conditional writes rather
// than 412. Keep the real immutable-object collision while exercising both.
type conditionalConflictS3 struct {
	*storage.MemoryS3Client
	conflict error
}

func (s *conditionalConflictS3) UploadSegment(ctx context.Context, key string, body []byte) error {
	err := s.MemoryS3Client.UploadSegment(ctx, key, body)
	if errors.Is(err, storage.ErrPreconditionFailed) {
		return fmt.Errorf("conditional segment PUT: %w", s.conflict)
	}
	return err
}

func TestProduceFencingConflictDoesNotBlockOtherPartitions(t *testing.T) {
	for _, conflict := range []error{storage.ErrPreconditionFailed, storage.ErrConditionalConflict} {
		t.Run(conflict.Error(), func(t *testing.T) {
			endpoints := testutil.StartEmbeddedEtcd(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			meta := defaultMetadata()
			other := meta.Topics[0].Partitions[0]
			other.Partition = 1
			meta.Topics[0].Partitions = append(meta.Topics[0].Partitions, other)
			store, err := metadata.NewEtcdStore(ctx, meta, metadata.EtcdStoreConfig{Endpoints: endpoints})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			s3 := &conditionalConflictS3{MemoryS3Client: storage.NewMemoryS3Client(), conflict: conflict}
			h := newHandler(store, s3, meta.Brokers[0], testLogger())
			defer h.coordinator.Stop()
			defer h.leaseManager.ReleaseAll()
			h.flushOnAck = true
			log, err := h.getPartitionLog(ctx, "orders", 0)
			if err != nil {
				t.Fatal(err)
			}
			storageName, err := h.topicStorageName(ctx, "orders")
			if err != nil {
				t.Fatal(err)
			}
			// Another writer claims the key after A restores its empty partition.
			winner := storage.NewPartitionLog(h.s3Namespace, storageName, 0, 0, s3, nil, h.logConfig, nil, nil, nil)
			batch, err := storage.NewRecordBatchFromBytes(testBatchBytes(0, 1, 2))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := winner.AppendBatches(ctx, []storage.RecordBatch{batch}, true); err != nil {
				t.Fatal(err)
			}
			produce := func(partitions ...int32) []kmsg.ProduceResponseTopicPartition {
				t.Helper()
				req := kmsg.NewPtrProduceRequest()
				req.Acks, req.TimeoutMillis = -1, 1000
				topic := kmsg.ProduceRequestTopic{Topic: "orders"}
				for _, partition := range partitions {
					topic.Partitions = append(topic.Partitions, kmsg.ProduceRequestTopicPartition{Partition: partition, Records: testBatchBytes(0, 0, 1)})
				}
				req.Topics = []kmsg.ProduceRequestTopic{topic}
				raw, err := h.handleProduce(ctx, &protocol.RequestHeader{APIVersion: 9}, req)
				if err != nil {
					t.Fatal(err)
				}
				return decodeKmsgResponse(t, 9, raw, kmsg.NewPtrProduceResponse).Topics[0].Partitions
			}
			// A collision must not reject a healthy partition in the same request.
			response := produce(0, 1)
			if response[0].ErrorCode == protocol.NONE || response[1].ErrorCode != protocol.NONE || response[1].BaseOffset != 0 {
				t.Fatalf("conflict leaked to another partition: %+v", response)
			}
			// Nor may it poison subsequent requests while the same lease is held.
			if p := produce(1)[0]; p.ErrorCode != protocol.NONE || p.BaseOffset != 1 {
				t.Fatalf("subsequent produce rejected: %+v", p)
			}
			if _, err := log.AppendBatches(ctx, []storage.RecordBatch{batch}, true); !errors.Is(err, conflict) {
				t.Fatalf("conflicting log no longer fenced: %v", err)
			}
			if !h.leaseManager.Owns("orders", 0) {
				t.Fatal("test lost the conflicting partition's lease")
			}
			if next, err := store.NextOffset(ctx, "orders", 0); err != nil || next != 0 {
				t.Fatalf("conflict advanced offsets: %d, %v", next, err)
			}
			if snap := h.s3Health.Snapshot(); snap.State != broker.S3StateHealthy || snap.ErrorRate != 0 {
				t.Fatalf("conflict degraded S3: %+v", snap)
			}
			if data, err := winner.Read(ctx, 0, 1<<20); err != nil || !bytes.Equal(data, batch.Bytes) {
				t.Fatalf("conflict changed winner's records: %x, %v", data, err)
			}
		})
	}
}
