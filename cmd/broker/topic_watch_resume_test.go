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

package main

import (
	"context"
	"errors"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/storage"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"
	"time"
)

type resumeTopicWatch struct {
	t          *testing.T
	cancel     context.CancelFunc
	watchCalls int
	gets       int
	compact    bool
	getError   bool
}

func (f *resumeTopicWatch) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	f.gets++
	if f.getError && f.gets == 1 {
		return nil, errors.New("temporary read failure")
	}
	return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 10}, Kvs: []*mvccpb.KeyValue{{Key: []byte(metadata.TopicGenerationKey("orders"))}}}, nil
}
func (f *resumeTopicWatch) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	f.watchCalls++
	ch := make(chan clientv3.WatchResponse, 1)
	want := int64(11)
	if f.watchCalls > 1 && !f.compact {
		want = 13
	}
	if rev := clientv3.OpGet(key, opts...).Rev(); rev != want {
		f.t.Errorf("watch revision %d, want %d", rev, want)
	}
	if f.watchCalls == 1 {
		if f.compact {
			ch <- clientv3.WatchResponse{Canceled: true, CompactRevision: 12}
		} else {
			ch <- clientv3.WatchResponse{Header: etcdserverpb.ResponseHeader{Revision: 12}, Events: []*clientv3.Event{{Kv: &mvccpb.KeyValue{Key: []byte(metadata.TopicGenerationKey("payments"))}}}}
		}
	} else {
		f.cancel()
	}
	close(ch)
	return ch
}
func TestTopicDeleteWatchResumesAfterInterruption(t *testing.T) {
	for _, compact := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		f := &resumeTopicWatch{t: t, cancel: cancel, compact: compact, getError: true}
		h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
		h.logs["orders"] = map[int32]*storage.PartitionLog{0: nil}
		h.logs["payments"] = map[int32]*storage.PartitionLog{0: nil}
		h.watchTopicDeletes(ctx, f)
		cancel()
		if f.watchCalls != 2 {
			t.Fatalf("watch calls %d", f.watchCalls)
		}
		if _, ok := h.logs["orders"]; ok {
			t.Fatal("snapshot did not retire old log")
		}
		if !compact {
			if _, ok := h.logs["payments"]; ok {
				t.Fatal("watch event not applied")
			}
		}
		want := 2
		if compact {
			want = 3
		}
		if f.gets != want {
			t.Fatalf("snapshot calls %d, want %d", f.gets, want)
		}
	}
}

func TestTopicDeleteWatchStopsDuringSnapshotRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
	h.watchTopicDeletes(ctx, &cancelingTopicSnapshot{cancel: cancel})
}

type cancelingTopicSnapshot struct{ cancel context.CancelFunc }

func (f *cancelingTopicSnapshot) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	f.cancel()
	return nil, errors.New("canceled snapshot")
}
func (f *cancelingTopicSnapshot) Watch(context.Context, string, ...clientv3.OpOption) clientv3.WatchChan {
	panic("watch should not start")
}
