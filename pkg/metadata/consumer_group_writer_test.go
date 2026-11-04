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

package metadata

import (
	"context"
	"errors"
	"testing"
	"time"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
)

func TestConsumerGroupWriteWaitRespectsContext(t *testing.T) {
	store := &EtcdStore{}
	unlock, err := store.lockConsumerGroupWrite(context.Background(), "g")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	// A different group remains independent of a blocked group.
	other, err := store.lockConsumerGroupWrite(context.Background(), "other")
	if err != nil {
		t.Fatal(err)
	}
	other()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := store.lockConsumerGroupWrite(ctx, "g"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting writer: %v", err)
	}
	group := &metadatapb.ConsumerGroup{GroupId: "g", Members: map[string]*metadatapb.GroupMember{"m": {}}}
	if _, err := store.putConsumerGroup(ctx, group); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting heartbeat: %v", err)
	}
	if err := store.CommitMemberConsumerOffset(ctx, "g", 1, "m", 1, "t", 0, 1, ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting commit: %v", err)
	}
	store.groupWriteMu.Lock()
	defer store.groupWriteMu.Unlock()
	if len(store.groupWrites) != 1 || store.groupWrites["g"].users != 1 {
		t.Fatalf("waiters leaked: %+v", store.groupWrites)
	}
}

func TestConsumerGroupWriteQueueIsReleased(t *testing.T) {
	store := &EtcdStore{}
	unlock, err := store.lockConsumerGroupWrite(context.Background(), "g")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		next, err := store.lockConsumerGroupWrite(context.Background(), "g")
		if err == nil {
			next()
		}
		result <- err
	}()
	unlock()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued writer blocked after release")
	}
	store.groupWriteMu.Lock()
	defer store.groupWriteMu.Unlock()
	if len(store.groupWrites) != 0 {
		t.Fatalf("group locks leaked: %+v", store.groupWrites)
	}
}
