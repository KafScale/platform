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

package metadata

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// Embedding only the historical interface deliberately hides optional methods.
type oldOffsetStore struct {
	Store
	offsets []ConsumerOffset
	err     error
	calls   int
}

func (s *oldOffsetStore) ListConsumerOffsets(context.Context) ([]ConsumerOffset, error) {
	s.calls++
	return s.offsets, s.err
}

type scopedOffsetStore struct {
	*oldOffsetStore
	group string
	calls int
}

func (s *scopedOffsetStore) ListGroupConsumerOffsets(_ context.Context, group string) ([]ConsumerOffset, error) {
	s.calls++
	s.group = group
	return s.offsets, s.err
}
func TestGroupOffsetsAdapter(t *testing.T) {
	want := ConsumerOffset{Group: "g", Topic: "t", Partition: 3, Offset: 42, Metadata: "checkpoint"}
	old := &oldOffsetStore{offsets: []ConsumerOffset{{Group: "g-other", Topic: "secret"}, want, {Group: "g/child", Topic: "nested"}}}
	got, err := ListGroupConsumerOffsets(context.Background(), old, "g")
	if err != nil || !reflect.DeepEqual(got, []ConsumerOffset{want}) || old.calls != 1 {
		t.Fatal(got, err, old.calls)
	}
	got, err = ListGroupConsumerOffsets(context.Background(), old, "missing")
	if err != nil || len(got) != 0 {
		t.Fatal(got, err)
	}
	sentinel := errors.New("backend unavailable")
	old.err = sentinel
	if got, err := ListGroupConsumerOffsets(context.Background(), old, "g"); !errors.Is(err, sentinel) || got != nil {
		t.Fatal(got, err)
	}
	scoped := &scopedOffsetStore{oldOffsetStore: &oldOffsetStore{offsets: []ConsumerOffset{want}}}
	got, err = ListGroupConsumerOffsets(context.Background(), scoped, "g")
	if err != nil || !reflect.DeepEqual(got, []ConsumerOffset{want}) || scoped.group != "g" || scoped.calls != 1 || scoped.oldOffsetStore.calls != 0 {
		t.Fatal(got, err, scoped)
	}
	scoped.err = sentinel
	if _, err := ListGroupConsumerOffsets(context.Background(), scoped, "g"); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	for _, store := range []Store{old, scoped} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := ListGroupConsumerOffsets(ctx, store, "g"); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
}
