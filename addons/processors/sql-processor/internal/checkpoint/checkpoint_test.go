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

package checkpoint

import (
	"context"
	"testing"
)

func TestNoopStore(t *testing.T) {
	store := New()
	lease, err := store.ClaimLease(context.Background(), "orders", 1, "worker")
	if err != nil {
		t.Fatalf("claim lease: %v", err)
	}
	if lease.Topic != "orders" || lease.Partition != 1 {
		t.Fatalf("unexpected lease: %+v", lease)
	}
	if err := store.RenewLease(context.Background(), lease); err != nil {
		t.Fatalf("renew lease: %v", err)
	}
	if err := store.ReleaseLease(context.Background(), lease); err != nil {
		t.Fatalf("release lease: %v", err)
	}
	offset, err := store.LoadOffset(context.Background(), "orders", 1)
	if err != nil {
		t.Fatalf("load offset: %v", err)
	}
	if offset.Topic != "orders" || offset.Partition != 1 {
		t.Fatalf("unexpected offset: %+v", offset)
	}
	if err := store.CommitOffset(context.Background(), offset); err != nil {
		t.Fatalf("commit offset: %v", err)
	}
}
