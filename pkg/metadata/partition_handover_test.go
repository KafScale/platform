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

package metadata

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
)

func TestPartitionHandoverHoldsPartitionForOtherBroker(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	brokerA := newLeaseManager(t, endpoints, "0", 10)
	brokerB := newLeaseManager(t, endpoints, "1", 10)
	ctx := context.Background()
	if err := brokerA.Acquire(ctx, "orders", 1); err != nil {
		t.Fatalf("broker 0 acquire: %v", err)
	}
	brokerA.Handover("orders", 1, 300*time.Millisecond)
	if brokerA.Owns("orders", 1) {
		t.Fatal("broker 0 still owns orders/1 after handover")
	}
	if err := brokerA.Acquire(ctx, "orders", 1); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("broker 0 acquire during hold = %v, want ErrNotOwner", err)
	}
	if err := brokerB.Acquire(ctx, "orders", 1); err != nil {
		t.Fatalf("broker 1 acquire after handover: %v", err)
	}
	owner, err := brokerA.CurrentOwner(ctx, "orders", 1)
	if err != nil || owner != "1" {
		t.Fatalf("owner = %q, %v; want 1", owner, err)
	}
	if err := brokerA.Acquire(ctx, "orders", 2); err != nil {
		t.Fatalf("hold must only cover the handed over partition: %v", err)
	}
	brokerB.Release("orders", 1)
	time.Sleep(350 * time.Millisecond)
	if err := brokerA.Acquire(ctx, "orders", 1); err != nil {
		t.Fatalf("broker 0 acquire after the hold: %v", err)
	}
}
