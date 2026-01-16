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

package discovery

import (
	"context"
	"testing"
	"time"
)

func TestBuildLeaseAcquireRelease(t *testing.T) {
	lease := newBuildLease()
	ctx := context.Background()
	release, err := lease.Acquire(ctx, "builder", time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := lease.Acquire(ctx, "other", time.Second); err == nil {
		t.Fatalf("expected lease held error")
	}
	release()
	if _, err := lease.Acquire(ctx, "other", time.Second); err != nil {
		t.Fatalf("expected acquire after release")
	}
}

func TestBuildLeaseHolderRequired(t *testing.T) {
	lease := newBuildLease()
	if _, err := lease.Acquire(context.Background(), "", time.Second); err == nil {
		t.Fatalf("expected holder required error")
	}
}

func TestBuildLeaseExpires(t *testing.T) {
	lease := newBuildLease()
	ctx := context.Background()
	_, err := lease.Acquire(ctx, "builder", 5*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := lease.Acquire(ctx, "other", time.Second); err != nil {
		t.Fatalf("expected acquire after expiration")
	}
}
