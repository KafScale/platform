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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/internal/testutil"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
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

func TestPartitionHandoverFencesInFlightAcquisition(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	committed, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: time.Second, DialOptions: []grpc.DialOption{
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			err := invoke(ctx, method, req, reply, cc, opts...)
			if method == "/etcdserverpb.KV/Txn" && err == nil {
				once.Do(func() { close(committed); <-resume })
			}
			return err
		}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	a := NewPartitionLeaseManager(cli, PartitionLeaseConfig{BrokerID: "0"})
	defer a.ReleaseAll()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- a.Acquire(ctx, "orders", 1) }()
	select {
	case <-committed:
	case <-ctx.Done():
		close(resume)
		t.Fatal("acquisition never reached the committed transaction")
	}
	a.Handover("orders", 1, time.Minute)
	close(resume)
	if err := <-result; !errors.Is(err, ErrNotOwner) {
		t.Fatalf("in-flight acquisition bypassed handover: %v", err)
	}
	if a.Owns("orders", 1) {
		t.Fatal("handed-over partition was reacquired")
	}
	b := newLeaseManager(t, endpoints, "1", 10)
	if err := b.Acquire(ctx, "orders", 1); err != nil {
		t.Fatalf("in-flight acquisition left a lease blocking the next broker: %v", err)
	}
}

func TestLateAcquireDoesNotReleaseLeaseWhileAppendDrains(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	draining, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	a := NewPartitionLeaseManagerWithOptions(newEtcdClientForTest(t, testutil.StartEmbeddedEtcd(t)), PartitionLeaseOptions{
		PartitionLeaseConfig: PartitionLeaseConfig{BrokerID: "0"},
		OnHandover: func(_ string, _ int32, release func()) {
			close(draining)
			<-resume
			release()
		},
	})
	defer a.ReleaseAll()
	defer once.Do(func() { close(resume) })
	if err := a.Acquire(ctx, "orders", 1); err != nil {
		t.Fatal(err)
	}
	go func() { a.Handover("orders", 1, time.Minute); close(done) }()
	select {
	case <-draining:
	case <-ctx.Done():
		t.Fatal("handover did not start draining")
	}
	// An Acquire that used this existing ownership resumes its final admission
	// check after handover has started. It must reject the new request without
	// releasing the lease protecting the already active append.
	if err := a.finishAcquire(partitionResourceID("orders", 1)); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("late request was admitted during draining: %v", err)
	}
	if !a.Owns("orders", 1) {
		t.Fatal("late acquisition cleanup released an actively draining lease")
	}
	once.Do(func() { close(resume) })
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("handover did not finish after draining")
	}
	if a.Owns("orders", 1) {
		t.Fatal("lease was not released after draining finished")
	}
}

func TestOlderHandoverCannotOverwriteNewerHold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var calls atomic.Int32
	entered := []chan struct{}{make(chan struct{}), make(chan struct{})}
	resume := []chan struct{}{make(chan struct{}), make(chan struct{})}
	done := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var once [2]sync.Once
	a := NewPartitionLeaseManagerWithOptions(newEtcdClientForTest(t, testutil.StartEmbeddedEtcd(t)), PartitionLeaseOptions{
		PartitionLeaseConfig: PartitionLeaseConfig{BrokerID: "0"},
		OnHandover: func(_ string, _ int32, release func()) {
			i := int(calls.Add(1)) - 1
			close(entered[i])
			<-resume[i]
			release()
		},
	})
	defer a.ReleaseAll()
	defer func() {
		for i := range resume {
			once[i].Do(func() { close(resume[i]) })
		}
	}()
	if err := a.Acquire(ctx, "orders", 1); err != nil {
		t.Fatal(err)
	}
	for i, hold := range []time.Duration{0, time.Hour} {
		go func() { a.Handover("orders", 1, hold); close(done[i]) }()
		select {
		case <-entered[i]:
		case <-ctx.Done():
			t.Fatal("handover did not start")
		}
	}
	// Complete the new handover first, then resume an older callback.
	for _, i := range []int{1, 0} {
		once[i].Do(func() { close(resume[i]) })
		select {
		case <-done[i]:
		case <-ctx.Done():
			t.Fatal("handover did not finish")
		}
	}
	if err := a.Acquire(ctx, "orders", 1); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("older handover removed the newer hold: %v", err)
	}
}

func TestPartitionReleaseLateDeleteKeepsReacquiredLease(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	deleting, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: time.Second, DialOptions: []grpc.DialOption{
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			if txn, ok := req.(*etcdserverpb.TxnRequest); ok {
				for _, op := range txn.Success {
					if op.GetRequestDeleteRange() != nil {
						once.Do(func() { close(deleting); <-resume })
					}
				}
			}
			return invoke(ctx, method, req, reply, cc, opts...)
		}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	a := NewPartitionLeaseManager(cli, PartitionLeaseConfig{BrokerID: "0"})
	defer a.ReleaseAll()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.Acquire(ctx, "orders", 1); err != nil {
		t.Fatal(err)
	}
	oldToken := a.OwnershipToken("orders", 1)
	done := make(chan struct{})
	go func() { a.Release("orders", 1); close(done) }()
	select {
	case <-deleting:
	case <-ctx.Done():
		close(resume)
		t.Fatal("release did not reach the delete transaction")
	}
	if err := a.Acquire(ctx, "orders", 1); err != nil {
		close(resume)
		t.Fatal(err)
	}
	if token := a.OwnershipToken("orders", 1); token == 0 || token == oldToken {
		close(resume)
		t.Fatal("partition did not get a new acquisition token")
	}
	close(resume)
	<-done
	owner, err := a.CurrentOwner(ctx, "orders", 1)
	if err != nil || owner != "0" || !a.Owns("orders", 1) {
		t.Fatalf("late release deleted the replacement acquisition: owner=%q, error=%v", owner, err)
	}
	b := newLeaseManager(t, endpoints, "1", 10)
	if err := b.Acquire(ctx, "orders", 1); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("second broker acquired a partition still owned locally: %v", err)
	}
}
