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
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type recordingBroker struct {
	addr   string
	mu     sync.Mutex
	frames map[int16]int
	dials  atomic.Int32
}

func (b *recordingBroker) seen(key int16) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.frames[key]
}

func startRecordingBroker(t *testing.T) *recordingBroker {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &recordingBroker{addr: listener.Addr().String(), frames: map[int16]int{}}
	var mu sync.Mutex
	var conns []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			b.dials.Add(1)
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			go func() {
				defer c.Close()
				for {
					frame, err := protocol.ReadFrame(c)
					if err != nil {
						return
					}
					header, req, err := protocol.ParseRequest(frame.Payload)
					if err != nil {
						return
					}
					b.mu.Lock()
					b.frames[header.APIKey]++
					b.mu.Unlock()
					if err := protocol.WriteFrame(c, protocol.EncodeResponse(header.CorrelationID, header.APIVersion, req.ResponseKind())); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
	})
	return b
}

// listenerTTLProxy builds a proxy on a real etcd store with partition and
// group routes for broker 0 already cached, the way main wires it.
func listenerTTLProxy(t *testing.T) (*proxy, *recordingBroker, *metadata.EtcdStore) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// Router constructors use this context for both their initial etcd read
	// and their watches. Bound setup without leaving a deadline on watches
	// that must keep running after this helper returns.
	setupTimer := time.AfterFunc(15*time.Second, cancel)
	defer setupTimer.Stop()
	endpoints := testutil.StartEmbeddedEtcd(t)
	broker := startRecordingBroker(t)
	putProxyTestSnapshot(t, endpoints, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{brokerAt(t, 0, broker.addr)},
		Topics:  []protocol.MetadataTopic{{Topic: kmsg.StringPtr("orders"), Partitions: []protocol.MetadataPartition{{Partition: 0, Leader: 0}}}},
	})
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if _, err := cli.Put(ctx, metadata.PartitionLeasePrefix()+"/orders/0", "0"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(ctx, metadata.GroupLeasePrefix()+"/g1", "0"); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("NewEtcdStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	p := lfsTestProxy(store, time.Minute)
	p.advertisedHost, p.advertisedPort = "entry", 29092
	if p.router, err = metadata.NewPartitionRouter(ctx, store.EtcdClient(), p.logger); err != nil {
		t.Fatalf("initialize partition router: %v", err)
	}
	t.Cleanup(p.router.Stop)
	if p.groupRouter, err = metadata.NewGroupRouter(ctx, store.EtcdClient(), p.logger); err != nil {
		t.Fatalf("initialize group router: %v", err)
	}
	t.Cleanup(p.groupRouter.Stop)
	p.refreshMetadataCache(ctx)
	if err := ctx.Err(); err != nil {
		t.Fatalf("proxy fixture setup exceeded 15s: %v", err)
	}
	if !p.isReady() || !p.cacheFresh() || p.router.LookupOwner("orders", 0) != "0" || p.groupRouter.LookupOwner("g1") != "0" {
		t.Fatal("proxy did not load routes with etcd up")
	}
	return p, broker, store
}

type listenerClient struct {
	t           *testing.T
	conn        net.Conn
	formatter   *kmsg.RequestFormatter
	correlation int32
}

func dialListener(t *testing.T, p *proxy) *listenerClient {
	t.Helper()
	server, client := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.handleConnection(ctx, server) }()
	t.Cleanup(func() {
		defer cancel()
		_ = client.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			cancel()
			_ = server.Close()
			t.Error("listener did not stop within 5s after client disconnect")
		}
	})
	return &listenerClient{t: t, conn: client, formatter: kmsg.NewRequestFormatter(kmsg.FormatterClientID("client"))}
}

func (c *listenerClient) expectClosed() {
	c.t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil && err != io.ErrClosedPipe {
		c.t.Fatal(err)
	}
	var extra [1]byte
	if n, err := c.conn.Read(extra[:]); n != 0 || err != io.EOF {
		c.t.Fatalf("listener did not close after stale-cache response: read %d bytes, error %v", n, err)
	}
}

func (c *listenerClient) roundTrip(req kmsg.Request) kmsg.Response {
	c.t.Helper()
	c.correlation++
	_ = c.conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.conn.Write(c.formatter.AppendRequest(nil, req, c.correlation)); err != nil {
		c.t.Fatalf("write %T: %v", req, err)
	}
	frame, err := protocol.ReadFrame(c.conn)
	if err != nil {
		c.t.Fatalf("read %T response: %v", req, err)
	}
	resp := req.ResponseKind()
	resp.SetVersion(req.GetVersion())
	skip := 4
	if resp.IsFlexible() {
		skip = 5
	}
	if err := resp.ReadFrom(frame.Payload[skip:]); err != nil {
		c.t.Fatalf("decode %T: %v", resp, err)
	}
	return resp
}

func ageBackendCache(p *proxy) {
	atomic.StoreInt64(&p.lastHealthy, time.Now().Add(-2*p.cacheTTL).UnixNano())
}

type listenerCase struct {
	name string
	key  int16
	req  func() kmsg.Request
	// code returns the error code a client sees for its request.
	code func(kmsg.Response) int16
	// served reports whether the response came from cluster state.
	served func(kmsg.Response) bool
}

func listenerCases() []listenerCase {
	return []listenerCase{
		{
			name: "metadata",
			key:  protocol.APIKeyMetadata,
			req: func() kmsg.Request {
				r := kmsg.NewPtrMetadataRequest()
				r.Version = 12
				topic := kmsg.NewMetadataRequestTopic()
				topic.Topic = kmsg.StringPtr("orders")
				r.Topics = append(r.Topics, topic)
				return r
			},
			code: func(r kmsg.Response) int16 { return r.(*kmsg.MetadataResponse).Topics[0].ErrorCode },
			served: func(r kmsg.Response) bool {
				m := r.(*kmsg.MetadataResponse)
				return len(m.Brokers) == 1 && m.Brokers[0].Host == "entry" && len(m.Topics) == 1 && m.Topics[0].ErrorCode == protocol.NONE
			},
		},
		{
			name: "find coordinator",
			key:  protocol.APIKeyFindCoordinator,
			req: func() kmsg.Request {
				r := kmsg.NewPtrFindCoordinatorRequest()
				r.Version = 3
				r.CoordinatorKey = "g1"
				return r
			},
			code: func(r kmsg.Response) int16 { return r.(*kmsg.FindCoordinatorResponse).ErrorCode },
			served: func(r kmsg.Response) bool {
				f := r.(*kmsg.FindCoordinatorResponse)
				return f.ErrorCode == protocol.NONE && f.Host == "entry"
			},
		},
		{
			name: "produce",
			key:  protocol.APIKeyProduce,
			req: func() kmsg.Request {
				r := kmsg.NewPtrProduceRequest()
				r.Version = 9
				r.Acks = 1
				r.TimeoutMillis = 1000
				topic := kmsg.NewProduceRequestTopic()
				topic.Topic = "orders"
				part := kmsg.NewProduceRequestTopicPartition()
				part.Records = []byte{}
				topic.Partitions = append(topic.Partitions, part)
				r.Topics = append(r.Topics, topic)
				return r
			},
			code: func(r kmsg.Response) int16 {
				p := r.(*kmsg.ProduceResponse)
				if len(p.Topics) == 0 || len(p.Topics[0].Partitions) == 0 {
					return -2
				}
				return p.Topics[0].Partitions[0].ErrorCode
			},
		},
		{
			name: "fetch",
			key:  protocol.APIKeyFetch,
			req: func() kmsg.Request {
				r := kmsg.NewPtrFetchRequest()
				r.Version = 12
				r.MaxWaitMillis = 10
				r.MaxBytes = 1 << 20
				topic := kmsg.NewFetchRequestTopic()
				topic.Topic = "orders"
				part := kmsg.NewFetchRequestTopicPartition()
				part.PartitionMaxBytes = 1 << 20
				topic.Partitions = append(topic.Partitions, part)
				r.Topics = append(r.Topics, topic)
				return r
			},
			code: func(r kmsg.Response) int16 { return r.(*kmsg.FetchResponse).ErrorCode },
		},
		{
			name: "join group",
			key:  protocol.APIKeyJoinGroup,
			req: func() kmsg.Request {
				r := kmsg.NewPtrJoinGroupRequest()
				r.Version = 5
				r.Group = "g1"
				r.SessionTimeoutMillis = 10000
				r.RebalanceTimeoutMillis = 10000
				r.ProtocolType = "consumer"
				return r
			},
			code: func(r kmsg.Response) int16 { return r.(*kmsg.JoinGroupResponse).ErrorCode },
		},
		{
			name: "list offsets",
			key:  protocol.APIKeyListOffsets,
			req: func() kmsg.Request {
				r := kmsg.NewPtrListOffsetsRequest()
				r.Version = 4
				r.ReplicaID = -1
				topic := kmsg.NewListOffsetsRequestTopic()
				topic.Topic = "orders"
				part := kmsg.NewListOffsetsRequestTopicPartition()
				part.Timestamp = -1
				topic.Partitions = append(topic.Partitions, part)
				r.Topics = append(r.Topics, topic)
				return r
			},
			code: func(r kmsg.Response) int16 {
				l := r.(*kmsg.ListOffsetsResponse)
				if len(l.Topics) == 0 || len(l.Topics[0].Partitions) == 0 {
					return -2
				}
				return l.Topics[0].Partitions[0].ErrorCode
			},
		},
	}
}

// TestListenerStopsRoutingPastCacheTTL sends each request once with etcd up,
// so routes, broker addresses and backend connections are cached, then again
// after etcd has been unreadable for longer than the cache TTL.
func TestListenerStopsRoutingPastCacheTTL(t *testing.T) {
	for _, tc := range listenerCases() {
		t.Run(tc.name, func(t *testing.T) {
			p, broker, store := listenerTTLProxy(t)
			client := dialListener(t, p)
			first := client.roundTrip(tc.req())
			if tc.served != nil && !tc.served(first) {
				t.Fatalf("not served with etcd up: %+v", first)
			}
			forwarded := broker.seen(tc.key)
			dials := broker.dials.Load()
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			ageBackendCache(p)
			resp := client.roundTrip(tc.req())
			if tc.served != nil && tc.served(resp) {
				t.Errorf("answered from the stale snapshot: %+v", resp)
			}
			if code := tc.code(resp); code != protocol.REQUEST_TIMED_OUT {
				t.Errorf("error code %d, want REQUEST_TIMED_OUT (%d)", code, protocol.REQUEST_TIMED_OUT)
			}
			if got := broker.seen(tc.key); got != forwarded {
				t.Errorf("forwarded %d request(s) to the broker from stale routes", got-forwarded)
			}
			if got := broker.dials.Load(); got != dials {
				t.Errorf("opened %d new broker connection(s) from stale routes", got-dials)
			}
			client.expectClosed()
		})
	}
}

// TestListenerServesDuringEtcdBlipWithinCacheTTL keeps today's behavior: an
// etcd outage shorter than the TTL does not interrupt the listener.
func TestListenerServesDuringEtcdBlipWithinCacheTTL(t *testing.T) {
	for _, tc := range listenerCases() {
		t.Run(tc.name, func(t *testing.T) {
			p, broker, store := listenerTTLProxy(t)
			client := dialListener(t, p)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			before := broker.seen(tc.key)
			resp := client.roundTrip(tc.req())
			if tc.served != nil {
				if !tc.served(resp) {
					t.Fatalf("not served during an etcd blip: %+v", resp)
				}
				return
			}
			if code := tc.code(resp); code == protocol.REQUEST_TIMED_OUT {
				t.Errorf("rejected during an etcd blip")
			}
			if broker.seen(tc.key) != before+1 {
				t.Errorf("request did not reach the broker during an etcd blip")
			}
		})
	}
}

// TestListenerRecoversWhenEtcdIsReadableAgain checks that a stale cache does
// not keep the listener closed once etcd answers again.
func TestListenerRecoversWhenEtcdIsReadableAgain(t *testing.T) {
	p, broker, _ := listenerTTLProxy(t)
	ageBackendCache(p)
	client := dialListener(t, p)
	cases := listenerCases()
	produce := cases[2]
	if code := produce.code(client.roundTrip(produce.req())); code == protocol.REQUEST_TIMED_OUT {
		t.Fatal("produce rejected although etcd is readable")
	}
	if broker.seen(protocol.APIKeyProduce) != 1 {
		t.Fatal("produce did not reach the broker")
	}
	if !p.cacheFresh() {
		t.Fatal("successful etcd read did not refresh the cache")
	}
}

// TestRoutingHelpersRefuseStaleCache covers callers below the dispatch gate.
func TestRoutingHelpersRefuseStaleCache(t *testing.T) {
	p, broker, store := listenerTTLProxy(t)
	ctx := context.Background()
	pool := newConnPool(time.Second)
	pool.dial = p.dialBackend
	defer pool.Close()
	addr := p.brokerIDToAddr(ctx, "0")
	conn, _, err := p.connectForAddr(ctx, addr, nil, pool)
	if err != nil {
		t.Fatalf("connect with etcd up: %v", err)
	}
	pool.Return(addr, conn)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	ageBackendCache(p)
	dials := broker.dials.Load()
	if conn, _, err := p.connectForAddr(ctx, addr, nil, pool); err == nil {
		_ = conn.Close()
		t.Error("connectForAddr reused a pooled connection from stale routes")
	}
	if conn, _, err := p.connectForAddr(ctx, addr, nil, newConnPool(time.Second)); err == nil {
		_ = conn.Close()
		t.Error("connectForAddr dialed a broker from stale routes")
	}
	req := listenerCases()[0].req()
	payload := kmsg.NewRequestFormatter().AppendRequest(nil, req, 1)[4:]
	header, _, err := protocol.ParseRequestHeader(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.handleMetadata(ctx, header, payload, pool); err == nil {
		t.Error("handleMetadata answered from the stale snapshot")
	}
	if broker.dials.Load() != dials {
		t.Error("opened a broker connection from stale routes")
	}
}
