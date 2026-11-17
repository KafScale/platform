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

package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/KafScale/platform/internal/testutil"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

type topicAdminClient struct {
	t      *testing.T
	client *http.Client
	url    string
	cookie *http.Cookie
}

func newTopicAdminClient(t *testing.T, store metadata.Store, s3Namespace string) *topicAdminClient {
	t.Helper()
	mux, err := NewMux(ServerOptions{Store: store, Auth: AuthConfig{Username: "demo", Password: "secret"}, S3Namespace: s3Namespace})
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	srv := newIPv4Server(t, mux)
	t.Cleanup(srv.Close)
	client := srv.Client()
	return &topicAdminClient{t: t, client: client, url: srv.URL, cookie: loginForTest(t, client, srv.URL, "demo", "secret")}
}

func (c *topicAdminClient) do(method, path, body string) (int, string) {
	c.t.Helper()
	req, err := http.NewRequest(method, c.url+path, strings.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(c.cookie)
	resp, err := c.client.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatalf("read %s %s: %v", method, path, err)
	}
	return resp.StatusCode, string(out)
}

func etcdTopicPartitions(t *testing.T, endpoints []string, name string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("open etcd store: %v", err)
	}
	defer func() { _ = store.Close() }()
	meta, err := store.Metadata(ctx, []string{name})
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	for _, topic := range meta.Topics {
		if topic.Topic != nil && *topic.Topic == name && topic.ErrorCode == 0 {
			return len(topic.Partitions)
		}
	}
	return 0
}

func startTopicAdminEtcd(t *testing.T, storageNamespace string) ([]string, *clientv3.Client, *metadata.EtcdStore) {
	t.Helper()
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	snapshot, err := json.Marshal(metadata.ClusterMetadata{Brokers: []protocol.MetadataBroker{{NodeID: 0, Host: "broker-0", Port: 9092}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Put(ctx, "/kafscale/metadata/snapshot", string(snapshot)); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{Endpoints: endpoints, StorageNamespace: storageNamespace})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return endpoints, cli, store
}

func TestConsoleTopicCreateAndDeleteReachEtcd(t *testing.T) {
	ctx := context.Background()
	endpoints, cli, store := startTopicAdminEtcd(t, "team-a")
	c := newTopicAdminClient(t, store, "team-a")

	status, body := c.do(http.MethodPost, "/ui/api/status/topics", `{"name":"orders","partitions":3,"replication_factor":1}`)
	if got := etcdTopicPartitions(t, endpoints, "orders"); got != 3 {
		t.Fatalf("after create (status %d %q) etcd has topic orders with %d partitions, want 3", status, body, got)
	}
	if status != http.StatusCreated {
		t.Fatalf("create status = %d %q, want 201", status, body)
	}
	if body != "{\"name\":\"orders\",\"partitions\":3,\"state\":\"ready\"}\n" {
		t.Fatalf("create body = %q, want exactly {\"name\":\"orders\",\"partitions\":3,\"state\":\"ready\"}", body)
	}

	status, body = c.do(http.MethodDelete, "/ui/api/status/topics/orders", "")
	if got := etcdTopicPartitions(t, endpoints, "orders"); got != 0 {
		t.Fatalf("after delete (status %d %q) etcd still has topic orders with %d partitions", status, body, got)
	}
	if status != http.StatusOK {
		t.Fatalf("delete status = %d %q, want 200", status, body)
	}
	deleted, err := cli.Get(ctx, metadata.DeletedTopicPrefix+"orders/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted.Kvs) != 1 || !strings.Contains(string(deleted.Kvs[0].Value), `"team-a/orders/"`) {
		t.Fatalf("deleted topic records = %v, want one record with prefix team-a/orders/", deleted.Kvs)
	}
}

func TestConsoleTopicAdminErrors(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 0, Host: "broker-0", Port: 9092}},
		Topics:  []protocol.MetadataTopic{{Topic: kmsg.StringPtr("existing"), Partitions: []protocol.MetadataPartition{{Partition: 0}}}},
	})
	c := newTopicAdminClient(t, store, "team-a")
	cases := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{name: "create existing", method: http.MethodPost, path: "/ui/api/status/topics", body: `{"name":"existing","partitions":1}`, want: http.StatusConflict},
		{name: "create invalid name", method: http.MethodPost, path: "/ui/api/status/topics", body: `{"name":"bad name","partitions":1}`, want: http.StatusBadRequest},
		{name: "create without partitions", method: http.MethodPost, path: "/ui/api/status/topics", body: `{"name":"fresh"}`, want: http.StatusBadRequest},
		{name: "create with too many partitions", method: http.MethodPost, path: "/ui/api/status/topics", body: `{"name":"fresh","partitions":10001}`, want: http.StatusBadRequest},
		{name: "create with too many replicas", method: http.MethodPost, path: "/ui/api/status/topics", body: `{"name":"fresh","partitions":1,"replication_factor":3}`, want: http.StatusBadRequest},
		{name: "create with trailing data", method: http.MethodPost, path: "/ui/api/status/topics", body: `{"name":"orders","partitions":1} garbage`, want: http.StatusBadRequest},
		{name: "create with a second object", method: http.MethodPost, path: "/ui/api/status/topics", body: `{"name":"orders","partitions":1}{"name":"other","partitions":1}`, want: http.StatusBadRequest},
		{name: "create malformed body", method: http.MethodPost, path: "/ui/api/status/topics", body: `{"name":`, want: http.StatusBadRequest},
		{name: "delete unknown", method: http.MethodDelete, path: "/ui/api/status/topics/missing", want: http.StatusNotFound},
		{name: "delete empty name", method: http.MethodDelete, path: "/ui/api/status/topics/", want: http.StatusBadRequest},
		{name: "delete nested path", method: http.MethodDelete, path: "/ui/api/status/topics/existing/extra", want: http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c.t = t
			if status, body := c.do(tc.method, tc.path, tc.body); status != tc.want {
				t.Fatalf("status = %d %q, want %d", status, body, tc.want)
			}
		})
	}
	meta, err := store.Metadata(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Topics) != 1 || *meta.Topics[0].Topic != "existing" {
		t.Fatalf("topics after failed requests = %+v, want only existing", meta.Topics)
	}
}

func TestConsoleTopicAdminWithoutStore(t *testing.T) {
	c := newTopicAdminClient(t, nil, "team-a")
	if status, body := c.do(http.MethodPost, "/ui/api/status/topics", `{"name":"orders","partitions":1}`); status != http.StatusServiceUnavailable {
		t.Fatalf("create status = %d %q, want 503", status, body)
	}
	if status, body := c.do(http.MethodDelete, "/ui/api/status/topics/orders", ""); status != http.StatusServiceUnavailable {
		t.Fatalf("delete status = %d %q, want 503", status, body)
	}
}

type unavailableStore struct {
	*metadata.InMemoryStore
}

func (unavailableStore) Available() bool { return false }

func TestConsoleTopicAdminEtcdUnavailable(t *testing.T) {
	store := unavailableStore{metadata.NewInMemoryStore(metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{NodeID: 0, Host: "broker-0", Port: 9092}},
		Topics:  []protocol.MetadataTopic{{Topic: kmsg.StringPtr("existing"), Partitions: []protocol.MetadataPartition{{Partition: 0}}}},
	})}
	c := newTopicAdminClient(t, store, "team-a")
	if status, body := c.do(http.MethodPost, "/ui/api/status/topics", `{"name":"orders","partitions":1}`); status != http.StatusServiceUnavailable {
		t.Fatalf("create status = %d %q, want 503", status, body)
	}
	if status, body := c.do(http.MethodDelete, "/ui/api/status/topics/existing", ""); status != http.StatusServiceUnavailable {
		t.Fatalf("delete status = %d %q, want 503", status, body)
	}
	meta, err := store.Metadata(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Topics) != 1 || *meta.Topics[0].Topic != "existing" {
		t.Fatalf("topics = %+v, want only existing", meta.Topics)
	}
}

func TestConsoleTopicDeleteRefusesWithoutS3Namespace(t *testing.T) {
	for _, namespace := range []string{"", "  "} {
		t.Run(fmt.Sprintf("namespace %q", namespace), func(t *testing.T) {
			ctx := context.Background()
			endpoints, cli, store := startTopicAdminEtcd(t, namespace)
			c := newTopicAdminClient(t, store, namespace)
			if status, body := c.do(http.MethodPost, "/ui/api/status/topics", `{"name":"orders","partitions":2}`); status != http.StatusCreated {
				t.Fatalf("create status = %d %q, want 201", status, body)
			}
			status, body := c.do(http.MethodDelete, "/ui/api/status/topics/orders", "")
			if status != http.StatusServiceUnavailable || !strings.Contains(body, "KAFSCALE_S3_NAMESPACE") {
				t.Fatalf("delete status = %d %q, want 503 naming KAFSCALE_S3_NAMESPACE", status, body)
			}
			if got := etcdTopicPartitions(t, endpoints, "orders"); got != 2 {
				t.Fatalf("after refused delete etcd has topic orders with %d partitions, want 2", got)
			}
			deleted, err := cli.Get(ctx, metadata.DeletedTopicPrefix, clientv3.WithPrefix())
			if err != nil {
				t.Fatal(err)
			}
			if len(deleted.Kvs) != 0 {
				t.Fatalf("deleted topic records = %v, want none", deleted.Kvs)
			}
		})
	}
}

type failingTopicStore struct {
	*metadata.InMemoryStore
	err error
}

func (s failingTopicStore) CreateTopic(context.Context, metadata.TopicSpec) (*protocol.MetadataTopic, error) {
	return nil, s.err
}

func (s failingTopicStore) DeleteTopic(context.Context, string) error {
	return s.err
}

func TestConsoleTopicAdminMapsEtcdFailures(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{name: "context deadline from the etcd client", err: context.DeadlineExceeded, want: http.StatusServiceUnavailable},
		{name: "wrapped context deadline", err: fmt.Errorf("read snapshot: %w", context.DeadlineExceeded), want: http.StatusServiceUnavailable},
		{name: "grpc unavailable", err: grpcstatus.Error(codes.Unavailable, "connection refused"), want: http.StatusServiceUnavailable},
		{name: "grpc deadline", err: grpcstatus.Error(codes.DeadlineExceeded, "deadline"), want: http.StatusServiceUnavailable},
		{name: "etcd request timeout", err: rpctypes.ErrTimeout, want: http.StatusServiceUnavailable},
		{name: "etcd no leader", err: rpctypes.ErrNoLeader, want: http.StatusServiceUnavailable},
		{name: "other failure", err: errors.New("snapshot is corrupt"), want: http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := failingTopicStore{InMemoryStore: metadata.NewInMemoryStore(metadata.ClusterMetadata{}), err: tc.err}
			c := newTopicAdminClient(t, store, "team-a")
			if status, body := c.do(http.MethodPost, "/ui/api/status/topics", `{"name":"orders","partitions":1}`); status != tc.want {
				t.Fatalf("create status = %d %q, want %d", status, body, tc.want)
			}
			if status, body := c.do(http.MethodDelete, "/ui/api/status/topics/orders", ""); status != tc.want {
				t.Fatalf("delete status = %d %q, want %d", status, body, tc.want)
			}
		})
	}
}
