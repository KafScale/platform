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
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

func TestBuildProxyMetadataResponseRewritesBrokers(t *testing.T) {
	meta := &metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "broker-1", Port: 9092},
		},
		Topics: []protocol.MetadataTopic{
			{
				Name:    "orders",
				TopicID: metadata.TopicIDForName("orders"),
				Partitions: []protocol.MetadataPartition{
					{
						PartitionIndex: 0,
						LeaderID:       1,
						ReplicaNodes:   []int32{1, 2},
						ISRNodes:       []int32{1},
					},
				},
			},
		},
	}

	resp := buildProxyMetadataResponse(meta, 12, 12, "proxy.example.com", 9092)
	if len(resp.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(resp.Brokers))
	}
	if resp.Brokers[0].NodeID != 0 || resp.Brokers[0].Host != "proxy.example.com" || resp.Brokers[0].Port != 9092 {
		t.Fatalf("unexpected broker: %+v", resp.Brokers[0])
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	part := resp.Topics[0].Partitions[0]
	if part.LeaderID != 0 {
		t.Fatalf("expected leader 0, got %d", part.LeaderID)
	}
	if len(part.ReplicaNodes) != 1 || part.ReplicaNodes[0] != 0 {
		t.Fatalf("expected replica nodes [0], got %+v", part.ReplicaNodes)
	}
	if len(part.ISRNodes) != 1 || part.ISRNodes[0] != 0 {
		t.Fatalf("expected ISR nodes [0], got %+v", part.ISRNodes)
	}
}

func TestBuildProxyMetadataResponsePreservesTopicErrors(t *testing.T) {
	meta := &metadata.ClusterMetadata{
		Topics: []protocol.MetadataTopic{
			{
				Name:      "missing",
				ErrorCode: protocol.UNKNOWN_TOPIC_OR_PARTITION,
			},
		},
	}
	resp := buildProxyMetadataResponse(meta, 1, 1, "proxy", 9092)
	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
	}
	if resp.Topics[0].ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatalf("expected error code %d, got %d", protocol.UNKNOWN_TOPIC_OR_PARTITION, resp.Topics[0].ErrorCode)
	}
}

func TestBuildNotReadyResponseProduce(t *testing.T) {
	payload := encodeProduceRequestV3("orders", 0)
	header, _, err := protocol.ParseRequestHeader(payload)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	p := &proxy{}
	respBytes, ok, err := p.buildNotReadyResponse(header, payload)
	if err != nil {
		t.Fatalf("build not-ready response: %v", err)
	}
	if !ok {
		t.Fatalf("expected produce response, got ok=false")
	}
	errCode, err := decodeProduceResponseError(respBytes, header.APIVersion)
	if err != nil {
		t.Fatalf("decode produce response: %v", err)
	}
	if errCode != protocol.REQUEST_TIMED_OUT {
		t.Fatalf("expected error %d, got %d", protocol.REQUEST_TIMED_OUT, errCode)
	}
}

func TestBuildNotReadyResponseFetch(t *testing.T) {
	payload := encodeFetchRequestV7("orders", 0)
	header, _, err := protocol.ParseRequestHeader(payload)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}
	p := &proxy{}
	respBytes, ok, err := p.buildNotReadyResponse(header, payload)
	if err != nil {
		t.Fatalf("build not-ready response: %v", err)
	}
	if !ok {
		t.Fatalf("expected fetch response, got ok=false")
	}
	topErr, partErr, err := decodeFetchResponseErrors(respBytes, header.APIVersion)
	if err != nil {
		t.Fatalf("decode fetch response: %v", err)
	}
	if topErr != protocol.REQUEST_TIMED_OUT {
		t.Fatalf("expected top-level error %d, got %d", protocol.REQUEST_TIMED_OUT, topErr)
	}
	if partErr != protocol.REQUEST_TIMED_OUT {
		t.Fatalf("expected partition error %d, got %d", protocol.REQUEST_TIMED_OUT, partErr)
	}
}

func TestSplitCSV(t *testing.T) {
	parts := splitCSV(" a, ,b,, c ")
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts got %d", len(parts))
	}
	if parts[0] != "a" || parts[1] != "b" || parts[2] != "c" {
		t.Fatalf("unexpected parts: %v", parts)
	}
	if out := splitCSV("   "); out != nil {
		t.Fatalf("expected nil for empty input, got %v", out)
	}
}

func TestEnvParsingHelpers(t *testing.T) {
	t.Setenv("PROXY_PORT", "9093")
	t.Setenv("PROXY_INT", "42")
	if got := envPort("PROXY_PORT", 9092); got != 9093 {
		t.Fatalf("expected 9093 got %d", got)
	}
	if got := envInt("PROXY_INT", 1); got != 42 {
		t.Fatalf("expected 42 got %d", got)
	}
	t.Setenv("PROXY_PORT", "bad")
	t.Setenv("PROXY_INT", "bad")
	if got := envPort("PROXY_PORT", 9092); got != 9092 {
		t.Fatalf("expected fallback got %d", got)
	}
	if got := envInt("PROXY_INT", 7); got != 7 {
		t.Fatalf("expected fallback got %d", got)
	}
}

func TestPortFromAddr(t *testing.T) {
	if got := portFromAddr("127.0.0.1:9099", 9092); got != 9099 {
		t.Fatalf("expected port 9099 got %d", got)
	}
	if got := portFromAddr("bad", 9092); got != 9092 {
		t.Fatalf("expected fallback got %d", got)
	}
}

type testWriter struct {
	buf bytes.Buffer
}

func (w *testWriter) Int8(v int8) {
	w.buf.WriteByte(byte(v))
}

func (w *testWriter) Int16(v int16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	w.buf.Write(tmp[:])
}

func (w *testWriter) Int32(v int32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	w.buf.Write(tmp[:])
}

func (w *testWriter) Int64(v int64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	w.buf.Write(tmp[:])
}

func (w *testWriter) String(v string) {
	if v == "" {
		w.Int16(0)
		return
	}
	w.Int16(int16(len(v)))
	w.buf.WriteString(v)
}

func (w *testWriter) NullableString(v *string) {
	if v == nil {
		w.Int16(-1)
		return
	}
	w.String(*v)
}

func (w *testWriter) Bytes(b []byte) {
	w.Int32(int32(len(b)))
	w.buf.Write(b)
}

func encodeProduceRequestV3(topic string, partition int32) []byte {
	w := &testWriter{}
	w.Int16(protocol.APIKeyProduce)
	w.Int16(3)
	w.Int32(42)
	clientID := "proxy-test"
	w.NullableString(&clientID)
	w.NullableString(nil)
	w.Int16(1)
	w.Int32(1000)
	w.Int32(1)
	w.String(topic)
	w.Int32(1)
	w.Int32(partition)
	w.Bytes(nil)
	return w.buf.Bytes()
}

func encodeFetchRequestV7(topic string, partition int32) []byte {
	w := &testWriter{}
	w.Int16(protocol.APIKeyFetch)
	w.Int16(7)
	w.Int32(7)
	clientID := "proxy-test"
	w.NullableString(&clientID)
	w.Int32(-1)
	w.Int32(1000)
	w.Int32(1)
	w.Int32(1048576)
	w.Int8(0)
	w.Int32(0)
	w.Int32(0)
	w.Int32(1)
	w.String(topic)
	w.Int32(1)
	w.Int32(partition)
	w.Int64(0)
	w.Int64(0)
	w.Int32(1048576)
	w.Int32(0)
	return w.buf.Bytes()
}

func decodeProduceResponseError(payload []byte, version int16) (int16, error) {
	r := bytes.NewReader(payload)
	if _, err := readInt32(r); err != nil {
		return 0, err
	}
	topicCount, err := readInt32(r)
	if err != nil {
		return 0, err
	}
	if topicCount < 1 {
		return 0, nil
	}
	if _, err := readString(r); err != nil {
		return 0, err
	}
	partCount, err := readInt32(r)
	if err != nil {
		return 0, err
	}
	if partCount < 1 {
		return 0, nil
	}
	if _, err := readInt32(r); err != nil {
		return 0, err
	}
	errCode, err := readInt16(r)
	if err != nil {
		return 0, err
	}
	return errCode, nil
}

func decodeFetchResponseErrors(payload []byte, version int16) (int16, int16, error) {
	r := bytes.NewReader(payload)
	if _, err := readInt32(r); err != nil {
		return 0, 0, err
	}
	if _, err := readInt32(r); err != nil {
		return 0, 0, err
	}
	errCode, err := readInt16(r)
	if err != nil {
		return 0, 0, err
	}
	if _, err := readInt32(r); err != nil {
		return 0, 0, err
	}
	topicCount, err := readInt32(r)
	if err != nil {
		return 0, 0, err
	}
	if topicCount < 1 {
		return errCode, 0, nil
	}
	if _, err := readString(r); err != nil {
		return 0, 0, err
	}
	partCount, err := readInt32(r)
	if err != nil {
		return 0, 0, err
	}
	if partCount < 1 {
		return errCode, 0, nil
	}
	if _, err := readInt32(r); err != nil {
		return 0, 0, err
	}
	partErr, err := readInt16(r)
	if err != nil {
		return 0, 0, err
	}
	return errCode, partErr, nil
}

func readInt16(r *bytes.Reader) (int16, error) {
	var v int16
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func readInt32(r *bytes.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func readString(r *bytes.Reader) (string, error) {
	var size int16
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return "", err
	}
	if size < 0 {
		return "", nil
	}
	buf := make([]byte, size)
	if _, err := r.Read(buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func makeProduceRequest(topics map[string][]int32) *protocol.ProduceRequest {
	req := &protocol.ProduceRequest{Acks: -1, TimeoutMs: 5000}
	for name, parts := range topics {
		topic := protocol.ProduceTopic{Name: name}
		for _, p := range parts {
			topic.Partitions = append(topic.Partitions, protocol.ProducePartition{
				Partition: p,
				Records:   []byte{1, 2, 3},
			})
		}
		req.Topics = append(req.Topics, topic)
	}
	return req
}

func TestGroupPartitionsByBrokerNoRouter(t *testing.T) {
	p := &proxy{}
	req := makeProduceRequest(map[string][]int32{
		"orders": {0, 1, 2},
		"events": {0},
	})
	groups := p.groupPartitionsByBroker(context.Background(), req, nil)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group (all round-robin), got %d", len(groups))
	}
	rr, ok := groups[""]
	if !ok {
		t.Fatalf("expected round-robin group (key=\"\"), got keys: %v", mapKeys(groups))
	}
	totalParts := 0
	for _, topic := range rr.Topics {
		totalParts += len(topic.Partitions)
	}
	if totalParts != 4 {
		t.Fatalf("expected 4 total partitions, got %d", totalParts)
	}
	if rr.Acks != -1 || rr.TimeoutMs != 5000 {
		t.Fatalf("sub-request should preserve acks/timeout: got acks=%d timeout=%d", rr.Acks, rr.TimeoutMs)
	}
}

func TestGroupPartitionsByBrokerNoRouterMultipleTopics(t *testing.T) {
	p := &proxy{}
	req := makeProduceRequest(map[string][]int32{
		"orders": {0, 1},
		"events": {0, 1, 2},
	})
	groups := p.groupPartitionsByBroker(context.Background(), req, nil)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	rr := groups[""]
	if rr == nil {
		t.Fatalf("expected round-robin group")
	}
	if countPartitions(rr) != 5 {
		t.Fatalf("expected 5 partitions, got %d", countPartitions(rr))
	}
	topicNames := make(map[string]int)
	for _, topic := range rr.Topics {
		topicNames[topic.Name] = len(topic.Partitions)
	}
	if topicNames["orders"] != 2 || topicNames["events"] != 3 {
		t.Fatalf("unexpected topic grouping: %v", topicNames)
	}
}

func TestGroupPartitionsByBrokerFiltersCorrectly(t *testing.T) {
	p := &proxy{}
	req := makeProduceRequest(map[string][]int32{
		"orders": {0, 1, 2},
		"events": {0, 1},
	})
	include := map[string]map[int32]bool{
		"orders": {1: true},
		"events": {0: true},
	}
	groups := p.groupPartitionsByBroker(context.Background(), req, include)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group (no router), got %d", len(groups))
	}
	rr := groups[""]
	if rr == nil {
		t.Fatalf("missing round-robin group")
	}
	if countPartitions(rr) != 2 {
		t.Fatalf("expected 2 filtered partitions, got %d", countPartitions(rr))
	}
}

func TestFindOrAddTopicResponse(t *testing.T) {
	resp := &protocol.ProduceResponse{}

	tr := findOrAddTopicResponse(resp, "orders")
	tr.Partitions = append(tr.Partitions, protocol.ProducePartitionResponse{Partition: 0})

	// Second call should return the same topic, not create a new one.
	tr2 := findOrAddTopicResponse(resp, "orders")
	if len(tr2.Partitions) != 1 {
		t.Fatalf("expected 1 partition in existing topic, got %d", len(tr2.Partitions))
	}

	// Different topic.
	tr3 := findOrAddTopicResponse(resp, "events")
	if len(tr3.Partitions) != 0 {
		t.Fatalf("expected 0 partitions in new topic, got %d", len(tr3.Partitions))
	}
	if len(resp.Topics) != 2 {
		t.Fatalf("expected 2 topics in response, got %d", len(resp.Topics))
	}
}

func TestAddErrorForAllPartitions(t *testing.T) {
	resp := &protocol.ProduceResponse{}
	req := makeProduceRequest(map[string][]int32{
		"orders": {0, 1},
		"events": {0},
	})
	addErrorForAllPartitions(resp, req, protocol.REQUEST_TIMED_OUT)

	if len(resp.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(resp.Topics))
	}
	total := 0
	for _, topic := range resp.Topics {
		for _, part := range topic.Partitions {
			if part.ErrorCode != protocol.REQUEST_TIMED_OUT {
				t.Fatalf("expected error %d, got %d", protocol.REQUEST_TIMED_OUT, part.ErrorCode)
			}
			if part.BaseOffset != -1 {
				t.Fatalf("expected base offset -1, got %d", part.BaseOffset)
			}
			total++
		}
	}
	if total != 3 {
		t.Fatalf("expected 3 partition errors, got %d", total)
	}
}

func TestUpdateBrokerAddrs(t *testing.T) {
	p := &proxy{brokerAddrs: make(map[string]string)}
	brokers := []protocol.MetadataBroker{
		{NodeID: 1, Host: "broker1", Port: 9092},
		{NodeID: 2, Host: "broker2", Port: 9093},
		{NodeID: 3, Host: "", Port: 0}, // should be skipped
	}
	p.updateBrokerAddrs(brokers)

	ctx := context.Background()
	if got := p.brokerIDToAddr(ctx, "1"); got != "broker1:9092" {
		t.Fatalf("broker 1: got %q, want %q", got, "broker1:9092")
	}
	if got := p.brokerIDToAddr(ctx, "2"); got != "broker2:9093" {
		t.Fatalf("broker 2: got %q, want %q", got, "broker2:9093")
	}
	if got := p.brokerIDToAddr(ctx, "3"); got != "" {
		t.Fatalf("broker 3 (empty host): got %q, want %q", got, "")
	}
}

func TestConnPoolBorrowReturn(t *testing.T) {
	pool := newConnPool(5 * time.Second)
	defer pool.Close()

	// No pooled connection: Borrow should fail for non-existent address
	// (we can't dial in a unit test, but we can test the pool hit path).
	// Simulate by manually inserting a connection.
	fakeConn := &fakeNetConn{}
	pool.Return("addr1", fakeConn)

	conn, err := pool.Borrow(context.Background(), "addr1")
	if err != nil {
		t.Fatalf("borrow should succeed for pooled conn: %v", err)
	}
	if conn != fakeConn {
		t.Fatalf("borrow should return the pooled conn")
	}
	// After borrow, pool should be empty for that address.
	if _, ok := pool.conns["addr1"]; ok {
		t.Fatalf("pool should be empty for addr1 after borrow")
	}

	// Return then Close should close all.
	pool.Return("addr1", fakeConn)
	pool.Close()
	if !fakeConn.closed {
		t.Fatalf("pool.Close should close all connections")
	}
}

// --- Test helpers ---

func countPartitions(req *protocol.ProduceRequest) int {
	n := 0
	for _, t := range req.Topics {
		n += len(t.Partitions)
	}
	return n
}

func mapKeys(m map[string]*protocol.ProduceRequest) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// fakeNetConn is a minimal net.Conn for pool tests.
type fakeNetConn struct {
	closed bool
}

func (c *fakeNetConn) Read(b []byte) (int, error)         { return 0, nil }
func (c *fakeNetConn) Write(b []byte) (int, error)        { return len(b), nil }
func (c *fakeNetConn) Close() error                       { c.closed = true; return nil }
func (c *fakeNetConn) LocalAddr() net.Addr                { return nil }
func (c *fakeNetConn) RemoteAddr() net.Addr               { return nil }
func (c *fakeNetConn) SetDeadline(time.Time) error        { return nil }
func (c *fakeNetConn) SetReadDeadline(time.Time) error    { return nil }
func (c *fakeNetConn) SetWriteDeadline(time.Time) error   { return nil }

func TestExtractGroupID(t *testing.T) {
	p := &proxy{}
	clientID := "test-client"

	// Helper to write a non-flexible request header.
	writeHeader := func(w *testWriter, apiKey, version int16) {
		w.Int16(apiKey)
		w.Int16(version)
		w.Int32(1) // correlation_id
		w.NullableString(&clientID)
	}

	tests := []struct {
		name    string
		apiKey  int16
		payload func() []byte
		want    string
	}{
		{
			name:   "JoinGroup",
			apiKey: protocol.APIKeyJoinGroup,
			payload: func() []byte {
				w := &testWriter{}
				writeHeader(w, protocol.APIKeyJoinGroup, 2)
				w.String("my-join-group")       // group_id
				w.Int32(30000)                   // session_timeout
				w.Int32(10000)                   // rebalance_timeout
				w.String("")                     // member_id
				w.String("consumer")             // protocol_type
				w.Int32(1)                       // protocol count
				w.String("range")                // protocol name
				w.Bytes([]byte{0x00, 0x01})      // protocol metadata
				return w.buf.Bytes()
			},
			want: "my-join-group",
		},
		{
			name:   "SyncGroup",
			apiKey: protocol.APIKeySyncGroup,
			payload: func() []byte {
				w := &testWriter{}
				writeHeader(w, protocol.APIKeySyncGroup, 1)
				w.String("my-sync-group") // group_id
				w.Int32(1)                // generation_id
				w.String("member-1")      // member_id
				w.Int32(0)                // assignments count
				return w.buf.Bytes()
			},
			want: "my-sync-group",
		},
		{
			name:   "Heartbeat",
			apiKey: protocol.APIKeyHeartbeat,
			payload: func() []byte {
				w := &testWriter{}
				writeHeader(w, protocol.APIKeyHeartbeat, 1)
				w.String("my-heartbeat-group") // group_id
				w.Int32(1)                     // generation_id
				w.String("member-1")           // member_id
				return w.buf.Bytes()
			},
			want: "my-heartbeat-group",
		},
		{
			name:   "LeaveGroup",
			apiKey: protocol.APIKeyLeaveGroup,
			payload: func() []byte {
				w := &testWriter{}
				writeHeader(w, protocol.APIKeyLeaveGroup, 0)
				w.String("my-leave-group") // group_id
				w.String("member-1")       // member_id
				return w.buf.Bytes()
			},
			want: "my-leave-group",
		},
		{
			name:   "OffsetCommit",
			apiKey: protocol.APIKeyOffsetCommit,
			payload: func() []byte {
				w := &testWriter{}
				writeHeader(w, protocol.APIKeyOffsetCommit, 3)
				w.String("my-commit-group") // group_id
				w.Int32(1)                  // generation_id
				w.String("member-1")        // member_id
				w.Int64(-1)                 // retention_time_ms (v2-v4)
				w.Int32(0)                  // topics count
				return w.buf.Bytes()
			},
			want: "my-commit-group",
		},
		{
			name:   "OffsetFetch",
			apiKey: protocol.APIKeyOffsetFetch,
			payload: func() []byte {
				w := &testWriter{}
				writeHeader(w, protocol.APIKeyOffsetFetch, 3)
				w.String("my-fetch-group") // group_id
				w.Int32(0)                 // topics count
				return w.buf.Bytes()
			},
			want: "my-fetch-group",
		},
		{
			name:   "DescribeGroups single",
			apiKey: protocol.APIKeyDescribeGroups,
			payload: func() []byte {
				w := &testWriter{}
				writeHeader(w, protocol.APIKeyDescribeGroups, 2)
				w.Int32(1)                      // groups count
				w.String("my-describe-group-1") // group[0]
				return w.buf.Bytes()
			},
			want: "my-describe-group-1",
		},
		{
			name:   "DescribeGroups multiple returns first",
			apiKey: protocol.APIKeyDescribeGroups,
			payload: func() []byte {
				w := &testWriter{}
				writeHeader(w, protocol.APIKeyDescribeGroups, 2)
				w.Int32(2)                      // groups count
				w.String("my-describe-group-1") // group[0]
				w.String("my-describe-group-2") // group[1]
				return w.buf.Bytes()
			},
			want: "my-describe-group-1",
		},
		{
			name:   "DescribeGroups empty returns blank",
			apiKey: protocol.APIKeyDescribeGroups,
			payload: func() []byte {
				w := &testWriter{}
				writeHeader(w, protocol.APIKeyDescribeGroups, 2)
				w.Int32(0) // groups count
				return w.buf.Bytes()
			},
			want: "",
		},
		{
			name:   "truncated payload returns blank",
			apiKey: protocol.APIKeyJoinGroup,
			payload: func() []byte {
				return []byte{0, 11, 0, 2} // just api_key + version, no body
			},
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := p.extractGroupID(tc.apiKey, tc.payload())
			if got != tc.want {
				t.Fatalf("extractGroupID() = %q, want %q", got, tc.want)
			}
		})
	}
}

// --- Fetch routing tests ---

func makeFetchRequest(topics map[string][]int32) *protocol.FetchRequest {
	req := &protocol.FetchRequest{
		ReplicaID:    -1,
		MaxWaitMs:    500,
		MinBytes:     1,
		MaxBytes:     1048576,
		SessionID:    0,
		SessionEpoch: -1,
	}
	for name, parts := range topics {
		topic := protocol.FetchTopicRequest{Name: name}
		for _, p := range parts {
			topic.Partitions = append(topic.Partitions, protocol.FetchPartitionRequest{
				Partition:   p,
				FetchOffset: 0,
				MaxBytes:    1048576,
			})
		}
		req.Topics = append(req.Topics, topic)
	}
	return req
}

func countFetchPartitions(req *protocol.FetchRequest) int {
	n := 0
	for _, t := range req.Topics {
		n += len(t.Partitions)
	}
	return n
}

func fetchMapKeys(m map[string]*protocol.FetchRequest) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func TestGroupFetchPartitionsByBrokerNoRouter(t *testing.T) {
	p := &proxy{}
	req := makeFetchRequest(map[string][]int32{
		"orders": {0, 1, 2},
		"events": {0},
	})
	groups := p.groupFetchPartitionsByBroker(context.Background(), req, nil)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group (all round-robin), got %d", len(groups))
	}
	rr, ok := groups[""]
	if !ok {
		t.Fatalf("expected round-robin group (key=\"\"), got keys: %v", fetchMapKeys(groups))
	}
	if countFetchPartitions(rr) != 4 {
		t.Fatalf("expected 4 total partitions, got %d", countFetchPartitions(rr))
	}
	if rr.MaxWaitMs != 500 || rr.MaxBytes != 1048576 {
		t.Fatalf("sub-request should preserve settings: got maxWait=%d maxBytes=%d", rr.MaxWaitMs, rr.MaxBytes)
	}
}

func TestGroupFetchPartitionsByBrokerNoRouterMultipleTopics(t *testing.T) {
	p := &proxy{}
	req := makeFetchRequest(map[string][]int32{
		"orders": {0, 1},
		"events": {0, 1, 2},
	})
	groups := p.groupFetchPartitionsByBroker(context.Background(), req, nil)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	rr := groups[""]
	if rr == nil {
		t.Fatalf("expected round-robin group")
	}
	if countFetchPartitions(rr) != 5 {
		t.Fatalf("expected 5 partitions, got %d", countFetchPartitions(rr))
	}
	topicNames := make(map[string]int)
	for _, topic := range rr.Topics {
		topicNames[topic.Name] = len(topic.Partitions)
	}
	if topicNames["orders"] != 2 || topicNames["events"] != 3 {
		t.Fatalf("unexpected topic grouping: %v", topicNames)
	}
}

func TestGroupFetchPartitionsByBrokerFiltersCorrectly(t *testing.T) {
	p := &proxy{}
	req := makeFetchRequest(map[string][]int32{
		"orders": {0, 1, 2},
		"events": {0, 1},
	})
	include := map[string]map[int32]bool{
		"orders": {1: true},
		"events": {0: true},
	}
	groups := p.groupFetchPartitionsByBroker(context.Background(), req, include)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group (no router), got %d", len(groups))
	}
	rr := groups[""]
	if rr == nil {
		t.Fatalf("missing round-robin group")
	}
	if countFetchPartitions(rr) != 2 {
		t.Fatalf("expected 2 filtered partitions, got %d", countFetchPartitions(rr))
	}
}

func TestFindOrAddFetchTopicResponse(t *testing.T) {
	resp := &protocol.FetchResponse{}
	topicID := [16]byte{1, 2, 3}

	tr := findOrAddFetchTopicResponse(resp, "orders", topicID)
	tr.Partitions = append(tr.Partitions, protocol.FetchPartitionResponse{Partition: 0})

	// Same topic should return the existing entry.
	tr2 := findOrAddFetchTopicResponse(resp, "orders", topicID)
	if len(tr2.Partitions) != 1 {
		t.Fatalf("expected 1 partition in existing topic, got %d", len(tr2.Partitions))
	}

	// Different topic.
	tr3 := findOrAddFetchTopicResponse(resp, "events", [16]byte{4, 5, 6})
	if len(tr3.Partitions) != 0 {
		t.Fatalf("expected 0 partitions in new topic, got %d", len(tr3.Partitions))
	}
	if len(resp.Topics) != 2 {
		t.Fatalf("expected 2 topics in response, got %d", len(resp.Topics))
	}

	// v12+: same topicID but different name should match on topicID alone.
	tr4 := findOrAddFetchTopicResponse(resp, "", topicID)
	tr4.Partitions = append(tr4.Partitions, protocol.FetchPartitionResponse{Partition: 1})
	if len(resp.Topics) != 2 {
		t.Fatalf("expected 2 topics after topicID-only match, got %d", len(resp.Topics))
	}
	// The entry found by topicID should now have 2 partitions (0 from before, 1 just added).
	tr5 := findOrAddFetchTopicResponse(resp, "orders", topicID)
	if len(tr5.Partitions) != 2 {
		t.Fatalf("expected 2 partitions after topicID-only merge, got %d", len(tr5.Partitions))
	}

	// Name-only match (zero topicID) should work for pre-v12 topics.
	tr6 := findOrAddFetchTopicResponse(resp, "logs", [16]byte{})
	tr6.Partitions = append(tr6.Partitions, protocol.FetchPartitionResponse{Partition: 0})
	tr7 := findOrAddFetchTopicResponse(resp, "logs", [16]byte{})
	if len(tr7.Partitions) != 1 {
		t.Fatalf("expected 1 partition for name-only topic, got %d", len(tr7.Partitions))
	}
	if len(resp.Topics) != 3 {
		t.Fatalf("expected 3 topics total, got %d", len(resp.Topics))
	}
}

func TestAddFetchErrorForAllPartitions(t *testing.T) {
	resp := &protocol.FetchResponse{}
	req := makeFetchRequest(map[string][]int32{
		"orders": {0, 1},
		"events": {0},
	})
	addFetchErrorForAllPartitions(resp, req, protocol.REQUEST_TIMED_OUT)

	if len(resp.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(resp.Topics))
	}
	total := 0
	for _, topic := range resp.Topics {
		for _, part := range topic.Partitions {
			if part.ErrorCode != protocol.REQUEST_TIMED_OUT {
				t.Fatalf("expected error %d, got %d", protocol.REQUEST_TIMED_OUT, part.ErrorCode)
			}
			total++
		}
	}
	if total != 3 {
		t.Fatalf("expected 3 partition errors, got %d", total)
	}
}

func TestUpdateTopicNames(t *testing.T) {
	p := &proxy{topicNames: make(map[[16]byte]string)}
	topicID1 := [16]byte{1, 2, 3}
	topicID2 := [16]byte{4, 5, 6}
	topics := []protocol.MetadataTopic{
		{Name: "orders", TopicID: topicID1},
		{Name: "events", TopicID: topicID2},
		{Name: "", TopicID: [16]byte{}}, // should be skipped
	}
	p.updateTopicNames(topics)

	ctx := context.Background()
	if got := p.resolveTopicID(ctx, topicID1); got != "orders" {
		t.Fatalf("resolveTopicID(1): got %q, want %q", got, "orders")
	}
	if got := p.resolveTopicID(ctx, topicID2); got != "events" {
		t.Fatalf("resolveTopicID(2): got %q, want %q", got, "events")
	}
	if got := p.resolveTopicID(ctx, [16]byte{9, 9, 9}); got != "" {
		t.Fatalf("resolveTopicID(unknown): got %q, want %q", got, "")
	}
}

func TestGroupFetchPartitionsByBrokerUnresolvedTopicIDs(t *testing.T) {
	// When multiple topics have unresolved names (empty string) but different
	// topic IDs, they must not be merged into a single FetchTopicRequest.
	idA := [16]byte{1, 2, 3}
	idB := [16]byte{4, 5, 6}
	p := &proxy{}
	req := &protocol.FetchRequest{
		ReplicaID:    -1,
		MaxWaitMs:    500,
		MinBytes:     1,
		MaxBytes:     1048576,
		SessionEpoch: -1,
		Topics: []protocol.FetchTopicRequest{
			{TopicID: idA, Partitions: []protocol.FetchPartitionRequest{{Partition: 0, MaxBytes: 1048576}}},
			{TopicID: idB, Partitions: []protocol.FetchPartitionRequest{{Partition: 0, MaxBytes: 1048576}}},
		},
	}
	groups := p.groupFetchPartitionsByBroker(context.Background(), req, nil)
	rr := groups[""]
	if rr == nil {
		t.Fatal("expected round-robin group")
	}
	if len(rr.Topics) != 2 {
		t.Fatalf("expected 2 topics (separate entries for different IDs), got %d", len(rr.Topics))
	}
	if rr.Topics[0].TopicID != idA || rr.Topics[1].TopicID != idB {
		t.Fatalf("topic IDs not preserved: got %x and %x", rr.Topics[0].TopicID, rr.Topics[1].TopicID)
	}
}

func TestGroupFetchPartitionsByBrokerUnresolvedFilter(t *testing.T) {
	// Verify that the include filter works correctly with fetchTopicKey for
	// unresolved topic IDs.
	idA := [16]byte{1, 2, 3}
	idB := [16]byte{4, 5, 6}
	p := &proxy{}
	req := &protocol.FetchRequest{
		ReplicaID:    -1,
		MaxWaitMs:    500,
		MinBytes:     1,
		MaxBytes:     1048576,
		SessionEpoch: -1,
		Topics: []protocol.FetchTopicRequest{
			{TopicID: idA, Partitions: []protocol.FetchPartitionRequest{
				{Partition: 0, MaxBytes: 1048576},
				{Partition: 1, MaxBytes: 1048576},
			}},
			{TopicID: idB, Partitions: []protocol.FetchPartitionRequest{
				{Partition: 0, MaxBytes: 1048576},
			}},
		},
	}
	// Only retry partition 1 of topic A.
	include := map[string]map[int32]bool{
		fetchTopicKey("", idA): {1: true},
	}
	groups := p.groupFetchPartitionsByBroker(context.Background(), req, include)
	rr := groups[""]
	if rr == nil {
		t.Fatal("expected round-robin group")
	}
	if len(rr.Topics) != 1 {
		t.Fatalf("expected 1 topic after filter, got %d", len(rr.Topics))
	}
	if rr.Topics[0].TopicID != idA {
		t.Fatalf("expected topic A, got %x", rr.Topics[0].TopicID)
	}
	if len(rr.Topics[0].Partitions) != 1 || rr.Topics[0].Partitions[0].Partition != 1 {
		t.Fatalf("expected only partition 1, got %v", rr.Topics[0].Partitions)
	}
}

func TestResolveFetchTopicNames(t *testing.T) {
	topicID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	p := &proxy{
		topicNames: map[[16]byte]string{topicID: "orders"},
	}
	req := &protocol.FetchRequest{
		Topics: []protocol.FetchTopicRequest{
			{TopicID: topicID}, // name not set, should be resolved
			{Name: "events"},  // already has name, should be left alone
		},
	}
	p.resolveFetchTopicNames(context.Background(), req)

	if req.Topics[0].Name != "orders" {
		t.Fatalf("topic[0] name: got %q, want %q", req.Topics[0].Name, "orders")
	}
	if req.Topics[1].Name != "events" {
		t.Fatalf("topic[1] name: got %q, want %q", req.Topics[1].Name, "events")
	}
}
