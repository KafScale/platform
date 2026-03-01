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
	groups := p.groupPartitionsByBroker(req, nil)
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
	groups := p.groupPartitionsByBroker(req, nil)
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
	groups := p.groupPartitionsByBroker(req, include)
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

	if got := p.brokerIDToAddr("1"); got != "broker1:9092" {
		t.Fatalf("broker 1: got %q, want %q", got, "broker1:9092")
	}
	if got := p.brokerIDToAddr("2"); got != "broker2:9093" {
		t.Fatalf("broker 2: got %q, want %q", got, "broker2:9093")
	}
	if got := p.brokerIDToAddr("3"); got != "" {
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
