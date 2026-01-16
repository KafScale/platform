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

package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgproto3/v2"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/decoder"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/discovery"
	kafsql "github.com/kafscale/platform/addons/processors/sql-processor/internal/sql"
)

type mockLister struct {
	segments []discovery.SegmentRef
}

func (m *mockLister) ListCompleted(ctx context.Context) ([]discovery.SegmentRef, error) {
	return m.segments, nil
}

type mockDecoder struct {
	records map[string][]decoder.Record
	keys    []string
}

func (m *mockDecoder) Decode(ctx context.Context, segmentKey, indexKey string, topic string, partition int32) ([]decoder.Record, error) {
	m.keys = append(m.keys, segmentKey)
	return m.records[segmentKey], nil
}

type mockResolver struct {
	topics     []string
	partitions map[string][]int32
}

func (m *mockResolver) Topics(ctx context.Context) ([]string, error) {
	_ = ctx
	return append([]string(nil), m.topics...), nil
}

func (m *mockResolver) Partitions(ctx context.Context, topic string) ([]int32, error) {
	_ = ctx
	partitions, ok := m.partitions[topic]
	if !ok {
		return nil, fmt.Errorf("unknown topic")
	}
	return append([]int32(nil), partitions...), nil
}

type blockingDecoder struct {
	records   []decoder.Record
	startedCh chan struct{}
	releaseCh chan struct{}
}

func (b *blockingDecoder) Decode(ctx context.Context, segmentKey, indexKey string, topic string, partition int32) ([]decoder.Record, error) {
	select {
	case b.startedCh <- struct{}{}:
	default:
	}
	<-b.releaseCh
	return b.records, nil
}

func newPipeBackend(t *testing.T) (*pgproto3.Backend, *pgproto3.Frontend, func()) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(serverConn), serverConn)
	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)
	cleanup := func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	}
	return backend, frontend, cleanup
}

func waitForDecoder(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
		return
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for decoder")
	}
}

func drainUntilDone(t *testing.T, frontend *pgproto3.Frontend) {
	t.Helper()
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		switch msg.(type) {
		case *pgproto3.CommandComplete:
			return
		case *pgproto3.ErrorResponse:
			t.Fatalf("error response")
		}
	}
}

func collectRows(t *testing.T, frontend *pgproto3.Frontend) [][][]byte {
	t.Helper()
	rows := make([][][]byte, 0)
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		switch m := msg.(type) {
		case *pgproto3.DataRow:
			copied := make([][]byte, len(m.Values))
			for i, value := range m.Values {
				if value == nil {
					continue
				}
				buf := make([]byte, len(value))
				copy(buf, value)
				copied[i] = buf
			}
			rows = append(rows, copied)
		case *pgproto3.CommandComplete:
			return rows
		case *pgproto3.ErrorResponse:
			t.Fatalf("error response: %s", m.Message)
		}
	}
}

func collectCommandTag(t *testing.T, frontend *pgproto3.Frontend) string {
	t.Helper()
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		switch m := msg.(type) {
		case *pgproto3.CommandComplete:
			return string(m.CommandTag)
		case *pgproto3.ErrorResponse:
			t.Fatalf("error response: %s", m.Message)
		}
	}
}

func readUntilReady(t *testing.T, frontend *pgproto3.Frontend) {
	t.Helper()
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			return
		}
	}
}

func readErrorResponse(t *testing.T, frontend *pgproto3.Frontend) *pgproto3.ErrorResponse {
	t.Helper()
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		if errMsg, ok := msg.(*pgproto3.ErrorResponse); ok {
			return errMsg
		}
	}
}

func sendStartupMessage(t *testing.T, conn net.Conn) {
	t.Helper()
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user": "test",
		},
	}
	buf, err := startup.Encode(nil)
	if err != nil {
		t.Fatalf("encode startup: %v", err)
	}
	if _, err := conn.Write(buf); err != nil {
		t.Fatalf("send startup: %v", err)
	}
}

func newTestServer(lister discovery.Lister, dec decoder.Decoder) *Server {
	cfg := config.Config{
		Query: config.QueryConfig{
			DefaultLimit:     1000,
			MaxUnbounded:     10000,
			RequireTimeBound: true,
		},
	}
	srv := New(cfg, nil)
	srv.lister = lister
	srv.listerInit = true
	srv.decoder = dec
	srv.decoderInit = true
	return srv
}

func newTestServerWithCache(lister discovery.Lister, dec decoder.Decoder) *Server {
	cfg := config.Config{
		Query: config.QueryConfig{
			DefaultLimit:     1000,
			MaxUnbounded:     10000,
			RequireTimeBound: true,
		},
		ResultCache: config.ResultCacheConfig{
			TTLSeconds: 60,
			MaxEntries: 10,
			MaxRows:    100,
		},
	}
	srv := New(cfg, nil)
	srv.lister = lister
	srv.listerInit = true
	srv.decoder = dec
	srv.decoderInit = true
	return srv
}

func TestHandleSelectTail(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now - 3000, Key: []byte("a")},
		{Topic: "orders", Partition: 0, Offset: 2, Timestamp: now - 2000, Key: []byte("b")},
		{Topic: "orders", Partition: 0, Offset: 3, Timestamp: now - 1000, Key: []byte("c")},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
	}})

	parsed, err := kafsql.Parse("SELECT _offset FROM orders TAIL 2;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed, nil)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle select: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if string(rows[0][0]) != "2" || string(rows[1][0]) != "3" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestDescribeTopic(t *testing.T) {
	cfg := config.Config{
		Metadata: config.MetaConfig{
			Topics: []config.TopicConfig{
				{
					Name: "orders",
					Schema: config.SchemaConfig{
						Columns: []config.SchemaColumn{
							{Name: "amount", Type: "double", Path: "$.amount"},
						},
					},
				},
			},
		},
	}
	srv := New(cfg, nil)

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.handleQuery(context.Background(), backend, "DESCRIBE orders;")
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if len(rows) != 9 {
		t.Fatalf("expected 9 rows, got %d", len(rows))
	}
	last := rows[len(rows)-1]
	if len(last) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(last))
	}
	if string(last[0]) != "amount" || string(last[1]) != "double precision" || string(last[2]) != "$.amount" {
		t.Fatalf("unexpected schema row: %+v", last)
	}
}

func TestResultCacheHit(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now.UnixMilli(), Key: []byte("a")},
	}
	dec := &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
	}}
	srv := newTestServerWithCache(&mockLister{segments: segments}, dec)

	query := "SELECT _offset FROM orders LAST 1h;"

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.handleQuery(context.Background(), backend, query)
	}()
	_ = collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle query: %v", err)
	}

	backend2, frontend2, cleanup2 := newPipeBackend(t)
	defer cleanup2()
	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- srv.handleQuery(context.Background(), backend2, query)
	}()
	_ = collectRows(t, frontend2)
	if err := <-errCh2; err != nil {
		t.Fatalf("handle query (cached): %v", err)
	}

	if len(dec.keys) != 1 {
		t.Fatalf("expected decoder called once, got %d", len(dec.keys))
	}
}

func TestExtendedProtocolExecute(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now - 2000},
		{Topic: "orders", Partition: 0, Offset: 2, Timestamp: now - 1000},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
	}})

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	errCh := make(chan error, 1)
	go func() {
		srv.handleConnection(context.Background(), serverConn)
		errCh <- nil
	}()

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)
	sendStartupMessage(t, clientConn)
	readUntilReady(t, frontend)

	if err := frontend.Send(&pgproto3.Parse{Name: "stmt", Query: "SELECT _offset FROM orders TAIL 1;"}); err != nil {
		t.Fatalf("send parse: %v", err)
	}
	if err := expectParseComplete(frontend); err != nil {
		t.Fatalf("parse complete: %v", err)
	}
	if err := frontend.Send(&pgproto3.Bind{DestinationPortal: "portal", PreparedStatement: "stmt"}); err != nil {
		t.Fatalf("send bind: %v", err)
	}
	if err := expectBindComplete(frontend); err != nil {
		t.Fatalf("bind complete: %v", err)
	}
	if err := frontend.Send(&pgproto3.Execute{Portal: "portal"}); err != nil {
		t.Fatalf("send execute: %v", err)
	}

	seenParse := true
	seenBind := true
	seenData := false
	seenComplete := false
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		switch m := msg.(type) {
		case *pgproto3.ParseComplete:
			seenParse = true
		case *pgproto3.BindComplete:
			seenBind = true
		case *pgproto3.DataRow:
			seenData = true
			if len(m.Values) != 1 || string(m.Values[0]) != "2" {
				t.Fatalf("unexpected row: %+v", m.Values)
			}
		case *pgproto3.CommandComplete:
			seenComplete = true
		case *pgproto3.ErrorResponse:
			t.Fatalf("error response: %s", m.Message)
		}
		if seenData && seenComplete {
			break
		}
	}

	if err := frontend.Send(&pgproto3.Sync{}); err != nil {
		t.Fatalf("send sync: %v", err)
	}
	readUntilReady(t, frontend)
	if !seenParse || !seenBind || !seenData || !seenComplete {
		t.Fatalf("missing messages parse=%t bind=%t data=%t complete=%t", seenParse, seenBind, seenData, seenComplete)
	}
}

func expectParseComplete(frontend *pgproto3.Frontend) error {
	for {
		msg, err := frontend.Receive()
		if err != nil {
			return err
		}
		switch msg.(type) {
		case *pgproto3.ParseComplete:
			return nil
		case *pgproto3.ErrorResponse:
			return errUnexpectedMessage(msg)
		}
	}
}

func expectBindComplete(frontend *pgproto3.Frontend) error {
	for {
		msg, err := frontend.Receive()
		if err != nil {
			return err
		}
		switch msg.(type) {
		case *pgproto3.BindComplete:
			return nil
		case *pgproto3.ErrorResponse:
			return errUnexpectedMessage(msg)
		}
	}
}

func errUnexpectedMessage(msg pgproto3.BackendMessage) error {
	if errMsg, ok := msg.(*pgproto3.ErrorResponse); ok {
		return fmt.Errorf("error response: %s", errMsg.Message)
	}
	return fmt.Errorf("unexpected message %T", msg)
}

func TestExtendedProtocolParseError(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	go srv.handleConnection(context.Background(), serverConn)

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)
	sendStartupMessage(t, clientConn)
	readUntilReady(t, frontend)

	err := frontend.Send(&pgproto3.Parse{Name: "stmt", Query: "SELECT * FROM orders WHERE _offset >= $1;"})
	if err != nil {
		t.Fatalf("send parse: %v", err)
	}
	if resp := readErrorResponse(t, frontend); resp.Message == "" {
		t.Fatalf("expected error response")
	}
	if err := frontend.Send(&pgproto3.Sync{}); err != nil {
		t.Fatalf("send sync: %v", err)
	}
	readUntilReady(t, frontend)
}

func TestExtendedProtocolBindUnknownStatement(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	go srv.handleConnection(context.Background(), serverConn)

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)
	sendStartupMessage(t, clientConn)
	readUntilReady(t, frontend)

	err := frontend.Send(&pgproto3.Bind{DestinationPortal: "portal", PreparedStatement: "missing"})
	if err != nil {
		t.Fatalf("send bind: %v", err)
	}
	if resp := readErrorResponse(t, frontend); resp.Message == "" {
		t.Fatalf("expected error response")
	}
	if err := frontend.Send(&pgproto3.Sync{}); err != nil {
		t.Fatalf("send sync: %v", err)
	}
	readUntilReady(t, frontend)
}

func TestExtendedProtocolExecuteUnknownPortal(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	go srv.handleConnection(context.Background(), serverConn)

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)
	sendStartupMessage(t, clientConn)
	readUntilReady(t, frontend)

	err := frontend.Send(&pgproto3.Execute{Portal: "missing"})
	if err != nil {
		t.Fatalf("send execute: %v", err)
	}
	if resp := readErrorResponse(t, frontend); resp.Message == "" {
		t.Fatalf("expected error response")
	}
	if err := frontend.Send(&pgproto3.Sync{}); err != nil {
		t.Fatalf("send sync: %v", err)
	}
	readUntilReady(t, frontend)
}

func TestExtendedProtocolDescribeUnknown(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	go srv.handleConnection(context.Background(), serverConn)

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)
	sendStartupMessage(t, clientConn)
	readUntilReady(t, frontend)

	err := frontend.Send(&pgproto3.Describe{ObjectType: 'S', Name: "missing"})
	if err != nil {
		t.Fatalf("send describe: %v", err)
	}
	if resp := readErrorResponse(t, frontend); resp.Message == "" {
		t.Fatalf("expected error response")
	}
	if err := frontend.Send(&pgproto3.Sync{}); err != nil {
		t.Fatalf("send sync: %v", err)
	}
	readUntilReady(t, frontend)
}

func TestExtendedProtocolDescribeFields(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	go srv.handleConnection(context.Background(), serverConn)

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)
	sendStartupMessage(t, clientConn)
	readUntilReady(t, frontend)

	if err := frontend.Send(&pgproto3.Parse{Name: "stmt", Query: "SELECT _offset FROM orders LAST 1h;"}); err != nil {
		t.Fatalf("send parse: %v", err)
	}
	if err := expectParseComplete(frontend); err != nil {
		t.Fatalf("parse complete: %v", err)
	}
	if err := frontend.Send(&pgproto3.Describe{ObjectType: 'S', Name: "stmt"}); err != nil {
		t.Fatalf("send describe: %v", err)
	}

	msg, err := frontend.Receive()
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if _, ok := msg.(*pgproto3.ParameterDescription); !ok {
		t.Fatalf("expected parameter description, got %T", msg)
	}
	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	desc, ok := msg.(*pgproto3.RowDescription)
	if !ok {
		t.Fatalf("expected row description, got %T", msg)
	}
	if len(desc.Fields) != 1 || string(desc.Fields[0].Name) != "_offset" {
		t.Fatalf("unexpected fields: %+v", desc.Fields)
	}

	if err := frontend.Send(&pgproto3.Sync{}); err != nil {
		t.Fatalf("send sync: %v", err)
	}
	readUntilReady(t, frontend)
}

func TestHandleSelectOrderBy(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 10, Timestamp: now - 5000},
		{Topic: "orders", Partition: 0, Offset: 11, Timestamp: now - 1000},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
	}})

	parsed, err := kafsql.Parse("SELECT _offset FROM orders ORDER BY _ts DESC LIMIT 1 LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed, nil)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle select: %v", err)
	}
	if len(rows) != 1 || string(rows[0][0]) != "11" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestHandleSelectRejectsUnbounded(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	parsed, err := kafsql.Parse("SELECT * FROM orders;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	backend, _, cleanup := newPipeBackend(t)
	defer cleanup()
	if _, err := srv.handleSelect(context.Background(), backend, parsed, nil); err == nil {
		t.Fatalf("expected unbounded query error")
	}
}

func TestHandleSelectOrderByInvalid(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	parsed, err := kafsql.Parse("SELECT * FROM orders ORDER BY _offset LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	backend, _, cleanup := newPipeBackend(t)
	defer cleanup()
	if _, err := srv.handleSelect(context.Background(), backend, parsed, nil); err == nil {
		t.Fatalf("expected order by error")
	}
}

func TestHandleSelectMaxRows(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	srv.cfg.Query.MaxRows = 1
	parsed, err := kafsql.Parse("SELECT * FROM orders LIMIT 5 LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	backend, _, cleanup := newPipeBackend(t)
	defer cleanup()
	if _, err := srv.handleSelect(context.Background(), backend, parsed, nil); err == nil {
		t.Fatalf("expected max rows error")
	}
}

func TestHandleSelectScanFullLimit(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	srv.cfg.Query.MaxUnbounded = 1
	parsed, err := kafsql.Parse("SELECT * FROM orders SCAN FULL LIMIT 5;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	backend, _, cleanup := newPipeBackend(t)
	defer cleanup()
	if _, err := srv.handleSelect(context.Background(), backend, parsed, nil); err == nil {
		t.Fatalf("expected scan full limit error")
	}
}

func TestHandleAggregateOrderByRejected(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	parsed, err := kafsql.Parse("SELECT COUNT(*) FROM orders ORDER BY _ts LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	backend, _, cleanup := newPipeBackend(t)
	defer cleanup()
	if _, err := srv.handleSelect(context.Background(), backend, parsed, nil); err == nil {
		t.Fatalf("expected aggregate order by error")
	}
}

func TestExplainRejectsUnbounded(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	parsed, err := kafsql.Parse("EXPLAIN SELECT * FROM orders;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	backend, _, cleanup := newPipeBackend(t)
	defer cleanup()
	if _, err := srv.handleExplain(context.Background(), backend, parsed); err == nil {
		t.Fatalf("expected explain unbounded error")
	}
}

func TestHandleSetCommandReset(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.handleQuery(context.Background(), backend, "RESET ALL;")
	}()

	tag := collectCommandTag(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if tag != "RESET" {
		t.Fatalf("unexpected command tag: %q", tag)
	}
}

func TestDescribeFields(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	parsed, err := kafsql.Parse("DESCRIBE orders;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	fields, err := srv.describeFields(parsed)
	if err != nil {
		t.Fatalf("describe fields: %v", err)
	}
	if len(fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(fields))
	}
	if string(fields[0].Name) != "column" || string(fields[1].Name) != "type" || string(fields[2].Name) != "path" {
		t.Fatalf("unexpected field names: %+v", fields)
	}
}

func TestHandleJoinRejectsMissingWindow(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	parsed, err := kafsql.Parse("SELECT * FROM orders o JOIN payments p ON o._key = p._key WITHIN 5m LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	parsed.TimeWindow = ""
	backend, _, cleanup := newPipeBackend(t)
	defer cleanup()
	if _, err := srv.handleSelect(context.Background(), backend, parsed, nil); err == nil {
		t.Fatalf("expected join window error")
	}
}

func TestHandleJoinRejectsTail(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	parsed, err := kafsql.Parse("SELECT * FROM orders o JOIN payments p ON o._key = p._key WITHIN 5m LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	parsed.Tail = "10"
	backend, _, cleanup := newPipeBackend(t)
	defer cleanup()
	if _, err := srv.handleSelect(context.Background(), backend, parsed, nil); err == nil {
		t.Fatalf("expected join tail error")
	}
}

func TestJoinHelpers(t *testing.T) {
	left := kafsql.JoinExpr{Kind: kafsql.JoinExprKey, Side: "right"}
	right := kafsql.JoinExpr{Kind: kafsql.JoinExprJSON, Side: "left"}
	cond := normalizeJoinCondition(&kafsql.JoinCondition{Left: left, Right: right})
	if cond.Left.Side != "left" || cond.Right.Side != "right" {
		t.Fatalf("expected swap to left/right sides")
	}

	if joinOutputName("right", "_offset") != "_right_offset" {
		t.Fatalf("unexpected right output name")
	}
	if joinOutputName("right", "value") != "_right_value" {
		t.Fatalf("unexpected right output name for non-implicit")
	}

	record := decoder.Record{Key: []byte("a"), Value: []byte(`{"id":"x"}`)}
	if joinKeyFromExpr(record, kafsql.JoinExpr{Kind: kafsql.JoinExprKey}) != "a" {
		t.Fatalf("expected key join value")
	}
	if joinKeyFromExpr(record, kafsql.JoinExpr{Kind: kafsql.JoinExprJSON, JSONPath: "$.id"}) != "x" {
		t.Fatalf("expected json join value")
	}
	if joinKeyFromExpr(record, kafsql.JoinExpr{Kind: kafsql.JoinExprJSON, JSONPath: "$.missing"}) != "" {
		t.Fatalf("expected missing json join value")
	}

	if !withinWindow(1000, 1500, 600*time.Millisecond) {
		t.Fatalf("expected within window")
	}
	if withinWindow(1000, 2000, 500*time.Millisecond) {
		t.Fatalf("expected outside window")
	}

	cols := joinDefaultColumns()
	if len(cols) == 0 || cols[8].Name != "_right_topic" {
		t.Fatalf("unexpected join default columns: %+v", cols)
	}
}

func TestCacheKeyRules(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	parsed := kafsql.Query{Type: kafsql.QuerySelect, Topic: "orders", Last: "1h"}
	key, ok := srv.cacheKey(parsed, "SELECT * FROM orders LAST 1h;")
	if !ok || key == "" {
		t.Fatalf("expected cache key for last")
	}
	parsed = kafsql.Query{Type: kafsql.QuerySelect, Topic: "orders", Tail: "10"}
	if _, ok := srv.cacheKey(parsed, "SELECT * FROM orders TAIL 10;"); ok {
		t.Fatalf("expected tail to skip cache")
	}
	parsed = kafsql.Query{Type: kafsql.QuerySelect, Topic: "orders", ScanFull: true}
	if _, ok := srv.cacheKey(parsed, "SELECT * FROM orders SCAN FULL;"); ok {
		t.Fatalf("expected scan full to skip cache")
	}
	min1 := int64(100)
	max1 := int64(200)
	parsed = kafsql.Query{Type: kafsql.QuerySelect, Topic: "orders", TsMin: &min1, TsMax: &max1}
	key1, ok := srv.cacheKey(parsed, "SELECT * FROM orders WHERE _ts BETWEEN 100 AND 200;")
	if !ok || key1 == "" {
		t.Fatalf("expected cache key for explicit time range")
	}
	max2 := int64(300)
	parsed = kafsql.Query{Type: kafsql.QuerySelect, Topic: "orders", TsMin: &min1, TsMax: &max2}
	key2, ok := srv.cacheKey(parsed, "SELECT * FROM orders WHERE _ts BETWEEN 100 AND 300;")
	if !ok || key2 == "" {
		t.Fatalf("expected cache key for new time range")
	}
	if key1 == key2 {
		t.Fatalf("expected cache keys to differ for time windows")
	}
}

func TestFilterSegments(t *testing.T) {
	minOffset := int64(10)
	maxOffset := int64(20)
	minTs := int64(1000)
	maxTs := int64(2000)
	lowTs := int64(900)
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "a"},
		{Topic: "orders", Partition: 1, SegmentKey: "b", MinOffset: &minOffset, MaxOffset: &maxOffset, MinTimestamp: &lowTs, MaxTimestamp: &lowTs},
		{Topic: "payments", Partition: 0, SegmentKey: "c"},
		{Topic: "orders", Partition: 1, SegmentKey: "d", MinTimestamp: &minTs, MaxTimestamp: &maxTs},
	}
	part := int32(1)
	min := int64(15)
	max := int64(18)
	parsed := kafsql.Query{Topic: "orders", Partition: &part, OffsetMin: &min, OffsetMax: &max}
	out := filterSegments(parsed, segments, nil, nil)
	if len(out) != 2 {
		t.Fatalf("expected 2 segments, got %d", len(out))
	}

	timeMin := int64(1500)
	timeMax := int64(1600)
	out = filterSegments(parsed, segments, &timeMin, &timeMax)
	if len(out) != 1 || out[0].SegmentKey != "d" {
		t.Fatalf("expected time-filtered segment, got %+v", out)
	}
}

func TestSegmentMatchesRanges(t *testing.T) {
	minOffset := int64(10)
	maxOffset := int64(20)
	minTs := int64(1000)
	maxTs := int64(2000)
	segment := discovery.SegmentRef{MinOffset: &minOffset, MaxOffset: &maxOffset, MinTimestamp: &minTs, MaxTimestamp: &maxTs}

	min := int64(25)
	if segmentMatchesOffsets(segment, &min, nil) {
		t.Fatalf("expected offset mismatch")
	}
	max := int64(5)
	if segmentMatchesOffsets(segment, nil, &max) {
		t.Fatalf("expected offset mismatch on max")
	}
	min = int64(15)
	max = int64(18)
	if !segmentMatchesOffsets(segment, &min, &max) {
		t.Fatalf("expected offset match")
	}

	min = int64(2100)
	if segmentMatchesTimestamps(segment, &min, nil) {
		t.Fatalf("expected timestamp mismatch")
	}
	max = int64(900)
	if segmentMatchesTimestamps(segment, nil, &max) {
		t.Fatalf("expected timestamp mismatch on max")
	}
	min = int64(1500)
	max = int64(1600)
	if !segmentMatchesTimestamps(segment, &min, &max) {
		t.Fatalf("expected timestamp match")
	}
}

func TestEstimateAndFormatBytes(t *testing.T) {
	segments := []discovery.SegmentRef{
		{SizeBytes: 100},
		{SizeBytes: 1024},
	}
	if estimateBytes(segments) != 1124 {
		t.Fatalf("unexpected byte estimate")
	}
	if formatBytes(10) != "10 B" {
		t.Fatalf("unexpected bytes format")
	}
	if formatBytes(2048) != "2.00 KB" {
		t.Fatalf("unexpected KB format")
	}
}

func TestParseLimitAndDuration(t *testing.T) {
	if _, err := parseLimit("x"); err == nil {
		t.Fatalf("expected parse limit error")
	}
	if value, err := parseLimit("12"); err != nil || value != 12 {
		t.Fatalf("unexpected parse limit result")
	}
	if _, err := parseDuration("bad"); err == nil {
		t.Fatalf("expected parse duration error")
	}
	if d, err := parseDuration("2d"); err != nil || d.Hours() != 48 {
		t.Fatalf("unexpected day duration")
	}
}

func TestEnforceScanLimits(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	srv.cfg.Query.MaxScanSegments = 1
	if err := srv.enforceScanLimits(2, 0); err == nil {
		t.Fatalf("expected max segments error")
	}
	srv.cfg.Query.MaxScanSegments = 0
	srv.cfg.Query.MaxScanBytes = 100
	if err := srv.enforceScanLimits(1, 200); err == nil {
		t.Fatalf("expected max bytes error")
	}
}

func TestAggregateHelpers(t *testing.T) {
	state := aggState{Count: 3, Sum: 6, HasSum: true}
	if string(aggStateValue(state, aggSpec{Func: "count"})) != "3" {
		t.Fatalf("unexpected count value")
	}
	if string(aggStateValue(state, aggSpec{Func: "sum"})) != "6" {
		t.Fatalf("unexpected sum value")
	}
	if string(aggStateValue(state, aggSpec{Func: "avg"})) != "2" {
		t.Fatalf("unexpected avg value")
	}

	minState := aggState{Kind: "number", MinSet: true, MinNum: 1.5, MaxSet: true, MaxNum: 9.5}
	if string(aggStateValue(minState, aggSpec{Func: "min"})) != "1.5" {
		t.Fatalf("unexpected min value")
	}
	if string(aggStateValue(minState, aggSpec{Func: "max"})) != "9.5" {
		t.Fatalf("unexpected max value")
	}

	tsState := aggState{Kind: "timestamp", MinSet: true, MinTS: 0, MaxSet: true, MaxTS: 1000}
	if aggStateValue(tsState, aggSpec{Func: "min"}) == nil {
		t.Fatalf("expected timestamp min value")
	}
}

func TestAggregateValueFromColumns(t *testing.T) {
	record := decoder.Record{
		Topic:     "orders",
		Partition: 2,
		Offset:    5,
		Timestamp: 1000,
		Key:       []byte("k"),
		Value:     []byte(`{"num":3,"ts":"2024-01-01 00:00:00"}`),
	}
	ctx := rowContext{left: record, leftSeg: "seg"}
	val, ok := aggValueFromColumn(ctx, resolvedColumn{Kind: columnImplicit, Column: "_offset"})
	if !ok || val.Kind != "number" || val.Num != 5 {
		t.Fatalf("unexpected implicit value")
	}
	val, ok = aggValueFromColumn(ctx, resolvedColumn{Kind: columnJSONValue, JSONPath: "$.num"})
	if !ok || val.Kind != "number" || val.Num != 3 {
		t.Fatalf("unexpected json value")
	}
	schema := config.SchemaColumn{Name: "num", Type: "double", Path: "$.num"}
	val, ok = aggValueFromSchema(record.Value, schema, record.Topic)
	if !ok || val.Kind != "number" || val.Num != 3 {
		t.Fatalf("unexpected schema value")
	}
	ts, ok := parseTimestampValue("2024-01-01 00:00:00")
	if !ok || ts == 0 {
		t.Fatalf("expected parsed timestamp")
	}
}

func TestBuildAggregatePlanErrors(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	parsed := kafsql.Query{
		Type:   kafsql.QuerySelect,
		Topic:  "orders",
		Select: []kafsql.SelectColumn{{Kind: kafsql.SelectColumnField, Column: "_offset"}},
	}
	if _, err := srv.buildAggregatePlan(parsed); err == nil {
		t.Fatalf("expected group by error")
	}

	parsed = kafsql.Query{
		Type:   kafsql.QuerySelect,
		Topic:  "orders",
		GroupBy: []string{"_partition"},
		Select: []kafsql.SelectColumn{{Kind: kafsql.SelectColumnField, Column: "_partition"}},
	}
	if _, err := srv.buildAggregatePlan(parsed); err == nil {
		t.Fatalf("expected missing aggregate error")
	}

	parsed = kafsql.Query{
		Type:   kafsql.QuerySelect,
		Topic:  "orders",
		GroupBy: []string{"_partition"},
		Select: []kafsql.SelectColumn{{Kind: kafsql.SelectColumnJSONValue}},
	}
	if _, err := srv.buildAggregatePlan(parsed); err == nil {
		t.Fatalf("expected json helper error")
	}
}

func TestHandleAggregateValues(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now, Key: []byte("b")},
		{Topic: "orders", Partition: 0, Offset: 2, Timestamp: now, Key: []byte("a")},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
	}})

	parsed, err := kafsql.Parse("SELECT min(_key) AS min_key, max(_key) AS max_key, sum(_offset) AS sum_offset, avg(_offset) AS avg_offset FROM orders LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed, nil)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle select: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if string(rows[0][0]) != "a" || string(rows[0][1]) != "b" || string(rows[0][2]) != "3" || string(rows[0][3]) != "1.5" {
		t.Fatalf("unexpected aggregate row: %+v", rows[0])
	}
}

func TestBuildGroupKeyAndNumericValue(t *testing.T) {
	if buildGroupKey(nil) != "all" {
		t.Fatalf("expected default group key")
	}
	key := buildGroupKey([][]byte{[]byte("a"), nil, []byte("b")})
	if key == "" || key == "all" {
		t.Fatalf("unexpected group key")
	}

	if _, ok := numericValue("x"); ok {
		t.Fatalf("expected numeric parse failure")
	}
	if value, ok := numericValue("12.5"); !ok || value.Num != 12.5 {
		t.Fatalf("unexpected numeric value")
	}
}

func TestEncodingHelpers(t *testing.T) {
	if encodeBytea(nil) != nil {
		t.Fatalf("expected nil bytea")
	}
	encoded := string(encodeBytea([]byte{0x01, 0x02}))
	if encoded != "\\x0102" {
		t.Fatalf("unexpected bytea encoding: %s", encoded)
	}
	headers := []decoder.Header{
		{Key: "a", Value: []byte("b")},
		{Key: "c", Value: []byte("d")},
	}
	jsonHeaders := headersToJSON(headers)
	if jsonHeaders == "{}" || jsonHeaders == "" {
		t.Fatalf("expected headers json")
	}
	if escapeJSON(`a"b`) != `a\"b` {
		t.Fatalf("unexpected json escape")
	}
	if formatTimestamp(0) != "1970-01-01 00:00:00.000" {
		t.Fatalf("unexpected timestamp format")
	}
}
func TestSendCached(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(serverConn), serverConn)
	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)

	entry := &cacheEntry{
		fields: []pgproto3.FieldDescription{
			{Name: []byte("col"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		},
		rows:      [][][]byte{{[]byte("value")}},
		rowsCount: 1,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- sendCached(backend, entry)
	}()

	msg, err := frontend.Receive()
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if _, ok := msg.(*pgproto3.RowDescription); !ok {
		t.Fatalf("expected row description")
	}
	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if row, ok := msg.(*pgproto3.DataRow); !ok || string(row.Values[0]) != "value" {
		t.Fatalf("unexpected data row")
	}
	msg, err = frontend.Receive()
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if _, ok := msg.(*pgproto3.CommandComplete); !ok {
		t.Fatalf("expected command complete")
	}
	if err := <-errCh; err != nil {
		t.Fatalf("send cached: %v", err)
	}
}

func TestHandleSetCommand(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.handleQuery(context.Background(), backend, "SET client_encoding = 'UTF8';")
	}()

	tag := collectCommandTag(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if tag != "SET" {
		t.Fatalf("unexpected command tag: %q", tag)
	}
}

func TestHandleShowTopics(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	srv.resolver = &mockResolver{topics: []string{"orders", "payments"}}
	srv.resolverInit = true

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.handleQuery(context.Background(), backend, "SHOW TOPICS;")
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if string(rows[0][0]) != "orders" || string(rows[1][0]) != "payments" {
		t.Fatalf("unexpected topics: %+v", rows)
	}
}

func TestHandleShowPartitions(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	srv.resolver = &mockResolver{
		topics: []string{"orders"},
		partitions: map[string][]int32{
			"orders": {0, 2},
		},
	}
	srv.resolverInit = true

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.handleQuery(context.Background(), backend, "SHOW PARTITIONS FROM orders;")
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if string(rows[0][0]) != "0" || string(rows[1][0]) != "2" {
		t.Fatalf("unexpected partitions: %+v", rows)
	}
}

func TestCatalogTablesAndColumns(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	srv.resolver = &mockResolver{topics: []string{"orders"}}
	srv.resolverInit = true
	srv.cfg.Metadata.Topics = []config.TopicConfig{
		{
			Name: "orders",
			Schema: config.SchemaConfig{
				Columns: []config.SchemaColumn{
					{Name: "id", Type: "int", Path: "$.id"},
				},
			},
		},
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.handleQuery(context.Background(), backend, "SELECT * FROM information_schema.tables;")
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("catalog tables: %v", err)
	}
	if len(rows) != 1 || string(rows[0][2]) != "orders" {
		t.Fatalf("unexpected catalog tables: %+v", rows)
	}

	backend2, frontend2, cleanup2 := newPipeBackend(t)
	defer cleanup2()
	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- srv.handleQuery(context.Background(), backend2, "SELECT * FROM information_schema.columns;")
	}()

	rows = collectRows(t, frontend2)
	if err := <-errCh2; err != nil {
		t.Fatalf("catalog columns: %v", err)
	}
	found := false
	for _, row := range rows {
		if string(row[2]) == "orders" && string(row[3]) == "id" && string(row[5]) == "integer" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected schema column in catalog: %+v", rows)
	}
}

func TestCatalogPgQueries(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	srv.resolver = &mockResolver{topics: []string{"orders"}}
	srv.resolverInit = true

	queries := []string{
		"SELECT * FROM pg_catalog.pg_namespace;",
		"SELECT * FROM pg_catalog.pg_type;",
		"SELECT * FROM pg_catalog.pg_database;",
		"SELECT * FROM pg_catalog.pg_class;",
	}
	for _, query := range queries {
		backend, frontend, cleanup := newPipeBackend(t)
		errCh := make(chan error, 1)
		go func(q string) {
			errCh <- srv.handleQuery(context.Background(), backend, q)
		}(query)

		rows := collectRows(t, frontend)
		cleanup()
		if err := <-errCh; err != nil {
			t.Fatalf("catalog query %q: %v", query, err)
		}
		if len(rows) == 0 {
			t.Fatalf("expected rows for %q", query)
		}
	}
}

func TestHandleSelectJSONHelpers(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now - 1000, Value: []byte(`{"status":"ok","meta":{"id":123}}`)},
		{Topic: "orders", Partition: 0, Offset: 2, Timestamp: now - 900, Value: []byte(`{"status":`)},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
	}})

	parsed, err := kafsql.Parse("SELECT json_value(_value, '$.status') AS status, json_query(_value, '$.meta') AS meta, json_exists(_value, '$.status') AS has_status FROM orders LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed, nil)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle select: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if string(rows[0][0]) != "ok" {
		t.Fatalf("unexpected json_value: %q", rows[0][0])
	}
	if string(rows[0][1]) != `{"id":123}` {
		t.Fatalf("unexpected json_query: %q", rows[0][1])
	}
	if string(rows[0][2]) != "true" {
		t.Fatalf("unexpected json_exists: %q", rows[0][2])
	}
	if rows[1][0] != nil || rows[1][1] != nil || rows[1][2] != nil {
		t.Fatalf("expected nulls for invalid json, got %+v", rows[1])
	}
}

func TestHandleAggregateGroupBy(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now - 1000},
		{Topic: "orders", Partition: 0, Offset: 2, Timestamp: now - 900},
		{Topic: "orders", Partition: 1, Offset: 3, Timestamp: now - 800},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
	}})

	parsed, err := kafsql.Parse("SELECT _partition, COUNT(*) AS total, SUM(_offset) AS sum_offset FROM orders GROUP BY _partition LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed, nil)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle select: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if string(rows[0][0]) != "0" || string(rows[0][1]) != "2" || string(rows[0][2]) != "3" {
		t.Fatalf("unexpected group 0: %+v", rows[0])
	}
	if string(rows[1][0]) != "1" || string(rows[1][1]) != "1" || string(rows[1][2]) != "3" {
		t.Fatalf("unexpected group 1: %+v", rows[1])
	}
}

func TestHandleJoinJSON(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-orders", IndexKey: "idx-orders"},
		{Topic: "payments", Partition: 0, SegmentKey: "seg-payments", IndexKey: "idx-payments"},
	}
	left := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now, Key: []byte("left-key"), Value: []byte(`{"id":"a"}`)},
	}
	right := []decoder.Record{
		{Topic: "payments", Partition: 0, Offset: 10, Timestamp: now, Key: []byte("right-key"), Value: []byte(`{"id":"a"}`)},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-orders":   left,
		"seg-payments": right,
	}})

	parsed, err := kafsql.Parse("SELECT o._key, p._value FROM orders o JOIN payments p ON json_value(o._value, '$.id') = json_value(p._value, '$.id') WITHIN 10m LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed, nil)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle join: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestHandleJoinLeftNoMatch(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-orders", IndexKey: "idx-orders"},
		{Topic: "payments", Partition: 0, SegmentKey: "seg-payments", IndexKey: "idx-payments"},
	}
	left := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now, Key: []byte("a")},
	}
	right := []decoder.Record{
		{Topic: "payments", Partition: 0, Offset: 10, Timestamp: now, Key: []byte("b")},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-orders":   left,
		"seg-payments": right,
	}})

	parsed, err := kafsql.Parse("SELECT o._key, p._key FROM orders o LEFT JOIN payments p ON o._key = p._key WITHIN 10m LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed, nil)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle join: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if string(rows[0][0]) != "\\x61" || rows[0][1] != nil {
		t.Fatalf("unexpected join row: %+v", rows[0])
	}
}

func TestHandleJoinLeftEmptyKey(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-orders", IndexKey: "idx-orders"},
		{Topic: "payments", Partition: 0, SegmentKey: "seg-payments", IndexKey: "idx-payments"},
	}
	left := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now},
	}
	right := []decoder.Record{
		{Topic: "payments", Partition: 0, Offset: 10, Timestamp: now, Key: []byte("a")},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-orders":   left,
		"seg-payments": right,
	}})

	parsed, err := kafsql.Parse("SELECT o._key, p._key FROM orders o LEFT JOIN payments p ON o._key = p._key WITHIN 10m LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed, nil)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle join: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != nil || rows[0][1] != nil {
		t.Fatalf("unexpected join row: %+v", rows[0])
	}
}

func TestHandleSelectPrunesByOffset(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	min0 := int64(0)
	max9 := int64(9)
	min10 := int64(10)
	max19 := int64(19)
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1", MinOffset: &min0, MaxOffset: &max9},
		{Topic: "orders", Partition: 0, SegmentKey: "seg-2", IndexKey: "idx-2", MinOffset: &min10, MaxOffset: &max19},
	}
	records1 := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 5, Timestamp: now - 1000},
	}
	records2 := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 15, Timestamp: now - 1000},
	}
	dec := &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records1,
		"seg-2": records2,
	}}
	srv := newTestServer(&mockLister{segments: segments}, dec)

	parsed, err := kafsql.Parse("SELECT _offset FROM orders WHERE _offset >= 12 LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed, nil)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle select: %v", err)
	}
	if len(rows) != 1 || string(rows[0][0]) != "15" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
	if len(dec.keys) != 1 || dec.keys[0] != "seg-2" {
		t.Fatalf("expected decoder to hit seg-2 only, got %v", dec.keys)
	}
}

func TestExplainSelect(t *testing.T) {
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1", SizeBytes: 1024},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{})

	parsed, err := kafsql.Parse("EXPLAIN SELECT * FROM orders LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleExplain(context.Background(), backend, parsed)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("explain: %v", err)
	}
	if len(rows) == 0 {
		t.Fatalf("expected plan rows")
	}
	if string(rows[0][0]) != "Query Plan" {
		t.Fatalf("unexpected plan header: %s", rows[0][0])
	}
}

func TestHandleSelectMaxSegments(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1", SizeBytes: 1024},
		{Topic: "orders", Partition: 0, SegmentKey: "seg-2", IndexKey: "idx-2", SizeBytes: 1024},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
		"seg-2": records,
	}})
	srv.cfg.Query.MaxScanSegments = 1

	parsed, err := kafsql.Parse("SELECT _offset FROM orders LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, _, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed, nil)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected max segments error")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for error")
	}
}

func TestQueryQueueFull(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	blocker := &blockingDecoder{
		records:   []decoder.Record{{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now}},
		startedCh: make(chan struct{}, 1),
		releaseCh: make(chan struct{}),
	}
	srv := newTestServer(&mockLister{segments: segments}, blocker)
	srv.cfg.Query.MaxConcurrent = 1
	srv.cfg.Query.QueueSize = 0
	srv.cfg.Query.QueueTimeoutSec = 1
	srv.limiter = newQueryLimiter(srv.cfg.Query.MaxConcurrent, srv.cfg.Query.QueueSize)

	backend1, frontend1, cleanup1 := newPipeBackend(t)
	defer cleanup1()
	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- srv.handleQuery(context.Background(), backend1, "SELECT _offset FROM orders LAST 1h;")
	}()

	msg, recvErr := frontend1.Receive()
	if recvErr != nil {
		t.Fatalf("receive row description: %v", recvErr)
	}
	if _, ok := msg.(*pgproto3.RowDescription); !ok {
		t.Fatalf("expected row description")
	}

	waitForDecoder(t, blocker.startedCh)

	backend2, _, cleanup2 := newPipeBackend(t)
	defer cleanup2()
	err := srv.handleQuery(context.Background(), backend2, "SELECT _offset FROM orders LAST 1h;")
	if !errors.Is(err, errQueueFull) {
		t.Fatalf("expected queue full error, got %v", err)
	}

	close(blocker.releaseCh)
	drainUntilDone(t, frontend1)
	if err := <-errCh1; err != nil {
		t.Fatalf("first query error: %v", err)
	}
}

func TestQueryQueueTimeout(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	blocker := &blockingDecoder{
		records:   []decoder.Record{{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now}},
		startedCh: make(chan struct{}, 1),
		releaseCh: make(chan struct{}),
	}
	srv := newTestServer(&mockLister{segments: segments}, blocker)
	srv.cfg.Query.MaxConcurrent = 1
	srv.cfg.Query.QueueSize = 1
	srv.cfg.Query.QueueTimeoutSec = 1
	srv.limiter = newQueryLimiter(srv.cfg.Query.MaxConcurrent, srv.cfg.Query.QueueSize)

	backend1, frontend1, cleanup1 := newPipeBackend(t)
	defer cleanup1()
	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- srv.handleQuery(context.Background(), backend1, "SELECT _offset FROM orders LAST 1h;")
	}()

	msg, recvErr := frontend1.Receive()
	if recvErr != nil {
		t.Fatalf("receive row description: %v", recvErr)
	}
	if _, ok := msg.(*pgproto3.RowDescription); !ok {
		t.Fatalf("expected row description")
	}

	waitForDecoder(t, blocker.startedCh)

	backend2, _, cleanup2 := newPipeBackend(t)
	defer cleanup2()
	err := srv.handleQuery(context.Background(), backend2, "SELECT _offset FROM orders LAST 1h;")
	if !errors.Is(err, errQueueTimeout) {
		t.Fatalf("expected queue timeout error, got %v", err)
	}

	close(blocker.releaseCh)
	drainUntilDone(t, frontend1)
	if err := <-errCh1; err != nil {
		t.Fatalf("first query error: %v", err)
	}
}
