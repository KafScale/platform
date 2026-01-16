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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgproto3/v2"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/decoder"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/discovery"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/metadata"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/metrics"
	kafsql "github.com/kafscale/platform/addons/processors/sql-processor/internal/sql"
)

type Server struct {
	listenAddr     string
	serverVersion  string
	clientEncoding string
	logger         *log.Logger
	resultCache    *resultCache
	limiter        *queryLimiter

	cfg          config.Config
	resolverMu   sync.Mutex
	resolver     metadata.Resolver
	resolverInit bool
	resolverErr  error

	listerMu   sync.Mutex
	lister     discovery.Lister
	listerInit bool
	listerErr  error

	decoderMu   sync.Mutex
	decoder     decoder.Decoder
	decoderInit bool
	decoderErr  error
}

func New(cfg config.Config, logger *log.Logger) *Server {
	addr := cfg.Server.Listen
	if addr == "" {
		addr = ":5432"
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Server{
		listenAddr:     addr,
		serverVersion:  cfg.Server.ServerVersion,
		clientEncoding: cfg.Server.ClientEncoding,
		logger:         logger,
		resultCache:    newResultCache(time.Duration(cfg.ResultCache.TTLSeconds)*time.Second, cfg.ResultCache.MaxEntries),
		limiter:        newQueryLimiter(cfg.Query.MaxConcurrent, cfg.Query.QueueSize),
		cfg:            cfg,
	}
}

func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	defer ln.Close()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			errCh <- err
			break
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleConnection(ctx, conn)
		}()
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	metrics.ConnectionsTotal.Inc()
	metrics.ConnectionsActive.Inc()
	defer metrics.ConnectionsActive.Dec()

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)
	if _, err := backend.ReceiveStartupMessage(); err != nil {
		s.logger.Printf("startup error: %v", err)
		return
	}

	_ = backend.Send(&pgproto3.AuthenticationOk{})
	_ = backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: s.clientEncoding})
	_ = backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: s.serverVersion})
	_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	state := connState{
		statements: make(map[string]preparedStatement),
		portals:    make(map[string]boundPortal),
	}

	for {
		msg, err := backend.Receive()
		if err != nil {
			return
		}

		switch msg := msg.(type) {
		case *pgproto3.Terminate:
			return
		case *pgproto3.Query:
			if err := s.handleQuery(ctx, backend, msg.String); err != nil {
				_ = backend.Send(&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Message:  err.Error(),
				})
			}
			_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		case *pgproto3.Parse:
			if state.failed {
				continue
			}
			if err := s.handleParse(&state, backend, msg); err != nil {
				state.failed = true
			}
		case *pgproto3.Bind:
			if state.failed {
				continue
			}
			if err := s.handleBind(&state, backend, msg); err != nil {
				state.failed = true
			}
		case *pgproto3.Describe:
			if state.failed {
				continue
			}
			if err := s.handleDescribe(&state, backend, msg); err != nil {
				state.failed = true
			}
		case *pgproto3.Execute:
			if state.failed {
				continue
			}
			if err := s.handleExecute(ctx, &state, backend, msg); err != nil {
				state.failed = true
			}
		case *pgproto3.Close:
			if state.failed {
				continue
			}
			if err := s.handleClose(&state, backend, msg); err != nil {
				state.failed = true
			}
		case *pgproto3.Sync:
			state.failed = false
			_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		case *pgproto3.Flush:
			continue
		default:
			_ = backend.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Message:  "unsupported message type",
			})
			_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		}
	}
}

type connState struct {
	statements map[string]preparedStatement
	portals    map[string]boundPortal
	failed     bool
}

type preparedStatement struct {
	query  string
	parsed kafsql.Query
}

type boundPortal struct {
	statement string
}

func (s *Server) handleParse(state *connState, backend *pgproto3.Backend, msg *pgproto3.Parse) error {
	query := strings.TrimSpace(msg.Query)
	if query == "" {
		return sendErrorResponse(backend, "empty query")
	}
	if hasParameters(query) {
		return sendErrorResponse(backend, "parameters are not supported")
	}
	parsed, err := kafsql.Parse(query)
	if err != nil {
		return sendErrorResponse(backend, err.Error())
	}
	state.statements[msg.Name] = preparedStatement{query: query, parsed: parsed}
	return backend.Send(&pgproto3.ParseComplete{})
}

func (s *Server) handleBind(state *connState, backend *pgproto3.Backend, msg *pgproto3.Bind) error {
	stmt, ok := state.statements[msg.PreparedStatement]
	if !ok {
		return sendErrorResponse(backend, "unknown prepared statement")
	}
	if len(msg.Parameters) > 0 {
		return sendErrorResponse(backend, "bind parameters are not supported")
	}
	state.portals[msg.DestinationPortal] = boundPortal{statement: msg.PreparedStatement}
	_ = stmt
	return backend.Send(&pgproto3.BindComplete{})
}

func (s *Server) handleDescribe(state *connState, backend *pgproto3.Backend, msg *pgproto3.Describe) error {
	stmt, ok := s.lookupStatement(state, msg.ObjectType, msg.Name)
	if !ok {
		return sendErrorResponse(backend, "unknown prepared statement")
	}
	if err := backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: nil}); err != nil {
		return err
	}
	fields, err := s.describeFields(stmt.parsed)
	if err != nil {
		return sendErrorResponse(backend, err.Error())
	}
	if len(fields) == 0 {
		return backend.Send(&pgproto3.NoData{})
	}
	return backend.Send(&pgproto3.RowDescription{Fields: fields})
}

func (s *Server) handleExecute(ctx context.Context, state *connState, backend *pgproto3.Backend, msg *pgproto3.Execute) error {
	stmt, ok := s.lookupStatement(state, 'P', msg.Portal)
	if !ok {
		return sendErrorResponse(backend, "unknown portal")
	}
	if err := s.handlePreparedQuery(ctx, backend, stmt); err != nil {
		return sendErrorResponse(backend, err.Error())
	}
	return nil
}

func (s *Server) handleClose(state *connState, backend *pgproto3.Backend, msg *pgproto3.Close) error {
	switch msg.ObjectType {
	case 'S':
		delete(state.statements, msg.Name)
	case 'P':
		delete(state.portals, msg.Name)
	default:
		return sendErrorResponse(backend, "unsupported close target")
	}
	return backend.Send(&pgproto3.CloseComplete{})
}

func (s *Server) lookupStatement(state *connState, objectType byte, name string) (preparedStatement, bool) {
	switch objectType {
	case 'S':
		stmt, ok := state.statements[name]
		return stmt, ok
	case 'P':
		portal, ok := state.portals[name]
		if !ok {
			return preparedStatement{}, false
		}
		stmt, ok := state.statements[portal.statement]
		return stmt, ok
	default:
		return preparedStatement{}, false
	}
}

func (s *Server) handlePreparedQuery(ctx context.Context, backend *pgproto3.Backend, stmt preparedStatement) error {
	start := time.Now()
	metrics.ActiveQueries.Inc()
	defer metrics.ActiveQueries.Dec()
	ctx, cancel := s.withQueryTimeout(ctx)
	defer cancel()
	release, err := s.limiter.acquire(time.Duration(s.cfg.Query.QueueTimeoutSec) * time.Second)
	if err != nil {
		return err
	}
	defer release()

	if result, handled, err := s.handleSetCommand(ctx, backend, stmt.query); handled {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.QueriesTotal.WithLabelValues("set", status).Inc()
		metrics.QueryDuration.WithLabelValues("set", status).Observe(float64(time.Since(start).Milliseconds()))
		metrics.QueryRows.WithLabelValues("set").Add(float64(result.rows))
		s.logger.Printf("query_complete type=set status=%s duration_ms=%d rows=%d segments=%d bytes=%d query=%q",
			status,
			time.Since(start).Milliseconds(),
			result.rows,
			result.segments,
			result.bytes,
			trimQuery(stmt.query),
		)
		return err
	}

	queryType := string(stmt.parsed.Type)
	var result queryResult
	var execErr error
	if stmt.parsed.Type == kafsql.QuerySelect {
		result, _, execErr = s.handleSelectWithCache(ctx, backend, stmt.parsed, stmt.query)
	} else {
		result, execErr = s.executeQuery(ctx, backend, stmt.parsed)
	}
	status := "success"
	if execErr != nil {
		status = "error"
	}
	metrics.QueriesTotal.WithLabelValues(queryType, status).Inc()
	metrics.QueryDuration.WithLabelValues(queryType, status).Observe(float64(time.Since(start).Milliseconds()))
	metrics.QueryRows.WithLabelValues(queryType).Add(float64(result.rows))
	metrics.QuerySegments.Add(float64(result.segments))
	metrics.QueryBytes.Add(float64(result.bytes))
	s.logger.Printf("query_complete type=%s status=%s duration_ms=%d rows=%d segments=%d bytes=%d query=%q",
		queryType,
		status,
		time.Since(start).Milliseconds(),
		result.rows,
		result.segments,
		result.bytes,
		trimQuery(stmt.query),
	)
	return execErr
}

func (s *Server) describeFields(parsed kafsql.Query) ([]pgproto3.FieldDescription, error) {
	switch parsed.Type {
	case kafsql.QueryShowTopics:
		return []pgproto3.FieldDescription{
			{Name: []byte("topic"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		}, nil
	case kafsql.QueryShowPartitions:
		return []pgproto3.FieldDescription{
			{Name: []byte("partition"), DataTypeOID: 23, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		}, nil
	case kafsql.QueryExplain:
		return []pgproto3.FieldDescription{
			{Name: []byte("QUERY PLAN"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		}, nil
	case kafsql.QueryDescribe:
		return []pgproto3.FieldDescription{
			{Name: []byte("column"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
			{Name: []byte("type"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
			{Name: []byte("path"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		}, nil
	case kafsql.QuerySelect:
		if parsed.JoinTopic != "" {
			cols, err := s.resolveJoinColumns(parsed)
			if err != nil {
				return nil, err
			}
			return buildRowDescription(cols), nil
		}
		if hasAggregates(parsed.Select) {
			plan, err := s.buildAggregatePlan(parsed)
			if err != nil {
				return nil, err
			}
			fields := make([]pgproto3.FieldDescription, 0, len(plan.outputs))
			for _, out := range plan.outputs {
				fields = append(fields, pgproto3.FieldDescription{Name: []byte(out.Name), DataTypeOID: out.DataType, DataTypeSize: -1, TypeModifier: -1, Format: 0})
			}
			return fields, nil
		}
		cols, err := s.resolveSelectColumns(parsed.Topic, parsed.Select)
		if err != nil {
			return nil, err
		}
		return buildRowDescription(cols), nil
	default:
		return nil, fmt.Errorf("unsupported statement for describe")
	}
}

func hasParameters(query string) bool {
	re := regexp.MustCompile(`\$\d+`)
	return re.MatchString(query)
}

func sendErrorResponse(backend *pgproto3.Backend, message string) error {
	return backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Message:  message,
	})
}

func (s *Server) send(backend *pgproto3.Backend, collector *rowCollector, msg pgproto3.BackendMessage) error {
	if collector != nil {
		collector.capture(msg)
	}
	return backend.Send(msg)
}

func sendCached(backend *pgproto3.Backend, entry *cacheEntry) error {
	if entry == nil {
		return nil
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: entry.fields}); err != nil {
		return err
	}
	for _, row := range entry.rows {
		if err := backend.Send(&pgproto3.DataRow{Values: row}); err != nil {
			return err
		}
	}
	tag := entry.commandTag
	if len(tag) == 0 {
		tag = commandTag(entry.rowsCount)
	}
	return backend.Send(&pgproto3.CommandComplete{CommandTag: tag})
}

func (s *Server) handleQuery(ctx context.Context, backend *pgproto3.Backend, query string) error {
	start := time.Now()
	metrics.ActiveQueries.Inc()
	defer metrics.ActiveQueries.Dec()
	ctx, cancel := s.withQueryTimeout(ctx)
	defer cancel()
	release, err := s.limiter.acquire(time.Duration(s.cfg.Query.QueueTimeoutSec) * time.Second)
	if err != nil {
		return err
	}
	defer release()

	if result, handled, err := s.handleCatalogQuery(ctx, backend, query); handled {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.QueriesTotal.WithLabelValues("catalog", status).Inc()
		metrics.QueryDuration.WithLabelValues("catalog", status).Observe(float64(time.Since(start).Milliseconds()))
		metrics.QueryRows.WithLabelValues("catalog").Add(float64(result.rows))
		s.logger.Printf("query_complete type=catalog status=%s duration_ms=%d rows=%d segments=%d bytes=%d query=%q",
			status,
			time.Since(start).Milliseconds(),
			result.rows,
			result.segments,
			result.bytes,
			trimQuery(query),
		)
		return err
	}

	if result, handled, err := s.handleSetCommand(ctx, backend, query); handled {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.QueriesTotal.WithLabelValues("set", status).Inc()
		metrics.QueryDuration.WithLabelValues("set", status).Observe(float64(time.Since(start).Milliseconds()))
		metrics.QueryRows.WithLabelValues("set").Add(float64(result.rows))
		s.logger.Printf("query_complete type=set status=%s duration_ms=%d rows=%d segments=%d bytes=%d query=%q",
			status,
			time.Since(start).Milliseconds(),
			result.rows,
			result.segments,
			result.bytes,
			trimQuery(query),
		)
		return err
	}

	parsed, err := kafsql.Parse(query)
	if err != nil {
		metrics.QueriesTotal.WithLabelValues("parse", "error").Inc()
		metrics.QueryDuration.WithLabelValues("parse", "error").Observe(float64(time.Since(start).Milliseconds()))
		s.logger.Printf("query_error type=parse err=%v query=%q", err, trimQuery(query))
		return err
	}

	queryType := string(parsed.Type)
	var result queryResult
	var execErr error
	if parsed.Type == kafsql.QuerySelect {
		result, _, execErr = s.handleSelectWithCache(ctx, backend, parsed, query)
	} else {
		result, execErr = s.executeQuery(ctx, backend, parsed)
	}
	status := "success"
	if execErr != nil {
		status = "error"
	}

	metrics.QueriesTotal.WithLabelValues(queryType, status).Inc()
	metrics.QueryDuration.WithLabelValues(queryType, status).Observe(float64(time.Since(start).Milliseconds()))
	metrics.QueryRows.WithLabelValues(queryType).Add(float64(result.rows))
	metrics.QuerySegments.Add(float64(result.segments))
	metrics.QueryBytes.Add(float64(result.bytes))

	s.logger.Printf("query_complete type=%s status=%s duration_ms=%d rows=%d segments=%d bytes=%d query=%q",
		queryType,
		status,
		time.Since(start).Milliseconds(),
		result.rows,
		result.segments,
		result.bytes,
		trimQuery(query),
	)
	return execErr
}

func (s *Server) handleSelectWithCache(ctx context.Context, backend *pgproto3.Backend, parsed kafsql.Query, query string) (queryResult, bool, error) {
	key, ok := s.cacheKey(parsed, query)
	if ok && s.resultCache != nil {
		if entry, hit := s.resultCache.Get(key); hit {
			if err := sendCached(backend, entry); err != nil {
				return queryResult{}, false, err
			}
			return queryResult{rows: entry.rowsCount, segments: entry.segments, bytes: entry.bytes}, true, nil
		}
	}

	var collector *rowCollector
	if ok && s.resultCache != nil {
		collector = newRowCollector(s.cfg.ResultCache.MaxRows)
	}
	result, err := s.handleSelect(ctx, backend, parsed, collector)
	if err != nil {
		return result, false, err
	}
	if ok && s.resultCache != nil {
		if entry := collector.entry(); entry != nil {
			entry.segments = result.segments
			entry.bytes = result.bytes
			s.resultCache.Set(key, entry)
		}
	}
	return result, false, nil
}

func (s *Server) cacheKey(parsed kafsql.Query, query string) (string, bool) {
	if parsed.Tail != "" || parsed.ScanFull {
		return "", false
	}
	if parsed.Last != "" {
		return cacheKey(query, "last="+parsed.Last), true
	}
	if parsed.TsMin == nil || parsed.TsMax == nil {
		return "", false
	}
	timeKey := fmt.Sprintf("ts=%d-%d", *parsed.TsMin, *parsed.TsMax)
	return cacheKey(query, timeKey), true
}

func (s *Server) handleCatalogQuery(ctx context.Context, backend *pgproto3.Backend, query string) (queryResult, bool, error) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return queryResult{}, false, nil
	}
	lower := strings.ToLower(strings.TrimSuffix(trimmed, ";"))
	if !strings.Contains(lower, "pg_catalog") && !strings.Contains(lower, "information_schema") {
		return queryResult{}, false, nil
	}

	switch {
	case strings.Contains(lower, "information_schema.tables"):
		rows, err := s.catalogTables(ctx, backend)
		return queryResult{rows: rows}, true, err
	case strings.Contains(lower, "information_schema.columns"):
		rows, err := s.catalogColumns(ctx, backend)
		return queryResult{rows: rows}, true, err
	case strings.Contains(lower, "pg_catalog.pg_tables"):
		rows, err := s.catalogPgTables(ctx, backend)
		return queryResult{rows: rows}, true, err
	case strings.Contains(lower, "pg_catalog.pg_namespace"):
		rows, err := s.catalogPgNamespace(ctx, backend)
		return queryResult{rows: rows}, true, err
	case strings.Contains(lower, "pg_catalog.pg_type"):
		rows, err := s.catalogPgType(ctx, backend)
		return queryResult{rows: rows}, true, err
	case strings.Contains(lower, "pg_catalog.pg_database"):
		rows, err := s.catalogPgDatabase(ctx, backend)
		return queryResult{rows: rows}, true, err
	case strings.Contains(lower, "pg_catalog.pg_class"):
		rows, err := s.catalogPgClass(ctx, backend)
		return queryResult{rows: rows}, true, err
	default:
		return queryResult{}, true, errors.New("unsupported catalog query")
	}
}

func (s *Server) handleSetCommand(ctx context.Context, backend *pgproto3.Backend, query string) (queryResult, bool, error) {
	_ = ctx
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return queryResult{}, false, nil
	}
	lower := strings.ToLower(trimmed)
	switch {
	case strings.HasPrefix(lower, "set "):
		if err := backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SET")}); err != nil {
			return queryResult{}, true, err
		}
		return queryResult{}, true, nil
	case strings.HasPrefix(lower, "reset "):
		if err := backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("RESET")}); err != nil {
			return queryResult{}, true, err
		}
		return queryResult{}, true, nil
	}
	return queryResult{}, false, nil
}

type queryResult struct {
	rows     int
	segments int
	bytes    int64
}

type columnKind int

const (
	columnImplicit columnKind = iota
	columnSchema
	columnJSONValue
	columnJSONQuery
	columnJSONExists
)

type resolvedColumn struct {
	Name     string
	Kind     columnKind
	Source   string
	Column   string
	JSONPath string
	Schema   config.SchemaColumn
}

type outputKind int

const (
	outputGroup outputKind = iota
	outputAggregate
)

type outputColumn struct {
	Name     string
	Kind     outputKind
	GroupIdx int
	AggIdx   int
	DataType uint32
}

type aggArgKind int

const (
	aggArgStar aggArgKind = iota
	aggArgColumn
)

type aggArg struct {
	Kind   aggArgKind
	Column resolvedColumn
}

type aggSpec struct {
	Func string
	Name string
	Arg  aggArg
}

type aggState struct {
	Func   string
	Count  int64
	Sum    float64
	HasSum bool
	MinSet bool
	MinNum float64
	MinTS  int64
	MinStr string
	MaxSet bool
	MaxNum float64
	MaxTS  int64
	MaxStr string
	Kind   string
}

type groupState struct {
	Values [][]byte
	Aggs   []aggState
}

func (s *Server) executeQuery(ctx context.Context, backend *pgproto3.Backend, parsed kafsql.Query) (queryResult, error) {
	switch parsed.Type {
	case kafsql.QueryShowTopics:
		rows, err := s.handleShowTopics(ctx, backend)
		return queryResult{rows: rows}, err
	case kafsql.QueryShowPartitions:
		rows, err := s.handleShowPartitions(ctx, backend, parsed.Topic)
		return queryResult{rows: rows}, err
	case kafsql.QueryDescribe:
		rows, err := s.handleDescribeTopic(ctx, backend, parsed.Topic)
		return queryResult{rows: rows}, err
	case kafsql.QuerySelect:
		return s.handleSelect(ctx, backend, parsed, nil)
	case kafsql.QueryExplain:
		return s.handleExplain(ctx, backend, parsed)
	default:
		return queryResult{}, errors.New("KAFSQL query execution not implemented")
	}
}

func (s *Server) handleShowTopics(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	resolver, err := s.getResolver()
	if err != nil {
		return 0, err
	}
	topics, err := resolver.Topics(ctx)
	if err != nil {
		return 0, err
	}

	fields := []pgproto3.FieldDescription{
		{Name: []byte("topic"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}

	for _, topic := range topics {
		if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(topic)}}); err != nil {
			return 0, err
		}
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(len(topics))})
	return len(topics), nil
}

func (s *Server) handleShowPartitions(ctx context.Context, backend *pgproto3.Backend, topic string) (int, error) {
	if topic == "" {
		return 0, errors.New("show partitions requires a topic")
	}
	resolver, err := s.getResolver()
	if err != nil {
		return 0, err
	}
	partitions, err := resolver.Partitions(ctx, topic)
	if err != nil {
		return 0, err
	}

	fields := []pgproto3.FieldDescription{
		{Name: []byte("partition"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}

	for _, partition := range partitions {
		if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(itoa(int(partition)))}}); err != nil {
			return 0, err
		}
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(len(partitions))})
	return len(partitions), nil
}

func (s *Server) handleDescribeTopic(ctx context.Context, backend *pgproto3.Backend, topic string) (int, error) {
	_ = ctx
	if topic == "" {
		return 0, errors.New("describe requires a topic")
	}

	fields := []pgproto3.FieldDescription{
		{Name: []byte("column"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("type"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("path"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}

	rows := 0
	implicit := []struct {
		name string
		typ  string
	}{
		{name: "_topic", typ: "text"},
		{name: "_partition", typ: "integer"},
		{name: "_offset", typ: "bigint"},
		{name: "_ts", typ: "timestamp without time zone"},
		{name: "_key", typ: "bytea"},
		{name: "_value", typ: "bytea"},
		{name: "_headers", typ: "jsonb"},
		{name: "_segment", typ: "text"},
	}
	for _, col := range implicit {
		if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(col.name), []byte(col.typ), nil}}); err != nil {
			return rows, err
		}
		rows++
	}

	for _, col := range s.schemaColumnsForTopic(topic) {
		name := strings.ToLower(col.Name)
		typ := schemaTypeName(col.Type)
		path := col.Path
		if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(name), []byte(typ), []byte(path)}}); err != nil {
			return rows, err
		}
		rows++
	}

	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(rows)})
	return rows, nil
}

func (s *Server) handleExplain(ctx context.Context, backend *pgproto3.Backend, parsed kafsql.Query) (queryResult, error) {
	if parsed.Explain == nil {
		return queryResult{}, errors.New("explain requires query")
	}
	inner := *parsed.Explain
	lines, err := s.explainQuery(ctx, inner)
	if err != nil {
		return queryResult{}, err
	}

	fields := []pgproto3.FieldDescription{
		{Name: []byte("plan"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return queryResult{}, err
	}
	for _, line := range lines {
		if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(line)}}); err != nil {
			return queryResult{}, err
		}
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(len(lines))})
	return queryResult{rows: len(lines)}, nil
}

func (s *Server) explainQuery(ctx context.Context, parsed kafsql.Query) ([]string, error) {
	if parsed.Type != kafsql.QuerySelect {
		return nil, errors.New("explain supports select only")
	}
	if parsed.Topic == "" {
		return nil, errors.New("select requires a topic")
	}
	if s.cfg.Query.RequireTimeBound && !parsed.ScanFull && parsed.Last == "" && parsed.Tail == "" && parsed.TsMin == nil && parsed.TsMax == nil {
		return nil, errors.New("unbounded query: add LAST, TAIL, or SCAN FULL")
	}

	lister, err := s.getLister()
	if err != nil {
		return nil, err
	}
	segments, err := lister.ListCompleted(ctx)
	if err != nil {
		return nil, err
	}

	timeMin := parsed.TsMin
	timeMax := parsed.TsMax
	if parsed.Last != "" {
		window, err := parseDuration(parsed.Last)
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC().UnixMilli()
		start := now - window.Milliseconds()
		timeMin = maxInt64Ptr(timeMin, start)
		if timeMax == nil {
			timeMax = &now
		}
	}

	if parsed.JoinTopic != "" {
		return s.explainJoin(parsed, segments, timeMin, timeMax)
	}
	return s.explainSelect(parsed, segments, timeMin, timeMax)
}

func (s *Server) explainSelect(parsed kafsql.Query, segments []discovery.SegmentRef, timeMin *int64, timeMax *int64) ([]string, error) {
	candidates := filterSegments(parsed, segments, timeMin, timeMax)
	estBytes := estimateBytes(candidates)
	lines := []string{
		"Query Plan",
		fmt.Sprintf("  Scan: %s", parsed.Topic),
		fmt.Sprintf("  Segments: %d", len(candidates)),
		fmt.Sprintf("  Estimated bytes: %s", formatBytes(estBytes)),
	}
	return lines, nil
}

func (s *Server) explainJoin(parsed kafsql.Query, segments []discovery.SegmentRef, timeMin *int64, timeMax *int64) ([]string, error) {
	left := parsed
	left.JoinTopic = ""
	right := parsed
	right.Topic = parsed.JoinTopic
	right.Partition = nil
	right.OffsetMin = nil
	right.OffsetMax = nil

	leftSegments := filterSegments(left, segments, timeMin, timeMax)
	rightSegments := filterSegments(right, segments, nil, nil)
	estBytes := estimateBytes(leftSegments) + estimateBytes(rightSegments)

	lines := []string{
		"Query Plan",
		fmt.Sprintf("  Join: %s %s %s", parsed.Topic, strings.ToUpper(parsed.JoinType), parsed.JoinTopic),
		fmt.Sprintf("  Left segments: %d", len(leftSegments)),
		fmt.Sprintf("  Right segments: %d", len(rightSegments)),
		fmt.Sprintf("  Estimated bytes: %s", formatBytes(estBytes)),
	}
	return lines, nil
}

func filterSegments(parsed kafsql.Query, segments []discovery.SegmentRef, timeMin *int64, timeMax *int64) []discovery.SegmentRef {
	out := make([]discovery.SegmentRef, 0)
	for _, segment := range segments {
		if segment.Topic != parsed.Topic {
			continue
		}
		if parsed.Partition != nil && segment.Partition != *parsed.Partition {
			continue
		}
		if !segmentMatchesOffsets(segment, parsed.OffsetMin, parsed.OffsetMax) {
			continue
		}
		if !segmentMatchesTimestamps(segment, timeMin, timeMax) {
			continue
		}
		out = append(out, segment)
	}
	return out
}

func estimateBytes(segments []discovery.SegmentRef) int64 {
	total := int64(0)
	for _, segment := range segments {
		if segment.SizeBytes > 0 {
			total += segment.SizeBytes
		}
	}
	return total
}

func formatBytes(value int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case value >= gb:
		return fmt.Sprintf("%.2f GB", float64(value)/float64(gb))
	case value >= mb:
		return fmt.Sprintf("%.2f MB", float64(value)/float64(mb))
	case value >= kb:
		return fmt.Sprintf("%.2f KB", float64(value)/float64(kb))
	default:
		return fmt.Sprintf("%d B", value)
	}
}

func (s *Server) catalogTables(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	tables, err := s.topicTables(ctx)
	if err != nil {
		return 0, err
	}
	fields := []pgproto3.FieldDescription{
		{Name: []byte("table_catalog"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("table_schema"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("table_name"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("table_type"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}
	for _, table := range tables {
		row := [][]byte{
			[]byte("kafsql"),
			[]byte("public"),
			[]byte(table),
			[]byte("BASE TABLE"),
		}
		if err := backend.Send(&pgproto3.DataRow{Values: row}); err != nil {
			return 0, err
		}
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(len(tables))})
	return len(tables), nil
}

func (s *Server) catalogColumns(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	tables, err := s.topicTables(ctx)
	if err != nil {
		return 0, err
	}
	fields := []pgproto3.FieldDescription{
		{Name: []byte("table_catalog"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("table_schema"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("table_name"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("column_name"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("ordinal_position"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
		{Name: []byte("data_type"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}

	rowCount := 0
	for _, table := range tables {
		cols := s.columnsForTopic(table)
		for i, col := range cols {
			row := [][]byte{
				[]byte("kafsql"),
				[]byte("public"),
				[]byte(table),
				[]byte(col.name),
				[]byte(itoa(i + 1)),
				[]byte(col.dataType),
			}
			if err := backend.Send(&pgproto3.DataRow{Values: row}); err != nil {
				return 0, err
			}
			rowCount++
		}
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(rowCount)})
	return rowCount, nil
}

func (s *Server) catalogPgTables(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	tables, err := s.topicTables(ctx)
	if err != nil {
		return 0, err
	}
	fields := []pgproto3.FieldDescription{
		{Name: []byte("schemaname"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("tablename"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("tableowner"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("tablespace"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("hasindexes"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1, Format: 0},
		{Name: []byte("hasrules"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1, Format: 0},
		{Name: []byte("hastriggers"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1, Format: 0},
		{Name: []byte("rowsecurity"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}
	for _, table := range tables {
		row := [][]byte{
			[]byte("public"),
			[]byte(table),
			[]byte("kafsql"),
			nil,
			[]byte("f"),
			[]byte("f"),
			[]byte("f"),
			[]byte("f"),
		}
		if err := backend.Send(&pgproto3.DataRow{Values: row}); err != nil {
			return 0, err
		}
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(len(tables))})
	return len(tables), nil
}

func (s *Server) catalogPgNamespace(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	fields := []pgproto3.FieldDescription{
		{Name: []byte("oid"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
		{Name: []byte("nspname"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}
	if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1"), []byte("pg_catalog")}}); err != nil {
		return 0, err
	}
	if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("2"), []byte("public")}}); err != nil {
		return 0, err
	}
	if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("3"), []byte("information_schema")}}); err != nil {
		return 0, err
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(3)})
	return 3, nil
}

func (s *Server) catalogPgType(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	fields := []pgproto3.FieldDescription{
		{Name: []byte("oid"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
		{Name: []byte("typname"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}
	rows := [][]byte{
		[]byte("16"), []byte("bool"),
		[]byte("17"), []byte("bytea"),
		[]byte("20"), []byte("int8"),
		[]byte("23"), []byte("int4"),
		[]byte("25"), []byte("text"),
		[]byte("701"), []byte("float8"),
		[]byte("1114"), []byte("timestamp"),
		[]byte("3802"), []byte("jsonb"),
	}
	rowCount := 0
	for i := 0; i < len(rows); i += 2 {
		if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{rows[i], rows[i+1]}}); err != nil {
			return 0, err
		}
		rowCount++
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(rowCount)})
	return rowCount, nil
}

func (s *Server) catalogPgDatabase(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	fields := []pgproto3.FieldDescription{
		{Name: []byte("oid"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
		{Name: []byte("datname"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("datistemplate"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1, Format: 0},
		{Name: []byte("datallowconn"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}
	row := [][]byte{[]byte("1"), []byte("kafsql"), []byte("f"), []byte("t")}
	if err := backend.Send(&pgproto3.DataRow{Values: row}); err != nil {
		return 0, err
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(1)})
	return 1, nil
}

func (s *Server) catalogPgClass(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	tables, err := s.topicTables(ctx)
	if err != nil {
		return 0, err
	}
	fields := []pgproto3.FieldDescription{
		{Name: []byte("oid"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
		{Name: []byte("relname"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("relkind"), DataTypeOID: 18, DataTypeSize: 1, TypeModifier: -1, Format: 0},
		{Name: []byte("relnamespace"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}
	rowCount := 0
	for i, table := range tables {
		row := [][]byte{
			[]byte(itoa(1000 + i)),
			[]byte(table),
			[]byte("r"),
			[]byte("2"),
		}
		if err := backend.Send(&pgproto3.DataRow{Values: row}); err != nil {
			return 0, err
		}
		rowCount++
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(rowCount)})
	return rowCount, nil
}

type catalogColumn struct {
	name     string
	dataType string
}

func (s *Server) topicTables(ctx context.Context) ([]string, error) {
	resolver, err := s.getResolver()
	if err != nil {
		return nil, err
	}
	topics, err := resolver.Topics(ctx)
	if err != nil {
		return nil, err
	}
	sort.Strings(topics)
	return topics, nil
}

func (s *Server) columnsForTopic(topic string) []catalogColumn {
	cols := []catalogColumn{
		{name: "_topic", dataType: "text"},
		{name: "_partition", dataType: "integer"},
		{name: "_offset", dataType: "bigint"},
		{name: "_ts", dataType: "timestamp without time zone"},
		{name: "_key", dataType: "bytea"},
		{name: "_value", dataType: "bytea"},
		{name: "_headers", dataType: "jsonb"},
		{name: "_segment", dataType: "text"},
	}
	schemaCols := s.schemaColumnsForTopic(topic)
	for _, col := range schemaCols {
		cols = append(cols, catalogColumn{
			name:     strings.ToLower(col.Name),
			dataType: schemaTypeName(col.Type),
		})
	}
	return cols
}

func schemaTypeName(value string) string {
	switch strings.ToLower(value) {
	case "int":
		return "integer"
	case "long":
		return "bigint"
	case "double":
		return "double precision"
	case "boolean":
		return "boolean"
	case "timestamp":
		return "timestamp without time zone"
	default:
		return "text"
	}
}

func (s *Server) getResolver() (metadata.Resolver, error) {
	s.resolverMu.Lock()
	defer s.resolverMu.Unlock()

	if s.resolverInit {
		return s.resolver, s.resolverErr
	}
	s.resolverInit = true
	s.resolver, s.resolverErr = metadata.NewResolver(s.cfg)
	return s.resolver, s.resolverErr
}

func (s *Server) getLister() (discovery.Lister, error) {
	s.listerMu.Lock()
	defer s.listerMu.Unlock()

	if s.listerInit {
		return s.lister, s.listerErr
	}
	s.listerInit = true
	s.lister, s.listerErr = discovery.New(s.cfg)
	return s.lister, s.listerErr
}

func (s *Server) getDecoder() (decoder.Decoder, error) {
	s.decoderMu.Lock()
	defer s.decoderMu.Unlock()

	if s.decoderInit {
		return s.decoder, s.decoderErr
	}
	s.decoderInit = true
	s.decoder, s.decoderErr = decoder.New(s.cfg)
	return s.decoder, s.decoderErr
}

func (s *Server) handleSelect(ctx context.Context, backend *pgproto3.Backend, parsed kafsql.Query, collector *rowCollector) (queryResult, error) {
	if parsed.JoinTopic != "" {
		return s.handleJoinSelect(ctx, backend, parsed, collector)
	}
	if parsed.Topic == "" {
		return queryResult{}, errors.New("select requires a topic")
	}

	if s.cfg.Query.RequireTimeBound && !parsed.ScanFull && parsed.Last == "" && parsed.Tail == "" && parsed.TsMin == nil && parsed.TsMax == nil {
		metrics.QueryUnboundedRejected.Inc()
		return queryResult{}, errors.New("unbounded query: add LAST, TAIL, or SCAN FULL")
	}

	limit := s.cfg.Query.DefaultLimit
	if parsed.Limit != "" {
		value, err := parseLimit(parsed.Limit)
		if err != nil {
			return queryResult{}, err
		}
		limit = value
	}
	tailCount := 0
	if parsed.Tail != "" {
		value, err := parseLimit(parsed.Tail)
		if err != nil {
			return queryResult{}, err
		}
		tailCount = value
		limit = value
	}
	if limit <= 0 {
		limit = s.cfg.Query.DefaultLimit
	}
	if s.cfg.Query.MaxRows > 0 && limit > s.cfg.Query.MaxRows {
		return queryResult{}, fmt.Errorf("limit exceeds max_rows (%d)", s.cfg.Query.MaxRows)
	}
	if parsed.ScanFull && limit > s.cfg.Query.MaxUnbounded {
		return queryResult{}, fmt.Errorf("scan full limit exceeds max_unbounded_scan (%d)", s.cfg.Query.MaxUnbounded)
	}
	if parsed.OrderBy != "" && parsed.OrderBy != "_ts" {
		return queryResult{}, errors.New("order by supports _ts only")
	}
	if parsed.OrderBy != "" && tailCount > 0 {
		return queryResult{}, errors.New("tail cannot be combined with order by")
	}

	lister, err := s.getLister()
	if err != nil {
		return queryResult{}, err
	}
	dec, err := s.getDecoder()
	if err != nil {
		return queryResult{}, err
	}

	segments, err := lister.ListCompleted(ctx)
	if err != nil {
		return queryResult{}, err
	}

	timeMin := parsed.TsMin
	timeMax := parsed.TsMax
	if parsed.Last != "" {
		window, err := parseDuration(parsed.Last)
		if err != nil {
			return queryResult{}, err
		}
		now := time.Now().UTC().UnixMilli()
		start := now - window.Milliseconds()
		timeMin = maxInt64Ptr(timeMin, start)
		if timeMax == nil {
			timeMax = &now
		}
	}
	if timeMin != nil && timeMax != nil && *timeMax < *timeMin {
		return queryResult{}, errors.New("time window is invalid")
	}

	candidates := filterSegments(parsed, segments, timeMin, timeMax)
	if err := s.enforceScanLimits(len(candidates), estimateBytes(candidates)); err != nil {
		return queryResult{}, err
	}

	if hasAggregates(parsed.Select) {
		if parsed.OrderBy != "" {
			return queryResult{}, errors.New("order by not supported with aggregates")
		}
		if parsed.Tail != "" {
			return queryResult{}, errors.New("tail not supported with aggregates")
		}
		return s.handleAggregateSelect(ctx, backend, parsed, candidates, timeMin, timeMax, limit, collector)
	}

	resolvedCols, err := s.resolveSelectColumns(parsed.Topic, parsed.Select)
	if err != nil {
		return queryResult{}, err
	}
	fields := buildRowDescription(resolvedCols)
	if err := s.send(backend, collector, &pgproto3.RowDescription{Fields: fields}); err != nil {
		return queryResult{}, err
	}

	sent := 0
	segmentsScanned := 0
	bytesScanned := int64(0)
	var rows []rowResult
	var tailRows []rowResult
	for _, segment := range candidates {
		if err := ctx.Err(); err != nil {
			return queryResult{}, err
		}
		segmentsScanned++
		records, err := dec.Decode(ctx, segment.SegmentKey, segment.IndexKey, segment.Topic, segment.Partition)
		if err != nil {
			return queryResult{}, err
		}
		for _, record := range records {
			if timeMin != nil && record.Timestamp < *timeMin {
				continue
			}
			if timeMax != nil && record.Timestamp > *timeMax {
				continue
			}
			if parsed.OffsetMin != nil && record.Offset < *parsed.OffsetMin {
				continue
			}
			if parsed.OffsetMax != nil && record.Offset > *parsed.OffsetMax {
				continue
			}
			bytesScanned += int64(len(record.Key) + len(record.Value))
			row := rowResult{
				values: buildRowValues(resolvedCols, rowContext{left: record, leftSeg: segment.SegmentKey}),
				ts:     record.Timestamp,
			}
			if parsed.OrderBy != "" {
				rows = append(rows, row)
				continue
			}
			if tailCount > 0 {
				tailRows = appendTailRow(tailRows, row, tailCount)
				continue
			}
			if err := s.send(backend, collector, &pgproto3.DataRow{Values: row.values}); err != nil {
				return queryResult{}, err
			}
			sent++
			if sent >= limit {
				_ = s.send(backend, collector, &pgproto3.CommandComplete{CommandTag: commandTag(sent)})
				return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
			}
		}
	}

	if parsed.OrderBy != "" {
		sort.Slice(rows, func(i, j int) bool {
			if parsed.OrderDesc {
				return rows[i].ts > rows[j].ts
			}
			return rows[i].ts < rows[j].ts
		})
		if limit > 0 && len(rows) > limit {
			rows = rows[:limit]
		}
		for _, row := range rows {
			if err := s.send(backend, collector, &pgproto3.DataRow{Values: row.values}); err != nil {
				return queryResult{}, err
			}
			sent++
		}
	} else if tailCount > 0 {
		for _, row := range tailRows {
			if err := s.send(backend, collector, &pgproto3.DataRow{Values: row.values}); err != nil {
				return queryResult{}, err
			}
			sent++
		}
	}

	_ = s.send(backend, collector, &pgproto3.CommandComplete{CommandTag: commandTag(sent)})
	return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
}

func (s *Server) resolveSelectColumns(topic string, cols []kafsql.SelectColumn) ([]resolvedColumn, error) {
	if len(cols) == 0 {
		cols = []kafsql.SelectColumn{{Kind: kafsql.SelectColumnStar, Raw: "*"}}
	}
	for _, col := range cols {
		if col.Kind == kafsql.SelectColumnAggregate {
			return nil, fmt.Errorf("aggregate columns require group by")
		}
	}

	schemaCols := s.schemaColumnsForTopic(topic)
	schemaMap := s.schemaColumnMapForTopic(topic)
	if len(cols) == 1 && cols[0].Kind == kafsql.SelectColumnStar {
		out := []resolvedColumn{
			{Name: "_topic", Kind: columnImplicit, Column: "_topic"},
			{Name: "_partition", Kind: columnImplicit, Column: "_partition"},
			{Name: "_offset", Kind: columnImplicit, Column: "_offset"},
			{Name: "_ts", Kind: columnImplicit, Column: "_ts"},
			{Name: "_key", Kind: columnImplicit, Column: "_key"},
			{Name: "_value", Kind: columnImplicit, Column: "_value"},
			{Name: "_headers", Kind: columnImplicit, Column: "_headers"},
			{Name: "_segment", Kind: columnImplicit, Column: "_segment"},
		}
		for _, col := range schemaCols {
			name := strings.ToLower(col.Name)
			out = append(out, resolvedColumn{Name: name, Kind: columnSchema, Column: name, Schema: col})
		}
		return out, nil
	}

	out := make([]resolvedColumn, 0, len(cols))
	for _, col := range cols {
		switch col.Kind {
		case kafsql.SelectColumnStar:
			return nil, fmt.Errorf("select * cannot be combined with other columns")
		case kafsql.SelectColumnJSONValue:
			name := col.Alias
			if name == "" {
				name = "json_value"
			}
			out = append(out, resolvedColumn{Name: name, Kind: columnJSONValue, JSONPath: col.JSONPath})
		case kafsql.SelectColumnJSONQuery:
			name := col.Alias
			if name == "" {
				name = "json_query"
			}
			out = append(out, resolvedColumn{Name: name, Kind: columnJSONQuery, JSONPath: col.JSONPath})
		case kafsql.SelectColumnJSONExists:
			name := col.Alias
			if name == "" {
				name = "json_exists"
			}
			out = append(out, resolvedColumn{Name: name, Kind: columnJSONExists, JSONPath: col.JSONPath})
		case kafsql.SelectColumnField:
			resolved, err := s.resolveColumnByName(topic, col.Column, schemaMap)
			if err != nil {
				return nil, err
			}
			if col.Alias != "" {
				resolved.Name = col.Alias
			}
			out = append(out, resolved)
		default:
			return nil, fmt.Errorf("unsupported select column")
		}
	}
	return out, nil
}

func (s *Server) resolveColumnByName(topic string, name string, schemaMap map[string]config.SchemaColumn) (resolvedColumn, error) {
	switch name {
	case "_topic", "_partition", "_offset", "_ts", "_key", "_value", "_headers", "_segment":
		return resolvedColumn{Name: name, Kind: columnImplicit, Column: name}, nil
	default:
		if schema, ok := schemaMap[name]; ok {
			return resolvedColumn{Name: name, Kind: columnSchema, Column: name, Schema: schema}, nil
		}
	}
	return resolvedColumn{}, fmt.Errorf("unknown column %q", name)
}

func buildRowDescription(cols []resolvedColumn) []pgproto3.FieldDescription {
	fields := make([]pgproto3.FieldDescription, 0, len(cols))
	for _, col := range cols {
		oid := columnTypeOID(col)
		fields = append(fields, pgproto3.FieldDescription{Name: []byte(col.Name), DataTypeOID: oid, DataTypeSize: -1, TypeModifier: -1, Format: 0})
	}
	return fields
}

type rowContext struct {
	left     decoder.Record
	right    *decoder.Record
	leftSeg  string
	rightSeg string
}

func buildRowValues(cols []resolvedColumn, ctx rowContext) [][]byte {
	values := make([][]byte, 0, len(cols))
	for _, col := range cols {
		values = append(values, columnValue(ctx, col))
	}
	return values
}

type rowResult struct {
	values [][]byte
	ts     int64
}

func appendTailRow(rows []rowResult, row rowResult, limit int) []rowResult {
	if limit <= 0 {
		return rows
	}
	if len(rows) < limit {
		return append(rows, row)
	}
	copy(rows, rows[1:])
	rows[len(rows)-1] = row
	return rows
}

func columnTypeOID(col resolvedColumn) uint32 {
	switch col.Kind {
	case columnSchema:
		return schemaTypeOID(col.Schema.Type)
	case columnJSONValue:
		return 25
	case columnJSONQuery:
		return 114
	case columnJSONExists:
		return 16
	default:
		switch col.Column {
		case "_partition":
			return 23
		case "_offset":
			return 20
		case "_ts":
			return 1114
		case "_key", "_value":
			return 17
		default:
			return 25
		}
	}
}

func columnValue(ctx rowContext, col resolvedColumn) []byte {
	record, seg, ok := selectRecord(ctx, col.Source)
	if !ok {
		return nil
	}
	switch col.Kind {
	case columnSchema:
		return schemaValue(record.Value, col.Schema, record.Topic)
	case columnJSONValue:
		return jsonValueBytes(record.Value, col.JSONPath)
	case columnJSONQuery:
		return jsonQueryBytes(record.Value, col.JSONPath)
	case columnJSONExists:
		return jsonExistsBytes(record.Value, col.JSONPath)
	default:
		switch col.Column {
		case "_topic":
			return []byte(record.Topic)
		case "_partition":
			return []byte(itoa(int(record.Partition)))
		case "_offset":
			return []byte(itoa64(record.Offset))
		case "_ts":
			return []byte(formatTimestamp(record.Timestamp))
		case "_key":
			return encodeBytea(record.Key)
		case "_value":
			return encodeBytea(record.Value)
		case "_headers":
			return []byte(headersToJSON(record.Headers))
		case "_segment":
			return []byte(seg)
		default:
			return nil
		}
	}
}

func selectRecord(ctx rowContext, source string) (decoder.Record, string, bool) {
	if source == "right" {
		if ctx.right == nil {
			return decoder.Record{}, "", false
		}
		return *ctx.right, ctx.rightSeg, true
	}
	return ctx.left, ctx.leftSeg, true
}

func jsonValueBytes(payload []byte, path string) []byte {
	value, ok := jsonLookup(payload, path, "")
	if !ok {
		return nil
	}
	switch v := value.(type) {
	case string:
		return []byte(v)
	case float64:
		return []byte(strconv.FormatFloat(v, 'f', -1, 64))
	case bool:
		return []byte(strconv.FormatBool(v))
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		return data
	}
}

func jsonQueryBytes(payload []byte, path string) []byte {
	value, ok := jsonLookup(payload, path, "")
	if !ok {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return data
}

func jsonExistsBytes(payload []byte, path string) []byte {
	root, ok := parseJSON(payload)
	if !ok {
		return nil
	}
	_, found := jsonPathValue(root, path, "")
	if found {
		return []byte("true")
	}
	return []byte("false")
}

func parseJSON(payload []byte) (interface{}, bool) {
	var root interface{}
	if err := json.Unmarshal(payload, &root); err != nil {
		metrics.InvalidJSON.Inc()
		return nil, false
	}
	return root, true
}

func maxInt64Ptr(current *int64, value int64) *int64 {
	if current == nil || value > *current {
		return &value
	}
	return current
}

func hasAggregates(cols []kafsql.SelectColumn) bool {
	for _, col := range cols {
		if col.Kind == kafsql.SelectColumnAggregate {
			return true
		}
	}
	return false
}

type aggregatePlan struct {
	groupCols []resolvedColumn
	aggs      []aggSpec
	outputs   []outputColumn
}

func (s *Server) handleAggregateSelect(ctx context.Context, backend *pgproto3.Backend, parsed kafsql.Query, segments []discovery.SegmentRef, timeMin *int64, timeMax *int64, limit int, collector *rowCollector) (queryResult, error) {
	plan, err := s.buildAggregatePlan(parsed)
	if err != nil {
		return queryResult{}, err
	}

	fields := make([]pgproto3.FieldDescription, 0, len(plan.outputs))
	for _, out := range plan.outputs {
		fields = append(fields, pgproto3.FieldDescription{Name: []byte(out.Name), DataTypeOID: out.DataType, DataTypeSize: -1, TypeModifier: -1, Format: 0})
	}
	if err := s.send(backend, collector, &pgproto3.RowDescription{Fields: fields}); err != nil {
		return queryResult{}, err
	}

	dec, err := s.getDecoder()
	if err != nil {
		return queryResult{}, err
	}

	groups := make(map[string]*groupState)
	segmentsScanned := 0
	bytesScanned := int64(0)
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return queryResult{}, err
		}
		segmentsScanned++
		records, err := dec.Decode(ctx, segment.SegmentKey, segment.IndexKey, segment.Topic, segment.Partition)
		if err != nil {
			return queryResult{}, err
		}
		for _, record := range records {
			if timeMin != nil && record.Timestamp < *timeMin {
				continue
			}
			if timeMax != nil && record.Timestamp > *timeMax {
				continue
			}
			if parsed.OffsetMin != nil && record.Offset < *parsed.OffsetMin {
				continue
			}
			if parsed.OffsetMax != nil && record.Offset > *parsed.OffsetMax {
				continue
			}
			bytesScanned += int64(len(record.Key) + len(record.Value))

			groupVals := make([][]byte, len(plan.groupCols))
			for i, col := range plan.groupCols {
				groupVals[i] = columnValue(rowContext{left: record, leftSeg: segment.SegmentKey}, col)
			}
			groupKey := buildGroupKey(groupVals)
			state := groups[groupKey]
			if state == nil {
				aggs := make([]aggState, len(plan.aggs))
				for i, spec := range plan.aggs {
					aggs[i] = aggState{Func: spec.Func}
				}
				state = &groupState{Values: groupVals, Aggs: aggs}
				groups[groupKey] = state
			}
			rowCtx := rowContext{left: record, leftSeg: segment.SegmentKey}
			for i, spec := range plan.aggs {
				updateAgg(&state.Aggs[i], spec, rowCtx)
			}
		}
	}

	groupKeys := make([]string, 0, len(groups))
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)

	sent := 0
	for _, key := range groupKeys {
		if limit > 0 && sent >= limit {
			break
		}
		state := groups[key]
		row := buildAggregateRow(plan, state)
		if err := s.send(backend, collector, &pgproto3.DataRow{Values: row}); err != nil {
			return queryResult{}, err
		}
		sent++
	}

	_ = s.send(backend, collector, &pgproto3.CommandComplete{CommandTag: commandTag(sent)})
	return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
}

func (s *Server) buildAggregatePlan(parsed kafsql.Query) (aggregatePlan, error) {
	schemaMap := s.schemaColumnMapForTopic(parsed.Topic)
	groupCols := make([]resolvedColumn, 0, len(parsed.GroupBy))
	groupIndex := make(map[string]int, len(parsed.GroupBy))
	for _, name := range parsed.GroupBy {
		col, err := s.resolveColumnByName(parsed.Topic, name, schemaMap)
		if err != nil {
			return aggregatePlan{}, err
		}
		groupIndex[col.Name] = len(groupCols)
		groupCols = append(groupCols, col)
	}

	hasAgg := false
	outputs := make([]outputColumn, 0, len(parsed.Select))
	aggs := make([]aggSpec, 0)
	for _, sel := range parsed.Select {
		switch sel.Kind {
		case kafsql.SelectColumnAggregate:
			hasAgg = true
			spec, err := s.buildAggSpec(parsed.Topic, sel, schemaMap)
			if err != nil {
				return aggregatePlan{}, err
			}
			aggIdx := len(aggs)
			aggs = append(aggs, spec)
			name := sel.Alias
			if name == "" {
				name = spec.Name
			}
			outputs = append(outputs, outputColumn{
				Name:     name,
				Kind:     outputAggregate,
				AggIdx:   aggIdx,
				DataType: aggResultOID(spec),
			})
		case kafsql.SelectColumnField:
			if len(parsed.GroupBy) == 0 {
				return aggregatePlan{}, errors.New("non-aggregate column requires group by")
			}
			idx, ok := groupIndex[sel.Column]
			if !ok {
				return aggregatePlan{}, fmt.Errorf("column %q must appear in group by", sel.Column)
			}
			name := sel.Alias
			if name == "" {
				name = sel.Column
			}
			outputs = append(outputs, outputColumn{
				Name:     name,
				Kind:     outputGroup,
				GroupIdx: idx,
				DataType: columnTypeOID(groupCols[idx]),
			})
		case kafsql.SelectColumnJSONValue, kafsql.SelectColumnJSONQuery, kafsql.SelectColumnJSONExists:
			return aggregatePlan{}, errors.New("json helpers are not supported in group by")
		case kafsql.SelectColumnStar:
			return aggregatePlan{}, errors.New("select * is not supported with aggregates")
		default:
			return aggregatePlan{}, errors.New("unsupported aggregate selection")
		}
	}

	if !hasAgg {
		return aggregatePlan{}, errors.New("aggregate query requires an aggregate function")
	}
	return aggregatePlan{groupCols: groupCols, aggs: aggs, outputs: outputs}, nil
}

func (s *Server) buildAggSpec(topic string, sel kafsql.SelectColumn, schemaMap map[string]config.SchemaColumn) (aggSpec, error) {
	spec := aggSpec{Func: sel.AggFunc, Name: sel.Alias}
	if sel.AggStar {
		spec.Arg = aggArg{Kind: aggArgStar}
		return spec, nil
	}
	if sel.AggJSONPath != "" {
		spec.Arg = aggArg{
			Kind: aggArgColumn,
			Column: resolvedColumn{
				Name:     "json_value",
				Kind:     columnJSONValue,
				JSONPath: sel.AggJSONPath,
			},
		}
		if spec.Name == "" {
			spec.Name = sel.AggFunc + "_json"
		}
		return spec, nil
	}
	col, err := s.resolveColumnByName(topic, sel.AggColumn, schemaMap)
	if err != nil {
		return aggSpec{}, err
	}
	spec.Arg = aggArg{Kind: aggArgColumn, Column: col}
	if spec.Name == "" {
		spec.Name = sel.AggFunc + "_" + sel.AggColumn
	}
	return spec, nil
}

func aggResultOID(spec aggSpec) uint32 {
	switch spec.Func {
	case "count":
		return 20
	case "sum", "avg":
		return 701
	case "min", "max":
		if spec.Arg.Kind == aggArgColumn {
			return columnTypeOID(spec.Arg.Column)
		}
		return 25
	default:
		return 25
	}
}

func buildGroupKey(values [][]byte) string {
	if len(values) == 0 {
		return "all"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		if value == nil {
			parts = append(parts, "<nil>")
		} else {
			parts = append(parts, string(value))
		}
	}
	return strings.Join(parts, "\x1f")
}

func updateAgg(state *aggState, spec aggSpec, ctx rowContext) {
	switch spec.Func {
	case "count":
		if spec.Arg.Kind == aggArgStar {
			state.Count++
			return
		}
		if _, ok := aggValueFromColumn(ctx, spec.Arg.Column); ok {
			state.Count++
		}
	case "sum":
		if value, ok := aggValueFromColumn(ctx, spec.Arg.Column); ok && value.Kind == "number" {
			state.Sum += value.Num
			state.HasSum = true
		}
	case "avg":
		if value, ok := aggValueFromColumn(ctx, spec.Arg.Column); ok && value.Kind == "number" {
			state.Sum += value.Num
			state.Count++
		}
	case "min":
		if value, ok := aggValueFromColumn(ctx, spec.Arg.Column); ok {
			updateAggMin(state, value)
		}
	case "max":
		if value, ok := aggValueFromColumn(ctx, spec.Arg.Column); ok {
			updateAggMax(state, value)
		}
	}
}

type aggValue struct {
	Kind string
	Num  float64
	TS   int64
	Str  string
}

func aggValueFromColumn(ctx rowContext, col resolvedColumn) (aggValue, bool) {
	record, seg, ok := selectRecord(ctx, col.Source)
	if !ok {
		return aggValue{}, false
	}
	switch col.Kind {
	case columnImplicit:
		switch col.Column {
		case "_partition":
			return aggValue{Kind: "number", Num: float64(record.Partition)}, true
		case "_offset":
			return aggValue{Kind: "number", Num: float64(record.Offset)}, true
		case "_ts":
			return aggValue{Kind: "timestamp", TS: record.Timestamp}, true
		case "_topic":
			return aggValue{Kind: "string", Str: record.Topic}, true
		case "_segment":
			return aggValue{Kind: "string", Str: seg}, true
		case "_key":
			return aggValue{Kind: "string", Str: string(record.Key)}, true
		case "_value":
			return aggValue{Kind: "string", Str: string(record.Value)}, true
		default:
			return aggValue{}, false
		}
	case columnSchema:
		return aggValueFromSchema(record.Value, col.Schema, record.Topic)
	case columnJSONValue:
		return aggValueFromJSON(record.Value, col.JSONPath)
	default:
		return aggValue{}, false
	}
}

func aggValueFromSchema(payload []byte, schema config.SchemaColumn, topic string) (aggValue, bool) {
	value, ok := jsonLookup(payload, schema.Path, topic)
	if !ok {
		return aggValue{}, false
	}
	switch strings.ToLower(schema.Type) {
	case "int", "long", "double":
		return numericValue(value)
	case "timestamp":
		if ts, ok := parseTimestampValue(value); ok {
			return aggValue{Kind: "timestamp", TS: ts}, true
		}
		return aggValue{}, false
	case "boolean":
		if b, ok := value.(bool); ok {
			return aggValue{Kind: "string", Str: strconv.FormatBool(b)}, true
		}
		return aggValue{}, false
	default:
		if s, ok := value.(string); ok {
			return aggValue{Kind: "string", Str: s}, true
		}
		return aggValue{Kind: "string", Str: fmt.Sprintf("%v", value)}, true
	}
}

func aggValueFromJSON(payload []byte, path string) (aggValue, bool) {
	value, ok := jsonLookup(payload, path, "")
	if !ok {
		return aggValue{}, false
	}
	if num, ok := numericValue(value); ok {
		return num, true
	}
	if s, ok := value.(string); ok {
		return aggValue{Kind: "string", Str: s}, true
	}
	return aggValue{Kind: "string", Str: fmt.Sprintf("%v", value)}, true
}

func numericValue(value interface{}) (aggValue, bool) {
	switch v := value.(type) {
	case float64:
		return aggValue{Kind: "number", Num: v}, true
	case int64:
		return aggValue{Kind: "number", Num: float64(v)}, true
	case int:
		return aggValue{Kind: "number", Num: float64(v)}, true
	case string:
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return aggValue{Kind: "number", Num: parsed}, true
		}
	}
	return aggValue{}, false
}

func parseTimestampValue(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case string:
		layouts := []string{
			"2006-01-02 15:04:05.000",
			"2006-01-02 15:04:05",
			time.RFC3339,
		}
		for _, layout := range layouts {
			if ts, err := time.Parse(layout, v); err == nil {
				return ts.UnixMilli(), true
			}
		}
	}
	return 0, false
}

func updateAggMin(state *aggState, value aggValue) {
	if state.Kind == "" {
		state.Kind = value.Kind
	}
	if state.Kind != value.Kind {
		return
	}
	switch value.Kind {
	case "timestamp":
		if !state.MinSet || value.TS < state.MinTS {
			state.MinTS = value.TS
			state.MinSet = true
		}
	case "number":
		if !state.MinSet || value.Num < state.MinNum {
			state.MinNum = value.Num
			state.MinSet = true
		}
	case "string":
		if !state.MinSet || value.Str < state.MinStr {
			state.MinStr = value.Str
			state.MinSet = true
		}
	}
}

func updateAggMax(state *aggState, value aggValue) {
	if state.Kind == "" {
		state.Kind = value.Kind
	}
	if state.Kind != value.Kind {
		return
	}
	switch value.Kind {
	case "timestamp":
		if !state.MaxSet || value.TS > state.MaxTS {
			state.MaxTS = value.TS
			state.MaxSet = true
		}
	case "number":
		if !state.MaxSet || value.Num > state.MaxNum {
			state.MaxNum = value.Num
			state.MaxSet = true
		}
	case "string":
		if !state.MaxSet || value.Str > state.MaxStr {
			state.MaxStr = value.Str
			state.MaxSet = true
		}
	}
}

func buildAggregateRow(plan aggregatePlan, state *groupState) [][]byte {
	values := make([][]byte, 0, len(plan.outputs))
	for _, out := range plan.outputs {
		switch out.Kind {
		case outputGroup:
			if out.GroupIdx < len(state.Values) {
				values = append(values, state.Values[out.GroupIdx])
			} else {
				values = append(values, nil)
			}
		case outputAggregate:
			values = append(values, aggStateValue(state.Aggs[out.AggIdx], plan.aggs[out.AggIdx]))
		}
	}
	return values
}

func segmentMatchesOffsets(segment discovery.SegmentRef, min *int64, max *int64) bool {
	if min == nil && max == nil {
		return true
	}
	if segment.MinOffset == nil && segment.MaxOffset == nil {
		return true
	}
	if min != nil && segment.MaxOffset != nil && *segment.MaxOffset < *min {
		return false
	}
	if max != nil && segment.MinOffset != nil && *segment.MinOffset > *max {
		return false
	}
	return true
}

func segmentMatchesTimestamps(segment discovery.SegmentRef, min *int64, max *int64) bool {
	if min == nil && max == nil {
		return true
	}
	if segment.MinTimestamp == nil && segment.MaxTimestamp == nil {
		return true
	}
	if min != nil && segment.MaxTimestamp != nil && *segment.MaxTimestamp < *min {
		return false
	}
	if max != nil && segment.MinTimestamp != nil && *segment.MinTimestamp > *max {
		return false
	}
	return true
}

func aggStateValue(state aggState, spec aggSpec) []byte {
	switch spec.Func {
	case "count":
		return []byte(strconv.FormatInt(state.Count, 10))
	case "sum":
		if !state.HasSum {
			return nil
		}
		return []byte(strconv.FormatFloat(state.Sum, 'f', -1, 64))
	case "avg":
		if state.Count == 0 {
			return nil
		}
		return []byte(strconv.FormatFloat(state.Sum/float64(state.Count), 'f', -1, 64))
	case "min":
		return aggMinMaxValue(state, true)
	case "max":
		return aggMinMaxValue(state, false)
	default:
		return nil
	}
}

func aggMinMaxValue(state aggState, min bool) []byte {
	if !state.MinSet && !state.MaxSet {
		return nil
	}
	switch state.Kind {
	case "timestamp":
		if min {
			return []byte(formatTimestamp(state.MinTS))
		}
		return []byte(formatTimestamp(state.MaxTS))
	case "number":
		if min {
			return []byte(strconv.FormatFloat(state.MinNum, 'f', -1, 64))
		}
		return []byte(strconv.FormatFloat(state.MaxNum, 'f', -1, 64))
	case "string":
		if min {
			return []byte(state.MinStr)
		}
		return []byte(state.MaxStr)
	default:
		return nil
	}
}

func (s *Server) handleJoinSelect(ctx context.Context, backend *pgproto3.Backend, parsed kafsql.Query, collector *rowCollector) (queryResult, error) {
	if parsed.Topic == "" || parsed.JoinTopic == "" {
		return queryResult{}, errors.New("join requires two topics")
	}
	if parsed.Tail != "" {
		return queryResult{}, errors.New("join does not support tail")
	}
	if parsed.ScanFull {
		return queryResult{}, errors.New("join does not support scan full")
	}
	if parsed.TimeWindow == "" || parsed.Last == "" {
		return queryResult{}, errors.New("join requires within <duration> and last <duration>")
	}
	if parsed.Partition != nil || parsed.OffsetMin != nil || parsed.OffsetMax != nil {
		return queryResult{}, errors.New("join does not support partition or offset filters")
	}
	if hasAggregates(parsed.Select) {
		return queryResult{}, errors.New("join does not support aggregates")
	}

	within, err := parseDuration(parsed.TimeWindow)
	if err != nil {
		return queryResult{}, err
	}
	last, err := parseDuration(parsed.Last)
	if err != nil {
		return queryResult{}, err
	}
	windowStart := time.Now().Add(-last)

	limit := s.cfg.Query.DefaultLimit
	if parsed.Limit != "" {
		value, err := parseLimit(parsed.Limit)
		if err != nil {
			return queryResult{}, err
		}
		limit = value
	}
	if limit <= 0 {
		limit = s.cfg.Query.DefaultLimit
	}
	if s.cfg.Query.MaxRows > 0 && limit > s.cfg.Query.MaxRows {
		return queryResult{}, fmt.Errorf("limit exceeds max_rows (%d)", s.cfg.Query.MaxRows)
	}

	joinOn := parsed.JoinOn
	if joinOn == nil {
		joinOn = &kafsql.JoinCondition{
			Left:  kafsql.JoinExpr{Kind: kafsql.JoinExprKey, Side: "left"},
			Right: kafsql.JoinExpr{Kind: kafsql.JoinExprKey, Side: "right"},
		}
	}
	joinOn = normalizeJoinCondition(joinOn)

	cols, err := s.resolveJoinColumns(parsed)
	if err != nil {
		return queryResult{}, err
	}
	fields := buildRowDescription(cols)
	if err := s.send(backend, collector, &pgproto3.RowDescription{Fields: fields}); err != nil {
		return queryResult{}, err
	}

	lister, err := s.getLister()
	if err != nil {
		return queryResult{}, err
	}
	dec, err := s.getDecoder()
	if err != nil {
		return queryResult{}, err
	}

	segments, err := lister.ListCompleted(ctx)
	if err != nil {
		return queryResult{}, err
	}

	timeMin := parsed.TsMin
	timeMax := parsed.TsMax
	if parsed.Last != "" {
		window, err := parseDuration(parsed.Last)
		if err != nil {
			return queryResult{}, err
		}
		now := time.Now().UTC().UnixMilli()
		start := now - window.Milliseconds()
		timeMin = maxInt64Ptr(timeMin, start)
		if timeMax == nil {
			timeMax = &now
		}
	}
	if timeMin != nil && timeMax != nil && *timeMax < *timeMin {
		return queryResult{}, errors.New("time window is invalid")
	}

	leftParsed := parsed
	leftParsed.JoinTopic = ""
	leftSegments := filterSegments(leftParsed, segments, timeMin, timeMax)
	rightParsed := parsed
	rightParsed.Topic = parsed.JoinTopic
	rightParsed.Partition = nil
	rightParsed.OffsetMin = nil
	rightParsed.OffsetMax = nil
	rightSegments := filterSegments(rightParsed, segments, nil, nil)

	if err := s.enforceScanLimits(len(leftSegments)+len(rightSegments), estimateBytes(leftSegments)+estimateBytes(rightSegments)); err != nil {
		return queryResult{}, err
	}

	leftRecords, leftBytes, err := s.loadRecords(ctx, dec, leftSegments, parsed.Topic)
	if err != nil {
		return queryResult{}, err
	}
	rightRecords, rightBytes, err := s.loadRecords(ctx, dec, rightSegments, parsed.JoinTopic)
	if err != nil {
		return queryResult{}, err
	}

	rightByKey := groupByJoinKey(rightRecords, joinOn.Right)
	sent := 0
	segmentsScanned := len(leftSegments) + len(rightSegments)
	bytesScanned := leftBytes + rightBytes
	for _, left := range leftRecords {
		if err := ctx.Err(); err != nil {
			return queryResult{}, err
		}
		if left.Record.Timestamp < windowStart.UnixMilli() {
			continue
		}
		key := joinKeyFromExpr(left.Record, joinOn.Left)
		if key == "" {
			if parsed.JoinType == "left" {
				values := buildRowValues(cols, rowContext{left: left.Record, leftSeg: left.SegmentKey})
				if err := s.send(backend, collector, &pgproto3.DataRow{Values: values}); err != nil {
					return queryResult{}, err
				}
				sent++
			}
			if sent >= limit {
				_ = s.send(backend, collector, &pgproto3.CommandComplete{CommandTag: commandTag(sent)})
				return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
			}
			continue
		}

		candidates := rightByKey[key]
		matched := false
		for _, right := range candidates {
			if !withinWindow(left.Record.Timestamp, right.Record.Timestamp, within) {
				continue
			}
			values := buildRowValues(cols, rowContext{
				left:     left.Record,
				right:    &right.Record,
				leftSeg:  left.SegmentKey,
				rightSeg: right.SegmentKey,
			})
			if err := s.send(backend, collector, &pgproto3.DataRow{Values: values}); err != nil {
				return queryResult{}, err
			}
			sent++
			matched = true
			if sent >= limit {
				_ = s.send(backend, collector, &pgproto3.CommandComplete{CommandTag: commandTag(sent)})
				return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
			}
		}

		if !matched && parsed.JoinType == "left" {
			values := buildRowValues(cols, rowContext{left: left.Record, leftSeg: left.SegmentKey})
			if err := s.send(backend, collector, &pgproto3.DataRow{Values: values}); err != nil {
				return queryResult{}, err
			}
			sent++
			if sent >= limit {
				_ = s.send(backend, collector, &pgproto3.CommandComplete{CommandTag: commandTag(sent)})
				return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
			}
		}
	}

	_ = s.send(backend, collector, &pgproto3.CommandComplete{CommandTag: commandTag(sent)})
	return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
}

type joinRecord struct {
	Record     decoder.Record
	SegmentKey string
}

func (s *Server) loadRecords(ctx context.Context, dec decoder.Decoder, segments []discovery.SegmentRef, topic string) ([]joinRecord, int64, error) {
	out := make([]joinRecord, 0)
	bytesScanned := int64(0)
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		if segment.Topic != topic {
			continue
		}
		records, err := dec.Decode(ctx, segment.SegmentKey, segment.IndexKey, segment.Topic, segment.Partition)
		if err != nil {
			return nil, 0, err
		}
		for _, record := range records {
			bytesScanned += int64(len(record.Key) + len(record.Value))
			out = append(out, joinRecord{Record: record, SegmentKey: segment.SegmentKey})
		}
	}
	return out, bytesScanned, nil
}

func (s *Server) resolveJoinColumns(parsed kafsql.Query) ([]resolvedColumn, error) {
	if len(parsed.Select) == 0 {
		return joinDefaultColumns(), nil
	}
	if len(parsed.Select) == 1 && parsed.Select[0].Kind == kafsql.SelectColumnStar {
		return joinDefaultColumns(), nil
	}

	leftSchema := s.schemaColumnMapForTopic(parsed.Topic)
	rightSchema := s.schemaColumnMapForTopic(parsed.JoinTopic)
	out := make([]resolvedColumn, 0, len(parsed.Select))
	for _, sel := range parsed.Select {
		if sel.Kind == kafsql.SelectColumnStar {
			return nil, errors.New("select * cannot be combined with other columns in join")
		}
		if sel.Kind == kafsql.SelectColumnAggregate {
			return nil, errors.New("join does not support aggregates")
		}
		side := joinSideFromSource(parsed, sel.Source)
		schemaMap := leftSchema
		topic := parsed.Topic
		if side == "right" {
			schemaMap = rightSchema
			topic = parsed.JoinTopic
		}
		switch sel.Kind {
		case kafsql.SelectColumnField:
			col, err := s.resolveColumnByName(topic, sel.Column, schemaMap)
			if err != nil {
				return nil, err
			}
			col.Source = side
			if sel.Alias != "" {
				col.Name = sel.Alias
			} else {
				col.Name = joinOutputName(side, col.Name)
			}
			out = append(out, col)
		case kafsql.SelectColumnJSONValue:
			name := sel.Alias
			if name == "" {
				name = joinOutputName(side, "json_value")
			}
			out = append(out, resolvedColumn{
				Name:     name,
				Kind:     columnJSONValue,
				JSONPath: sel.JSONPath,
				Source:   side,
			})
		case kafsql.SelectColumnJSONQuery:
			name := sel.Alias
			if name == "" {
				name = joinOutputName(side, "json_query")
			}
			out = append(out, resolvedColumn{
				Name:     name,
				Kind:     columnJSONQuery,
				JSONPath: sel.JSONPath,
				Source:   side,
			})
		case kafsql.SelectColumnJSONExists:
			name := sel.Alias
			if name == "" {
				name = joinOutputName(side, "json_exists")
			}
			out = append(out, resolvedColumn{
				Name:     name,
				Kind:     columnJSONExists,
				JSONPath: sel.JSONPath,
				Source:   side,
			})
		default:
			return nil, errors.New("unsupported join select column")
		}
	}
	return out, nil
}

func joinSideFromSource(parsed kafsql.Query, source string) string {
	if source == "" || source == parsed.TopicAlias || source == parsed.Topic {
		return "left"
	}
	if source == parsed.JoinAlias || source == parsed.JoinTopic {
		return "right"
	}
	return "left"
}

func joinOutputName(side string, name string) string {
	if side != "right" {
		return name
	}
	if strings.HasPrefix(name, "_") {
		return "_right" + name
	}
	return "_right_" + name
}

func normalizeJoinCondition(cond *kafsql.JoinCondition) *kafsql.JoinCondition {
	left := cond.Left
	right := cond.Right
	if left.Side == "" {
		left.Side = "left"
	}
	if right.Side == "" {
		right.Side = "right"
	}
	if left.Side == "right" && right.Side == "left" {
		left, right = right, left
	}
	return &kafsql.JoinCondition{Left: left, Right: right}
}

func groupByJoinKey(records []joinRecord, expr kafsql.JoinExpr) map[string][]joinRecord {
	out := make(map[string][]joinRecord)
	for _, record := range records {
		key := joinKeyFromExpr(record.Record, expr)
		if key == "" {
			continue
		}
		out[key] = append(out[key], record)
	}
	return out
}

func segmentsByTopic(segments []discovery.SegmentRef, topic string) int {
	count := 0
	for _, segment := range segments {
		if segment.Topic == topic {
			count++
		}
	}
	return count
}

func joinKeyFromExpr(record decoder.Record, expr kafsql.JoinExpr) string {
	switch expr.Kind {
	case kafsql.JoinExprJSON:
		value, ok := jsonLookup(record.Value, expr.JSONPath, "")
		if !ok {
			return ""
		}
		return fmt.Sprintf("%v", value)
	default:
		if len(record.Key) == 0 {
			return ""
		}
		return string(record.Key)
	}
}

func withinWindow(left, right int64, window time.Duration) bool {
	diff := left - right
	if diff < 0 {
		diff = -diff
	}
	return time.Duration(diff)*time.Millisecond <= window
}

func joinDefaultColumns() []resolvedColumn {
	return []resolvedColumn{
		{Name: "_topic", Kind: columnImplicit, Column: "_topic", Source: "left"},
		{Name: "_partition", Kind: columnImplicit, Column: "_partition", Source: "left"},
		{Name: "_offset", Kind: columnImplicit, Column: "_offset", Source: "left"},
		{Name: "_ts", Kind: columnImplicit, Column: "_ts", Source: "left"},
		{Name: "_key", Kind: columnImplicit, Column: "_key", Source: "left"},
		{Name: "_value", Kind: columnImplicit, Column: "_value", Source: "left"},
		{Name: "_headers", Kind: columnImplicit, Column: "_headers", Source: "left"},
		{Name: "_segment", Kind: columnImplicit, Column: "_segment", Source: "left"},
		{Name: "_right_topic", Kind: columnImplicit, Column: "_topic", Source: "right"},
		{Name: "_right_partition", Kind: columnImplicit, Column: "_partition", Source: "right"},
		{Name: "_right_offset", Kind: columnImplicit, Column: "_offset", Source: "right"},
		{Name: "_right_ts", Kind: columnImplicit, Column: "_ts", Source: "right"},
		{Name: "_right_key", Kind: columnImplicit, Column: "_key", Source: "right"},
		{Name: "_right_value", Kind: columnImplicit, Column: "_value", Source: "right"},
		{Name: "_right_headers", Kind: columnImplicit, Column: "_headers", Source: "right"},
		{Name: "_right_segment", Kind: columnImplicit, Column: "_segment", Source: "right"},
	}
}

func (s *Server) schemaColumnMapForTopic(topic string) map[string]config.SchemaColumn {
	if topic == "" {
		return nil
	}
	for _, entry := range s.cfg.Metadata.Topics {
		if entry.Name != topic {
			continue
		}
		out := make(map[string]config.SchemaColumn, len(entry.Schema.Columns))
		for _, col := range entry.Schema.Columns {
			name := strings.ToLower(col.Name)
			out[name] = col
		}
		return out
	}
	return nil
}

func (s *Server) schemaColumnsForTopic(topic string) []config.SchemaColumn {
	if topic == "" {
		return nil
	}
	for _, entry := range s.cfg.Metadata.Topics {
		if entry.Name != topic {
			continue
		}
		return entry.Schema.Columns
	}
	return nil
}

func schemaTypeOID(schemaType string) uint32 {
	switch strings.ToLower(schemaType) {
	case "int":
		return 23
	case "long":
		return 20
	case "double":
		return 701
	case "boolean":
		return 16
	case "timestamp":
		return 1114
	default:
		return 25
	}
}

func schemaValue(payload []byte, schema config.SchemaColumn, topic string) []byte {
	value, ok := jsonLookup(payload, schema.Path, topic)
	if !ok {
		return nil
	}
	switch strings.ToLower(schema.Type) {
	case "string":
		if s, ok := value.(string); ok {
			return []byte(s)
		}
	case "int":
		if f, ok := value.(float64); ok {
			return []byte(strconv.FormatInt(int64(f), 10))
		}
	case "long":
		if f, ok := value.(float64); ok {
			return []byte(strconv.FormatInt(int64(f), 10))
		}
	case "double":
		if f, ok := value.(float64); ok {
			return []byte(strconv.FormatFloat(f, 'f', -1, 64))
		}
	case "boolean":
		if b, ok := value.(bool); ok {
			return []byte(strconv.FormatBool(b))
		}
	case "timestamp":
		if s, ok := value.(string); ok {
			return []byte(s)
		}
		if f, ok := value.(float64); ok {
			return []byte(formatTimestamp(int64(f)))
		}
	}
	return nil
}

func jsonLookup(payload []byte, path string, topic string) (interface{}, bool) {
	root, ok := parseJSON(payload)
	if !ok {
		return nil, false
	}
	return jsonPathValue(root, path, topic)
}

func jsonPathValue(root interface{}, path string, topic string) (interface{}, bool) {
	trimmed := strings.TrimSpace(path)
	trimmed = strings.TrimPrefix(trimmed, "$.")
	trimmed = strings.TrimPrefix(trimmed, "$")
	if trimmed == "" {
		return nil, false
	}
	current := root
	for _, part := range strings.Split(trimmed, ".") {
		if part == "" {
			return nil, false
		}
		obj, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		next, ok := obj[part]
		if !ok {
			if topic != "" {
				metrics.SchemaMiss.WithLabelValues(topic).Inc()
			}
			return nil, false
		}
		current = next
	}
	return current, true
}

func formatTimestamp(ms int64) string {
	ts := time.UnixMilli(ms).UTC()
	return ts.Format("2006-01-02 15:04:05.000")
}

func encodeBytea(data []byte) []byte {
	if data == nil {
		return nil
	}
	encoded := make([]byte, 2+hex.EncodedLen(len(data)))
	encoded[0] = '\\'
	encoded[1] = 'x'
	hex.Encode(encoded[2:], data)
	return encoded
}

func headersToJSON(headers []decoder.Header) string {
	if len(headers) == 0 {
		return "{}"
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, header := range headers {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`"`)
		b.WriteString(escapeJSON(header.Key))
		b.WriteString(`":"`)
		b.WriteString(escapeJSON(string(header.Value)))
		b.WriteString(`"`)
	}
	b.WriteByte('}')
	return b.String()
}

func escapeJSON(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return replacer.Replace(value)
}

func parseLimit(raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid limit")
	}
	return value, nil
}

func commandTag(rows int) []byte {
	return []byte("SELECT " + itoa(rows))
}

func (s *Server) withQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.cfg.Query.TimeoutSeconds <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, time.Duration(s.cfg.Query.TimeoutSeconds)*time.Second)
}

func (s *Server) enforceScanLimits(segments int, bytes int64) error {
	if s.cfg.Query.MaxScanSegments > 0 && segments > s.cfg.Query.MaxScanSegments {
		return fmt.Errorf("scan segments exceeds max_scan_segments (%d)", s.cfg.Query.MaxScanSegments)
	}
	if s.cfg.Query.MaxScanBytes > 0 && bytes > 0 && bytes > s.cfg.Query.MaxScanBytes {
		return fmt.Errorf("scan bytes exceeds max_scan_bytes (%s)", formatBytes(s.cfg.Query.MaxScanBytes))
	}
	return nil
}

func parseDuration(raw string) (time.Duration, error) {
	if strings.HasSuffix(raw, "d") {
		value := strings.TrimSuffix(raw, "d")
		days, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf("invalid duration")
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid duration")
	}
	return parsed, nil
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	neg := value < 0
	if neg {
		value = -value
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func itoa64(value int64) string {
	if value == 0 {
		return "0"
	}
	neg := value < 0
	if neg {
		value = -value
	}
	var buf [32]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func trimQuery(query string) string {
	trimmed := strings.TrimSpace(query)
	if len(trimmed) > 512 {
		return trimmed[:512] + "..."
	}
	return trimmed
}
