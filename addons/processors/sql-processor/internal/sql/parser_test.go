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

package sql

import "testing"

func TestParseShowTopics(t *testing.T) {
	q, err := Parse("SHOW TOPICS;")
	if err != nil {
		t.Fatalf("expected show topics, got error: %v", err)
	}
	if q.Type != QueryShowTopics {
		t.Fatalf("expected show topics, got %v", q.Type)
	}
}

func TestParseShowPartitions(t *testing.T) {
	q, err := Parse("SHOW PARTITIONS FROM orders;")
	if err != nil {
		t.Fatalf("expected show partitions, got error: %v", err)
	}
	if q.Type != QueryShowPartitions || q.Topic != "orders" {
		t.Fatalf("unexpected query: %+v", q)
	}
}

func TestParseSelectWithFilters(t *testing.T) {
	q, err := Parse("SELECT _offset FROM orders WHERE _partition = 2 AND _offset >= 10 LIMIT 5;")
	if err != nil {
		t.Fatalf("expected select, got error: %v", err)
	}
	if q.Type != QuerySelect || q.Topic != "orders" {
		t.Fatalf("unexpected query: %+v", q)
	}
	if len(q.Select) != 1 || q.Select[0].Kind != SelectColumnField || q.Select[0].Column != "_offset" {
		t.Fatalf("unexpected select columns: %+v", q.Select)
	}
	if q.Partition == nil || *q.Partition != 2 {
		t.Fatalf("expected partition filter, got %+v", q.Partition)
	}
	if q.OffsetMin == nil || *q.OffsetMin != 10 {
		t.Fatalf("expected offset min, got %+v", q.OffsetMin)
	}
	if q.Limit != "5" {
		t.Fatalf("expected limit 5, got %q", q.Limit)
	}
}

func TestParseSelectScanFull(t *testing.T) {
	q, err := Parse("SELECT * FROM orders SCAN FULL LIMIT 10;")
	if err != nil {
		t.Fatalf("expected select, got error: %v", err)
	}
	if !q.ScanFull {
		t.Fatalf("expected scan full flag")
	}
}

func TestParseSelectJoin(t *testing.T) {
	q, err := Parse("SELECT * FROM orders o JOIN payments p ON o._key = p._key WITHIN 10m LAST 1h;")
	if err != nil {
		t.Fatalf("expected select join, got error: %v", err)
	}
	if q.JoinTopic != "payments" || q.JoinType != "inner" {
		t.Fatalf("unexpected join: %+v", q)
	}
	if q.JoinOn == nil {
		t.Fatalf("expected join condition")
	}
	if q.TimeWindow != "10m" || q.Last != "1h" {
		t.Fatalf("unexpected time bounds: %+v", q)
	}
}

func TestParseOrderBy(t *testing.T) {
	q, err := Parse("SELECT * FROM orders ORDER BY _ts DESC LIMIT 10;")
	if err != nil {
		t.Fatalf("expected select, got error: %v", err)
	}
	if q.OrderBy != "_ts" || !q.OrderDesc {
		t.Fatalf("unexpected order by: %+v", q)
	}
}

func TestParseGroupByAggregate(t *testing.T) {
	q, err := Parse("SELECT _partition, COUNT(*) FROM orders GROUP BY _partition;")
	if err != nil {
		t.Fatalf("expected select, got error: %v", err)
	}
	if len(q.GroupBy) != 1 || q.GroupBy[0] != "_partition" {
		t.Fatalf("unexpected group by: %+v", q.GroupBy)
	}
	if len(q.Select) != 2 {
		t.Fatalf("unexpected select columns: %+v", q.Select)
	}
}

func TestParseJSONValue(t *testing.T) {
	q, err := Parse("SELECT json_value(_value, '$.status') AS status FROM orders;")
	if err != nil {
		t.Fatalf("expected select, got error: %v", err)
	}
	if len(q.Select) != 1 || q.Select[0].Kind != SelectColumnJSONValue {
		t.Fatalf("unexpected select columns: %+v", q.Select)
	}
	if q.Select[0].Alias != "status" {
		t.Fatalf("unexpected alias: %+v", q.Select[0])
	}
}

func TestParseJSONQuery(t *testing.T) {
	q, err := Parse("SELECT json_query(_value, '$.meta') FROM orders;")
	if err != nil {
		t.Fatalf("expected select, got error: %v", err)
	}
	if len(q.Select) != 1 || q.Select[0].Kind != SelectColumnJSONQuery {
		t.Fatalf("unexpected select columns: %+v", q.Select)
	}
	if q.Select[0].Alias != "json_query" {
		t.Fatalf("unexpected alias: %+v", q.Select[0])
	}
}

func TestParseJSONExists(t *testing.T) {
	q, err := Parse("SELECT json_exists(_value, '$.id') FROM orders;")
	if err != nil {
		t.Fatalf("expected select, got error: %v", err)
	}
	if len(q.Select) != 1 || q.Select[0].Kind != SelectColumnJSONExists {
		t.Fatalf("unexpected select columns: %+v", q.Select)
	}
	if q.Select[0].Alias != "json_exists" {
		t.Fatalf("unexpected alias: %+v", q.Select[0])
	}
}

func TestParseExplain(t *testing.T) {
	q, err := Parse("EXPLAIN SELECT * FROM orders LAST 1h;")
	if err != nil {
		t.Fatalf("expected explain, got error: %v", err)
	}
	if q.Type != QueryExplain || q.Explain == nil || q.Explain.Type != QuerySelect {
		t.Fatalf("unexpected explain query: %+v", q)
	}
}

func TestParseInvalid(t *testing.T) {
	if _, err := Parse("INSERT INTO orders VALUES (1);"); err == nil {
		t.Fatalf("expected error for unsupported statement")
	}
}
