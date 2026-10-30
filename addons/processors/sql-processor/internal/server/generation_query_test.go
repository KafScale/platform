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
	"testing"
	"time"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/decoder"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/discovery"
	kafsql "github.com/kafscale/platform/addons/processors/sql-processor/internal/sql"
)

func generationTestServer() *Server {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", StorageTopic: "orders", SegmentKey: "gen-0", IndexKey: "idx-0"},
		{Topic: "orders", StorageTopic: "orders@1", SegmentKey: "gen-1", IndexKey: "idx-1"},
		{Topic: "orders", StorageTopic: "orders@2", SegmentKey: "gen-2", IndexKey: "idx-2"},
	}
	records := map[string][]decoder.Record{}
	for i, segment := range segments {
		records[segment.SegmentKey] = []decoder.Record{{Topic: "orders", Offset: int64(i), Timestamp: now}}
	}
	return newTestServer(&mockLister{segments: segments}, &mockDecoder{records: records})
}

func selectValues(t *testing.T, srv *Server) []string {
	t.Helper()
	parsed, err := kafsql.Parse("SELECT _offset FROM orders SCAN FULL;")
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	values := make([]string, 0, len(rows))
	for _, row := range rows {
		values = append(values, string(row[0]))
	}
	return values
}

func TestSelectReadsOnlyLatestGenerationWithoutEtcd(t *testing.T) {
	values := selectValues(t, generationTestServer())
	if len(values) != 1 || values[0] != "2" {
		t.Fatalf("expected only the latest generation, got %v", values)
	}
}
