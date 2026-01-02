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

package checkpoint

import (
	"encoding/json"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestMinWatermark(t *testing.T) {
	entries := []offsetState{
		{Offset: 10, LastTimestampMs: 100},
		{Offset: 5, LastTimestampMs: 50},
		{Offset: 12, LastTimestampMs: 120},
	}

	kvs := make([]*mvccpb.KeyValue, 0, len(entries))
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		kvs = append(kvs, &mvccpb.KeyValue{Value: data})
	}

	min, err := minWatermark(kvs)
	if err != nil {
		t.Fatalf("minWatermark: %v", err)
	}
	if min.Offset != 5 {
		t.Fatalf("expected min offset 5, got %d", min.Offset)
	}
	if min.LastTimestampMs != 50 {
		t.Fatalf("expected min timestamp 50, got %d", min.LastTimestampMs)
	}
}
