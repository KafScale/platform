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
