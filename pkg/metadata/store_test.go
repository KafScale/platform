package metadata

import (
	"context"
	"testing"

	"github.com/alo/kafscale/pkg/protocol"
)

func TestInMemoryStoreMetadata_AllTopics(t *testing.T) {
	clusterID := "cluster-1"
	store := NewInMemoryStore(ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "localhost", Port: 9092},
		},
		ControllerID: 1,
		Topics: []protocol.MetadataTopic{
			{Name: "orders"},
			{Name: "payments"},
		},
		ClusterID: &clusterID,
	})

	meta, err := store.Metadata(context.Background(), nil)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}

	if len(meta.Brokers) != 1 || meta.Brokers[0].NodeID != 1 {
		t.Fatalf("unexpected brokers: %#v", meta.Brokers)
	}
	if len(meta.Topics) != 2 {
		t.Fatalf("expected 2 topics got %d", len(meta.Topics))
	}
	if meta.ClusterID == nil || *meta.ClusterID != "cluster-1" {
		t.Fatalf("cluster id mismatch: %#v", meta.ClusterID)
	}
}

func TestInMemoryStoreMetadata_FilterTopics(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{
		Topics: []protocol.MetadataTopic{
			{Name: "orders"},
		},
	})

	meta, err := store.Metadata(context.Background(), []string{"orders", "missing"})
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if len(meta.Topics) != 2 {
		t.Fatalf("expected 2 topics got %d", len(meta.Topics))
	}
	if meta.Topics[1].ErrorCode != 3 {
		t.Fatalf("expected missing topic error code 3 got %d", meta.Topics[1].ErrorCode)
	}
}

func TestInMemoryStoreMetadata_ContextCancel(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Metadata(ctx, nil); err == nil {
		t.Fatalf("expected context error")
	}
}

func TestInMemoryStoreUpdate(t *testing.T) {
	store := NewInMemoryStore(ClusterMetadata{})
	store.Update(ClusterMetadata{
		ControllerID: 2,
	})
	meta, err := store.Metadata(context.Background(), nil)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if meta.ControllerID != 2 {
		t.Fatalf("controller id mismatch: %d", meta.ControllerID)
	}
}

func TestCloneMetadataIsolation(t *testing.T) {
	clusterID := "cluster"
	store := NewInMemoryStore(ClusterMetadata{
		Brokers:   []protocol.MetadataBroker{{NodeID: 1}},
		ClusterID: &clusterID,
	})

	meta, err := store.Metadata(context.Background(), nil)
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	meta.Brokers[0].NodeID = 99
	if meta.ClusterID == nil {
		t.Fatalf("expected cluster id copy")
	}
	// fetch again ensure original unaffected
	meta2, _ := store.Metadata(context.Background(), nil)
	if meta2.Brokers[0].NodeID != 1 {
		t.Fatalf("store state mutated via clone")
	}
}
