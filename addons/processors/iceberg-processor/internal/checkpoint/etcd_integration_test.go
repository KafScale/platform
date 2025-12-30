package checkpoint

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/config"
)

func TestLeaseExpirationRecoveryEtcd(t *testing.T) {
	endpoints := etcdEndpointsFromEnv(t)

	cfg := config.Config{
		Etcd: config.EtcdConfig{Endpoints: endpoints},
		Offsets: config.OffsetConfig{
			Backend:         "etcd",
			LeaseTTLSeconds: 1,
			KeyPrefix:       "processors-test-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		},
	}

	store, err := NewEtcdStore(cfg)
	if err != nil {
		t.Fatalf("NewEtcdStore: %v", err)
	}

	ctx := context.Background()
	if _, err := store.ClaimLease(ctx, "orders", 0, "worker-a"); err != nil {
		t.Fatalf("ClaimLease worker-a: %v", err)
	}
	if _, err := store.ClaimLease(ctx, "orders", 0, "worker-b"); err == nil {
		t.Fatalf("expected lease conflict")
	}

	time.Sleep(2 * time.Second)
	if _, err := store.ClaimLease(ctx, "orders", 0, "worker-b"); err != nil {
		t.Fatalf("expected lease after expiration: %v", err)
	}
}

func TestCommitAndLoadOffsetEtcd(t *testing.T) {
	endpoints := etcdEndpointsFromEnv(t)

	cfg := config.Config{
		Etcd: config.EtcdConfig{Endpoints: endpoints},
		Offsets: config.OffsetConfig{
			Backend:   "etcd",
			KeyPrefix: "processors-test-" + strconv.FormatInt(time.Now().UnixNano(), 10),
		},
	}

	store, err := NewEtcdStore(cfg)
	if err != nil {
		t.Fatalf("NewEtcdStore: %v", err)
	}

	ctx := context.Background()
	state := OffsetState{Topic: "orders", Partition: 0, Offset: 42, Timestamp: 1234}
	if err := store.CommitOffset(ctx, state); err != nil {
		t.Fatalf("CommitOffset: %v", err)
	}
	loaded, err := store.LoadOffset(ctx, "orders", 0)
	if err != nil {
		t.Fatalf("LoadOffset: %v", err)
	}
	if loaded.Offset != 42 || loaded.Timestamp != 1234 {
		t.Fatalf("unexpected loaded offset state: %#v", loaded)
	}
}

func etcdEndpointsFromEnv(t *testing.T) []string {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv("ICEBERG_PROCESSOR_ETCD_ENDPOINTS"))
	if raw == "" {
		t.Skip("ICEBERG_PROCESSOR_ETCD_ENDPOINTS not set")
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		t.Skip("ICEBERG_PROCESSOR_ETCD_ENDPOINTS empty")
	}
	return out
}
