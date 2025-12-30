package sink

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/config"
)

func TestIcebergWriteSmoke(t *testing.T) {
	catalogURI := os.Getenv("ICEBERG_PROCESSOR_CATALOG_URI")
	if catalogURI == "" {
		t.Skip("ICEBERG_PROCESSOR_CATALOG_URI not set")
	}
	catalogType := os.Getenv("ICEBERG_PROCESSOR_CATALOG_TYPE")
	if catalogType == "" {
		catalogType = "rest"
	}
	warehouse := os.Getenv("ICEBERG_PROCESSOR_WAREHOUSE")
	if warehouse == "" {
		t.Skip("ICEBERG_PROCESSOR_WAREHOUSE not set")
	}

	cfg := config.Config{
		Iceberg: config.IcebergConfig{
			Catalog: config.CatalogConfig{
				Type:  catalogType,
				URI:   catalogURI,
				Token: os.Getenv("ICEBERG_PROCESSOR_CATALOG_TOKEN"),
			},
			Warehouse: warehouse,
		},
		Mappings: []config.Mapping{
			{
				Topic:               "orders",
				Table:               "default.orders",
				Mode:                "append",
				CreateTableIfAbsent: true,
			},
		},
	}

	writer, err := New(cfg)
	if err != nil {
		t.Fatalf("New writer: %v", err)
	}

	records := []Record{
		{
			Topic:     "orders",
			Partition: 0,
			Offset:    1,
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte("k1"),
			Value:     []byte(`{"id":1}`),
		},
	}
	if err := writer.Write(context.Background(), records); err != nil {
		t.Fatalf("Write: %v", err)
	}
}
