package sink

import (
	"context"

	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/decoder"
)

// Record models a decoded record payload passed to the sink.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []decoder.Header
}

// Writer writes records to a downstream system (Iceberg, OLAP, etc).
type Writer interface {
	Write(ctx context.Context, records []Record) error
	Close(ctx context.Context) error
}
