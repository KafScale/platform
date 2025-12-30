package sink

import "context"

// Record models a decoded record payload passed to the sink.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Payload   []byte
}

// Writer writes records to a downstream system (Iceberg, OLAP, etc).
type Writer interface {
	Write(ctx context.Context, records []Record) error
	Close(ctx context.Context) error
}

// New returns a placeholder sink.
func New() Writer {
	return &noopSink{}
}

type noopSink struct{}

func (n *noopSink) Write(ctx context.Context, records []Record) error {
	return nil
}

func (n *noopSink) Close(ctx context.Context) error {
	return nil
}
