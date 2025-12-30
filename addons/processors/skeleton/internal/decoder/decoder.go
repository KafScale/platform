package decoder

import "context"

// Batch represents a decoded record batch.
type Batch struct {
	Topic     string
	Partition int32
	Offset    int64
	Payload   []byte
}

// Decoder parses Kafscale segment data into batches.
type Decoder interface {
	Decode(ctx context.Context, segmentKey, indexKey string) ([]Batch, error)
}

// New returns a placeholder decoder.
func New() Decoder {
	return &noopDecoder{}
}

type noopDecoder struct{}

func (n *noopDecoder) Decode(ctx context.Context, segmentKey, indexKey string) ([]Batch, error) {
	return nil, nil
}
