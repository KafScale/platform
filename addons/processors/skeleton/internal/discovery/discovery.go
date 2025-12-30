package discovery

import "context"

// SegmentRef identifies a completed segment and its index.
type SegmentRef struct {
	Topic     string
	Partition int32
	BaseOffset int64
	SegmentKey string
	IndexKey   string
}

// Lister discovers completed segments for topics/partitions.
type Lister interface {
	ListCompleted(ctx context.Context) ([]SegmentRef, error)
}

// New returns a placeholder lister.
func New() Lister {
	return &noopLister{}
}

type noopLister struct{}

func (n *noopLister) ListCompleted(ctx context.Context) ([]SegmentRef, error) {
	return nil, nil
}
