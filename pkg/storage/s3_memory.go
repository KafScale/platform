package storage

import (
	"context"
	"fmt"
	"sync"
)

// MemoryS3Client is an in-memory implementation of S3Client for development/testing.
type MemoryS3Client struct {
	mu    sync.Mutex
	data  map[string][]byte
	index map[string][]byte
}

// NewMemoryS3Client initializes the in-memory S3 client.
func NewMemoryS3Client() *MemoryS3Client {
	return &MemoryS3Client{
		data:  make(map[string][]byte),
		index: make(map[string][]byte),
	}
}

func (m *MemoryS3Client) UploadSegment(ctx context.Context, key string, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), body...)
	return nil
}

func (m *MemoryS3Client) UploadIndex(ctx context.Context, key string, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.index[key] = append([]byte(nil), body...)
	return nil
}

func (m *MemoryS3Client) DownloadSegment(ctx context.Context, key string, rng *ByteRange) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if data, ok := m.data[key]; ok {
		return append([]byte(nil), data...), nil
	}
	return nil, fmt.Errorf("segment %s not found", key)
}
