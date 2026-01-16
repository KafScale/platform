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

package storage

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// MemoryS3Client is an in-memory implementation of S3Client for development/testing.
type MemoryS3Client struct {
	mu          sync.Mutex
	data        map[string][]byte
	index       map[string][]byte
	bucketReady bool
}

// NewMemoryS3Client initializes the in-memory S3 client.
func NewMemoryS3Client() *MemoryS3Client {
	return &MemoryS3Client{
		data:  make(map[string][]byte),
		index: make(map[string][]byte),
	}
}

func (m *MemoryS3Client) EnsureBucket(ctx context.Context) error {
	m.mu.Lock()
	m.bucketReady = true
	m.mu.Unlock()
	return nil
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
		if rng == nil {
			return append([]byte(nil), data...), nil
		}
		start := rng.Start
		end := rng.End
		if start < 0 {
			start = 0
		}
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}
		if start > end || start >= int64(len(data)) {
			return nil, fmt.Errorf("segment %s range %d-%d invalid", key, rng.Start, rng.End)
		}
		return append([]byte(nil), data[start:end+1]...), nil
	}
	return nil, fmt.Errorf("segment %s not found", key)
}

func (m *MemoryS3Client) DownloadIndex(ctx context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if data, ok := m.index[key]; ok {
		return append([]byte(nil), data...), nil
	}
	return nil, fmt.Errorf("index %s not found", key)
}

func (m *MemoryS3Client) ListSegments(ctx context.Context, prefix string) ([]S3Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]S3Object, 0)
	for key, data := range m.data {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		out = append(out, S3Object{
			Key:  key,
			Size: int64(len(data)),
		})
	}
	return out, nil
}
