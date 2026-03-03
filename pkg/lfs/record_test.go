// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package lfs

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"
)

// mockStreamFetcher implements StreamFetcher for testing.
type mockStreamFetcher struct {
	blobs map[string][]byte
	err   error
}

func (m *mockStreamFetcher) Stream(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	if m.err != nil {
		return nil, 0, m.err
	}
	blob, ok := m.blobs[key]
	if !ok {
		return nil, 0, errors.New("not found")
	}
	return io.NopCloser(newBytesReader(blob)), int64(len(blob)), nil
}

func TestRecordIsLFS(t *testing.T) {
	tests := []struct {
		name     string
		raw      []byte
		expected bool
	}{
		{"non-LFS", []byte("plain text"), false},
		{"LFS envelope", []byte(`{"kfs_lfs":1,"bucket":"b","key":"k","sha256":"s"}`), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := NewRecord(tt.raw, nil)
			if got := rec.IsLFS(); got != tt.expected {
				t.Errorf("IsLFS() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestRecordRaw(t *testing.T) {
	raw := []byte("test data")
	rec := NewRecord(raw, nil)
	if string(rec.Raw()) != string(raw) {
		t.Errorf("Raw() = %q, want %q", rec.Raw(), raw)
	}
}

func TestRecordValueNonLFS(t *testing.T) {
	raw := []byte("plain text")
	rec := NewRecord(raw, nil)

	val, err := rec.Value(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != string(raw) {
		t.Errorf("Value() = %q, want %q", val, raw)
	}
}

func TestRecordValueLFS(t *testing.T) {
	blob := []byte("resolved blob content")
	hash := sha256.Sum256(blob)
	checksum := hex.EncodeToString(hash[:])

	fetcher := &mockFetcher{
		blobs: map[string][]byte{"key": blob},
	}
	consumer := NewConsumer(fetcher)

	env := Envelope{Version: 1, Bucket: "b", Key: "key", Size: int64(len(blob)), SHA256: checksum}
	envBytes, _ := EncodeEnvelope(env)

	rec := NewRecord(envBytes, consumer)

	val, err := rec.Value(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(val) != string(blob) {
		t.Errorf("Value() = %q, want %q", val, blob)
	}
}

func TestRecordValueCaching(t *testing.T) {
	blob := []byte("blob")
	hash := sha256.Sum256(blob)
	checksum := hex.EncodeToString(hash[:])

	callCount := 0
	fetcher := &mockFetcher{
		blobs: map[string][]byte{"key": blob},
	}
	// Wrap to count calls
	wrappedFetcher := &countingFetcher{fetcher: fetcher, count: &callCount}
	consumer := NewConsumer(wrappedFetcher)

	env := Envelope{Version: 1, Bucket: "b", Key: "key", Size: int64(len(blob)), SHA256: checksum}
	envBytes, _ := EncodeEnvelope(env)

	rec := NewRecord(envBytes, consumer)

	// First call
	_, _ = rec.Value(context.Background())
	// Second call should use cache
	_, _ = rec.Value(context.Background())

	if callCount != 1 {
		t.Errorf("expected 1 fetch call, got %d", callCount)
	}
}

type countingFetcher struct {
	fetcher *mockFetcher
	count   *int
}

func (c *countingFetcher) Fetch(ctx context.Context, key string) ([]byte, error) {
	*c.count++
	return c.fetcher.Fetch(ctx, key)
}

func TestRecordValueNoConsumer(t *testing.T) {
	env := Envelope{Version: 1, Bucket: "b", Key: "k", Size: 100, SHA256: "abc123"}
	envBytes, _ := EncodeEnvelope(env)

	rec := NewRecord(envBytes, nil)

	_, err := rec.Value(context.Background())
	if err == nil {
		t.Fatal("expected error when no consumer")
	}

	var lfsErr *LfsError
	if !errors.As(err, &lfsErr) {
		t.Fatalf("expected LfsError, got %T", err)
	}
	if !errors.Is(lfsErr.Err, ErrNoConsumer) {
		t.Errorf("expected ErrNoConsumer, got %v", lfsErr.Err)
	}
}

func TestRecordEnvelope(t *testing.T) {
	env := Envelope{
		Version:     1,
		Bucket:      "bucket",
		Key:         "key",
		Size:        100,
		SHA256:      "abc",
		ContentType: "text/plain",
	}
	envBytes, _ := EncodeEnvelope(env)

	rec := NewRecord(envBytes, nil)
	gotEnv, err := rec.Envelope()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotEnv.Bucket != "bucket" {
		t.Errorf("Bucket = %s, want bucket", gotEnv.Bucket)
	}
	if gotEnv.ContentType != "text/plain" {
		t.Errorf("ContentType = %s, want text/plain", gotEnv.ContentType)
	}
}

func TestRecordEnvelopeNonLFS(t *testing.T) {
	rec := NewRecord([]byte("plain"), nil)
	env, err := rec.Envelope()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env != nil {
		t.Errorf("expected nil envelope for non-LFS, got %+v", env)
	}
}

func TestRecordSize(t *testing.T) {
	// Non-LFS
	plain := []byte("12345")
	rec := NewRecord(plain, nil)
	size, err := rec.Size()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size != 5 {
		t.Errorf("Size() = %d, want 5", size)
	}

	// LFS
	env := Envelope{Version: 1, Bucket: "b", Key: "k", Size: 1024, SHA256: "abc"}
	envBytes, _ := EncodeEnvelope(env)
	rec = NewRecord(envBytes, nil)
	size, err = rec.Size()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size != 1024 {
		t.Errorf("Size() = %d, want 1024", size)
	}
}

func TestRecordContentType(t *testing.T) {
	// Non-LFS
	rec := NewRecord([]byte("plain"), nil)
	if ct := rec.ContentType(); ct != "" {
		t.Errorf("ContentType() = %q, want empty", ct)
	}

	// LFS
	env := Envelope{Version: 1, Bucket: "b", Key: "k", Size: 100, SHA256: "abc", ContentType: "image/png"}
	envBytes, _ := EncodeEnvelope(env)
	rec = NewRecord(envBytes, nil)
	if ct := rec.ContentType(); ct != "image/png" {
		t.Errorf("ContentType() = %q, want image/png", ct)
	}
}

func TestRecordValueStreamNonLFS(t *testing.T) {
	raw := []byte("plain text data")
	rec := NewRecord(raw, nil)

	reader, length, err := rec.ValueStream(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer reader.Close()

	if length != int64(len(raw)) {
		t.Errorf("length = %d, want %d", length, len(raw))
	}

	data, _ := io.ReadAll(reader)
	if string(data) != string(raw) {
		t.Errorf("stream data = %q, want %q", data, raw)
	}
}

func TestRecordValueStreamLFS(t *testing.T) {
	blob := []byte("streamed blob content")
	hash := sha256.Sum256(blob)
	checksum := hex.EncodeToString(hash[:])

	streamFetcher := &mockStreamFetcher{
		blobs: map[string][]byte{"key": blob},
	}

	env := Envelope{Version: 1, Bucket: "b", Key: "key", Size: int64(len(blob)), SHA256: checksum}
	envBytes, _ := EncodeEnvelope(env)

	rec := NewRecord(envBytes, nil, WithStreamFetcher(streamFetcher))

	reader, length, err := rec.ValueStream(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if length != int64(len(blob)) {
		t.Errorf("length = %d, want %d", length, len(blob))
	}

	data, _ := io.ReadAll(reader)
	if string(data) != string(blob) {
		t.Errorf("stream data = %q, want %q", data, blob)
	}

	// Close validates checksum
	if err := reader.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestRecordValueStreamNoFetcher(t *testing.T) {
	env := Envelope{Version: 1, Bucket: "b", Key: "k", Size: 100, SHA256: "abc"}
	envBytes, _ := EncodeEnvelope(env)

	rec := NewRecord(envBytes, nil) // No stream fetcher

	_, _, err := rec.ValueStream(context.Background())
	if err == nil {
		t.Fatal("expected error when no stream fetcher")
	}

	var lfsErr *LfsError
	if !errors.As(err, &lfsErr) {
		t.Fatalf("expected LfsError, got %T", err)
	}
	if !errors.Is(lfsErr.Err, ErrNoStreamFetcher) {
		t.Errorf("expected ErrNoStreamFetcher, got %v", lfsErr.Err)
	}
}

func TestRecordValueStreamMD5Checksum(t *testing.T) {
	blob := []byte("blob content")
	md5sum := md5.Sum(blob)
	sha := sha256.Sum256(blob)

	streamFetcher := &mockStreamFetcher{
		blobs: map[string][]byte{"key": blob},
	}

	env := Envelope{
		Version:     1,
		Bucket:      "b",
		Key:         "key",
		Size:        int64(len(blob)),
		SHA256:      hex.EncodeToString(sha[:]),
		Checksum:    hex.EncodeToString(md5sum[:]),
		ChecksumAlg: "md5",
	}
	envBytes, _ := EncodeEnvelope(env)

	rec := NewRecord(envBytes, nil, WithStreamFetcher(streamFetcher))

	reader, _, err := rec.ValueStream(context.Background())
	if err != nil {
		t.Fatalf("unexpected error getting stream: %v", err)
	}
	_, _ = io.ReadAll(reader)
	if err := reader.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestRecordValueStreamChecksumMismatch(t *testing.T) {
	blob := []byte("blob content")
	wrongChecksum := "0000000000000000000000000000000000000000000000000000000000000000"

	streamFetcher := &mockStreamFetcher{
		blobs: map[string][]byte{"key": blob},
	}

	env := Envelope{Version: 1, Bucket: "b", Key: "key", Size: int64(len(blob)), SHA256: wrongChecksum}
	envBytes, _ := EncodeEnvelope(env)

	rec := NewRecord(envBytes, nil, WithStreamFetcher(streamFetcher))

	reader, _, err := rec.ValueStream(context.Background())
	if err != nil {
		t.Fatalf("unexpected error getting stream: %v", err)
	}

	// Read all data
	_, _ = io.ReadAll(reader)

	// Close should fail with checksum error
	err = reader.Close()
	if err == nil {
		t.Fatal("expected checksum error on Close")
	}

	var checksumErr *ChecksumError
	if !errors.As(err, &checksumErr) {
		t.Fatalf("expected ChecksumError, got %T: %v", err, err)
	}
}

func TestRecordValueStreamChecksumDisabled(t *testing.T) {
	blob := []byte("blob content")
	wrongChecksum := "0000000000000000000000000000000000000000000000000000000000000000"

	streamFetcher := &mockStreamFetcher{
		blobs: map[string][]byte{"key": blob},
	}

	env := Envelope{Version: 1, Bucket: "b", Key: "key", Size: int64(len(blob)), SHA256: wrongChecksum}
	envBytes, _ := EncodeEnvelope(env)

	rec := NewRecord(envBytes, nil,
		WithStreamFetcher(streamFetcher),
		WithRecordChecksumValidation(false),
	)

	reader, _, err := rec.ValueStream(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, _ := io.ReadAll(reader)
	if string(data) != string(blob) {
		t.Errorf("data = %q, want %q", data, blob)
	}

	// Close should succeed even with wrong checksum
	if err := reader.Close(); err != nil {
		t.Fatalf("Close() should succeed with validation disabled: %v", err)
	}
}
