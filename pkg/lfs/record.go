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
	"io"
	"sync"
)

// StreamFetcher downloads LFS blobs as streams from storage.
type StreamFetcher interface {
	Stream(ctx context.Context, key string) (io.ReadCloser, int64, error)
}

// Record wraps a Kafka record value with lazy LFS resolution.
// If the value contains an LFS envelope, the actual blob is fetched
// from S3 on first access to Value() or ValueStream().
//
// Example usage:
//
//	consumer := lfs.NewConsumer(s3Client)
//	for _, record := range kafkaRecords {
//	    rec := lfs.NewRecord(record.Value, consumer)
//	    data, err := rec.Value(ctx)
//	    if err != nil {
//	        log.Error("failed to resolve LFS", "error", err)
//	        continue
//	    }
//	    // data contains the resolved blob (or original value if not LFS)
//	}
type Record struct {
	raw              []byte
	consumer         *Consumer
	streamFetcher    StreamFetcher
	validateChecksum bool

	// cached resolution
	mu       sync.Mutex
	resolved bool
	value    []byte
	envelope *Envelope
	err      error
}

// RecordOption configures a Record.
type RecordOption func(*Record)

// WithStreamFetcher sets a stream fetcher for ValueStream() support.
func WithStreamFetcher(fetcher StreamFetcher) RecordOption {
	return func(r *Record) {
		r.streamFetcher = fetcher
	}
}

// WithRecordChecksumValidation enables/disables checksum validation.
func WithRecordChecksumValidation(enabled bool) RecordOption {
	return func(r *Record) {
		r.validateChecksum = enabled
	}
}

// NewRecord creates a Record that wraps a raw Kafka message value.
// If the value is an LFS envelope, it will be resolved lazily on first access.
func NewRecord(raw []byte, consumer *Consumer, opts ...RecordOption) *Record {
	r := &Record{
		raw:              raw,
		consumer:         consumer,
		validateChecksum: true,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// IsLFS returns true if this record contains an LFS envelope.
func (r *Record) IsLFS() bool {
	return IsLfsEnvelope(r.raw)
}

// Raw returns the original record value without resolution.
func (r *Record) Raw() []byte {
	return r.raw
}

// Envelope returns the LFS envelope if present, nil otherwise.
// Does not fetch the blob, just parses the envelope metadata.
func (r *Record) Envelope() (*Envelope, error) {
	if !r.IsLFS() {
		return nil, nil
	}
	env, err := DecodeEnvelope(r.raw)
	if err != nil {
		return nil, &LfsError{Op: "decode", Err: err}
	}
	return &env, nil
}

// Value returns the resolved blob content.
// If the record is an LFS envelope, fetches the blob from S3.
// If not an LFS envelope, returns the original value.
// Results are cached after first resolution.
func (r *Record) Value(ctx context.Context) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.resolved {
		return r.value, r.err
	}

	r.resolved = true

	if !IsLfsEnvelope(r.raw) {
		r.value = r.raw
		return r.value, nil
	}

	if r.consumer == nil {
		r.err = &LfsError{Op: "resolve", Err: ErrNoConsumer}
		return nil, r.err
	}

	env, blob, err := r.consumer.UnwrapEnvelope(ctx, r.raw)
	r.envelope = env
	if err != nil {
		r.err = err
		return nil, r.err
	}

	r.value = blob
	return r.value, nil
}

// ValueStream returns a streaming reader for the blob content.
// This is more memory-efficient for large blobs.
// Note: The caller must close the returned reader.
// If not an LFS envelope, returns a reader over the raw value.
func (r *Record) ValueStream(ctx context.Context) (io.ReadCloser, int64, error) {
	if !r.IsLFS() {
		return io.NopCloser(newBytesReader(r.raw)), int64(len(r.raw)), nil
	}

	env, err := DecodeEnvelope(r.raw)
	if err != nil {
		return nil, 0, &LfsError{Op: "decode", Err: err}
	}

	if r.streamFetcher == nil {
		return nil, 0, &LfsError{Op: "stream", Err: ErrNoStreamFetcher}
	}

	reader, length, err := r.streamFetcher.Stream(ctx, env.Key)
	if err != nil {
		return nil, 0, &LfsError{Op: "stream", Err: err}
	}

	if r.validateChecksum {
		alg, expected, ok, err := EnvelopeChecksum(env)
		if err != nil {
			return nil, 0, &LfsError{Op: "checksum", Err: err}
		}
		if ok {
			hasher, err := NewChecksumHasher(alg)
			if err != nil {
				return nil, 0, &LfsError{Op: "checksum", Err: err}
			}
			return &checksumReader{
				reader:   reader,
				expected: expected,
				hasher:   hasher,
				alg:      alg,
			}, length, nil
		}
	}

	return reader, length, nil
}

// Size returns the size of the blob.
// For LFS records, returns the size from the envelope without fetching.
// For non-LFS records, returns the length of the raw value.
func (r *Record) Size() (int64, error) {
	if !r.IsLFS() {
		return int64(len(r.raw)), nil
	}

	env, err := DecodeEnvelope(r.raw)
	if err != nil {
		return 0, &LfsError{Op: "decode", Err: err}
	}

	return env.Size, nil
}

// ContentType returns the content type from the LFS envelope.
// Returns empty string for non-LFS records.
func (r *Record) ContentType() string {
	if !r.IsLFS() {
		return ""
	}

	env, err := DecodeEnvelope(r.raw)
	if err != nil {
		return ""
	}

	return env.ContentType
}

// bytesReader wraps a byte slice for io.Reader interface.
type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// checksumReader wraps a reader and validates checksum on close.
type checksumReader struct {
	reader   io.ReadCloser
	expected string
	hasher   interface {
		Write([]byte) (int, error)
		Sum([]byte) []byte
	}
	alg    ChecksumAlg
	closed bool
}

func (r *checksumReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if n > 0 {
		r.hasher.Write(p[:n])
	}
	return n, err
}

func (r *checksumReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Read any remaining data to complete the hash
	remaining, _ := io.ReadAll(r.reader)
	if len(remaining) > 0 {
		r.hasher.Write(remaining)
	}

	err := r.reader.Close()
	if err != nil {
		return err
	}

	actual := formatChecksum(r.hasher.Sum(nil))
	if actual != r.expected {
		return &ChecksumError{Expected: r.expected, Actual: actual}
	}

	return nil
}
