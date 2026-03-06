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
	"testing"
)

// mockFetcher is a test implementation of BlobFetcher.
type mockFetcher struct {
	blobs map[string][]byte
	err   error
}

func (m *mockFetcher) Fetch(ctx context.Context, key string) ([]byte, error) {
	if m.err != nil {
		return nil, m.err
	}
	blob, ok := m.blobs[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return blob, nil
}

func TestConsumerUnwrapNonLFS(t *testing.T) {
	fetcher := &mockFetcher{blobs: make(map[string][]byte)}
	consumer := NewConsumer(fetcher)

	// Plain text should pass through unchanged
	plainText := []byte("hello world")
	result, err := consumer.Unwrap(context.Background(), plainText)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result) != string(plainText) {
		t.Errorf("expected %q, got %q", plainText, result)
	}
}

func TestConsumerUnwrapLFS(t *testing.T) {
	blob := []byte("this is the actual blob content")
	hash := sha256.Sum256(blob)
	checksum := hex.EncodeToString(hash[:])

	fetcher := &mockFetcher{
		blobs: map[string][]byte{
			"default/test-topic/lfs/2026/02/01/obj-123": blob,
		},
	}
	consumer := NewConsumer(fetcher)

	envelope := Envelope{
		Version: 1,
		Bucket:  "kafscale",
		Key:     "default/test-topic/lfs/2026/02/01/obj-123",
		Size:    int64(len(blob)),
		SHA256:  checksum,
	}
	envBytes, err := EncodeEnvelope(envelope)
	if err != nil {
		t.Fatalf("failed to encode envelope: %v", err)
	}

	result, err := consumer.Unwrap(context.Background(), envBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result) != string(blob) {
		t.Errorf("expected blob content, got %q", result)
	}
}

func TestConsumerUnwrapMD5Checksum(t *testing.T) {
	blob := []byte("md5-blob")
	sha := sha256.Sum256(blob)
	md5sum := md5.Sum(blob)

	fetcher := &mockFetcher{
		blobs: map[string][]byte{
			"default/test-topic/lfs/2026/02/01/obj-123": blob,
		},
	}
	consumer := NewConsumer(fetcher)

	envelope := Envelope{
		Version:     1,
		Bucket:      "kafscale",
		Key:         "default/test-topic/lfs/2026/02/01/obj-123",
		Size:        int64(len(blob)),
		SHA256:      hex.EncodeToString(sha[:]),
		Checksum:    hex.EncodeToString(md5sum[:]),
		ChecksumAlg: "md5",
	}
	envBytes, err := EncodeEnvelope(envelope)
	if err != nil {
		t.Fatalf("failed to encode envelope: %v", err)
	}

	result, err := consumer.Unwrap(context.Background(), envBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result) != string(blob) {
		t.Errorf("expected blob content, got %q", result)
	}
}

func TestConsumerUnwrapChecksumMismatch(t *testing.T) {
	blob := []byte("this is the actual blob content")
	wrongChecksum := "0000000000000000000000000000000000000000000000000000000000000000"

	fetcher := &mockFetcher{
		blobs: map[string][]byte{
			"default/test-topic/lfs/2026/02/01/obj-123": blob,
		},
	}
	consumer := NewConsumer(fetcher)

	envelope := Envelope{
		Version: 1,
		Bucket:  "kafscale",
		Key:     "default/test-topic/lfs/2026/02/01/obj-123",
		Size:    int64(len(blob)),
		SHA256:  wrongChecksum,
	}
	envBytes, err := EncodeEnvelope(envelope)
	if err != nil {
		t.Fatalf("failed to encode envelope: %v", err)
	}

	_, err = consumer.Unwrap(context.Background(), envBytes)
	if err == nil {
		t.Fatal("expected checksum error, got nil")
	}

	var checksumErr *ChecksumError
	if !errors.As(err, &checksumErr) {
		t.Fatalf("expected ChecksumError, got %T: %v", err, err)
	}
	if checksumErr.Expected != wrongChecksum {
		t.Errorf("expected Expected=%s, got %s", wrongChecksum, checksumErr.Expected)
	}
}

func TestConsumerUnwrapChecksumDisabled(t *testing.T) {
	blob := []byte("this is the actual blob content")
	wrongChecksum := "0000000000000000000000000000000000000000000000000000000000000000"

	fetcher := &mockFetcher{
		blobs: map[string][]byte{
			"default/test-topic/lfs/2026/02/01/obj-123": blob,
		},
	}
	consumer := NewConsumer(fetcher, WithChecksumValidation(false))

	envelope := Envelope{
		Version: 1,
		Bucket:  "kafscale",
		Key:     "default/test-topic/lfs/2026/02/01/obj-123",
		Size:    int64(len(blob)),
		SHA256:  wrongChecksum,
	}
	envBytes, err := EncodeEnvelope(envelope)
	if err != nil {
		t.Fatalf("failed to encode envelope: %v", err)
	}

	// Should succeed because checksum validation is disabled
	result, err := consumer.Unwrap(context.Background(), envBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result) != string(blob) {
		t.Errorf("expected blob content, got %q", result)
	}
}

func TestConsumerUnwrapFetchError(t *testing.T) {
	fetcher := &mockFetcher{
		err: errors.New("s3 connection failed"),
	}
	consumer := NewConsumer(fetcher)

	hash := sha256.Sum256([]byte("test"))
	envelope := Envelope{
		Version: 1,
		Bucket:  "kafscale",
		Key:     "some/key",
		Size:    100,
		SHA256:  hex.EncodeToString(hash[:]),
	}
	envBytes, err := EncodeEnvelope(envelope)
	if err != nil {
		t.Fatalf("failed to encode envelope: %v", err)
	}

	_, err = consumer.Unwrap(context.Background(), envBytes)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var lfsErr *LfsError
	if !errors.As(err, &lfsErr) {
		t.Fatalf("expected LfsError, got %T: %v", err, err)
	}
	if lfsErr.Op != "fetch" {
		t.Errorf("expected Op=fetch, got %s", lfsErr.Op)
	}
}

func TestConsumerUnwrapInvalidEnvelope(t *testing.T) {
	fetcher := &mockFetcher{}
	consumer := NewConsumer(fetcher)

	// Invalid JSON that looks like an envelope but missing required fields
	// Must be > 15 bytes to pass IsLfsEnvelope length check
	invalid := []byte(`{"kfs_lfs": 1, "bucket": "b"}`)

	_, err := consumer.Unwrap(context.Background(), invalid)
	if err == nil {
		t.Fatal("expected error for invalid envelope, got nil")
	}

	var lfsErr *LfsError
	if !errors.As(err, &lfsErr) {
		t.Fatalf("expected LfsError, got %T: %v", err, err)
	}
	if lfsErr.Op != "decode" {
		t.Errorf("expected Op=decode, got %s", lfsErr.Op)
	}
}

func TestConsumerUnwrapEnvelope(t *testing.T) {
	blob := []byte("blob data")
	hash := sha256.Sum256(blob)
	checksum := hex.EncodeToString(hash[:])

	fetcher := &mockFetcher{
		blobs: map[string][]byte{"key": blob},
	}
	consumer := NewConsumer(fetcher)

	envelope := Envelope{
		Version:     1,
		Bucket:      "bucket",
		Key:         "key",
		Size:        int64(len(blob)),
		SHA256:      checksum,
		ContentType: "application/octet-stream",
		ProxyID:     "proxy-1",
	}
	envBytes, _ := EncodeEnvelope(envelope)

	env, data, err := consumer.UnwrapEnvelope(context.Background(), envBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env == nil {
		t.Fatal("expected envelope, got nil")
	}
	if env.Bucket != "bucket" {
		t.Errorf("expected Bucket=bucket, got %s", env.Bucket)
	}
	if env.ContentType != "application/octet-stream" {
		t.Errorf("expected ContentType, got %s", env.ContentType)
	}
	if string(data) != string(blob) {
		t.Errorf("expected blob data, got %q", data)
	}
}

func TestConsumerUnwrapEnvelopeNonLFS(t *testing.T) {
	fetcher := &mockFetcher{}
	consumer := NewConsumer(fetcher)

	plain := []byte("not an envelope")
	env, data, err := consumer.UnwrapEnvelope(context.Background(), plain)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if env != nil {
		t.Errorf("expected nil envelope, got %+v", env)
	}
	if string(data) != string(plain) {
		t.Errorf("expected original data, got %q", data)
	}
}
