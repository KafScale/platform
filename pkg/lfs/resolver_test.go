// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"testing"
)

type fakeS3Reader struct {
	payload []byte
	err     error
}

func (f fakeS3Reader) Fetch(ctx context.Context, key string) ([]byte, error) {
	return f.payload, f.err
}

func (f fakeS3Reader) Stream(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	return nil, 0, f.err
}

func TestResolverNonEnvelope(t *testing.T) {
	r := NewResolver(ResolverConfig{ValidateChecksum: true}, nil)
	res, ok, err := r.Resolve(context.Background(), []byte("plain"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false for non-envelope")
	}
	if string(res.Payload) != "plain" {
		t.Fatalf("unexpected payload: %s", res.Payload)
	}
}

func TestResolverEnvelopeChecksum(t *testing.T) {
	payload := []byte("hello")
	checksum, err := ComputeChecksum(ChecksumSHA256, payload)
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	env := Envelope{
		Version:     1,
		Bucket:      "b",
		Key:         "k",
		Size:        int64(len(payload)),
		SHA256:      checksum,
		Checksum:    checksum,
		ChecksumAlg: string(ChecksumSHA256),
		ContentType: "text/plain",
	}
	encoded, err := EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	r := NewResolver(ResolverConfig{ValidateChecksum: true}, fakeS3Reader{payload: payload})
	res, ok, err := r.Resolve(context.Background(), encoded)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if res.ChecksumAlg != string(ChecksumSHA256) {
		t.Fatalf("unexpected checksum alg: %s", res.ChecksumAlg)
	}
	if string(res.Payload) != "hello" {
		t.Fatalf("unexpected payload: %s", res.Payload)
	}
}

func TestResolverChecksumMismatch(t *testing.T) {
	payload := []byte("hello")
	checksum, err := ComputeChecksum(ChecksumSHA256, []byte("other"))
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	env := Envelope{
		Version:     1,
		Bucket:      "b",
		Key:         "k",
		Size:        int64(len(payload)),
		SHA256:      checksum,
		Checksum:    checksum,
		ChecksumAlg: string(ChecksumSHA256),
	}
	encoded, err := EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	r := NewResolver(ResolverConfig{ValidateChecksum: true}, fakeS3Reader{payload: payload})
	_, ok, err := r.Resolve(context.Background(), encoded)
	if err == nil {
		t.Fatalf("expected checksum error")
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if _, isChecksum := err.(*ChecksumError); !isChecksum {
		t.Fatalf("expected ChecksumError, got %T", err)
	}
}

func TestResolverMaxSize(t *testing.T) {
	payload := []byte("hello")
	checksum, err := ComputeChecksum(ChecksumSHA256, payload)
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	env := Envelope{
		Version: 1,
		Bucket:  "b",
		Key:     "k",
		Size:    int64(len(payload)),
		SHA256:  checksum,
	}
	encoded, err := EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	r := NewResolver(ResolverConfig{MaxSize: 2, ValidateChecksum: true}, fakeS3Reader{payload: payload})
	_, ok, err := r.Resolve(context.Background(), encoded)
	if err == nil {
		t.Fatalf("expected max size error")
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
}

func TestResolverMissingS3Reader(t *testing.T) {
	payload := []byte("hello")
	checksum, err := ComputeChecksum(ChecksumSHA256, payload)
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	env := Envelope{
		Version: 1,
		Bucket:  "b",
		Key:     "k",
		Size:    int64(len(payload)),
		SHA256:  checksum,
	}
	encoded, err := EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	r := NewResolver(ResolverConfig{ValidateChecksum: true}, nil)
	_, ok, err := r.Resolve(context.Background(), encoded)
	if err == nil {
		t.Fatalf("expected error for missing s3 reader")
	}
	if !ok {
		t.Fatalf("expected ok=true")
	}
}
