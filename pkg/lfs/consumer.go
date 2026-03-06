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
)

// BlobFetcher downloads LFS blobs from storage.
type BlobFetcher interface {
	Fetch(ctx context.Context, key string) ([]byte, error)
}

// Consumer unwraps LFS envelope records by fetching the blob from storage.
type Consumer struct {
	fetcher          BlobFetcher
	validateChecksum bool
}

// ConsumerOption configures the Consumer.
type ConsumerOption func(*Consumer)

// WithChecksumValidation enables SHA256 validation on fetched blobs.
func WithChecksumValidation(enabled bool) ConsumerOption {
	return func(c *Consumer) {
		c.validateChecksum = enabled
	}
}

// NewConsumer creates a Consumer that fetches LFS blobs.
func NewConsumer(fetcher BlobFetcher, opts ...ConsumerOption) *Consumer {
	c := &Consumer{
		fetcher:          fetcher,
		validateChecksum: true,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Unwrap checks if value is an LFS envelope and fetches the blob.
// Returns the original value if not an envelope.
func (c *Consumer) Unwrap(ctx context.Context, value []byte) ([]byte, error) {
	if !IsLfsEnvelope(value) {
		return value, nil
	}

	env, err := DecodeEnvelope(value)
	if err != nil {
		return nil, &LfsError{Op: "decode", Err: err}
	}

	blob, err := c.fetcher.Fetch(ctx, env.Key)
	if err != nil {
		return nil, &LfsError{Op: "fetch", Err: err}
	}

	if c.validateChecksum {
		alg, expected, ok, err := EnvelopeChecksum(env)
		if err != nil {
			return nil, &LfsError{Op: "checksum", Err: err}
		}
		if ok {
			actual, err := ComputeChecksum(alg, blob)
			if err != nil {
				return nil, &LfsError{Op: "checksum", Err: err}
			}
			if actual != expected {
				return nil, &ChecksumError{Expected: expected, Actual: actual}
			}
		}
	}

	return blob, nil
}

// UnwrapEnvelope returns the envelope and fetched blob for records that are envelopes.
// Returns nil envelope and original value if not an envelope.
func (c *Consumer) UnwrapEnvelope(ctx context.Context, value []byte) (*Envelope, []byte, error) {
	if !IsLfsEnvelope(value) {
		return nil, value, nil
	}

	env, err := DecodeEnvelope(value)
	if err != nil {
		return nil, nil, &LfsError{Op: "decode", Err: err}
	}

	blob, err := c.fetcher.Fetch(ctx, env.Key)
	if err != nil {
		return &env, nil, &LfsError{Op: "fetch", Err: err}
	}

	if c.validateChecksum {
		alg, expected, ok, err := EnvelopeChecksum(env)
		if err != nil {
			return &env, nil, &LfsError{Op: "checksum", Err: err}
		}
		if ok {
			actual, err := ComputeChecksum(alg, blob)
			if err != nil {
				return &env, nil, &LfsError{Op: "checksum", Err: err}
			}
			if actual != expected {
				return &env, nil, &ChecksumError{Expected: expected, Actual: actual}
			}
		}
	}

	return &env, blob, nil
}
