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
	"fmt"
)

// ResolverConfig controls LFS resolution behavior.
type ResolverConfig struct {
	MaxSize          int64
	ValidateChecksum bool
}

// ResolvedRecord holds the resolved payload and metadata.
type ResolvedRecord struct {
	Envelope    Envelope
	Payload     []byte
	ContentType string
	BlobSize    int64
	Checksum    string
	ChecksumAlg string
}

// Resolver fetches LFS payloads and validates integrity.
type Resolver struct {
	cfg ResolverConfig
	s3  S3Reader
}

// NewResolver creates a resolver with the provided S3 reader.
func NewResolver(cfg ResolverConfig, s3 S3Reader) *Resolver {
	return &Resolver{cfg: cfg, s3: s3}
}

// Resolve resolves a record value. It returns ok=false if the value is not an LFS envelope.
func (r *Resolver) Resolve(ctx context.Context, value []byte) (ResolvedRecord, bool, error) {
	if !IsLfsEnvelope(value) {
		return ResolvedRecord{Payload: value, BlobSize: int64(len(value))}, false, nil
	}
	env, err := DecodeEnvelope(value)
	if err != nil {
		return ResolvedRecord{}, true, err
	}
	if r.s3 == nil {
		return ResolvedRecord{}, true, fmt.Errorf("s3 reader not configured")
	}

	payload, err := r.s3.Fetch(ctx, env.Key)
	if err != nil {
		return ResolvedRecord{}, true, err
	}
	if r.cfg.MaxSize > 0 && int64(len(payload)) > r.cfg.MaxSize {
		return ResolvedRecord{}, true, fmt.Errorf("payload size %d exceeds max %d", len(payload), r.cfg.MaxSize)
	}

	checksumAlg, expected, ok, err := EnvelopeChecksum(env)
	if err != nil {
		return ResolvedRecord{}, true, err
	}
	if r.cfg.ValidateChecksum && ok {
		computed, err := ComputeChecksum(checksumAlg, payload)
		if err != nil {
			return ResolvedRecord{}, true, err
		}
		if computed != expected {
			return ResolvedRecord{}, true, &ChecksumError{Expected: expected, Actual: computed}
		}
	}

	return ResolvedRecord{
		Envelope:    env,
		Payload:     payload,
		ContentType: env.ContentType,
		BlobSize:    int64(len(payload)),
		Checksum:    expected,
		ChecksumAlg: string(checksumAlg),
	}, true, nil
}
