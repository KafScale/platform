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
	"bytes"
	"encoding/json"
	"errors"
)

// Envelope describes the pointer metadata for an LFS payload stored in S3.
type Envelope struct {
	Version         int               `json:"kfs_lfs"`
	Bucket          string            `json:"bucket"`
	Key             string            `json:"key"`
	Size            int64             `json:"size"`
	SHA256          string            `json:"sha256"`
	Checksum        string            `json:"checksum,omitempty"`
	ChecksumAlg     string            `json:"checksum_alg,omitempty"`
	ContentType     string            `json:"content_type,omitempty"`
	OriginalHeaders map[string]string `json:"original_headers,omitempty"`
	CreatedAt       string            `json:"created_at,omitempty"`
	ProxyID         string            `json:"proxy_id,omitempty"`
}

// EncodeEnvelope serializes an envelope to JSON.
func EncodeEnvelope(env Envelope) ([]byte, error) {
	if env.Bucket == "" || env.Key == "" || env.SHA256 == "" || env.Version == 0 {
		return nil, errors.New("invalid envelope")
	}
	return json.Marshal(env)
}

// DecodeEnvelope parses JSON bytes into an Envelope.
func DecodeEnvelope(data []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, err
	}
	if env.Version == 0 || env.Bucket == "" || env.Key == "" || env.SHA256 == "" {
		return Envelope{}, errors.New("invalid envelope: missing required fields")
	}
	return env, nil
}

// IsLfsEnvelope detects an LFS envelope via a quick JSON marker check.
func IsLfsEnvelope(value []byte) bool {
	if len(value) < 15 {
		return false
	}
	if value[0] != '{' {
		return false
	}
	max := 50
	if len(value) < max {
		max = len(value)
	}
	return bytes.Contains(value[:max], []byte(`"kfs_lfs"`))
}
