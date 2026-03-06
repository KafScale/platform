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
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"hash/crc32"
	"strings"
)

// ChecksumAlg describes the checksum algorithm used for LFS validation.
type ChecksumAlg string

const (
	ChecksumSHA256 ChecksumAlg = "sha256"
	ChecksumMD5    ChecksumAlg = "md5"
	ChecksumCRC32  ChecksumAlg = "crc32"
	ChecksumNone   ChecksumAlg = "none"
)

// NormalizeChecksumAlg normalizes an algorithm name; empty defaults to sha256.
func NormalizeChecksumAlg(raw string) (ChecksumAlg, error) {
	val := strings.ToLower(strings.TrimSpace(raw))
	if val == "" {
		return ChecksumSHA256, nil
	}
	switch ChecksumAlg(val) {
	case ChecksumSHA256, ChecksumMD5, ChecksumCRC32, ChecksumNone:
		return ChecksumAlg(val), nil
	default:
		return "", errors.New("unsupported checksum algorithm")
	}
}

// NewChecksumHasher returns a hash.Hash for the requested algorithm.
func NewChecksumHasher(alg ChecksumAlg) (hash.Hash, error) {
	switch alg {
	case ChecksumSHA256:
		return sha256.New(), nil
	case ChecksumMD5:
		return md5.New(), nil
	case ChecksumCRC32:
		return crc32.NewIEEE(), nil
	case ChecksumNone:
		return nil, nil
	default:
		return nil, errors.New("unsupported checksum algorithm")
	}
}

// ComputeChecksum computes a checksum for the given data and algorithm.
func ComputeChecksum(alg ChecksumAlg, data []byte) (string, error) {
	if alg == ChecksumNone {
		return "", nil
	}
	h, err := NewChecksumHasher(alg)
	if err != nil {
		return "", err
	}
	if _, err := h.Write(data); err != nil {
		return "", err
	}
	return formatChecksum(h.Sum(nil)), nil
}

// formatChecksum encodes a checksum digest as lowercase hex.
func formatChecksum(sum []byte) string {
	return hex.EncodeToString(sum)
}

// EnvelopeChecksum returns the algorithm + expected checksum for an envelope.
// If alg is none, ok is false (no validation).
func EnvelopeChecksum(env Envelope) (ChecksumAlg, string, bool, error) {
	alg, err := NormalizeChecksumAlg(env.ChecksumAlg)
	if err != nil {
		return "", "", false, err
	}
	switch alg {
	case ChecksumNone:
		return alg, "", false, nil
	case ChecksumSHA256:
		if env.Checksum != "" {
			return ChecksumSHA256, env.Checksum, true, nil
		}
		if env.SHA256 != "" {
			return ChecksumSHA256, env.SHA256, true, nil
		}
		return ChecksumSHA256, "", false, nil
	case ChecksumMD5, ChecksumCRC32:
		if env.Checksum != "" {
			return alg, env.Checksum, true, nil
		}
		// Fallback to SHA256 if present for backward compatibility.
		if env.SHA256 != "" {
			return ChecksumSHA256, env.SHA256, true, nil
		}
		return alg, "", false, nil
	default:
		return "", "", false, errors.New("unsupported checksum algorithm")
	}
}
