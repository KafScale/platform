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
	"testing"
)

func TestNormalizeChecksumAlg(t *testing.T) {
	tests := []struct {
		input   string
		want    ChecksumAlg
		wantErr bool
	}{
		{"", ChecksumSHA256, false},
		{"sha256", ChecksumSHA256, false},
		{"SHA256", ChecksumSHA256, false},
		{"  sha256  ", ChecksumSHA256, false},
		{"md5", ChecksumMD5, false},
		{"MD5", ChecksumMD5, false},
		{"crc32", ChecksumCRC32, false},
		{"none", ChecksumNone, false},
		{"unknown", "", true},
		{"blake2b", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := NormalizeChecksumAlg(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NormalizeChecksumAlg(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("NormalizeChecksumAlg(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNewChecksumHasher(t *testing.T) {
	tests := []struct {
		alg     ChecksumAlg
		wantNil bool
		wantErr bool
	}{
		{ChecksumSHA256, false, false},
		{ChecksumMD5, false, false},
		{ChecksumCRC32, false, false},
		{ChecksumNone, true, false},
		{ChecksumAlg("unknown"), true, true},
	}
	for _, tt := range tests {
		t.Run(string(tt.alg), func(t *testing.T) {
			h, err := NewChecksumHasher(tt.alg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewChecksumHasher(%q) error = %v, wantErr %v", tt.alg, err, tt.wantErr)
			}
			if (h == nil) != tt.wantNil {
				t.Fatalf("NewChecksumHasher(%q) nil = %v, wantNil %v", tt.alg, h == nil, tt.wantNil)
			}
		})
	}
}

func TestComputeChecksum(t *testing.T) {
	data := []byte("hello world")

	sha, err := ComputeChecksum(ChecksumSHA256, data)
	if err != nil {
		t.Fatalf("ComputeChecksum(sha256) error: %v", err)
	}
	if sha == "" {
		t.Fatal("expected non-empty sha256 checksum")
	}

	md, err := ComputeChecksum(ChecksumMD5, data)
	if err != nil {
		t.Fatalf("ComputeChecksum(md5) error: %v", err)
	}
	if md == "" {
		t.Fatal("expected non-empty md5 checksum")
	}

	crc, err := ComputeChecksum(ChecksumCRC32, data)
	if err != nil {
		t.Fatalf("ComputeChecksum(crc32) error: %v", err)
	}
	if crc == "" {
		t.Fatal("expected non-empty crc32 checksum")
	}

	none, err := ComputeChecksum(ChecksumNone, data)
	if err != nil {
		t.Fatalf("ComputeChecksum(none) error: %v", err)
	}
	if none != "" {
		t.Fatalf("expected empty checksum for none, got %q", none)
	}

	_, err = ComputeChecksum(ChecksumAlg("unsupported"), data)
	if err == nil {
		t.Fatal("expected error for unsupported algorithm")
	}
}

func TestComputeChecksumDeterministic(t *testing.T) {
	data := []byte("deterministic test")
	c1, _ := ComputeChecksum(ChecksumSHA256, data)
	c2, _ := ComputeChecksum(ChecksumSHA256, data)
	if c1 != c2 {
		t.Fatalf("checksums should be deterministic: %s != %s", c1, c2)
	}
}

func TestEnvelopeChecksum(t *testing.T) {
	t.Run("none algorithm", func(t *testing.T) {
		env := Envelope{ChecksumAlg: "none"}
		alg, sum, ok, err := EnvelopeChecksum(env)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("none should return ok=false")
		}
		if alg != ChecksumNone {
			t.Fatalf("expected none, got %s", alg)
		}
		if sum != "" {
			t.Fatalf("expected empty sum, got %s", sum)
		}
	})

	t.Run("sha256 with checksum field", func(t *testing.T) {
		env := Envelope{ChecksumAlg: "sha256", Checksum: "abc123"}
		alg, sum, ok, err := EnvelopeChecksum(env)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || alg != ChecksumSHA256 || sum != "abc123" {
			t.Fatalf("unexpected: alg=%s sum=%s ok=%v", alg, sum, ok)
		}
	})

	t.Run("sha256 fallback to SHA256 field", func(t *testing.T) {
		env := Envelope{ChecksumAlg: "sha256", SHA256: "sha-field"}
		alg, sum, ok, err := EnvelopeChecksum(env)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || alg != ChecksumSHA256 || sum != "sha-field" {
			t.Fatalf("unexpected: alg=%s sum=%s ok=%v", alg, sum, ok)
		}
	})

	t.Run("sha256 no checksum available", func(t *testing.T) {
		env := Envelope{ChecksumAlg: "sha256"}
		_, _, ok, err := EnvelopeChecksum(env)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("expected ok=false when no checksum available")
		}
	})

	t.Run("empty alg defaults to sha256", func(t *testing.T) {
		env := Envelope{SHA256: "abc"}
		alg, sum, ok, err := EnvelopeChecksum(env)
		if err != nil {
			t.Fatal(err)
		}
		if alg != ChecksumSHA256 || sum != "abc" || !ok {
			t.Fatalf("unexpected: alg=%s sum=%s ok=%v", alg, sum, ok)
		}
	})

	t.Run("md5 with checksum field", func(t *testing.T) {
		env := Envelope{ChecksumAlg: "md5", Checksum: "md5-sum"}
		alg, sum, ok, err := EnvelopeChecksum(env)
		if err != nil {
			t.Fatal(err)
		}
		if alg != ChecksumMD5 || sum != "md5-sum" || !ok {
			t.Fatalf("unexpected: alg=%s sum=%s ok=%v", alg, sum, ok)
		}
	})

	t.Run("md5 fallback to SHA256", func(t *testing.T) {
		env := Envelope{ChecksumAlg: "md5", SHA256: "sha-fallback"}
		alg, sum, ok, err := EnvelopeChecksum(env)
		if err != nil {
			t.Fatal(err)
		}
		if alg != ChecksumSHA256 || sum != "sha-fallback" || !ok {
			t.Fatalf("unexpected: alg=%s sum=%s ok=%v", alg, sum, ok)
		}
	})

	t.Run("crc32 no checksum", func(t *testing.T) {
		env := Envelope{ChecksumAlg: "crc32"}
		alg, _, ok, err := EnvelopeChecksum(env)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("expected ok=false")
		}
		if alg != ChecksumCRC32 {
			t.Fatalf("expected crc32, got %s", alg)
		}
	})

	t.Run("unsupported algorithm", func(t *testing.T) {
		env := Envelope{ChecksumAlg: "blake2b"}
		_, _, _, err := EnvelopeChecksum(env)
		if err == nil {
			t.Fatal("expected error for unsupported algorithm")
		}
	})
}
