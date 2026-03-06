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
	"encoding/json"
	"testing"
)

func TestEncodeEnvelope(t *testing.T) {
	env := Envelope{
		Version: 1,
		Bucket:  "bucket",
		Key:     "ns/topic/lfs/2026/01/31/obj-123",
		Size:    42,
		SHA256:  "abc",
	}
	payload, err := EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("EncodeEnvelope error: %v", err)
	}
	var decoded Envelope
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if decoded.Bucket != env.Bucket || decoded.Key != env.Key || decoded.SHA256 != env.SHA256 {
		t.Fatalf("unexpected decoded envelope: %+v", decoded)
	}
}

func TestEncodeEnvelopeInvalid(t *testing.T) {
	tests := []struct {
		name string
		env  Envelope
	}{
		{"empty", Envelope{}},
		{"no version", Envelope{Bucket: "b", Key: "k", SHA256: "s"}},
		{"no bucket", Envelope{Version: 1, Key: "k", SHA256: "s"}},
		{"no key", Envelope{Version: 1, Bucket: "b", SHA256: "s"}},
		{"no sha256", Envelope{Version: 1, Bucket: "b", Key: "k"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := EncodeEnvelope(tt.env)
			if err == nil {
				t.Fatalf("expected error for invalid envelope")
			}
		})
	}
}

func TestDecodeEnvelope(t *testing.T) {
	original := Envelope{
		Version:     1,
		Bucket:      "kafscale",
		Key:         "default/topic/lfs/2026/02/01/obj-abc",
		Size:        1024,
		SHA256:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		ContentType: "application/json",
		ProxyID:     "proxy-1",
	}
	encoded, err := EncodeEnvelope(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, err := DecodeEnvelope(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded.Version != original.Version {
		t.Errorf("Version: got %d, want %d", decoded.Version, original.Version)
	}
	if decoded.Bucket != original.Bucket {
		t.Errorf("Bucket: got %s, want %s", decoded.Bucket, original.Bucket)
	}
	if decoded.Key != original.Key {
		t.Errorf("Key: got %s, want %s", decoded.Key, original.Key)
	}
	if decoded.Size != original.Size {
		t.Errorf("Size: got %d, want %d", decoded.Size, original.Size)
	}
	if decoded.SHA256 != original.SHA256 {
		t.Errorf("SHA256: got %s, want %s", decoded.SHA256, original.SHA256)
	}
	if decoded.ContentType != original.ContentType {
		t.Errorf("ContentType: got %s, want %s", decoded.ContentType, original.ContentType)
	}
	if decoded.ProxyID != original.ProxyID {
		t.Errorf("ProxyID: got %s, want %s", decoded.ProxyID, original.ProxyID)
	}
}

func TestDecodeEnvelopeInvalid(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{"invalid json", []byte(`not json`)},
		{"empty json", []byte(`{}`)},
		{"missing version", []byte(`{"kfs_lfs":0,"bucket":"b","key":"k","sha256":"s"}`)},
		{"missing bucket", []byte(`{"kfs_lfs":1,"key":"k","sha256":"s"}`)},
		{"missing key", []byte(`{"kfs_lfs":1,"bucket":"b","sha256":"s"}`)},
		{"missing sha256", []byte(`{"kfs_lfs":1,"bucket":"b","key":"k"}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeEnvelope(tt.input)
			if err == nil {
				t.Fatalf("expected error for invalid envelope")
			}
		})
	}
}

func TestIsLfsEnvelope(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected bool
	}{
		{"valid envelope", []byte(`{"kfs_lfs":1,"bucket":"b","key":"k","sha256":"abc"}`), true},
		{"valid with spaces", []byte(`{ "kfs_lfs": 1, "bucket": "b" }`), true},
		{"plain text", []byte("plain"), false},
		{"empty", []byte{}, false},
		{"too short", []byte(`{"kfs"}`), false},
		{"not json object", []byte(`["kfs_lfs"]`), false},
		{"no marker", []byte(`{"version":1,"bucket":"b"}`), false},
		{"binary data", []byte{0x00, 0x01, 0x02, 0x03}, false},
		{"marker past 50 bytes", []byte(`{"bucket":"very-long-bucket-name-here","key":"very-long-key","sha256":"abc","kfs_lfs":1}`), false}, // marker past first 50 bytes
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsLfsEnvelope(tt.input)
			if got != tt.expected {
				t.Errorf("IsLfsEnvelope() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	env := Envelope{
		Version:         1,
		Bucket:          "kafscale-lfs",
		Key:             "prod/events/lfs/2026/02/01/obj-550e8400-e29b-41d4-a716-446655440000",
		Size:            5242880,
		SHA256:          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		ContentType:     "image/png",
		OriginalHeaders: map[string]string{"user-id": "123", "source": "upload"},
		CreatedAt:       "2026-02-01T12:00:00Z",
		ProxyID:         "lfs-proxy-0",
	}

	encoded, err := EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	if !IsLfsEnvelope(encoded) {
		t.Fatal("encoded envelope not detected as LFS")
	}

	decoded, err := DecodeEnvelope(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded.OriginalHeaders["user-id"] != "123" {
		t.Errorf("OriginalHeaders not preserved: %v", decoded.OriginalHeaders)
	}
	if decoded.CreatedAt != "2026-02-01T12:00:00Z" {
		t.Errorf("CreatedAt not preserved: %s", decoded.CreatedAt)
	}
}
