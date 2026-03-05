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

package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
)

func TestByteReaderInt16(t *testing.T) {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, 42)
	r := newByteReader(buf)
	v, err := r.Int16()
	if err != nil {
		t.Fatal(err)
	}
	if v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}

	// Error: insufficient bytes
	r2 := newByteReader([]byte{0x01})
	_, err = r2.Int16()
	if err == nil {
		t.Fatal("expected error for insufficient bytes")
	}
}

func TestByteReaderInt32(t *testing.T) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, 1234)
	r := newByteReader(buf)
	v, err := r.Int32()
	if err != nil {
		t.Fatal(err)
	}
	if v != 1234 {
		t.Fatalf("expected 1234, got %d", v)
	}
}

func TestByteReaderInt64(t *testing.T) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 567890)
	r := newByteReader(buf)
	v, err := r.Int64()
	if err != nil {
		t.Fatal(err)
	}
	if v != 567890 {
		t.Fatalf("expected 567890, got %d", v)
	}

	// Error path
	_, err = newByteReader(nil).Int64()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestByteReaderUUID(t *testing.T) {
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	r := newByteReader(uuid[:])
	got, err := r.UUID()
	if err != nil {
		t.Fatal(err)
	}
	if got != uuid {
		t.Fatalf("UUID mismatch")
	}

	// Error path
	_, err = newByteReader(nil).UUID()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestByteReaderBool(t *testing.T) {
	r := newByteReader([]byte{1})
	v, err := r.Bool()
	if err != nil {
		t.Fatal(err)
	}
	if !v {
		t.Fatal("expected true")
	}

	r2 := newByteReader([]byte{0})
	v2, err := r2.Bool()
	if err != nil {
		t.Fatal(err)
	}
	if v2 {
		t.Fatal("expected false")
	}

	// Invalid bool value
	r3 := newByteReader([]byte{42})
	_, err = r3.Bool()
	if err == nil {
		t.Fatal("expected error for invalid bool byte")
	}

	// Error path
	_, err = newByteReader(nil).Bool()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestByteReaderString(t *testing.T) {
	// Build: length (2 bytes) + string bytes
	str := "hello"
	buf := make([]byte, 2+len(str))
	binary.BigEndian.PutUint16(buf, uint16(len(str)))
	copy(buf[2:], str)

	r := newByteReader(buf)
	got, err := r.String()
	if err != nil {
		t.Fatal(err)
	}
	if got != str {
		t.Fatalf("expected %q, got %q", str, got)
	}

	// Negative length
	negBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(negBuf, 0xFFFF) // -1 as int16
	r2 := newByteReader(negBuf)
	_, err = r2.String()
	if err == nil {
		t.Fatal("expected error for negative string length")
	}
}

func TestByteReaderCompactString(t *testing.T) {
	str := "test"
	// Compact encoding: varint(len+1) + bytes
	buf := []byte{byte(len(str) + 1)}
	buf = append(buf, str...)

	r := newByteReader(buf)
	got, err := r.CompactString()
	if err != nil {
		t.Fatal(err)
	}
	if got != str {
		t.Fatalf("expected %q, got %q", str, got)
	}
}

func TestByteReaderInt8(t *testing.T) {
	r := newByteReader([]byte{0x7F})
	v, err := r.Int8()
	if err != nil {
		t.Fatal(err)
	}
	if v != 127 {
		t.Fatalf("expected 127, got %d", v)
	}

	_, err = newByteReader(nil).Int8()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestByteReaderBytes(t *testing.T) {
	data := []byte("binary")
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf, uint32(len(data)))
	copy(buf[4:], data)

	r := newByteReader(buf)
	got, err := r.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("bytes mismatch")
	}

	// Negative length
	negBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(negBuf, 0xFFFFFFFF) // -1 as int32
	r2 := newByteReader(negBuf)
	_, err = r2.Bytes()
	if err == nil {
		t.Fatal("expected error for negative bytes length")
	}
}

func TestByteReaderRemaining(t *testing.T) {
	r := newByteReader([]byte{1, 2, 3, 4, 5})
	if r.remaining() != 5 {
		t.Fatalf("expected 5, got %d", r.remaining())
	}
	_, _ = r.read(3)
	if r.remaining() != 2 {
		t.Fatalf("expected 2, got %d", r.remaining())
	}
}

func TestByteWriterBasic(t *testing.T) {
	w := newByteWriter(0)
	w.Int16(42)
	w.Int32(1234)
	w.Int64(567890)

	data := w.Bytes()
	r := newByteReader(data)

	v16, _ := r.Int16()
	v32, _ := r.Int32()
	v64, _ := r.Int64()

	if v16 != 42 || v32 != 1234 || v64 != 567890 {
		t.Fatalf("round-trip mismatch: %d %d %d", v16, v32, v64)
	}
}

func TestByteWriterString(t *testing.T) {
	w := newByteWriter(0)
	w.String("hello")

	r := newByteReader(w.Bytes())
	got, err := r.String()
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Fatalf("expected 'hello', got %q", got)
	}
}

func TestByteWriterCompactString(t *testing.T) {
	w := newByteWriter(0)
	w.CompactString("test")

	r := newByteReader(w.Bytes())
	got, err := r.CompactString()
	if err != nil {
		t.Fatal(err)
	}
	if got != "test" {
		t.Fatalf("expected 'test', got %q", got)
	}
}

func TestFrameReadNegativeLength(t *testing.T) {
	// Construct a frame with negative length
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, 0x80000000) // -2147483648 as int32
	_, err := ReadFrame(bytes.NewReader(buf))
	if err == nil {
		t.Fatal("expected error for negative frame length")
	}
	if !strings.Contains(err.Error(), "invalid frame length") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFrameReadTruncated(t *testing.T) {
	// Length says 100 bytes but only 3 available
	buf := make([]byte, 7)
	binary.BigEndian.PutUint32(buf, 100)
	buf[4] = 1
	buf[5] = 2
	buf[6] = 3
	_, err := ReadFrame(bytes.NewReader(buf))
	if err == nil {
		t.Fatal("expected error for truncated payload")
	}
}

func TestFrameReadEmpty(t *testing.T) {
	_, err := ReadFrame(bytes.NewReader(nil))
	if err == nil {
		t.Fatal("expected error for empty reader")
	}
}

func TestEncodeResponse(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	data, err := EncodeResponse(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 7 { // 4 length + 3 payload
		t.Fatalf("expected 7 bytes, got %d", len(data))
	}
	length := int32(binary.BigEndian.Uint32(data[:4]))
	if length != 3 {
		t.Fatalf("expected length 3, got %d", length)
	}
	if !bytes.Equal(data[4:], payload) {
		t.Fatal("payload mismatch")
	}
}

func TestEncodeResponseEmpty(t *testing.T) {
	data, err := EncodeResponse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(data))
	}
}

func TestSkipTaggedFields(t *testing.T) {
	// Empty tagged fields (0 tags)
	r := newByteReader([]byte{0})
	err := r.SkipTaggedFields()
	if err != nil {
		t.Fatalf("SkipTaggedFields: %v", err)
	}

	// Insufficient data
	r2 := newByteReader(nil)
	err = r2.SkipTaggedFields()
	if err == nil {
		t.Fatal("expected error for empty reader")
	}

	// Tagged fields with actual data: 1 tag, tag_id=0, size=3, data=[0x01,0x02,0x03]
	w := newByteWriter(16)
	w.UVarint(1)      // count = 1
	w.UVarint(0)      // tag id
	w.UVarint(3)      // size = 3
	w.write([]byte{1, 2, 3})
	r3 := newByteReader(w.Bytes())
	if err := r3.SkipTaggedFields(); err != nil {
		t.Fatalf("SkipTaggedFields with data: %v", err)
	}
	if r3.remaining() != 0 {
		t.Fatalf("expected 0 remaining, got %d", r3.remaining())
	}

	// Tagged field with zero-size data
	w2 := newByteWriter(8)
	w2.UVarint(1) // count = 1
	w2.UVarint(5) // tag id
	w2.UVarint(0) // size = 0
	r4 := newByteReader(w2.Bytes())
	if err := r4.SkipTaggedFields(); err != nil {
		t.Fatalf("SkipTaggedFields zero-size: %v", err)
	}
}

func TestWriteTaggedFields(t *testing.T) {
	w := newByteWriter(4)
	w.WriteTaggedFields(0)
	r := newByteReader(w.Bytes())
	count, err := r.UVarint()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected 0 count, got %d", count)
	}

	w2 := newByteWriter(4)
	w2.WriteTaggedFields(3)
	r2 := newByteReader(w2.Bytes())
	count2, err := r2.UVarint()
	if err != nil {
		t.Fatal(err)
	}
	if count2 != 3 {
		t.Fatalf("expected 3 count, got %d", count2)
	}
}

func TestNullableStringRoundTrip(t *testing.T) {
	// Non-nil string
	w := newByteWriter(16)
	s := "hello"
	w.NullableString(&s)
	r := newByteReader(w.Bytes())
	got, err := r.NullableString()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != "hello" {
		t.Fatalf("expected 'hello', got %v", got)
	}

	// Nil string
	w2 := newByteWriter(4)
	w2.NullableString(nil)
	r2 := newByteReader(w2.Bytes())
	got2, err := r2.NullableString()
	if err != nil {
		t.Fatal(err)
	}
	if got2 != nil {
		t.Fatalf("expected nil, got %v", got2)
	}

	// Error path: invalid negative length (not -1)
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, 0xFFFE) // -2 as int16
	r3 := newByteReader(buf)
	_, err = r3.NullableString()
	if err == nil {
		t.Fatal("expected error for invalid negative length")
	}
}

func TestCompactNullableStringRoundTrip(t *testing.T) {
	// Non-nil string
	w := newByteWriter(16)
	s := "world"
	w.CompactNullableString(&s)
	r := newByteReader(w.Bytes())
	got, err := r.CompactNullableString()
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != "world" {
		t.Fatalf("expected 'world', got %v", got)
	}

	// Nil string
	w2 := newByteWriter(4)
	w2.CompactNullableString(nil)
	r2 := newByteReader(w2.Bytes())
	got2, err := r2.CompactNullableString()
	if err != nil {
		t.Fatal(err)
	}
	if got2 != nil {
		t.Fatalf("expected nil, got %v", got2)
	}
}

func TestCompactStringNull(t *testing.T) {
	// CompactString with null encoding (varint 0 = length -1)
	r := newByteReader([]byte{0})
	_, err := r.CompactString()
	if err == nil {
		t.Fatal("expected error for null compact string")
	}
	if !strings.Contains(err.Error(), "null") {
		t.Fatalf("expected null error, got: %v", err)
	}
}

func TestCompactBytesRoundTrip(t *testing.T) {
	// Non-nil bytes
	w := newByteWriter(16)
	w.CompactBytes([]byte{1, 2, 3})
	r := newByteReader(w.Bytes())
	got, err := r.CompactBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("bytes mismatch: %v", got)
	}

	// Nil bytes
	w2 := newByteWriter(4)
	w2.CompactBytes(nil)
	r2 := newByteReader(w2.Bytes())
	got2, err := r2.CompactBytes()
	if err != nil {
		t.Fatal(err)
	}
	if got2 != nil {
		t.Fatalf("expected nil, got %v", got2)
	}

	// Empty bytes
	w3 := newByteWriter(4)
	w3.CompactBytes([]byte{})
	r3 := newByteReader(w3.Bytes())
	got3, err := r3.CompactBytes()
	if err != nil {
		t.Fatal(err)
	}
	if len(got3) != 0 {
		t.Fatalf("expected empty, got %v", got3)
	}
}

func TestBytesWithLengthRoundTrip(t *testing.T) {
	w := newByteWriter(16)
	w.BytesWithLength([]byte("data"))
	r := newByteReader(w.Bytes())
	got, err := r.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "data" {
		t.Fatalf("expected 'data', got %q", got)
	}
}

func TestUVarintRoundTrip(t *testing.T) {
	for _, val := range []uint64{0, 1, 127, 128, 16383, 16384, 1<<63 - 1} {
		w := newByteWriter(16)
		w.UVarint(val)
		r := newByteReader(w.Bytes())
		got, err := r.UVarint()
		if err != nil {
			t.Fatalf("UVarint(%d): %v", val, err)
		}
		if got != val {
			t.Fatalf("expected %d, got %d", val, got)
		}
	}

	// Error path
	_, err := newByteReader(nil).UVarint()
	if err == nil {
		t.Fatal("expected error for empty reader")
	}
}

func TestCompactArrayLenRoundTrip(t *testing.T) {
	// Null array (length -1)
	w := newByteWriter(4)
	w.CompactArrayLen(-1)
	r := newByteReader(w.Bytes())
	got, err := r.CompactArrayLen()
	if err != nil {
		t.Fatal(err)
	}
	if got != -1 {
		t.Fatalf("expected -1, got %d", got)
	}

	// Empty array (length 0)
	w2 := newByteWriter(4)
	w2.CompactArrayLen(0)
	r2 := newByteReader(w2.Bytes())
	got2, err := r2.CompactArrayLen()
	if err != nil {
		t.Fatal(err)
	}
	if got2 != 0 {
		t.Fatalf("expected 0, got %d", got2)
	}

	// Normal array
	w3 := newByteWriter(4)
	w3.CompactArrayLen(5)
	r3 := newByteReader(w3.Bytes())
	got3, err := r3.CompactArrayLen()
	if err != nil {
		t.Fatal(err)
	}
	if got3 != 5 {
		t.Fatalf("expected 5, got %d", got3)
	}
}

func TestWriteFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	frame, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if !bytes.Equal(frame.Payload, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestWriteFrameError(t *testing.T) {
	// Writer that fails on first write
	err := WriteFrame(&failWriter{failAt: 0}, []byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "write frame size") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Writer that fails on second write (payload)
	err = WriteFrame(&failWriter{failAt: 1}, []byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "write frame payload") {
		t.Fatalf("unexpected error: %v", err)
	}
}

type failWriter struct {
	count  int
	failAt int
}

func (w *failWriter) Write(p []byte) (int, error) {
	if w.count == w.failAt {
		return 0, fmt.Errorf("write error")
	}
	w.count++
	return len(p), nil
}

func TestByteWriterBoolUUID(t *testing.T) {
	w := newByteWriter(32)
	w.Bool(true)
	w.Bool(false)
	uuid := [16]byte{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160}
	w.UUID(uuid)
	w.Int8(-42)

	r := newByteReader(w.Bytes())
	b1, _ := r.Bool()
	b2, _ := r.Bool()
	gotUUID, _ := r.UUID()
	i8, _ := r.Int8()

	if !b1 || b2 {
		t.Fatalf("bool mismatch: %v %v", b1, b2)
	}
	if gotUUID != uuid {
		t.Fatal("UUID mismatch")
	}
	if i8 != -42 {
		t.Fatalf("expected -42, got %d", i8)
	}
}

func TestByteWriterStringEmpty(t *testing.T) {
	w := newByteWriter(4)
	w.String("")
	r := newByteReader(w.Bytes())
	got, err := r.String()
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestByteWriterCompactStringEmpty(t *testing.T) {
	w := newByteWriter(4)
	w.CompactString("")
	r := newByteReader(w.Bytes())
	got, err := r.CompactString()
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestAPIKeyMethods(t *testing.T) {
	tests := []struct {
		name string
		req  Request
		key  int16
	}{
		{"ApiVersions", ApiVersionsRequest{}, APIKeyApiVersion},
		{"Produce", ProduceRequest{}, APIKeyProduce},
		{"Fetch", FetchRequest{}, APIKeyFetch},
		{"Metadata", MetadataRequest{}, APIKeyMetadata},
		{"CreateTopics", CreateTopicsRequest{}, APIKeyCreateTopics},
		{"DeleteTopics", DeleteTopicsRequest{}, APIKeyDeleteTopics},
		{"ListOffsets", ListOffsetsRequest{}, APIKeyListOffsets},
		{"FindCoordinator", FindCoordinatorRequest{}, APIKeyFindCoordinator},
		{"JoinGroup", JoinGroupRequest{}, APIKeyJoinGroup},
		{"SyncGroup", SyncGroupRequest{}, APIKeySyncGroup},
		{"Heartbeat", HeartbeatRequest{}, APIKeyHeartbeat},
		{"LeaveGroup", LeaveGroupRequest{}, APIKeyLeaveGroup},
		{"OffsetCommit", OffsetCommitRequest{}, APIKeyOffsetCommit},
		{"OffsetFetch", OffsetFetchRequest{}, APIKeyOffsetFetch},
		{"OffsetForLeaderEpoch", OffsetForLeaderEpochRequest{}, APIKeyOffsetForLeaderEpoch},
		{"DescribeConfigs", DescribeConfigsRequest{}, APIKeyDescribeConfigs},
		{"AlterConfigs", AlterConfigsRequest{}, APIKeyAlterConfigs},
		{"CreatePartitions", CreatePartitionsRequest{}, APIKeyCreatePartitions},
		{"DeleteGroups", DeleteGroupsRequest{}, APIKeyDeleteGroups},
		{"DescribeGroups", DescribeGroupsRequest{}, APIKeyDescribeGroups},
		{"ListGroups", ListGroupsRequest{}, APIKeyListGroups},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.req.APIKey(); got != tc.key {
				t.Fatalf("expected APIKey %d, got %d", tc.key, got)
			}
		})
	}
}

func TestParseRequestHeaderError(t *testing.T) {
	// Too short for APIKey
	_, _, err := ParseRequestHeader([]byte{0x00})
	if err == nil {
		t.Fatal("expected error for short header")
	}

	// Too short for version
	_, _, err = ParseRequestHeader([]byte{0x00, 0x00, 0x01})
	if err == nil {
		t.Fatal("expected error for missing version")
	}
}

func TestParseRequestUnknownAPIKey(t *testing.T) {
	w := newByteWriter(16)
	w.Int16(9999) // unknown API key
	w.Int16(0)
	w.Int32(1)
	w.NullableString(nil)

	_, _, err := ParseRequest(w.Bytes())
	if err == nil {
		t.Fatal("expected error for unknown API key")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIsFlexibleRequest(t *testing.T) {
	if !isFlexibleRequest(APIKeyApiVersion, 3) {
		t.Fatal("ApiVersion v3 should be flexible")
	}
	if isFlexibleRequest(APIKeyApiVersion, 2) {
		t.Fatal("ApiVersion v2 should not be flexible")
	}
	if !isFlexibleRequest(APIKeyProduce, 9) {
		t.Fatal("Produce v9 should be flexible")
	}
	if isFlexibleRequest(APIKeyProduce, 8) {
		t.Fatal("Produce v8 should not be flexible")
	}
	if isFlexibleRequest(12345, 0) {
		t.Fatal("unknown key should not be flexible")
	}
}

func TestByteReaderStringTruncated(t *testing.T) {
	// String with length=10 but only 3 bytes of data
	buf := make([]byte, 2+3)
	binary.BigEndian.PutUint16(buf, 10)
	copy(buf[2:], "abc")
	r := newByteReader(buf)
	_, err := r.String()
	if err == nil {
		t.Fatal("expected error for truncated string data")
	}
}

func TestCompactBytesReadError(t *testing.T) {
	// Compact bytes with length indicating more data than available
	w := newByteWriter(4)
	w.UVarint(100) // length+1 = 100, so length = 99
	r := newByteReader(w.Bytes())
	_, err := r.CompactBytes()
	if err == nil {
		t.Fatal("expected error for truncated compact bytes")
	}
}

func TestCompactNullableStringReadError(t *testing.T) {
	// Compact nullable string with length indicating more data than available
	w := newByteWriter(4)
	w.UVarint(50) // length+1 = 50, so length = 49
	r := newByteReader(w.Bytes())
	_, err := r.CompactNullableString()
	if err == nil {
		t.Fatal("expected error for truncated compact nullable string")
	}
}

func TestByteReaderCompactStringErrors(t *testing.T) {
	// Empty reader — compactLength fails
	r := newByteReader(nil)
	_, err := r.CompactString()
	if err == nil {
		t.Fatal("expected error for empty CompactString")
	}

	// compactLength OK but read fails (length > remaining)
	w := newByteWriter(4)
	w.UVarint(100) // length+1 = 100
	r = newByteReader(w.Bytes())
	_, err = r.CompactString()
	if err == nil {
		t.Fatal("expected error for truncated CompactString data")
	}
}

func TestByteReaderNullableStringError(t *testing.T) {
	// Int16 read fails
	r := newByteReader(nil)
	_, err := r.NullableString()
	if err == nil {
		t.Fatal("expected error for empty NullableString")
	}

	// Valid length but truncated data
	w := newByteWriter(4)
	w.Int16(10) // length 10 but no data
	r = newByteReader(w.Bytes())
	_, err = r.NullableString()
	if err == nil {
		t.Fatal("expected error for truncated NullableString data")
	}
}

func TestByteReaderBytesError(t *testing.T) {
	r := newByteReader(nil)
	_, err := r.Bytes()
	if err == nil {
		t.Fatal("expected error for empty Bytes")
	}

	// Valid length but truncated data
	w := newByteWriter(4)
	w.Int32(100)
	r = newByteReader(w.Bytes())
	_, err = r.Bytes()
	if err == nil {
		t.Fatal("expected error for truncated Bytes data")
	}
}

func TestByteReaderCompactBytesError(t *testing.T) {
	r := newByteReader(nil)
	_, err := r.CompactBytes()
	if err == nil {
		t.Fatal("expected error for empty CompactBytes")
	}
}

func TestByteReaderSkipTaggedFieldsErrors(t *testing.T) {
	// numTags read fails
	r := newByteReader(nil)
	if err := r.SkipTaggedFields(); err == nil {
		t.Fatal("expected error for empty reader")
	}

	// numTags > 0 but tag read fails
	w := newByteWriter(4)
	w.UVarint(1) // 1 tagged field
	r = newByteReader(w.Bytes())
	if err := r.SkipTaggedFields(); err == nil {
		t.Fatal("expected error for truncated tagged fields")
	}

	// tag OK but size read fails
	w = newByteWriter(8)
	w.UVarint(1) // 1 tagged field
	w.UVarint(0) // tag = 0
	r = newByteReader(w.Bytes())
	if err := r.SkipTaggedFields(); err == nil {
		t.Fatal("expected error for missing tagged field size")
	}
}

func TestByteReaderCompactLengthError(t *testing.T) {
	r := newByteReader(nil)
	_, err := r.CompactArrayLen()
	if err == nil {
		t.Fatal("expected error for empty CompactArrayLen")
	}
}
