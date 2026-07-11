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

	data := w.Bytes()
	r := newByteReader(data)

	v16, _ := r.Int16()
	v32, _ := r.Int32()

	if v16 != 42 || v32 != 1234 {
		t.Fatalf("round-trip mismatch: %d %d", v16, v32)
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
	w.UVarint(1) // count = 1
	w.UVarint(0) // tag id
	w.UVarint(3) // size = 3
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
