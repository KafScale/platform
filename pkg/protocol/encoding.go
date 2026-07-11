// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"encoding/binary"
	"fmt"
)

// byteReader is a minimal binary reader used to parse Kafka request headers
// and response header fields that kmsg does not handle.
type byteReader struct {
	buf []byte
	pos int
}

func newByteReader(b []byte) *byteReader {
	return &byteReader{buf: b}
}

func (r *byteReader) remaining() int {
	return len(r.buf) - r.pos
}

func (r *byteReader) read(n int) ([]byte, error) {
	if r.remaining() < n {
		return nil, fmt.Errorf("insufficient bytes: need %d have %d", n, r.remaining())
	}
	start := r.pos
	r.pos += n
	return r.buf[start:r.pos], nil
}

func (r *byteReader) Int16() (int16, error) {
	b, err := r.read(2)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}

func (r *byteReader) Int32() (int32, error) {
	b, err := r.read(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func (r *byteReader) NullableString() (*string, error) {
	l, err := r.Int16()
	if err != nil {
		return nil, err
	}
	if l == -1 {
		return nil, nil
	}
	if l < 0 {
		return nil, fmt.Errorf("invalid string length: %d", l)
	}
	b, err := r.read(int(l))
	if err != nil {
		return nil, err
	}
	str := string(b)
	return &str, nil
}

func (r *byteReader) UVarint() (uint64, error) {
	val, n := binary.Uvarint(r.buf[r.pos:])
	if n <= 0 {
		return 0, fmt.Errorf("read uvarint: %d", n)
	}
	r.pos += n
	return val, nil
}

func (r *byteReader) SkipTaggedFields() error {
	count, err := r.UVarint()
	if err != nil {
		return err
	}
	for i := uint64(0); i < count; i++ {
		if _, err := r.UVarint(); err != nil {
			return err
		}
		size, err := r.UVarint()
		if err != nil {
			return err
		}
		if size == 0 {
			continue
		}
		if _, err := r.read(int(size)); err != nil {
			return err
		}
	}
	return nil
}

// byteWriter is a minimal binary writer used in tests to construct Kafka
// request headers that kmsg does not handle.
type byteWriter struct {
	buf []byte
}

func newByteWriter(capacity int) *byteWriter {
	return &byteWriter{buf: make([]byte, 0, capacity)}
}

func (w *byteWriter) write(b []byte) {
	w.buf = append(w.buf, b...)
}

func (w *byteWriter) Int16(v int16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	w.write(tmp[:])
}

func (w *byteWriter) Int32(v int32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	w.write(tmp[:])
}

func (w *byteWriter) String(v string) {
	if v == "" {
		w.Int16(0)
		return
	}
	if len(v) > 0x7fff {
		panic("string too long")
	}
	w.Int16(int16(len(v)))
	w.write([]byte(v))
}

func (w *byteWriter) NullableString(v *string) {
	if v == nil {
		w.Int16(-1)
		return
	}
	w.String(*v)
}

func (w *byteWriter) UVarint(v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	w.write(tmp[:n])
}

func (w *byteWriter) WriteTaggedFields(count int) {
	if count == 0 {
		w.UVarint(0)
		return
	}
	w.UVarint(uint64(count))
}

func (w *byteWriter) Bytes() []byte {
	return w.buf
}
