package protocol

import (
	"encoding/binary"
	"fmt"
)

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

func (r *byteReader) Int64() (int64, error) {
	b, err := r.read(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

func (r *byteReader) Bool() (bool, error) {
	b, err := r.read(1)
	if err != nil {
		return false, err
	}
	switch b[0] {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("invalid bool: %d", b[0])
	}
}

func (r *byteReader) String() (string, error) {
	l, err := r.Int16()
	if err != nil {
		return "", err
	}
	if l < 0 {
		return "", fmt.Errorf("invalid string length: %d", l)
	}
	b, err := r.read(int(l))
	if err != nil {
		return "", err
	}
	return string(b), nil
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

func (w *byteWriter) Int64(v int64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	w.write(tmp[:])
}

func (w *byteWriter) Bool(v bool) {
	if v {
		w.write([]byte{1})
	} else {
		w.write([]byte{0})
	}
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

func (r *byteReader) Bytes() ([]byte, error) {
	length, err := r.Int32()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid bytes length %d", length)
	}
	b, err := r.read(int(length))
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (w *byteWriter) BytesWithLength(b []byte) {
	w.Int32(int32(len(b)))
	w.write(b)
}

func (w *byteWriter) Bytes() []byte {
	return w.buf
}
