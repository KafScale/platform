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
	"io"
	"net"
	"time"
)

// Frame represents a Kafka request or response frame.
type Frame struct {
	Length  int32
	Payload []byte
}

// ReadFrame reads a single size-prefixed frame from r.
func ReadFrame(r io.Reader) (*Frame, error) {
	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return nil, fmt.Errorf("read frame size: %w", err)
	}
	length := int32(binary.BigEndian.Uint32(lengthBuf[:]))
	if length < 0 {
		return nil, fmt.Errorf("invalid frame length %d", length)
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read frame payload: %w", err)
	}
	return &Frame{Length: length, Payload: payload}, nil
}

// WriteFrame writes payload prefixed with its length to w.
func WriteFrame(w io.Writer, payload []byte) error {
	var lengthBuf [4]byte
	if len(payload) > int(^uint32(0)>>1) {
		return fmt.Errorf("payload too large: %d", len(payload))
	}
	binary.BigEndian.PutUint32(lengthBuf[:], uint32(len(payload)))
	if _, err := w.Write(lengthBuf[:]); err != nil {
		return fmt.Errorf("write frame size: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("write frame payload: %w", err)
	}
	return nil
}

// DefaultResponseWriteTimeout bounds a client that stops reading responses.
const DefaultResponseWriteTimeout = 30 * time.Second

const ResponseWriteChunk = 256 << 10

func WriteFrameWithDeadline(conn net.Conn, payload []byte, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = DefaultResponseWriteTimeout
	}
	if len(payload) > int(^uint32(0)>>1) {
		return fmt.Errorf("payload too large: %d", len(payload))
	}
	defer conn.SetWriteDeadline(time.Time{})
	var lengthBuf [4]byte
	binary.BigEndian.PutUint32(lengthBuf[:], uint32(len(payload)))
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	if _, err := conn.Write(lengthBuf[:]); err != nil {
		return fmt.Errorf("write frame size: %w", err)
	}
	for start := 0; start < len(payload); start += ResponseWriteChunk {
		end := min(start+ResponseWriteChunk, len(payload))
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		if _, err := conn.Write(payload[start:end]); err != nil {
			return fmt.Errorf("write frame payload: %w", err)
		}
	}
	return nil
}
