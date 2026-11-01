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
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

func TestFrameReadWrite(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	var buf bytes.Buffer

	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	frame, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}

	if frame.Length != int32(len(payload)) {
		t.Fatalf("unexpected frame length: %d", frame.Length)
	}
	if !bytes.Equal(frame.Payload, payload) {
		t.Fatalf("payload mismatch: %v vs %v", frame.Payload, payload)
	}
}

func TestWriteFrameWithDeadlineResetsPerChunk(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	payload := bytes.Repeat([]byte{7}, 4*ResponseWriteChunk)
	done := make(chan error, 1)
	go func() {
		done <- WriteFrameWithDeadline(server, payload, 150*time.Millisecond)
	}()
	buf := make([]byte, 4+len(payload))
	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(client, buf[:4]); err != nil {
		t.Fatal(err)
	}
	for start := 4; start < len(buf); start += ResponseWriteChunk {
		time.Sleep(60 * time.Millisecond)
		if _, err := io.ReadFull(client, buf[start:start+ResponseWriteChunk]); err != nil {
			t.Fatalf("read at %d: %v, write: %v", start, err, <-done)
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("slow but steady reader failed: %v", err)
	}
	if !bytes.Equal(buf[4:], payload) {
		t.Fatal("payload mismatch")
	}
}

func TestWriteFrameWithDeadlineStalledReader(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	done := make(chan error, 1)
	go func() {
		done <- WriteFrameWithDeadline(server, bytes.Repeat([]byte{1}, 2*ResponseWriteChunk), 50*time.Millisecond)
	}()
	if _, err := io.ReadFull(client, make([]byte, 4)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stalled reader did not time out")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write did not time out")
	}
}
