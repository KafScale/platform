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
	"testing"
)

func FuzzFrameRoundTrip(f *testing.F) {
	f.Add([]byte("kafscale"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 1<<20 {
			return
		}
		var buf bytes.Buffer
		if err := WriteFrame(&buf, payload); err != nil {
			t.Fatalf("write frame: %v", err)
		}
		frame, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		if int(frame.Length) != len(payload) {
			t.Fatalf("length mismatch: got %d want %d", frame.Length, len(payload))
		}
		if !bytes.Equal(frame.Payload, payload) {
			t.Fatalf("payload mismatch")
		}
	})
}
