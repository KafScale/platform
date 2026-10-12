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

package testutil

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// AssertApiVersionsNegotiation checks wire encoding and same-connection retries.
func AssertApiVersionsNegotiation(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for i, version := range []int16{5, 0, 32767, 4, -1, 3, 1, 2, 0} {
		req := kmsg.NewPtrApiVersionsRequest()
		req.SetVersion(version)
		req.ClientSoftwareName = "negotiation-test"
		req.ClientSoftwareVersion = "1.0"
		payload := binary.BigEndian.AppendUint16(nil, uint16(req.Key()))
		payload = binary.BigEndian.AppendUint16(payload, uint16(version))
		correlation := int32(i + 1)
		payload = binary.BigEndian.AppendUint32(payload, uint32(correlation))
		payload = binary.BigEndian.AppendUint16(payload, 0xffff) // null client ID
		if req.IsFlexible() {
			payload = append(payload, 0) // header tags
		}
		responseVersion, errorCode := version, int16(protocol.NONE)
		if version < 0 || version > protocol.ApiVersionsMaxVersion {
			// Deliberately opaque: an unsupported body must never be decoded.
			payload = append(payload, 0xff)
			responseVersion, errorCode = 0, protocol.UNSUPPORTED_VERSION
		} else {
			payload = req.AppendTo(payload)
		}
		if err := protocol.WriteFrame(conn, payload); err != nil {
			t.Fatalf("write v%d: %v", version, err)
		}
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			t.Fatalf("read v%d: %v", version, err)
		}
		if len(frame.Payload) < 4 || int32(binary.BigEndian.Uint32(frame.Payload[:4])) != correlation {
			t.Fatalf("v%d: incorrect correlation ID", version)
		}
		resp := kmsg.NewPtrApiVersionsResponse()
		resp.SetVersion(responseVersion)
		if err := resp.ReadFrom(frame.Payload[4:]); err != nil {
			t.Fatalf("decode v%d response as v%d: %v", version, responseVersion, err)
		}
		if resp.ErrorCode != errorCode {
			t.Fatalf("v%d: error code %d, want %d", version, resp.ErrorCode, errorCode)
		}
		found := false
		for _, key := range resp.ApiKeys {
			if key.ApiKey == protocol.APIKeyApiVersion {
				found = true
				if key.MinVersion != 0 || key.MaxVersion != protocol.ApiVersionsMaxVersion {
					t.Fatalf("v%d: unexpected supported versions %+v", version, key)
				}
			}
		}
		if !found {
			t.Fatalf("v%d: missing ApiVersions supported range", version)
		}
	}
}
