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

package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/KafScale/platform/pkg/protocol"
)

func encodeSaslHandshakeRequest(header *protocol.RequestHeader, mechanism string) ([]byte, error) {
	if header == nil {
		return nil, errors.New("nil header")
	}
	w := newByteWriter(0)
	w.Int16(header.APIKey)
	w.Int16(header.APIVersion)
	w.Int32(header.CorrelationID)
	w.NullableString(header.ClientID)
	w.String(mechanism)
	return w.Bytes(), nil
}

func encodeSaslAuthenticateRequest(header *protocol.RequestHeader, authBytes []byte) ([]byte, error) {
	if header == nil {
		return nil, errors.New("nil header")
	}
	w := newByteWriter(0)
	w.Int16(header.APIKey)
	w.Int16(header.APIVersion)
	w.Int32(header.CorrelationID)
	w.NullableString(header.ClientID)
	w.BytesWithLength(authBytes)
	return w.Bytes(), nil
}

func buildSaslPlainAuthBytes(username, password string) []byte {
	// PLAIN: 0x00 + username + 0x00 + password
	buf := make([]byte, 0, len(username)+len(password)+2)
	buf = append(buf, 0)
	buf = append(buf, []byte(username)...)
	buf = append(buf, 0)
	buf = append(buf, []byte(password)...)
	return buf
}

func readSaslResponse(r io.Reader) error {
	frame, err := protocol.ReadFrame(r)
	if err != nil {
		return err
	}
	if len(frame.Payload) < 6 {
		return fmt.Errorf("invalid SASL response length %d", len(frame.Payload))
	}
	// First 4 bytes are correlation ID
	errorCode := int16(binary.BigEndian.Uint16(frame.Payload[4:6]))
	if errorCode != 0 {
		return fmt.Errorf("sasl error code %d", errorCode)
	}
	return nil
}
