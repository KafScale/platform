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

// lfsbyteWriter is a minimal byte buffer used for SASL and produce encoding
// in the LFS module.
type lfsbyteWriter struct {
	buf []byte
}

func newLFSByteWriter(capacity int) *lfsbyteWriter {
	return &lfsbyteWriter{buf: make([]byte, 0, capacity)}
}

func (w *lfsbyteWriter) write(b []byte) {
	w.buf = append(w.buf, b...)
}

func (w *lfsbyteWriter) Int16(v int16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	w.write(tmp[:])
}

func (w *lfsbyteWriter) Int32(v int32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	w.write(tmp[:])
}

func (w *lfsbyteWriter) String(v string) {
	w.Int16(int16(len(v)))
	if len(v) > 0 {
		w.write([]byte(v))
	}
}

func (w *lfsbyteWriter) NullableString(v *string) {
	if v == nil {
		w.Int16(-1)
		return
	}
	w.String(*v)
}

func (w *lfsbyteWriter) CompactString(v string) {
	w.compactLength(len(v))
	if len(v) > 0 {
		w.write([]byte(v))
	}
}

func (w *lfsbyteWriter) CompactNullableString(v *string) {
	if v == nil {
		w.compactLength(-1)
		return
	}
	w.CompactString(*v)
}

func (w *lfsbyteWriter) BytesWithLength(b []byte) {
	w.Int32(int32(len(b)))
	w.write(b)
}

func (w *lfsbyteWriter) CompactBytes(b []byte) {
	if b == nil {
		w.compactLength(-1)
		return
	}
	w.compactLength(len(b))
	w.write(b)
}

func (w *lfsbyteWriter) UVarint(v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	w.write(tmp[:n])
}

func (w *lfsbyteWriter) CompactArrayLen(length int) {
	if length < 0 {
		w.UVarint(0)
		return
	}
	w.UVarint(uint64(length) + 1)
}

func (w *lfsbyteWriter) WriteTaggedFields(count int) {
	if count == 0 {
		w.UVarint(0)
		return
	}
	w.UVarint(uint64(count))
}

func (w *lfsbyteWriter) compactLength(length int) {
	if length < 0 {
		w.UVarint(0)
		return
	}
	w.UVarint(uint64(length) + 1)
}

func (w *lfsbyteWriter) Bytes() []byte {
	return w.buf
}

const (
	lfsAPIKeySaslHandshake    int16 = 17
	lfsAPIKeySaslAuthenticate int16 = 36
)

func lfsEncodeSaslHandshakeRequest(header *protocol.RequestHeader, mechanism string) ([]byte, error) {
	if header == nil {
		return nil, errors.New("nil header")
	}
	w := newLFSByteWriter(0)
	w.Int16(header.APIKey)
	w.Int16(header.APIVersion)
	w.Int32(header.CorrelationID)
	w.NullableString(header.ClientID)
	w.String(mechanism)
	return w.Bytes(), nil
}

func lfsEncodeSaslAuthenticateRequest(header *protocol.RequestHeader, authBytes []byte) ([]byte, error) {
	if header == nil {
		return nil, errors.New("nil header")
	}
	w := newLFSByteWriter(0)
	w.Int16(header.APIKey)
	w.Int16(header.APIVersion)
	w.Int32(header.CorrelationID)
	w.NullableString(header.ClientID)
	w.BytesWithLength(authBytes)
	return w.Bytes(), nil
}

func lfsBuildSaslPlainAuthBytes(username, password string) []byte {
	buf := make([]byte, 0, len(username)+len(password)+2)
	buf = append(buf, 0)
	buf = append(buf, []byte(username)...)
	buf = append(buf, 0)
	buf = append(buf, []byte(password)...)
	return buf
}

func lfsReadSaslResponse(r io.Reader) error {
	frame, err := protocol.ReadFrame(r)
	if err != nil {
		return err
	}
	if len(frame.Payload) < 6 {
		return fmt.Errorf("invalid SASL response length %d", len(frame.Payload))
	}
	errorCode := int16(binary.BigEndian.Uint16(frame.Payload[4:6]))
	if errorCode != 0 {
		return fmt.Errorf("sasl error code %d", errorCode)
	}
	return nil
}

// lfsEncodeProduceRequest encodes a ProduceRequest for the HTTP API produce path.
func lfsEncodeProduceRequest(header *protocol.RequestHeader, req *protocol.ProduceRequest) ([]byte, error) {
	if header == nil || req == nil {
		return nil, errors.New("nil header or request")
	}
	flexible := lfsIsFlexibleRequest(header.APIKey, header.APIVersion)
	w := newLFSByteWriter(0)
	w.Int16(header.APIKey)
	w.Int16(header.APIVersion)
	w.Int32(header.CorrelationID)
	w.NullableString(header.ClientID)
	if flexible {
		w.WriteTaggedFields(0)
	}

	if header.APIVersion >= 3 {
		if flexible {
			w.CompactNullableString(req.TransactionalID)
		} else {
			w.NullableString(req.TransactionalID)
		}
	}
	w.Int16(req.Acks)
	w.Int32(req.TimeoutMs)
	if flexible {
		w.CompactArrayLen(len(req.Topics))
	} else {
		w.Int32(int32(len(req.Topics)))
	}
	for _, topic := range req.Topics {
		if flexible {
			w.CompactString(topic.Name)
			w.CompactArrayLen(len(topic.Partitions))
		} else {
			w.String(topic.Name)
			w.Int32(int32(len(topic.Partitions)))
		}
		for _, partition := range topic.Partitions {
			w.Int32(partition.Partition)
			if flexible {
				w.CompactBytes(partition.Records)
				w.WriteTaggedFields(0)
			} else {
				w.BytesWithLength(partition.Records)
			}
		}
		if flexible {
			w.WriteTaggedFields(0)
		}
	}
	if flexible {
		w.WriteTaggedFields(0)
	}

	return w.Bytes(), nil
}

func lfsIsFlexibleRequest(apiKey, version int16) bool {
	switch apiKey {
	case protocol.APIKeyApiVersion:
		return version >= 3
	case protocol.APIKeyProduce:
		return version >= 9
	case protocol.APIKeyMetadata:
		return version >= 9
	case protocol.APIKeyFetch:
		return version >= 12
	case protocol.APIKeyFindCoordinator:
		return version >= 3
	default:
		return false
	}
}
