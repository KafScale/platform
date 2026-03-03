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
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// RequestHeader carries the parsed Kafka request header fields.
// kmsg does not handle headers — they must be parsed/written manually.
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      *string
}

// ParseRequestHeader decodes the header portion from raw frame bytes and
// returns the header plus the remaining body bytes.
func ParseRequestHeader(b []byte) (*RequestHeader, []byte, error) {
	r := newByteReader(b)
	apiKey, err := r.Int16()
	if err != nil {
		return nil, nil, fmt.Errorf("read api key: %w", err)
	}
	version, err := r.Int16()
	if err != nil {
		return nil, nil, fmt.Errorf("read api version: %w", err)
	}
	correlationID, err := r.Int32()
	if err != nil {
		return nil, nil, fmt.Errorf("read correlation id: %w", err)
	}
	clientID, err := r.NullableString()
	if err != nil {
		return nil, nil, fmt.Errorf("read client id: %w", err)
	}

	// Flexible versions (KIP-482) add tagged fields to the header.
	req := kmsg.RequestForKey(apiKey)
	if req != nil {
		req.SetVersion(version)
		if req.IsFlexible() {
			if err := r.SkipTaggedFields(); err != nil {
				return nil, nil, fmt.Errorf("skip header tags: %w", err)
			}
		}
	}

	header := &RequestHeader{
		APIKey:        apiKey,
		APIVersion:    version,
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	return header, b[r.pos:], nil
}

// ParseRequest decodes a full request frame (header + body) into the parsed
// header and a concrete kmsg request type.
func ParseRequest(b []byte) (*RequestHeader, kmsg.Request, error) {
	header, body, err := ParseRequestHeader(b)
	if err != nil {
		return nil, nil, err
	}
	return ParseRequestBody(header, body)
}

// ParseRequestBody decodes the body bytes into a concrete kmsg request using
// the API key and version from the already-parsed header. Use this when the
// header has already been parsed via ParseRequestHeader to avoid re-parsing.
func ParseRequestBody(header *RequestHeader, body []byte) (*RequestHeader, kmsg.Request, error) {
	req := kmsg.RequestForKey(header.APIKey)
	if req == nil {
		return nil, nil, fmt.Errorf("unsupported api key %d", header.APIKey)
	}
	req.SetVersion(header.APIVersion)

	if err := req.ReadFrom(body); err != nil {
		return nil, nil, fmt.Errorf("decode %s v%d: %w",
			kmsg.NameForKey(header.APIKey), header.APIVersion, err)
	}

	return header, req, nil
}
