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

import "github.com/twmb/franz-go/pkg/kmsg"

// encodeResponseHeader builds the response header bytes that must be prepended
// to the kmsg response body. For non-flexible versions this is just the
// correlation ID (4 bytes). For flexible versions it also includes an empty
// tagged field section (1 byte: varint 0).
func encodeResponseHeader(correlationID int32, flexible bool) []byte {
	if flexible {
		buf := make([]byte, 5)
		buf[0] = byte(correlationID >> 24)
		buf[1] = byte(correlationID >> 16)
		buf[2] = byte(correlationID >> 8)
		buf[3] = byte(correlationID)
		buf[4] = 0 // empty tagged fields
		return buf
	}
	buf := make([]byte, 4)
	buf[0] = byte(correlationID >> 24)
	buf[1] = byte(correlationID >> 16)
	buf[2] = byte(correlationID >> 8)
	buf[3] = byte(correlationID)
	return buf
}

// EncodeResponse serializes a kmsg response with the proper header prepended.
// The API key and flexibility are derived from the response itself.
//
// ApiVersions (key 18) is a special case per KIP-511: the response body uses
// flexible encoding starting at version 3, but the response *header* is always
// the non-flexible v0 header (no tagged-fields byte). Clients rely on this to
// implement the v0 fallback during version negotiation.
func EncodeResponse(correlationID int32, apiVersion int16, resp kmsg.Response) []byte {
	resp.SetVersion(apiVersion)
	flexibleHeader := resp.IsFlexible() && resp.Key() != APIKeyApiVersion
	header := encodeResponseHeader(correlationID, flexibleHeader)
	return resp.AppendTo(header)
}

// GroupResponseErrorCode extracts the top-level error code from already-
// serialized group response bytes, accounting for flexible headers and
// version-dependent layout.
func GroupResponseErrorCode(apiKey int16, apiVersion int16, resp []byte) (int16, bool) {
	body, ok := SkipResponseHeader(apiKey, apiVersion, resp)
	if !ok {
		return 0, false
	}

	switch apiKey {
	case APIKeyJoinGroup:
		return decodeJoinGroupErrorCode(apiVersion, body)
	case APIKeySyncGroup:
		return decodeSyncGroupErrorCode(apiVersion, body)
	case APIKeyHeartbeat:
		return decodeHeartbeatErrorCode(apiVersion, body)
	case APIKeyLeaveGroup:
		return decodeLeaveGroupErrorCode(apiVersion, body)
	case APIKeyOffsetCommit:
		return decodeOffsetCommitErrorCode(apiVersion, body)
	case APIKeyOffsetFetch:
		return decodeOffsetFetchErrorCode(apiVersion, body)
	case APIKeyDescribeGroups:
		return decodeDescribeGroupsErrorCode(apiVersion, body)
	default:
		return 0, false
	}
}

// SkipResponseHeader skips the correlation ID and (for flexible versions) the
// tagged fields section. Returns the remaining body bytes.
func SkipResponseHeader(apiKey, apiVersion int16, data []byte) ([]byte, bool) {
	if len(data) < 4 {
		return nil, false
	}
	pos := 4 // skip correlation_id

	kResp := kmsg.ResponseForKey(apiKey)
	if kResp == nil {
		return nil, false
	}
	kResp.SetVersion(apiVersion)
	if kResp.IsFlexible() {
		if pos >= len(data) {
			return nil, false
		}
		// Tagged fields length is a varint; always 0 for our responses.
		r := newByteReader(data[pos:])
		if err := r.SkipTaggedFields(); err != nil {
			return nil, false
		}
		pos += r.pos
	}
	return data[pos:], true
}

func decodeJoinGroupErrorCode(version int16, body []byte) (int16, bool) {
	resp := kmsg.NewPtrJoinGroupResponse()
	resp.SetVersion(version)
	if err := resp.ReadFrom(body); err != nil {
		return 0, false
	}
	return resp.ErrorCode, true
}

func decodeSyncGroupErrorCode(version int16, body []byte) (int16, bool) {
	resp := kmsg.NewPtrSyncGroupResponse()
	resp.SetVersion(version)
	if err := resp.ReadFrom(body); err != nil {
		return 0, false
	}
	return resp.ErrorCode, true
}

func decodeHeartbeatErrorCode(version int16, body []byte) (int16, bool) {
	resp := kmsg.NewPtrHeartbeatResponse()
	resp.SetVersion(version)
	if err := resp.ReadFrom(body); err != nil {
		return 0, false
	}
	return resp.ErrorCode, true
}

func decodeLeaveGroupErrorCode(version int16, body []byte) (int16, bool) {
	resp := kmsg.NewPtrLeaveGroupResponse()
	resp.SetVersion(version)
	if err := resp.ReadFrom(body); err != nil {
		return 0, false
	}
	return resp.ErrorCode, true
}

func decodeOffsetCommitErrorCode(version int16, body []byte) (int16, bool) {
	resp := kmsg.NewPtrOffsetCommitResponse()
	resp.SetVersion(version)
	if err := resp.ReadFrom(body); err != nil {
		return 0, false
	}
	for _, topic := range resp.Topics {
		for _, part := range topic.Partitions {
			if part.ErrorCode != 0 {
				return part.ErrorCode, true
			}
		}
	}
	return 0, true
}

func decodeOffsetFetchErrorCode(version int16, body []byte) (int16, bool) {
	resp := kmsg.NewPtrOffsetFetchResponse()
	resp.SetVersion(version)
	if err := resp.ReadFrom(body); err != nil {
		return 0, false
	}
	for _, topic := range resp.Topics {
		for _, part := range topic.Partitions {
			if part.ErrorCode != 0 {
				return part.ErrorCode, true
			}
		}
	}
	// v2+ has a top-level error code.
	if version >= 2 {
		return resp.ErrorCode, true
	}
	return 0, true
}

func decodeDescribeGroupsErrorCode(version int16, body []byte) (int16, bool) {
	resp := kmsg.NewPtrDescribeGroupsResponse()
	resp.SetVersion(version)
	if err := resp.ReadFrom(body); err != nil {
		return 0, false
	}
	if len(resp.Groups) == 0 {
		return 0, true
	}
	return resp.Groups[0].ErrorCode, true
}
