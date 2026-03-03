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
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)


// TestGroupResponseErrorCode_RoundTrip encodes known responses via EncodeResponse,
// then verifies GroupResponseErrorCode extracts the correct error code.
func TestGroupResponseErrorCode_RoundTrip(t *testing.T) {
	tests := []struct {
		name       string
		apiKey     int16
		apiVersion int16
		encode     func() []byte
		wantCode   int16
	}{
		{
			name:       "JoinGroup v2 NOT_COORDINATOR",
			apiKey:     APIKeyJoinGroup,
			apiVersion: 2,
			encode: func() []byte {
				resp := kmsg.NewPtrJoinGroupResponse()
				resp.ErrorCode = NOT_COORDINATOR
				return EncodeResponse(1, 2, resp)
			},
			wantCode: NOT_COORDINATOR,
		},
		{
			name:       "JoinGroup v5 NOT_COORDINATOR (flexible)",
			apiKey:     APIKeyJoinGroup,
			apiVersion: 5,
			encode: func() []byte {
				resp := kmsg.NewPtrJoinGroupResponse()
				resp.ErrorCode = NOT_COORDINATOR
				return EncodeResponse(2, 5, resp)
			},
			wantCode: NOT_COORDINATOR,
		},
		{
			name:       "JoinGroup v2 success",
			apiKey:     APIKeyJoinGroup,
			apiVersion: 2,
			encode: func() []byte {
				resp := kmsg.NewPtrJoinGroupResponse()
				resp.ErrorCode = NONE
				resp.Protocol = kmsg.StringPtr("range")
				resp.LeaderID = "member-1"
				resp.MemberID = "member-1"
				return EncodeResponse(3, 2, resp)
			},
			wantCode: NONE,
		},
		{
			name:       "SyncGroup v1 NOT_COORDINATOR",
			apiKey:     APIKeySyncGroup,
			apiVersion: 1,
			encode: func() []byte {
				resp := kmsg.NewPtrSyncGroupResponse()
				resp.ErrorCode = NOT_COORDINATOR
				return EncodeResponse(4, 1, resp)
			},
			wantCode: NOT_COORDINATOR,
		},
		{
			name:       "SyncGroup v4 NOT_COORDINATOR (flexible)",
			apiKey:     APIKeySyncGroup,
			apiVersion: 4,
			encode: func() []byte {
				resp := kmsg.NewPtrSyncGroupResponse()
				resp.ErrorCode = NOT_COORDINATOR
				return EncodeResponse(5, 4, resp)
			},
			wantCode: NOT_COORDINATOR,
		},
		{
			name:       "Heartbeat v1 NOT_COORDINATOR",
			apiKey:     APIKeyHeartbeat,
			apiVersion: 1,
			encode: func() []byte {
				resp := kmsg.NewPtrHeartbeatResponse()
				resp.ErrorCode = NOT_COORDINATOR
				return EncodeResponse(6, 1, resp)
			},
			wantCode: NOT_COORDINATOR,
		},
		{
			name:       "Heartbeat v4 NOT_COORDINATOR (flexible)",
			apiKey:     APIKeyHeartbeat,
			apiVersion: 4,
			encode: func() []byte {
				resp := kmsg.NewPtrHeartbeatResponse()
				resp.ErrorCode = NOT_COORDINATOR
				return EncodeResponse(7, 4, resp)
			},
			wantCode: NOT_COORDINATOR,
		},
		{
			name:       "LeaveGroup v0 NOT_COORDINATOR",
			apiKey:     APIKeyLeaveGroup,
			apiVersion: 0,
			encode: func() []byte {
				resp := kmsg.NewPtrLeaveGroupResponse()
				resp.ErrorCode = NOT_COORDINATOR
				return EncodeResponse(8, 0, resp)
			},
			wantCode: NOT_COORDINATOR,
		},
		{
			name:       "OffsetCommit v3 partition error",
			apiKey:     APIKeyOffsetCommit,
			apiVersion: 3,
			encode: func() []byte {
				resp := kmsg.NewPtrOffsetCommitResponse()
				topic := kmsg.NewOffsetCommitResponseTopic()
				topic.Topic = "test-topic"
				part := kmsg.NewOffsetCommitResponseTopicPartition()
				part.Partition = 0
				part.ErrorCode = NOT_COORDINATOR
				topic.Partitions = append(topic.Partitions, part)
				resp.Topics = append(resp.Topics, topic)
				return EncodeResponse(9, 3, resp)
			},
			wantCode: NOT_COORDINATOR,
		},
		{
			name:       "OffsetCommit v3 success (partition 16, no false positive)",
			apiKey:     APIKeyOffsetCommit,
			apiVersion: 3,
			encode: func() []byte {
				resp := kmsg.NewPtrOffsetCommitResponse()
				topic := kmsg.NewOffsetCommitResponseTopic()
				topic.Topic = "test-topic"
				part := kmsg.NewOffsetCommitResponseTopicPartition()
				part.Partition = 16
				part.ErrorCode = NONE
				topic.Partitions = append(topic.Partitions, part)
				resp.Topics = append(resp.Topics, topic)
				return EncodeResponse(10, 3, resp)
			},
			wantCode: NONE,
		},
		{
			name:       "OffsetFetch v3 NOT_COORDINATOR (top-level)",
			apiKey:     APIKeyOffsetFetch,
			apiVersion: 3,
			encode: func() []byte {
				resp := kmsg.NewPtrOffsetFetchResponse()
				resp.ErrorCode = NOT_COORDINATOR
				topic := kmsg.NewOffsetFetchResponseTopic()
				topic.Topic = "test-topic"
				part := kmsg.NewOffsetFetchResponseTopicPartition()
				part.Partition = 0
				part.Offset = -1
				part.ErrorCode = NOT_COORDINATOR
				topic.Partitions = append(topic.Partitions, part)
				resp.Topics = append(resp.Topics, topic)
				return EncodeResponse(11, 3, resp)
			},
			wantCode: NOT_COORDINATOR,
		},
		{
			name:       "OffsetFetch v5 success (offset 16, no false positive)",
			apiKey:     APIKeyOffsetFetch,
			apiVersion: 5,
			encode: func() []byte {
				resp := kmsg.NewPtrOffsetFetchResponse()
				resp.ErrorCode = NONE
				topic := kmsg.NewOffsetFetchResponseTopic()
				topic.Topic = "test-topic"
				part := kmsg.NewOffsetFetchResponseTopicPartition()
				part.Partition = 0
				part.Offset = 16
				part.ErrorCode = NONE
				topic.Partitions = append(topic.Partitions, part)
				resp.Topics = append(resp.Topics, topic)
				return EncodeResponse(12, 5, resp)
			},
			wantCode: NONE,
		},
		{
			name:       "DescribeGroups v5 NOT_COORDINATOR",
			apiKey:     APIKeyDescribeGroups,
			apiVersion: 5,
			encode: func() []byte {
				resp := kmsg.NewPtrDescribeGroupsResponse()
				group := kmsg.NewDescribeGroupsResponseGroup()
				group.ErrorCode = NOT_COORDINATOR
				group.Group = "my-group"
				resp.Groups = append(resp.Groups, group)
				return EncodeResponse(13, 5, resp)
			},
			wantCode: NOT_COORDINATOR,
		},
		{
			name:       "DescribeGroups v5 success",
			apiKey:     APIKeyDescribeGroups,
			apiVersion: 5,
			encode: func() []byte {
				resp := kmsg.NewPtrDescribeGroupsResponse()
				group := kmsg.NewDescribeGroupsResponseGroup()
				group.ErrorCode = NONE
				group.Group = "my-group"
				group.State = "Stable"
				resp.Groups = append(resp.Groups, group)
				return EncodeResponse(14, 5, resp)
			},
			wantCode: NONE,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := tc.encode()
			gotCode, ok := GroupResponseErrorCode(tc.apiKey, tc.apiVersion, resp)
			if !ok {
				t.Fatalf("GroupResponseErrorCode returned ok=false for valid response")
			}
			if gotCode != tc.wantCode {
				t.Fatalf("GroupResponseErrorCode = %d, want %d", gotCode, tc.wantCode)
			}
		})
	}
}

func TestGroupResponseErrorCode_Truncated(t *testing.T) {
	_, ok := GroupResponseErrorCode(APIKeyJoinGroup, 2, []byte{0, 0, 0, 1})
	if ok {
		t.Fatalf("expected ok=false for truncated JoinGroup response")
	}

	_, ok = GroupResponseErrorCode(APIKeyLeaveGroup, 0, []byte{0, 0})
	if ok {
		t.Fatalf("expected ok=false for truncated LeaveGroup response")
	}
}

func TestGroupResponseErrorCode_UnsupportedKey(t *testing.T) {
	_, ok := GroupResponseErrorCode(APIKeyProduce, 9, []byte{0, 0, 0, 1, 0, 0, 0, 0})
	if ok {
		t.Fatalf("expected ok=false for unsupported api key")
	}
}
