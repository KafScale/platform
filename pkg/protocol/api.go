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

// API keys supported by Kafscale in milestone 1.
const (
	APIKeyProduce              int16 = 0
	APIKeyFetch                int16 = 1
	APIKeyMetadata             int16 = 3
	APIKeyOffsetCommit         int16 = 8
	APIKeyOffsetFetch          int16 = 9
	APIKeyFindCoordinator      int16 = 10
	APIKeyJoinGroup            int16 = 11
	APIKeyHeartbeat            int16 = 12
	APIKeyLeaveGroup           int16 = 13
	APIKeySyncGroup            int16 = 14
	APIKeyDescribeGroups       int16 = 15
	APIKeyListGroups           int16 = 16
	APIKeyApiVersion           int16 = 18
	APIKeyCreateTopics         int16 = 19
	APIKeyDeleteTopics         int16 = 20
	APIKeyOffsetForLeaderEpoch int16 = 23
	APIKeyListOffsets          int16 = 2
	APIKeyDescribeConfigs      int16 = 32
	APIKeyAlterConfigs         int16 = 33
	APIKeyCreatePartitions     int16 = 37
	APIKeyDeleteGroups         int16 = 42
)

// ApiVersion describes the supported version range for an API.
type ApiVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}
