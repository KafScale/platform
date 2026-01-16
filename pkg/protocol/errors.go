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

const (
	NONE                         int16 = 0
	OFFSET_OUT_OF_RANGE          int16 = 1
	UNKNOWN_TOPIC_OR_PARTITION   int16 = 3
	UNKNOWN_TOPIC_ID             int16 = 100
	UNKNOWN_SERVER_ERROR         int16 = -1
	REQUEST_TIMED_OUT            int16 = 7
	COORDINATOR_LOAD_IN_PROGRESS int16 = 14
	COORDINATOR_NOT_AVAILABLE    int16 = 15
	INVALID_REQUEST              int16 = 42
	ILLEGAL_GENERATION           int16 = 22
	UNKNOWN_MEMBER_ID            int16 = 25
	REBALANCE_IN_PROGRESS        int16 = 27
	GROUP_ID_NOT_FOUND           int16 = 69
	INVALID_TOPIC_EXCEPTION      int16 = 17
	INVALID_CONFIG               int16 = 40
	TOPIC_ALREADY_EXISTS         int16 = 36
	TOPIC_AUTHORIZATION_FAILED   int16 = 29
	INVALID_PARTITIONS           int16 = 37
	UNSUPPORTED_VERSION          int16 = 35
)
