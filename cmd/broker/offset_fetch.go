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

package main

import (
	"github.com/KafScale/platform/pkg/acl"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func (h *handler) authorizedOffsetFetch(principal string, topics []kmsg.OffsetFetchResponseTopic, listAll bool) []kmsg.OffsetFetchResponseTopic {
	result := make([]kmsg.OffsetFetchResponseTopic, 0, len(topics))
	for _, topic := range topics {
		if !h.allowTopic(principal, topic.Topic, acl.ActionFetch) {
			h.recordAuthzDeniedWithPrincipal(principal, acl.ActionFetch, acl.ResourceTopic, topic.Topic)
			if listAll {
				continue
			}
			// Do not expose committed offsets or metadata for a denied topic.
			topic.Partitions = append([]kmsg.OffsetFetchResponseTopicPartition(nil), topic.Partitions...)
			for i := range topic.Partitions {
				part := &topic.Partitions[i]
				part.ErrorCode = protocol.TOPIC_AUTHORIZATION_FAILED
				part.Offset, part.LeaderEpoch, part.Metadata = -1, -1, nil
			}
		}
		result = append(result, topic)
	}
	return result
}
