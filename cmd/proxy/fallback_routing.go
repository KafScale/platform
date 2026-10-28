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

import "github.com/twmb/franz-go/pkg/kmsg"

// Unknown owners may use any broker. Send them with an existing routed group
// instead of excluding the healthy broker or making a second long-poll request.
func fallbackTarget[T any](groups map[string]T) string {
	target := ""
	for addr := range groups {
		if addr != "" && (target == "" || addr < target) {
			target = addr
		}
	}
	return target
}

func coalesceProduceFallback(groups map[string]*kmsg.ProduceRequest) {
	fallback := groups[""]
	target := fallbackTarget(groups)
	if fallback == nil || target == "" {
		return
	}
	merged := *groups[target]
	merged.Topics = append([]kmsg.ProduceRequestTopic(nil), merged.Topics...)
	for i := range merged.Topics {
		merged.Topics[i].Partitions = append([]kmsg.ProduceRequestTopicPartition(nil), merged.Topics[i].Partitions...)
	}
	for _, topic := range fallback.Topics {
		index := -1
		for i := range merged.Topics {
			if merged.Topics[i].Topic == topic.Topic {
				index = i
				break
			}
		}
		if index < 0 {
			copy := topic
			copy.Partitions = append([]kmsg.ProduceRequestTopicPartition(nil), topic.Partitions...)
			merged.Topics = append(merged.Topics, copy)
		} else {
			merged.Topics[index].Partitions = append(merged.Topics[index].Partitions, topic.Partitions...)
		}
	}
	groups[target] = &merged
	delete(groups, "")
}

func coalesceFetchFallback(groups map[string]*kmsg.FetchRequest) {
	fallback := groups[""]
	target := fallbackTarget(groups)
	if fallback == nil || target == "" {
		return
	}
	merged := *groups[target]
	merged.Topics = append([]kmsg.FetchRequestTopic(nil), merged.Topics...)
	for i := range merged.Topics {
		merged.Topics[i].Partitions = append([]kmsg.FetchRequestTopicPartition(nil), merged.Topics[i].Partitions...)
	}
	for _, topic := range fallback.Topics {
		index := -1
		for i := range merged.Topics {
			if fetchTopicKey(merged.Topics[i].Topic, merged.Topics[i].TopicID) == fetchTopicKey(topic.Topic, topic.TopicID) {
				index = i
				break
			}
		}
		if index < 0 {
			copy := topic
			copy.Partitions = append([]kmsg.FetchRequestTopicPartition(nil), topic.Partitions...)
			merged.Topics = append(merged.Topics, copy)
		} else {
			merged.Topics[index].Partitions = append(merged.Topics[index].Partitions, topic.Partitions...)
		}
	}
	groups[target] = &merged
	delete(groups, "")
}
