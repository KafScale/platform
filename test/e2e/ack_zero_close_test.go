// Copyright 2026 KafScale team.
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

//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func testAckZeroClose(t *testing.T, addr, topic string, run func(*testing.T, string, string, ...string) string) {
	t.Helper()
	topic += "-ack-zero"
	create := kmsg.NewPtrCreateTopicsRequest()
	create.Version, create.TimeoutMillis = 2, 5000
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	if response := directGroupRequest(t, addr, create).(*kmsg.CreateTopicsResponse); len(response.Topics) != 1 || response.Topics[0].ErrorCode != 0 {
		t.Fatalf("create acks=0 topic: %+v", response)
	}
	const producers, perProducer = 20, 50
	for producer := range producers {
		var input strings.Builder
		for message := range perProducer {
			fmt.Fprintf(&input, "%04d\n", producer*perProducer+message)
		}
		// Every invocation sends acks=0 and exits immediately after its writes.
		run(t, addr, input.String(), "-P", "-q", "-t", topic, "-p", "0", "-X", "acks=0", "-X", "enable.idempotence=false", "-X", "message.timeout.ms=5000")
	}
	offsets := kmsg.NewPtrListOffsetsRequest()
	offsets.Version, offsets.ReplicaID = 1, -1
	offsets.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0, Timestamp: -1}}}}
	for deadline := time.Now().Add(10 * time.Second); ; {
		response := directGroupRequest(t, addr, offsets).(*kmsg.ListOffsetsResponse)
		if len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 1 {
			t.Fatalf("acks=0 offsets: %+v", response)
		}
		part := response.Topics[0].Partitions[0]
		if part.ErrorCode == 0 && part.Offset == producers*perProducer {
			break
		}
		if part.Offset > producers*perProducer || time.Now().After(deadline) {
			t.Fatalf("acks=0 messages lost or duplicated after close: %+v, want %d", part, producers*perProducer)
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := run(t, addr, "", "-C", "-q", "-t", topic, "-p", "0", "-o", "beginning", "-c", "1000", "-e")
	seen := make(map[string]int)
	for _, message := range strings.Fields(got) {
		seen[message]++
	}
	if len(seen) != producers*perProducer {
		t.Fatalf("acks=0 unique records: got %d want %d", len(seen), producers*perProducer)
	}
	for message := range producers * perProducer {
		if seen[fmt.Sprintf("%04d", message)] != 1 {
			t.Fatalf("acks=0 record %04d missing or duplicated", message)
		}
	}
}
