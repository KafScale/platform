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

package main

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// kmsg writes a tagged field whenever it differs from its default, so structs with
// a non-empty Default() must come from the generated constructors. The tests encode
// and decode at the same version: a decoded value that differs from the default
// proves the tag was on the wire.

func TestProduceErrorResponseHasNoTaggedFieldsAtV9(t *testing.T) {
	const version int16 = 9
	req := kmsg.NewPtrProduceRequest()
	req.SetVersion(version)
	topic := kmsg.NewProduceRequestTopic()
	topic.Topic = "orders"
	for _, id := range []int32{0, 1, 2} {
		part := kmsg.NewProduceRequestTopicPartition()
		part.Partition = id
		topic.Partitions = append(topic.Partitions, part)
	}
	req.Topics = append(req.Topics, topic)

	resp := kmsg.NewPtrProduceResponse()
	resp.SetVersion(version)
	addErrorForAllPartitions(resp, req, protocol.REQUEST_TIMED_OUT)

	body, ok := protocol.SkipResponseHeader(protocol.APIKeyProduce, version, protocol.EncodeResponse(7, version, resp))
	if !ok {
		t.Fatal("could not skip the response header")
	}
	decoded := kmsg.NewPtrProduceResponse()
	decoded.SetVersion(version)
	if err := decoded.ReadFrom(body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	want := kmsg.NewProduceResponseTopicPartition().CurrentLeader
	parts := 0
	for _, tp := range decoded.Topics {
		for _, p := range tp.Partitions {
			parts++
			if !reflect.DeepEqual(p.CurrentLeader, want) {
				t.Errorf("partition %d: CurrentLeader = %+v, want %+v", p.Partition, p.CurrentLeader, want)
			}
		}
	}
	if parts != 3 {
		t.Fatalf("decoded %d partitions, want 3", parts)
	}
}

func TestFetchErrorPartitionsKeepDefaults(t *testing.T) {
	req := kmsg.NewPtrFetchRequest()
	req.SetVersion(12)
	topic := kmsg.NewFetchRequestTopic()
	topic.Topic = "orders"
	part := kmsg.NewFetchRequestTopicPartition()
	part.Partition = 3
	topic.Partitions = append(topic.Partitions, part)
	req.Topics = append(req.Topics, topic)

	resp := kmsg.NewPtrFetchResponse()
	resp.SetVersion(12)
	addFetchErrorForAllPartitions(resp, req, protocol.REQUEST_TIMED_OUT)

	want := kmsg.NewFetchResponseTopicPartition()
	want.Partition = 3
	want.ErrorCode = protocol.REQUEST_TIMED_OUT
	if got := resp.Topics[0].Partitions[0]; !reflect.DeepEqual(got, want) {
		t.Errorf("error partition = %+v, want %+v", got, want)
	}
}

func fetchRequestForSplit(version int16) *kmsg.FetchRequest {
	req := kmsg.NewPtrFetchRequest()
	req.SetVersion(version)
	req.MaxWaitMillis = 500
	req.MinBytes = 1
	req.MaxBytes = 1 << 20
	req.IsolationLevel = 1
	topic := kmsg.NewFetchRequestTopic()
	topic.Topic = "orders"
	for _, id := range []int32{0, 1} {
		part := kmsg.NewFetchRequestTopicPartition()
		part.Partition = id
		part.FetchOffset = 42
		topic.Partitions = append(topic.Partitions, part)
	}
	req.Topics = append(req.Topics, topic)
	return req
}

func TestFetchSubRequestHasNoTaggedFields(t *testing.T) {
	want := kmsg.NewFetchRequest().ReplicaState
	for _, version := range []int16{12, 13} {
		req := fetchRequestForSplit(version)
		groups := (&proxy{}).groupFetchPartitionsByBroker(context.Background(), req, nil)
		if len(groups) != 1 {
			t.Fatalf("v%d: got %d groups, want 1", version, len(groups))
		}
		for _, sub := range groups {
			decoded := kmsg.NewPtrFetchRequest()
			decoded.SetVersion(version)
			if err := decoded.ReadFrom(sub.AppendTo(nil)); err != nil {
				t.Fatalf("v%d: decode: %v", version, err)
			}
			if !reflect.DeepEqual(decoded.ReplicaState, want) {
				t.Errorf("v%d: ReplicaState = %+v, want %+v", version, decoded.ReplicaState, want)
			}
		}
	}
}

func TestSubRequestsKeepClientFields(t *testing.T) {
	fetch := fetchRequestForSplit(13)
	for _, sub := range (&proxy{}).groupFetchPartitionsByBroker(context.Background(), fetch, nil) {
		if sub.Version != fetch.Version || sub.ReplicaID != fetch.ReplicaID ||
			sub.MaxWaitMillis != fetch.MaxWaitMillis || sub.MinBytes != fetch.MinBytes ||
			sub.MaxBytes != fetch.MaxBytes || sub.IsolationLevel != fetch.IsolationLevel ||
			sub.SessionID != fetch.SessionID || sub.SessionEpoch != fetch.SessionEpoch {
			t.Errorf("fetch sub-request differs from the client request: %+v", sub)
		}
		if !reflect.DeepEqual(sub.Topics[0].Partitions, fetch.Topics[0].Partitions) {
			t.Errorf("fetch partitions changed: %+v", sub.Topics[0].Partitions)
		}
	}

	txn := "txn-1"
	produce := kmsg.NewPtrProduceRequest()
	produce.SetVersion(9)
	produce.Acks = -1
	produce.TimeoutMillis = 4321
	produce.TransactionID = &txn
	topic := kmsg.NewProduceRequestTopic()
	topic.Topic = "orders"
	part := kmsg.NewProduceRequestTopicPartition()
	part.Partition = 3
	topic.Partitions = append(topic.Partitions, part)
	produce.Topics = append(produce.Topics, topic)
	for _, sub := range (&proxy{}).groupPartitionsByBroker(context.Background(), produce, nil) {
		if sub.Version != 9 || sub.Acks != -1 || sub.TimeoutMillis != 4321 || sub.TransactionID != &txn {
			t.Errorf("produce sub-request differs from the client request: %+v", sub)
		}
		if len(sub.Topics) != 1 || len(sub.Topics[0].Partitions) != 1 || sub.Topics[0].Partitions[0].Partition != 3 {
			t.Errorf("produce partitions changed: %+v", sub.Topics)
		}
	}
}

// TestNoKmsgLiteralsWithDefaults scans non-test code for kmsg request and response
// literals. Only structs with an empty Default() may be built that way, and the
// list of those is checked by reflection.
func TestNoKmsgLiteralsWithDefaults(t *testing.T) {
	emptyDefault := map[string]interface{ Default() }{
		"ProduceResponse":              &kmsg.ProduceResponse{},
		"ProduceResponseTopic":         &kmsg.ProduceResponseTopic{},
		"FetchResponse":                &kmsg.FetchResponse{},
		"FetchResponseTopic":           &kmsg.FetchResponseTopic{},
		"ApiVersionsResponseApiKey":    &kmsg.ApiVersionsResponseApiKey{},
		"JoinGroupResponseMember":      &kmsg.JoinGroupResponseMember{},
		"ProduceRequestTopic":          &kmsg.ProduceRequestTopic{},
		"ProduceRequestTopicPartition": &kmsg.ProduceRequestTopicPartition{},
		"FetchRequestTopic":            &kmsg.FetchRequestTopic{},
	}
	for name, zero := range emptyDefault {
		typ := reflect.TypeOf(zero).Elem()
		defaulted := reflect.New(typ)
		defaulted.Interface().(interface{ Default() }).Default()
		if !reflect.DeepEqual(reflect.Zero(typ).Interface(), defaulted.Elem().Interface()) {
			t.Errorf("kmsg.%s has a non-empty Default(); remove it from the list", name)
		}
	}

	const name = `([A-Z][A-Za-z]*(?:Request|Response)[A-Za-z]*)`
	plain := regexp.MustCompile(`(?:^|[^\]])kmsg\.` + name + `\{`)
	elided := regexp.MustCompile(`\[\]kmsg\.` + name + `\{\s*\{`) // []kmsg.X{{...}}

	err := filepath.Walk(filepath.Join("..", ".."), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(src), "\n") {
			matches := append(plain.FindAllStringSubmatch(line, -1), elided.FindAllStringSubmatch(line, -1)...)
			for _, m := range matches {
				if _, ok := emptyDefault[m[1]]; !ok {
					t.Errorf("%s:%d: kmsg.%s built as a literal, use kmsg.New%s()", path, i+1, m[1], m[1])
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
