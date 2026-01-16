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

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestOpsAPI(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	etcd, endpoints := startEmbeddedEtcd(t)
	defer etcd.Close()

	brokerAddr := freeAddr(t)
	metricsAddr := freeAddr(t)
	controlAddr := freeAddr(t)
	brokerCmd, brokerLogs := startBrokerWithEtcd(t, ctx, brokerAddr, metricsAddr, controlAddr, endpoints)
	t.Cleanup(func() { stopBroker(t, brokerCmd) })
	waitForBroker(t, brokerLogs, brokerAddr)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("create client: %v\nbroker logs:\n%s", err, brokerLogs.String())
	}
	defer client.Close()

	topic := fmt.Sprintf("ops-api-%d", time.Now().UnixNano())
	groupID := fmt.Sprintf("ops-group-%d", time.Now().UnixNano())

	if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("ops")}).FirstErr(); err != nil {
		t.Fatalf("produce failed: %v\nbroker logs:\n%s", err, brokerLogs.String())
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		t.Fatalf("create consumer: %v\nbroker logs:\n%s", err, brokerLogs.String())
	}
	defer consumer.CloseAllowingRebalance()

	consumeDeadline := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(consumeDeadline) {
			t.Fatalf("timed out waiting for consumer group join\nbroker logs:\n%s", brokerLogs.String())
		}
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %+v\nbroker logs:\n%s", errs, brokerLogs.String())
		}
		seen := false
		fetches.EachRecord(func(record *kgo.Record) {
			seen = true
		})
		if seen {
			break
		}
	}

	t.Run("ListGroups", func(t *testing.T) {
		req := kmsg.NewPtrListGroupsRequest()
		req.Version = 5
		req.StatesFilter = []string{"Stable"}
		req.TypesFilter = []string{"classic"}
		resp, err := req.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("list groups request failed: %v\nbroker logs:\n%s", err, brokerLogs.String())
		}
		found := false
		for _, group := range resp.Groups {
			if group.Group == groupID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected group %s in list response: %+v", groupID, resp.Groups)
		}
	})

	t.Run("DescribeGroups", func(t *testing.T) {
		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Version = 5
		req.Groups = []string{groupID}
		resp, err := req.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("describe groups request failed: %v\nbroker logs:\n%s", err, brokerLogs.String())
		}
		if len(resp.Groups) != 1 || resp.Groups[0].Group != groupID {
			t.Fatalf("unexpected describe groups response: %+v", resp.Groups)
		}
	})

	t.Run("OffsetForLeaderEpoch", func(t *testing.T) {
		req := kmsg.NewPtrOffsetForLeaderEpochRequest()
		req.Version = 3
		req.ReplicaID = -1
		req.Topics = []kmsg.OffsetForLeaderEpochRequestTopic{
			{
				Topic: topic,
				Partitions: []kmsg.OffsetForLeaderEpochRequestTopicPartition{
					{Partition: 0, CurrentLeaderEpoch: -1, LeaderEpoch: 0},
				},
			},
		}
		resp, err := req.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("offset for leader epoch request failed: %v\nbroker logs:\n%s", err, brokerLogs.String())
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("unexpected offset response: %+v", resp.Topics)
		}
		if resp.Topics[0].Partitions[0].EndOffset < 1 {
			t.Fatalf("unexpected end offset: %+v", resp.Topics[0].Partitions[0])
		}
	})

	t.Run("DescribeAndAlterConfigs", func(t *testing.T) {
		describe := kmsg.NewPtrDescribeConfigsRequest()
		describe.Version = 4
		describe.Resources = []kmsg.DescribeConfigsRequestResource{
			{
				ResourceType: kmsg.ConfigResourceTypeTopic,
				ResourceName: topic,
				ConfigNames:  []string{"retention.ms"},
			},
		}
		resp, err := describe.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("describe configs failed: %v\nbroker logs:\n%s", err, brokerLogs.String())
		}
		if len(resp.Resources) != 1 || resp.Resources[0].ResourceName != topic {
			t.Fatalf("unexpected describe configs response: %+v", resp.Resources)
		}

		alter := kmsg.NewPtrAlterConfigsRequest()
		alter.Version = 1
		value := "120000"
		alter.Resources = []kmsg.AlterConfigsRequestResource{
			{
				ResourceType: kmsg.ConfigResourceTypeTopic,
				ResourceName: topic,
				Configs: []kmsg.AlterConfigsRequestResourceConfig{
					{Name: "retention.ms", Value: &value},
				},
			},
		}
		alterResp, err := alter.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("alter configs failed: %v\nbroker logs:\n%s", err, brokerLogs.String())
		}
		if len(alterResp.Resources) != 1 || alterResp.Resources[0].ErrorCode != 0 {
			t.Fatalf("unexpected alter configs response: %+v", alterResp.Resources)
		}

		verify := kmsg.NewPtrDescribeConfigsRequest()
		verify.Version = 4
		verify.Resources = []kmsg.DescribeConfigsRequestResource{
			{
				ResourceType: kmsg.ConfigResourceTypeTopic,
				ResourceName: topic,
				ConfigNames:  []string{"retention.ms"},
			},
		}
		verifyResp, err := verify.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("describe configs verify failed: %v\nbroker logs:\n%s", err, brokerLogs.String())
		}
		if len(verifyResp.Resources) != 1 || len(verifyResp.Resources[0].Configs) != 1 {
			t.Fatalf("unexpected verify response: %+v", verifyResp.Resources)
		}
		got := ""
		if verifyResp.Resources[0].Configs[0].Value != nil {
			got = *verifyResp.Resources[0].Configs[0].Value
		}
		if got != value {
			t.Fatalf("expected retention.ms=%s got %q", value, got)
		}
	})

	t.Run("CreatePartitions", func(t *testing.T) {
		create := kmsg.NewPtrCreatePartitionsRequest()
		create.Version = 3
		create.Topics = []kmsg.CreatePartitionsRequestTopic{
			{Topic: topic, Count: 2},
		}
		resp, err := create.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("create partitions failed: %v\nbroker logs:\n%s", err, brokerLogs.String())
		}
		if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
			t.Fatalf("unexpected create partitions response: %+v", resp.Topics)
		}

		meta := kmsg.NewPtrMetadataRequest()
		meta.Version = 12
		topicName := topic
		meta.Topics = []kmsg.MetadataRequestTopic{{Topic: &topicName}}
		metaResp, err := meta.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("metadata request failed: %v\nbroker logs:\n%s", err, brokerLogs.String())
		}
		if len(metaResp.Topics) != 1 || len(metaResp.Topics[0].Partitions) < 2 {
			t.Fatalf("expected >=2 partitions, got: %+v", metaResp.Topics)
		}
	})

	t.Run("DeleteGroups", func(t *testing.T) {
		deleteReq := kmsg.NewPtrDeleteGroupsRequest()
		deleteReq.Version = 2
		deleteReq.Groups = []string{groupID}
		resp, err := deleteReq.RequestWith(ctx, client)
		if err != nil {
			t.Fatalf("delete groups failed: %v\nbroker logs:\n%s", err, brokerLogs.String())
		}
		if len(resp.Groups) != 1 || resp.Groups[0].ErrorCode != 0 {
			t.Fatalf("unexpected delete groups response: %+v", resp.Groups)
		}
	})
}
