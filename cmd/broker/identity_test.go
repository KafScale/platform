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
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/KafScale/platform/pkg/authn"
	"github.com/KafScale/platform/pkg/broker"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func brokerIdentityEnv(t *testing.T) (string, []byte) {
	t.Helper()
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_FAIL_OPEN", "false")
	path := filepath.Join(t.TempDir(), "key")
	key := bytes.Repeat([]byte{23}, 32)
	if err := os.WriteFile(path, key, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAFSCALE_PROXY_IDENTITY_KEY_FILE", path)
	return path, key
}
func TestLoadBrokerIdentityContext(t *testing.T) {
	t.Setenv("KAFSCALE_PROXY_IDENTITY_KEY_FILE", "")
	t.Setenv("KAFSCALE_PRINCIPAL_SOURCE", "")
	t.Setenv("KAFSCALE_PROXY_PROTOCOL", "false")
	t.Setenv("KAFSCALE_ACL_ENABLED", "false")
	if f, err := loadBrokerConnContext(testLogger()); err != nil || f != nil {
		t.Fatal(f, err)
	}
	path, key := brokerIdentityEnv(t)
	for _, tc := range []struct{ k, v string }{{"KAFSCALE_ACL_ENABLED", "false"}, {"KAFSCALE_ACL_FAIL_OPEN", "true"}, {"KAFSCALE_PROXY_IDENTITY_KEY_FILE", "/missing"}} {
		t.Run(tc.k, func(t *testing.T) {
			t.Setenv(tc.k, tc.v)
			if _, err := loadBrokerConnContext(testLogger()); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
	f, err := loadBrokerConnContext(testLogger())
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	done := make(chan error, 1)
	go func(server net.Conn) {
		defer server.Close()
		wrapped, info, err := f(server)
		if err == nil && (wrapped != server || info.Principal != "service-a") {
			t.Error("identity lost", info)
		}
		done <- err
		if err == nil {
			_, _ = io.Copy(io.Discard, server)
		}
	}(server)
	if err := authn.SendIdentity(client, key, "service-a"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	client.Close()
	server, client = net.Pipe()
	client.Close()
	if _, _, err := f(server); err == nil {
		t.Fatal("unauthenticated connection accepted")
	}
	server.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	server, client = net.Pipe()
	defer server.Close()
	defer client.Close()
	if _, _, err := f(server); err == nil {
		t.Fatal("removed key accepted")
	}
}
func TestAuthenticatedPrincipalOverridesClientID(t *testing.T) {
	_, key := brokerIdentityEnv(t)
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"admin","allow":[{"action":"*","resource":"*","name":"*"}]}]}`)
	h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
	f, err := loadBrokerConnContext(testLogger())
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	done := make(chan *broker.ConnContext, 1)
	go func() {
		defer server.Close()
		_, info, err := f(server)
		if err != nil {
			t.Error(err)
		}
		done <- info
		if err == nil {
			_, _ = io.Copy(io.Discard, server)
		}
	}()
	if err := authn.SendIdentity(client, key, "service-a"); err != nil {
		t.Fatal(err)
	}
	info := <-done
	client.Close()
	ctx := broker.ContextWithConnInfo(context.Background(), info)
	spoof := "admin"
	header := &protocol.RequestHeader{ClientID: &spoof, APIVersion: 0, CorrelationID: 4}
	if principalFromContext(ctx, header) != "service-a" {
		t.Fatal("client ID overrode verified identity")
	}
	topic := "orders"
	requests := []struct {
		req   kmsg.Request
		check func([]byte)
	}{
		{&kmsg.ListOffsetsRequest{Topics: []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0}}}}}, func(data []byte) {
			r := decodeKmsgResponse(t, 0, data, kmsg.NewPtrListOffsetsResponse)
			if r.Topics[0].Partitions[0].ErrorCode != protocol.TOPIC_AUTHORIZATION_FAILED {
				t.Fatal(r)
			}
		}},
		{&kmsg.JoinGroupRequest{Group: "private"}, func(data []byte) {
			r := decodeKmsgResponse(t, 0, data, kmsg.NewPtrJoinGroupResponse)
			if r.ErrorCode != protocol.GROUP_AUTHORIZATION_FAILED {
				t.Fatal(r)
			}
		}},
		{&kmsg.CreateTopicsRequest{Topics: []kmsg.CreateTopicsRequestTopic{{Topic: "forbidden", NumPartitions: 1}}}, func(data []byte) {
			r := decodeKmsgResponse(t, 0, data, kmsg.NewPtrCreateTopicsResponse)
			if r.Topics[0].ErrorCode != protocol.TOPIC_AUTHORIZATION_FAILED {
				t.Fatal(r)
			}
		}},
	}
	for _, tc := range requests {
		header.APIKey = tc.req.Key()
		data, err := h.Handle(ctx, header, tc.req)
		if err != nil {
			t.Fatal(err)
		}
		tc.check(data)
	}
}
func TestMetadataAuthorizationDoesNotMutateStore(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny","principals":[{"name":"reader","allow":[{"action":"fetch","resource":"topic","name":"read"}]},{"name":"writer","allow":[{"action":"produce","resource":"topic","name":"read"}]},{"name":"admin","allow":[{"action":"admin","resource":"cluster","name":"*"}]}]}`)
	h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
	a, b := "read", "private"
	topics := []kmsg.MetadataResponseTopic{{Topic: &a}, {Topic: &b, TopicID: [16]byte{9}, IsInternal: true, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0}}}, {ErrorCode: protocol.UNKNOWN_TOPIC_ID}}
	for _, principal := range []string{"reader", "writer", "admin"} {
		got := h.authorizedMetadata(principal, topics, false, false)
		if got[0].ErrorCode != 0 {
			t.Fatal(got)
		}
		if principal != "admin" && (got[1].ErrorCode != protocol.TOPIC_AUTHORIZATION_FAILED || len(got[1].Partitions) != 0 || got[1].TopicID != [16]byte{} || got[1].IsInternal) {
			t.Fatal(got)
		}
	}
	listed := h.authorizedMetadata("reader", topics, true, false)
	if len(listed) != 2 {
		t.Fatal(listed)
	}
	if topics[1].ErrorCode != 0 || len(topics[1].Partitions) != 1 {
		t.Fatal("mutated shared metadata")
	}
}
func TestMetadataCannotAutoCreateUnauthorizedTopic(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny"}`)
	store := metadata.NewInMemoryStore(defaultMetadata())
	h := newTestHandler(store)
	h.autoCreateTopics = true
	name := "forbidden"
	id := "admin"
	request := kmsg.NewPtrMetadataRequest()
	request.Version = 4
	request.AllowAutoTopicCreation = true
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}
	ctx := broker.ContextWithConnInfo(context.Background(), &broker.ConnContext{Principal: "service-a"})
	if _, err := h.Handle(ctx, &protocol.RequestHeader{APIKey: protocol.APIKeyMetadata, APIVersion: 4, ClientID: &id}, request); err != nil {
		t.Fatal(err)
	}
	all, err := store.Metadata(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range all.Topics {
		if topic.Topic != nil && *topic.Topic == name {
			t.Fatal("unauthorized metadata request created a topic")
		}
	}
}

func TestAuthenticatedMetadataLegacyListAll(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny"}`)
	h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
	id := "admin"
	ctx := broker.ContextWithConnInfo(context.Background(), &broker.ConnContext{Principal: "denied"})
	request := kmsg.NewPtrMetadataRequest()
	request.Version = 0
	request.Topics = []kmsg.MetadataRequestTopic{}
	data, err := h.Handle(ctx, &protocol.RequestHeader{APIKey: protocol.APIKeyMetadata, APIVersion: 0, ClientID: &id}, request)
	if err != nil {
		t.Fatal(err)
	}
	response := decodeKmsgResponse(t, 0, data, kmsg.NewPtrMetadataResponse)
	if len(response.Topics) != 0 {
		t.Fatal("legacy metadata exposed unauthorized topics", response.Topics)
	}
	h.traceKafka = true
	request.Version = 12
	request.Topics = []kmsg.MetadataRequestTopic{{TopicID: [16]byte{1}}}
	if _, err := h.Handle(ctx, &protocol.RequestHeader{APIKey: protocol.APIKeyMetadata, APIVersion: 12, ClientID: &id}, request); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataAutoCreateWithoutClientFlag(t *testing.T) {
	for _, version := range []int16{1, 4} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			t.Setenv("KAFSCALE_ACL_ENABLED", "false")
			store := metadata.NewInMemoryStore(defaultMetadata())
			h := newTestHandler(store)
			h.autoCreateTopics = true
			name := "legacy-auto-create"
			request := kmsg.NewPtrMetadataRequest()
			request.Version = version
			request.AllowAutoTopicCreation = false
			request.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}
			if _, err := h.Handle(context.Background(), &protocol.RequestHeader{APIKey: protocol.APIKeyMetadata, APIVersion: version}, request); err != nil {
				t.Fatal(err)
			}
			all, err := store.Metadata(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, topic := range all.Topics {
				if topic.Topic != nil && *topic.Topic == name {
					return
				}
			}
			t.Fatal("broker auto-create was disabled by absent client flag")
		})
	}
}

func TestDeniedMetadataByIDDoesNotRevealTopic(t *testing.T) {
	t.Setenv("KAFSCALE_ACL_ENABLED", "true")
	t.Setenv("KAFSCALE_ACL_JSON", `{"default_policy":"deny"}`)
	h := newTestHandler(metadata.NewInMemoryStore(defaultMetadata()))
	name := "private"
	id := [16]byte{9}
	topics := []kmsg.MetadataResponseTopic{{Topic: &name, TopicID: id, IsInternal: true}}
	denied := h.authorizedMetadata("denied", topics, false, true)
	if len(denied) != 1 || denied[0].ErrorCode != protocol.UNKNOWN_TOPIC_ID || denied[0].Topic != nil || denied[0].TopicID != id || denied[0].IsInternal {
		t.Fatal(denied)
	}
}
