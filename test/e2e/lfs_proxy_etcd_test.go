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

//go:build e2e

package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
)

func startLfsProxyEtcd(t *testing.T, brokerHost string, brokerPort int32, topics ...string) []string {
	t.Helper()
	etcd, endpoints := startEmbeddedEtcd(t)
	t.Cleanup(func() {
		etcd.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{{
			NodeID: 0,
			Host:   brokerHost,
			Port:   brokerPort,
		}},
	}, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("create etcd store: %v", err)
	}

	for _, topic := range topics {
		if topic == "" {
			continue
		}
		if _, err := store.CreateTopic(ctx, metadata.TopicSpec{
			Name:              topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}); err != nil && !errors.Is(err, metadata.ErrTopicExists) {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}

	return endpoints
}
