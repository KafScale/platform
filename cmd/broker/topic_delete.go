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
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KafScale/platform/pkg/metadata"
)

func (h *handler) topicStorageName(ctx context.Context, topic string) (string, error) {
	etcdStore, ok := h.store.(*metadata.EtcdStore)
	if !ok {
		return topic, nil
	}
	generation, err := etcdStore.TopicGeneration(ctx, topic)
	if err != nil {
		return "", err
	}
	return metadata.TopicStorageName(topic, generation), nil
}

func (h *handler) deleteTopic(ctx context.Context, name string) error {
	defer h.dropTopicLogs(name)
	if etcdStore, ok := h.store.(*metadata.EtcdStore); ok {
		return etcdStore.RetireTopic(ctx, name, h.s3Namespace)
	}
	return h.store.DeleteTopic(ctx, name)
}

func (h *handler) dropTopicLogs(topic string) {
	h.logMu.Lock()
	delete(h.logs, topic)
	h.logMu.Unlock()
}

func (h *handler) startTopicDeleteWatch(ctx context.Context) {
	etcdStore, ok := h.store.(*metadata.EtcdStore)
	if !ok {
		return
	}
	go h.watchTopicDeletes(ctx, etcdStore.EtcdClient())
}

func (h *handler) watchTopicDeletes(ctx context.Context, cli *clientv3.Client) {
	for {
		for resp := range cli.Watch(ctx, metadata.TopicGenerationPrefix, clientv3.WithPrefix()) {
			for _, ev := range resp.Events {
				h.dropTopicLogs(strings.TrimPrefix(string(ev.Kv.Key), metadata.TopicGenerationPrefix))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
