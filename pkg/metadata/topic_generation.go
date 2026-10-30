// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	TopicGenerationPrefix = "/kafscale/topic-generations/"
	DeletedTopicPrefix    = "/kafscale/deleted-topics/"
)

var ErrTopicGenerationChanged = errors.New("topic generation changed")

type DeletedTopic struct {
	Name       string `json:"name"`
	TopicID    string `json:"topic_id"`
	Generation int64  `json:"generation"`
	Prefix     string `json:"prefix"`
	DeletedAt  string `json:"deleted_at"`
}

func TopicGenerationKey(topic string) string {
	return TopicGenerationPrefix + topic
}

func DeletedTopicKey(topic string, generation int64) string {
	return fmt.Sprintf("%s%s/%d", DeletedTopicPrefix, topic, generation)
}

func TopicStorageName(topic string, generation int64) string {
	if generation == 0 {
		return topic
	}
	return fmt.Sprintf("%s@%d", topic, generation)
}

func (s *EtcdStore) TopicGeneration(ctx context.Context, topic string) (int64, error) {
	resp, err := s.client.Get(ctx, TopicGenerationKey(topic))
	s.recordEtcdResult(err)
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) == 0 {
		return 0, nil
	}
	return strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
}

const maxTxnOps = 128

func (s *EtcdStore) RetireTopic(ctx context.Context, name, namespace string) error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	state, err := s.metadata.Metadata(ctx, nil)
	if err != nil {
		return err
	}
	remaining := *state
	remaining.Topics = nil
	var topicID [16]byte
	found := false
	for _, topic := range state.Topics {
		if *topic.Topic == name {
			topicID = topic.TopicID
			found = true
			continue
		}
		remaining.Topics = append(remaining.Topics, topic)
	}
	if !found {
		return ErrUnknownTopic
	}
	snapshot, err := json.Marshal(remaining)
	if err != nil {
		return err
	}
	generation, err := s.TopicGeneration(ctx, name)
	if err != nil {
		return err
	}
	record, err := json.Marshal(DeletedTopic{
		Name:       name,
		TopicID:    fmt.Sprintf("%x", topicID),
		Generation: generation,
		Prefix:     path.Join(namespace, TopicStorageName(name, generation)) + "/",
		DeletedAt:  time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return err
	}
	offsetKeys, err := s.consumerOffsetKeys(ctx, name)
	if err != nil {
		return err
	}
	genKey := TopicGenerationKey(name)
	next := strconv.FormatInt(generation+1, 10)
	ops := []clientv3.Op{
		clientv3.OpPut(snapshotKey(), string(snapshot)),
		clientv3.OpDelete(fmt.Sprintf("/kafscale/topics/%s/", name), clientv3.WithPrefix()),
		clientv3.OpPut(genKey, next),
		clientv3.OpPut(DeletedTopicKey(name, generation), string(record)),
	}
	first := min(len(offsetKeys), maxTxnOps-len(ops))
	for _, key := range offsetKeys[:first] {
		ops = append(ops, clientv3.OpDelete(key))
	}
	if err := s.commitGuarded(ctx, generationIs(genKey, generation), ops); err != nil {
		return err
	}
	if err := s.metadata.DeleteTopic(ctx, name); err != nil && !errors.Is(err, ErrUnknownTopic) {
		return err
	}
	rest := offsetKeys[first:]
	for len(rest) > 0 {
		batch := rest[:min(len(rest), maxTxnOps)]
		rest = rest[len(batch):]
		ops := make([]clientv3.Op, 0, len(batch))
		for _, key := range batch {
			ops = append(ops, clientv3.OpDelete(key))
		}
		if err := s.commitGuarded(ctx, clientv3.Compare(clientv3.Value(genKey), "=", next), ops); err != nil {
			return err
		}
	}
	return nil
}

func generationIs(key string, generation int64) clientv3.Cmp {
	if generation == 0 {
		return clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	}
	return clientv3.Compare(clientv3.Value(key), "=", strconv.FormatInt(generation, 10))
}

func (s *EtcdStore) commitGuarded(ctx context.Context, guard clientv3.Cmp, ops []clientv3.Op) error {
	txnCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := s.client.Txn(txnCtx).If(guard).Then(ops...).Commit()
	s.recordEtcdResult(err)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrTopicGenerationChanged
	}
	return nil
}

func (s *EtcdStore) consumerOffsetKeys(ctx context.Context, topic string) ([]string, error) {
	getCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := s.client.Get(getCtx, "/kafscale/consumers/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	s.recordEtcdResult(err)
	if err != nil {
		return nil, err
	}
	marker := fmt.Sprintf("/offsets/%s/", topic)
	var keys []string
	for _, kv := range resp.Kvs {
		if strings.Contains(string(kv.Key), marker) {
			keys = append(keys, string(kv.Key))
		}
	}
	return keys, nil
}
