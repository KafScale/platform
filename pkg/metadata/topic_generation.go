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
	"strconv"

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

func (s *EtcdStore) RetireTopic(ctx context.Context, record DeletedTopic) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	key := TopicGenerationKey(record.Name)
	current := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	if record.Generation > 0 {
		current = clientv3.Compare(clientv3.Value(key), "=", strconv.FormatInt(record.Generation, 10))
	}
	resp, err := s.client.Txn(ctx).If(current).Then(
		clientv3.OpPut(key, strconv.FormatInt(record.Generation+1, 10)),
		clientv3.OpPut(DeletedTopicKey(record.Name, record.Generation), string(payload)),
	).Commit()
	s.recordEtcdResult(err)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrTopicGenerationChanged
	}
	return nil
}

func (s *EtcdStore) RestoreTopicGeneration(ctx context.Context, topic string, generation int64) error {
	key := TopicGenerationKey(topic)
	restore := clientv3.OpPut(key, strconv.FormatInt(generation, 10))
	if generation == 0 {
		restore = clientv3.OpDelete(key)
	}
	resp, err := s.client.Txn(ctx).If(
		clientv3.Compare(clientv3.Value(key), "=", strconv.FormatInt(generation+1, 10)),
	).Then(restore, clientv3.OpDelete(DeletedTopicKey(topic, generation))).Commit()
	s.recordEtcdResult(err)
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return ErrTopicGenerationChanged
	}
	return nil
}
