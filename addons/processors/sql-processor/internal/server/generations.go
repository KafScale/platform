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

package server

import (
	"context"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/discovery"
)

const topicGenerationPrefix = "/kafscale/topic-generations/"

type generationSource interface {
	Generations(ctx context.Context) (map[string]int64, error)
}

type etcdGenerations struct {
	client *clientv3.Client
}

func (e *etcdGenerations) Generations(ctx context.Context) (map[string]int64, error) {
	getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := e.client.Get(getCtx, topicGenerationPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		generation, err := strconv.ParseInt(string(kv.Value), 10, 64)
		if err != nil {
			return nil, err
		}
		out[strings.TrimPrefix(string(kv.Key), topicGenerationPrefix)] = generation
	}
	return out, nil
}

func (s *Server) getGenerations() (generationSource, error) {
	s.generationsMu.Lock()
	defer s.generationsMu.Unlock()
	if s.generationsInit {
		return s.generations, s.generationsErr
	}
	s.generationsInit = true
	if len(s.cfg.Metadata.Etcd.Endpoints) == 0 {
		return nil, nil
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: s.cfg.Metadata.Etcd.Endpoints, DialTimeout: 5 * time.Second})
	if err != nil {
		s.generationsErr = err
		return nil, err
	}
	s.generations = &etcdGenerations{client: client}
	return s.generations, nil
}

func (s *Server) listCurrentSegments(ctx context.Context, lister discovery.Lister) ([]discovery.SegmentRef, error) {
	segments, err := lister.ListCompleted(ctx)
	if err != nil {
		return nil, err
	}
	source, err := s.getGenerations()
	if err != nil {
		return nil, err
	}
	var current map[string]int64
	if source != nil {
		current, err = source.Generations(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		current = latestGenerations(segments)
	}
	out := segments[:0:0]
	for _, segment := range segments {
		if storageGeneration(segment) == current[segment.Topic] {
			out = append(out, segment)
		}
	}
	return out, nil
}

func latestGenerations(segments []discovery.SegmentRef) map[string]int64 {
	out := make(map[string]int64)
	for _, segment := range segments {
		if generation := storageGeneration(segment); generation > out[segment.Topic] {
			out[segment.Topic] = generation
		}
	}
	return out
}

func storageGeneration(segment discovery.SegmentRef) int64 {
	suffix := strings.TrimPrefix(segment.CheckpointTopic(), segment.Topic)
	if !strings.HasPrefix(suffix, "@") {
		return 0
	}
	generation, err := strconv.ParseInt(suffix[1:], 10, 64)
	if err != nil {
		return 0
	}
	return generation
}
