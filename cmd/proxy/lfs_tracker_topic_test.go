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

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

type stubTrackerAdmin struct {
	existing int
	setTo    int
}

func (s *stubTrackerAdmin) CreateTopics(_ context.Context, _ int32, _ int16, _ map[string]*string, topics ...string) (kadm.CreateTopicResponses, error) {
	return kadm.CreateTopicResponses{topics[0]: {Topic: topics[0], Err: kerr.TopicAlreadyExists}}, nil
}

func (s *stubTrackerAdmin) ListTopics(_ context.Context, topics ...string) (kadm.TopicDetails, error) {
	parts := kadm.PartitionDetails{}
	for i := 0; i < s.existing; i++ {
		parts[int32(i)] = kadm.PartitionDetail{Topic: topics[0], Partition: int32(i)}
	}
	return kadm.TopicDetails{topics[0]: {Topic: topics[0], Partitions: parts}}, nil
}

func (s *stubTrackerAdmin) UpdatePartitions(_ context.Context, set int, topics ...string) (kadm.CreatePartitionsResponses, error) {
	s.setTo = set
	return kadm.CreatePartitionsResponses{topics[0]: {Topic: topics[0]}}, nil
}

func TestEnsureTrackerTopicGrowsAutoCreatedTopic(t *testing.T) {
	var buf bytes.Buffer
	admin := &stubTrackerAdmin{existing: 1}
	cfg := TrackerConfig{Topic: "__lfs_ops_state", Partitions: 3, ReplicationFactor: 1}
	if err := ensureTrackerTopicWith(context.Background(), admin, cfg, slog.New(slog.NewTextHandler(&buf, nil))); err != nil {
		t.Fatal(err)
	}
	if admin.setTo != 3 {
		t.Fatalf("expected partitions raised to 3, got %d", admin.setTo)
	}
	if !strings.Contains(buf.String(), "partitions=3") {
		t.Fatal(buf.String())
	}
}

func TestEnsureTrackerTopicLogsActualPartitions(t *testing.T) {
	var buf bytes.Buffer
	admin := &stubTrackerAdmin{existing: 5}
	cfg := TrackerConfig{Topic: "__lfs_ops_state", Partitions: 3, ReplicationFactor: 1}
	if err := ensureTrackerTopicWith(context.Background(), admin, cfg, slog.New(slog.NewTextHandler(&buf, nil))); err != nil {
		t.Fatal(err)
	}
	if admin.setTo != 0 || !strings.Contains(buf.String(), "partitions=5") {
		t.Fatal(admin.setTo, buf.String())
	}
}
