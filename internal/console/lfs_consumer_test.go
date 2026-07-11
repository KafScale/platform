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

package console

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNewLFSConsumerNoBrokers(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	consumer, err := NewLFSConsumer(context.Background(), LFSConsumerConfig{}, handlers, nil)
	if err != nil {
		t.Fatalf("NewLFSConsumer: %v", err)
	}
	if consumer != nil {
		t.Fatal("expected nil consumer when no brokers configured")
	}
}

func TestLFSConsumerStatusInitial(t *testing.T) {
	c := &LFSConsumer{
		statusMu: sync.RWMutex{},
	}
	status := c.Status()
	if status.Connected {
		t.Fatal("expected not connected initially")
	}
	if status.LastError != "" {
		t.Fatalf("expected empty error: %q", status.LastError)
	}
	if status.LastPollAt != "" {
		t.Fatalf("expected empty poll time: %q", status.LastPollAt)
	}
}

func TestLFSConsumerSetError(t *testing.T) {
	c := &LFSConsumer{
		statusMu: sync.RWMutex{},
	}
	c.setError(errors.New("kafka unreachable"))
	status := c.Status()
	if status.LastError != "kafka unreachable" {
		t.Fatalf("error: %q", status.LastError)
	}
	if status.LastErrorAt == "" {
		t.Fatal("expected error time set")
	}
}

func TestLFSConsumerSetErrorNil(t *testing.T) {
	c := &LFSConsumer{
		statusMu: sync.RWMutex{},
	}
	c.setError(nil) // should not panic or set anything
	status := c.Status()
	if status.LastError != "" {
		t.Fatalf("expected empty error: %q", status.LastError)
	}
}

func TestLFSConsumerSetPollSuccess(t *testing.T) {
	c := &LFSConsumer{
		statusMu: sync.RWMutex{},
	}
	// Set an error first
	c.setError(errors.New("temp error"))
	time.Sleep(time.Millisecond)
	// Poll success should clear the error
	c.setPollSuccess()
	status := c.Status()
	if !status.Connected {
		t.Fatal("expected connected after poll success")
	}
	if status.LastError != "" {
		t.Fatalf("expected error cleared: %q", status.LastError)
	}
	if status.LastPollAt == "" {
		t.Fatal("expected poll time set")
	}
}

func TestLFSConsumerProcessRecord(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	c := &LFSConsumer{
		handlers: handlers,
		logger:   nil,
	}
	// Use log.Default() for the consumer logger
	c.logger = handlers.logger

	event := LFSEvent{
		EventType: "upload_completed",
		Topic:     "test-topic",
		S3Key:     "key-1",
		Size:      512,
		Timestamp: "2026-01-01T00:00:00Z",
	}
	data, _ := json.Marshal(event)
	record := &kgo.Record{Value: data}
	c.processRecord(record)

	handlers.mu.RLock()
	defer handlers.mu.RUnlock()
	if handlers.stats.TotalObjects != 1 {
		t.Fatalf("expected 1 object, got %d", handlers.stats.TotalObjects)
	}
}

func TestLFSConsumerProcessRecordNil(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	c := &LFSConsumer{
		handlers: handlers,
		logger:   handlers.logger,
	}
	// nil record should not panic
	c.processRecord(nil)
	// empty record should not panic
	c.processRecord(&kgo.Record{Value: nil})
	c.processRecord(&kgo.Record{Value: []byte{}})
}

func TestLFSConsumerProcessRecordInvalidJSON(t *testing.T) {
	handlers := NewLFSHandlers(LFSConfig{}, nil)
	c := &LFSConsumer{
		handlers: handlers,
		logger:   handlers.logger,
	}
	record := &kgo.Record{Value: []byte("not json")}
	c.processRecord(record) // should log error but not panic

	handlers.mu.RLock()
	defer handlers.mu.RUnlock()
	if handlers.stats.TotalObjects != 0 {
		t.Fatalf("expected 0 objects after invalid record")
	}
}

func TestLFSConsumerProcessRecordNilHandlers(t *testing.T) {
	c := &LFSConsumer{
		handlers: nil,
		logger:   NewLFSHandlers(LFSConfig{}, nil).logger,
	}
	event := LFSEvent{EventType: "upload_completed", Topic: "t", S3Key: "k", Size: 1}
	data, _ := json.Marshal(event)
	record := &kgo.Record{Value: data}
	c.processRecord(record) // should not panic even with nil handlers
}
