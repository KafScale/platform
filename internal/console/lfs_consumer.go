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
	"log"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// LFSConsumer consumes events from the __lfs_ops_state topic
type LFSConsumer struct {
	client   *kgo.Client
	topic    string
	handlers *LFSHandlers
	logger   *log.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	statusMu   sync.RWMutex
	lastError  string
	lastErrorAt time.Time
	lastPollAt time.Time
}

// LFSConsumerConfig holds configuration for the LFS consumer
type LFSConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

// NewLFSConsumer creates a new LFS tracker events consumer
func NewLFSConsumer(ctx context.Context, cfg LFSConsumerConfig, handlers *LFSHandlers, logger *log.Logger) (*LFSConsumer, error) {
	if logger == nil {
		logger = log.Default()
	}

	if len(cfg.Brokers) == 0 {
		logger.Println("lfs consumer: no brokers configured")
		return nil, nil
	}

	if cfg.Topic == "" {
		cfg.Topic = "__lfs_ops_state"
	}

	if cfg.GroupID == "" {
		cfg.GroupID = "kafscale-console-lfs"
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	consumerCtx, cancel := context.WithCancel(ctx)

	c := &LFSConsumer{
		client:   client,
		topic:    cfg.Topic,
		handlers: handlers,
		logger:   logger,
		ctx:      consumerCtx,
		cancel:   cancel,
	}

	return c, nil
}

// Start begins consuming events
func (c *LFSConsumer) Start() {
	c.wg.Add(1)
	go c.consumeLoop()
	c.logger.Printf("lfs consumer started, topic=%s", c.topic)
}

// consumeLoop continuously polls for new events
func (c *LFSConsumer) consumeLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		fetches := c.client.PollFetches(c.ctx)
		if fetches.IsClientClosed() {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				c.logger.Printf("lfs consumer fetch error: topic=%s partition=%d error=%v",
					err.Topic, err.Partition, err.Err)
				c.setError(err.Err)
			}
			continue
		}

		c.setPollSuccess()
		fetches.EachRecord(func(record *kgo.Record) {
			c.processRecord(record)
		})

		// Commit offsets
		if err := c.client.CommitUncommittedOffsets(c.ctx); err != nil {
			c.logger.Printf("lfs consumer commit error: %v", err)
		}
	}
}

// processRecord handles a single tracker event record
func (c *LFSConsumer) processRecord(record *kgo.Record) {
	if record == nil || len(record.Value) == 0 {
		return
	}

	// Parse the event
	var event LFSEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		c.logger.Printf("lfs consumer: failed to parse event: %v", err)
		return
	}

	// Forward to handlers for processing
	if c.handlers != nil {
		c.handlers.ProcessEvent(event)
	}
}

// Close stops the consumer and releases resources
func (c *LFSConsumer) Close() error {
	c.cancel()
	c.wg.Wait()
	c.client.Close()
	c.logger.Println("lfs consumer closed")
	return nil
}

// Status returns the current consumer status.
func (c *LFSConsumer) Status() LFSConsumerStatus {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()

	status := LFSConsumerStatus{
		Connected: c.lastPollAt.After(time.Time{}),
		LastError: c.lastError,
	}
	if !c.lastErrorAt.IsZero() {
		status.LastErrorAt = c.lastErrorAt.UTC().Format(time.RFC3339)
	}
	if !c.lastPollAt.IsZero() {
		status.LastPollAt = c.lastPollAt.UTC().Format(time.RFC3339)
	}
	return status
}

func (c *LFSConsumer) setError(err error) {
	if err == nil {
		return
	}
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.lastError = err.Error()
	c.lastErrorAt = time.Now()
}

func (c *LFSConsumer) setPollSuccess() {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.lastPollAt = time.Now()
	if c.lastErrorAt.Before(c.lastPollAt) {
		c.lastError = ""
		c.lastErrorAt = time.Time{}
	}
}
