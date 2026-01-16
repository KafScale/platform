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

package processor

import (
	"context"
	"os"
	"time"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/checkpoint"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/decoder"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/discovery"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/sink"
)

// Processor wires discovery, decoding, checkpointing, and sink writing.
type Processor struct {
	cfg      config.Config
	discover discovery.Lister
	decode   decoder.Decoder
	store    checkpoint.Store
	sink     sink.Writer
	locks    *topicLocker
}

const leaseRenewInterval = 10 * time.Second

func New(cfg config.Config) (*Processor, error) {
	discover, err := discovery.New(cfg)
	if err != nil {
		return nil, err
	}
	dec, err := decoder.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Processor{
		cfg:      cfg,
		discover: discover,
		decode:   dec,
		store:    checkpoint.New(),
		sink:     sink.New(),
		locks:    newTopicLocker(),
	}, nil
}

func (p *Processor) Run(ctx context.Context) error {
	ownerID, err := os.Hostname()
	if err != nil || ownerID == "" {
		ownerID = "worker"
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var (
		activeLease checkpoint.Lease
		hasLease    bool
		stopRenew   func()
		leaseLost   = make(chan error, 1)
	)
	defer func() {
		if hasLease {
			stopRenew()
			_ = p.store.ReleaseLease(ctx, activeLease)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			_ = p.sink.Close(ctx)
			return nil
		case err := <-leaseLost:
			if hasLease {
				stopRenew()
				_ = p.store.ReleaseLease(ctx, activeLease)
				hasLease = false
			}
			if err != nil {
				continue
			}
			continue
		case <-ticker.C:
			segments, err := p.discover.ListCompleted(ctx)
			if err != nil {
				continue
			}

			if !hasLease {
				for _, seg := range segments {
					lease, err := p.store.ClaimLease(ctx, seg.Topic, seg.Partition, ownerID)
					if err != nil {
						continue
					}
					activeLease = lease
					hasLease = true
					stopRenew = p.startLeaseRenewal(ctx, lease, leaseLost)
					break
				}
			}

			if !hasLease {
				continue
			}

			for _, seg := range segments {
				if seg.Topic != activeLease.Topic || seg.Partition != activeLease.Partition {
					continue
				}

				state, err := p.store.LoadOffset(ctx, seg.Topic, seg.Partition)
				if err != nil {
					continue
				}

				records, err := p.decode.Decode(ctx, seg.SegmentKey, seg.IndexKey, seg.Topic, seg.Partition)
				if err != nil {
					continue
				}
				if len(records) == 0 {
					continue
				}

				mapped := mapRecords(records)
				if dropped := len(mapped); dropped > 0 {
					mapped = filterRecords(mapped, state.Offset)
					dropped -= len(mapped)
				}
				if len(mapped) == 0 {
					continue
				}

				unlock := p.locks.Lock(seg.Topic)
				err = p.sink.Write(ctx, mapped)
				unlock()
				if err != nil {
					continue
				}

				last := mapped[len(mapped)-1]
				_ = p.store.CommitOffset(ctx, checkpoint.OffsetState{
					Topic:     last.Topic,
					Partition: last.Partition,
					Offset:    last.Offset,
					Timestamp: time.Now().UnixMilli(),
				})
			}
		}
	}
}

func mapRecords(records []decoder.Record) []sink.Record {
	out := make([]sink.Record, 0, len(records))
	for _, record := range records {
		out = append(out, sink.Record{
			Topic:     record.Topic,
			Partition: record.Partition,
			Offset:    record.Offset,
			Payload:   record.Value,
		})
	}
	return out
}

func (p *Processor) startLeaseRenewal(ctx context.Context, lease checkpoint.Lease, leaseLost chan<- error) func() {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(leaseRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := p.store.RenewLease(ctx, lease); err != nil {
					select {
					case leaseLost <- err:
					default:
					}
					return
				}
			}
		}
	}()
	return func() { close(done) }
}

func filterRecords(records []sink.Record, offset int64) []sink.Record {
	if len(records) == 0 {
		return records
	}

	filtered := records[:0]
	for _, record := range records {
		if record.Offset > offset {
			filtered = append(filtered, record)
		}
	}
	return filtered
}
