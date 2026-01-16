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

package processor

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/checkpoint"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/config"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/decoder"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/discovery"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/metrics"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/schema"
	"github.com/novatechflow/kafscale/addons/processors/iceberg-processor/internal/sink"
)

var leaseRenewInterval = 10 * time.Second

// Processor wires discovery, decoding, checkpointing, and sink writing.
type Processor struct {
	cfg       config.Config
	discover  discovery.Lister
	decode    decoder.Decoder
	store     checkpoint.Store
	sink      sink.Writer
	validator schema.Validator
}

func New(cfg config.Config) (*Processor, error) {
	lister, err := discovery.New(cfg)
	if err != nil {
		return nil, err
	}
	decoderClient, err := decoder.New(cfg)
	if err != nil {
		return nil, err
	}
	writer, err := sink.New(cfg)
	if err != nil {
		return nil, err
	}
	validator, err := schema.New(cfg)
	if err != nil {
		return nil, err
	}
	store, err := checkpoint.New(cfg)
	if err != nil {
		return nil, err
	}

	return &Processor{
		cfg:       cfg,
		discover:  lister,
		decode:    decoderClient,
		store:     store,
		sink:      writer,
		validator: validator,
	}, nil
}

func (p *Processor) Run(ctx context.Context) error {
	ownerID, err := os.Hostname()
	if err != nil || ownerID == "" {
		ownerID = "worker"
	}

	pollInterval := time.Duration(p.cfg.Processor.PollIntervalSeconds) * time.Second
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	ticker := time.NewTicker(pollInterval)
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
				log.Printf("lease renewal failed: %v", err)
				metrics.ErrorsTotal.WithLabelValues("checkpoint").Inc()
			}
			continue
		case <-ticker.C:
			segments, err := p.discover.ListCompleted(ctx)
			if err != nil {
				metrics.ErrorsTotal.WithLabelValues("discover").Inc()
				continue
			}

			if !hasLease {
				for _, seg := range segments {
					lease, err := p.store.ClaimLease(ctx, seg.Topic, seg.Partition, ownerID)
					if err != nil {
						metrics.ErrorsTotal.WithLabelValues("checkpoint").Inc()
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
					metrics.ErrorsTotal.WithLabelValues("checkpoint").Inc()
					continue
				}

				decoded, err := p.decode.Decode(ctx, seg.SegmentKey, seg.IndexKey, seg.Topic, seg.Partition)
				if err != nil {
					metrics.ErrorsTotal.WithLabelValues("decode").Inc()
					continue
				}

				records := mapRecords(decoded)
				if dropped := len(records); dropped > 0 {
					records = filterRecords(records, state.Offset)
					dropped -= len(records)
					if dropped > 0 {
						metrics.RecordsTotal.WithLabelValues(seg.Topic, "dropped").Add(float64(dropped))
					}
				}
				records, invalid, err := validateRecords(ctx, records, p.validator)
				if err != nil {
					metrics.ErrorsTotal.WithLabelValues("schema").Inc()
					return err
				}
				if invalid > 0 {
					metrics.RecordsTotal.WithLabelValues(seg.Topic, "invalid").Add(float64(invalid))
				}
				if len(records) == 0 {
					continue
				}

				start := time.Now()
				if err := p.sink.Write(ctx, records); err != nil {
					first := records[0]
					last := records[len(records)-1]
					log.Printf("sink write failed topic=%s partition=%d offsets=%d-%d: %T %v", first.Topic, first.Partition, first.Offset, last.Offset, err, err)
					log.Printf("sink write error details: %+v", err)
					metrics.ErrorsTotal.WithLabelValues("sink").Inc()
					continue
				}
				metrics.WriteLatency.WithLabelValues(seg.Topic).Observe(float64(time.Since(start).Milliseconds()))

				last := records[len(records)-1]
				if err := p.store.CommitOffset(ctx, checkpoint.OffsetState{
					Topic:     last.Topic,
					Partition: last.Partition,
					Offset:    last.Offset,
					Timestamp: time.Now().UnixMilli(),
				}); err != nil {
					metrics.ErrorsTotal.WithLabelValues("checkpoint").Inc()
					continue
				}
				metrics.RecordsTotal.WithLabelValues(seg.Topic, "written").Add(float64(len(records)))
				metrics.BatchesTotal.WithLabelValues(seg.Topic).Inc()
				metrics.LastOffset.WithLabelValues(seg.Topic, fmt.Sprintf("%d", seg.Partition)).Set(float64(last.Offset))
				metrics.WatermarkOffset.WithLabelValues(seg.Topic, fmt.Sprintf("%d", seg.Partition)).Set(float64(last.Offset))
				metrics.WatermarkTimestamp.WithLabelValues(seg.Topic, fmt.Sprintf("%d", seg.Partition)).Set(float64(last.Timestamp))
			}
		}
	}
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

func mapRecords(records []decoder.Record) []sink.Record {
	out := make([]sink.Record, 0, len(records))
	for _, record := range records {
		out = append(out, sink.Record{
			Topic:     record.Topic,
			Partition: record.Partition,
			Offset:    record.Offset,
			Timestamp: record.Timestamp,
			Key:       record.Key,
			Value:     record.Value,
			Headers:   record.Headers,
		})
	}
	return out
}

func validateRecords(ctx context.Context, records []sink.Record, validator schema.Validator) ([]sink.Record, int, error) {
	if validator == nil || validator.Mode() == schema.ModeOff {
		return records, 0, nil
	}

	valid := records[:0]
	invalid := 0
	for _, record := range records {
		if err := validator.Validate(ctx, record.Topic, record.Value); err != nil {
			if validator.Mode() == schema.ModeStrict {
				return nil, invalid, err
			}
			invalid++
			continue
		}
		valid = append(valid, record)
	}
	return valid, invalid, nil
}

func (p *Processor) startLeaseRenewal(ctx context.Context, lease checkpoint.Lease, leaseLost chan<- error) func() {
	renewCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(leaseRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				if err := p.store.RenewLease(renewCtx, lease); err != nil {
					select {
					case leaseLost <- err:
					default:
					}
					return
				}
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}
