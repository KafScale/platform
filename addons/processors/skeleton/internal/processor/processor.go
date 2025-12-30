package processor

import (
	"context"
	"os"
	"time"

	"github.com/novatechflow/kafscale/addons/processors/skeleton/internal/checkpoint"
	"github.com/novatechflow/kafscale/addons/processors/skeleton/internal/config"
	"github.com/novatechflow/kafscale/addons/processors/skeleton/internal/decoder"
	"github.com/novatechflow/kafscale/addons/processors/skeleton/internal/discovery"
	"github.com/novatechflow/kafscale/addons/processors/skeleton/internal/sink"
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
	return &Processor{
		cfg:      cfg,
		discover: discovery.New(),
		decode:   decoder.New(),
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

				batches, err := p.decode.Decode(ctx, seg.SegmentKey, seg.IndexKey)
				if err != nil {
					continue
				}

				records := mapBatches(batches)
				if len(records) == 0 {
					continue
				}

				if dropped := len(records); dropped > 0 {
					records = filterRecords(records, state.Offset)
					dropped -= len(records)
				}
				if len(records) == 0 {
					continue
				}

				unlock := p.locks.Lock(seg.Topic)
				err = p.sink.Write(ctx, records)
				unlock()
				if err != nil {
					continue
				}

				last := records[len(records)-1]
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

func mapBatches(batches []decoder.Batch) []sink.Record {
	records := make([]sink.Record, 0, len(batches))
	for _, batch := range batches {
		records = append(records, sink.Record{
			Topic:     batch.Topic,
			Partition: batch.Partition,
			Offset:    batch.Offset,
			Payload:   batch.Payload,
		})
	}
	return records
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
