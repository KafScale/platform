// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"sync"
	"time"

	"github.com/KafScale/platform/addons/processors/iceberg-processor/internal/config"
	"github.com/KafScale/platform/addons/processors/iceberg-processor/internal/metrics"
	"github.com/KafScale/platform/addons/processors/iceberg-processor/internal/sink"
	"github.com/KafScale/platform/pkg/lfs"
)

const (
	lfsModeOff       = "off"
	lfsModeResolve   = "resolve"
	lfsModeReference = "reference"
	lfsModeSkip      = "skip"
	lfsModeHybrid    = "hybrid"
)

type lfsJob struct {
	idx    int
	record sink.Record
}

type lfsResult struct {
	idx           int
	record        sink.Record
	keep          bool
	resolved      bool
	resolvedBytes int64
	err           error
}

func (p *Processor) resolveLfsRecords(ctx context.Context, records []sink.Record, lfsCfg config.LfsConfig, topic string) ([]sink.Record, error) {
	if len(records) == 0 || lfsCfg.Mode == lfsModeOff {
		return records, nil
	}
	if p.lfsS3 == nil && (lfsCfg.Mode == lfsModeResolve || lfsCfg.Mode == lfsModeHybrid) {
		return nil, fmt.Errorf("lfs s3 reader not configured")
	}

	out := make([]*sink.Record, len(records))
	jobs := make(chan lfsJob)
	results := make(chan lfsResult, len(records))
	workers := lfsCfg.ResolveConcurrency
	if workers <= 0 {
		workers = 1
	}

	resolver := lfs.NewResolver(lfs.ResolverConfig{
		MaxSize:          lfsCfg.MaxInlineSize,
		ValidateChecksum: lfsCfg.ChecksumEnabled(),
	}, p.lfsS3)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				res := resolveLfsRecord(ctx, resolver, job.record, lfsCfg)
				res.idx = job.idx
				results <- res
			}
		}()
	}

	for idx, record := range records {
		if !lfs.IsLfsEnvelope(record.Value) {
			out[idx] = &record
			continue
		}
		if lfsCfg.Mode == lfsModeSkip {
			metrics.RecordsTotal.WithLabelValues(topic, "skipped_lfs").Inc()
			continue
		}
		env, err := lfs.DecodeEnvelope(record.Value)
		if err != nil {
			metrics.LfsResolutionErrorsTotal.WithLabelValues(topic, "decode").Inc()
			continue
		}

		switch lfsCfg.Mode {
		case lfsModeReference:
			if lfsCfg.StoreMetadata {
				record = attachLfsMetadata(record, lfsMetadataFromEnvelope(env))
			}
			out[idx] = &record
		case lfsModeHybrid:
			if env.Size > 0 && env.Size <= lfsCfg.MaxInlineSize {
				jobs <- lfsJob{idx: idx, record: record}
				continue
			}
			if lfsCfg.StoreMetadata {
				record = attachLfsMetadata(record, lfsMetadataFromEnvelope(env))
			}
			out[idx] = &record
		case lfsModeResolve:
			jobs <- lfsJob{idx: idx, record: record}
		default:
			out[idx] = &record
		}
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		if res.err != nil {
			metrics.LfsResolutionErrorsTotal.WithLabelValues(topic, "resolve").Inc()
			log.Printf("lfs resolve failed topic=%s offset=%d: %v", topic, res.record.Offset, res.err)
			continue
		}
		if res.keep {
			out[res.idx] = &res.record
		}
		if res.resolved {
			metrics.LfsResolvedTotal.WithLabelValues(topic).Inc()
			metrics.LfsResolvedBytesTotal.WithLabelValues(topic).Add(float64(res.resolvedBytes))
		}
	}

	filtered := make([]sink.Record, 0, len(records))
	for _, record := range out {
		if record == nil {
			continue
		}
		filtered = append(filtered, *record)
	}
	return filtered, nil
}

func resolveLfsRecord(ctx context.Context, resolver *lfs.Resolver, record sink.Record, lfsCfg config.LfsConfig) lfsResult {
	start := time.Now()
	resolved, ok, err := resolver.Resolve(ctx, record.Value)
	if err != nil {
		return lfsResult{record: record, err: err}
	}
	if !ok {
		return lfsResult{record: record, keep: true}
	}
	record.Value = resolved.Payload
	if lfsCfg.StoreMetadata {
		record = attachLfsMetadata(record, lfsMetadataFromResolved(resolved))
	}
	metrics.LfsResolutionDurationSeconds.WithLabelValues(record.Topic).Observe(time.Since(start).Seconds())
	return lfsResult{record: record, keep: true, resolved: true, resolvedBytes: resolved.BlobSize}
}

func attachLfsMetadata(record sink.Record, values map[string]interface{}) sink.Record {
	if record.Columns == nil {
		record.Columns = make(map[string]interface{}, len(values))
	}
	for key, value := range values {
		record.Columns[key] = value
	}
	return record
}

func lfsMetadataFromEnvelope(env lfs.Envelope) map[string]interface{} {
	checksum := env.Checksum
	checksumAlg := env.ChecksumAlg
	if checksum == "" {
		checksum = env.SHA256
		if checksumAlg == "" {
			checksumAlg = "sha256"
		}
	}
	return map[string]interface{}{
		"lfs_content_type": env.ContentType,
		"lfs_blob_size":    env.Size,
		"lfs_checksum":     checksum,
		"lfs_checksum_alg": checksumAlg,
		"lfs_bucket":       env.Bucket,
		"lfs_key":          env.Key,
	}
}

func lfsMetadataFromResolved(resolved lfs.ResolvedRecord) map[string]interface{} {
	checksum := resolved.Checksum
	checksumAlg := resolved.ChecksumAlg
	if checksum == "" {
		checksum = resolved.Envelope.SHA256
		if checksumAlg == "" {
			checksumAlg = "sha256"
		}
	}
	return map[string]interface{}{
		"lfs_content_type": resolved.ContentType,
		"lfs_blob_size":    resolved.BlobSize,
		"lfs_checksum":     checksum,
		"lfs_checksum_alg": checksumAlg,
		"lfs_bucket":       resolved.Envelope.Bucket,
		"lfs_key":          resolved.Envelope.Key,
	}
}
