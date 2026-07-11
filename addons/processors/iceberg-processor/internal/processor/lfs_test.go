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
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/KafScale/platform/addons/processors/iceberg-processor/internal/config"
	"github.com/KafScale/platform/addons/processors/iceberg-processor/internal/decoder"
	"github.com/KafScale/platform/addons/processors/iceberg-processor/internal/discovery"
	"github.com/KafScale/platform/addons/processors/iceberg-processor/internal/sink"
	"github.com/KafScale/platform/pkg/lfs"
)

type fakeS3Reader struct {
	payloads map[string][]byte
}

func (f *fakeS3Reader) Fetch(ctx context.Context, key string) ([]byte, error) {
	return f.payloads[key], nil
}

func (f *fakeS3Reader) Stream(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	return io.NopCloser(bytes.NewReader(nil)), 0, nil
}

func TestResolveLfsRecordsResolveMode(t *testing.T) {
	payload := []byte("hello")
	checksum, err := lfs.ComputeChecksum(lfs.ChecksumSHA256, payload)
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	envBytes := mustEnvelope(t, lfs.Envelope{
		Version: 1,
		Bucket:  "bucket",
		Key:     "key",
		Size:    int64(len(payload)),
		SHA256:  checksum,
	})

	p := &Processor{lfsS3: &fakeS3Reader{payloads: map[string][]byte{"key": payload}}}
	cfg := config.LfsConfig{
		Mode:               lfsModeResolve,
		StoreMetadata:      true,
		ResolveConcurrency: 1,
		ValidateChecksum:   boolPtr(true),
	}

	out, err := p.resolveLfsRecords(context.Background(), []sink.Record{{Topic: "t", Offset: 1, Value: envBytes}}, cfg, "t")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 record, got %d", len(out))
	}
	if string(out[0].Value) != string(payload) {
		t.Fatalf("expected resolved payload")
	}
	if out[0].Columns["lfs_key"] != "key" {
		t.Fatalf("expected lfs metadata")
	}
}

func TestResolveLfsRecordsReferenceMode(t *testing.T) {
	envBytes := mustEnvelope(t, lfs.Envelope{
		Version: 1,
		Bucket:  "bucket",
		Key:     "key",
		Size:    10,
		SHA256:  "abc",
	})
	p := &Processor{}
	cfg := config.LfsConfig{Mode: lfsModeReference, StoreMetadata: true}

	out, err := p.resolveLfsRecords(context.Background(), []sink.Record{{Topic: "t", Offset: 1, Value: envBytes}}, cfg, "t")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 record, got %d", len(out))
	}
	if out[0].Columns["lfs_bucket"] != "bucket" {
		t.Fatalf("expected metadata from envelope")
	}
	if string(out[0].Value) != string(envBytes) {
		t.Fatalf("expected envelope value to remain")
	}
}

func TestResolveLfsRecordsSkipMode(t *testing.T) {
	envBytes := mustEnvelope(t, lfs.Envelope{Version: 1, Bucket: "b", Key: "k", Size: 1, SHA256: "abc"})
	p := &Processor{}
	cfg := config.LfsConfig{Mode: lfsModeSkip}

	out, err := p.resolveLfsRecords(context.Background(), []sink.Record{{Topic: "t", Offset: 1, Value: envBytes}}, cfg, "t")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expected 0 records, got %d", len(out))
	}
}

func TestResolveLfsRecordsHybridMode(t *testing.T) {
	payload := []byte("hello")
	checksum, err := lfs.ComputeChecksum(lfs.ChecksumSHA256, payload)
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	envBytes := mustEnvelope(t, lfs.Envelope{Version: 1, Bucket: "b", Key: "k", Size: int64(len(payload)), SHA256: checksum})

	p := &Processor{lfsS3: &fakeS3Reader{payloads: map[string][]byte{"k": payload}}}
	cfg := config.LfsConfig{Mode: lfsModeHybrid, MaxInlineSize: int64(len(payload)), StoreMetadata: true, ResolveConcurrency: 1, ValidateChecksum: boolPtr(true)}

	out, err := p.resolveLfsRecords(context.Background(), []sink.Record{{Topic: "t", Offset: 1, Value: envBytes}}, cfg, "t")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 record, got %d", len(out))
	}
	if string(out[0].Value) != string(payload) {
		t.Fatalf("expected resolved payload")
	}
}

func mustEnvelope(t *testing.T, env lfs.Envelope) []byte {
	t.Helper()
	data, err := lfs.EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}
	return data
}

func boolPtr(value bool) *bool {
	return &value
}

func TestProcessorResolvesLfsRecords(t *testing.T) {
	payload := []byte("hello")
	checksum, err := lfs.ComputeChecksum(lfs.ChecksumSHA256, payload)
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	envBytes := mustEnvelope(t, lfs.Envelope{
		Version: 1,
		Bucket:  "bucket",
		Key:     "key",
		Size:    int64(len(payload)),
		SHA256:  checksum,
	})

	segments := []discovery.SegmentRef{
		{
			Topic:      "orders",
			Partition:  0,
			BaseOffset: 0,
			SegmentKey: "segment-0",
			IndexKey:   "index-0",
		},
	}
	records := map[string][]decoder.Record{
		"segment-0": {
			{Topic: "orders", Partition: 0, Offset: 10, Timestamp: 1, Value: envBytes},
		},
	}

	store := &testStore{}
	sinkWriter := &testSink{writes: make(chan struct{}, 1)}
	p := &Processor{
		cfg: config.Config{
			Processor: config.ProcessorConfig{PollIntervalSeconds: 1},
			Mappings: []config.Mapping{
				{
					Topic:               "orders",
					Table:               "prod.orders",
					Mode:                "append",
					CreateTableIfAbsent: true,
					Lfs: config.LfsConfig{
						Mode:               lfsModeResolve,
						StoreMetadata:      true,
						ResolveConcurrency: 1,
						ValidateChecksum:   boolPtr(true),
					},
				},
			},
		},
		discover:       &testLister{segments: segments},
		decode:         &testDecoder{records: records},
		store:          store,
		sink:           sinkWriter,
		validator:      nil,
		lfsS3:          &fakeS3Reader{payloads: map[string][]byte{"key": payload}},
		mappingByTopic: map[string]config.Mapping{"orders": {Topic: "orders", Lfs: config.LfsConfig{Mode: lfsModeResolve, StoreMetadata: true, ResolveConcurrency: 1, ValidateChecksum: boolPtr(true)}}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = p.Run(ctx)
	}()

	select {
	case <-sinkWriter.writes:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for sink write")
	}
	cancel()

	if len(sinkWriter.all) != 1 {
		t.Fatalf("expected 1 record, got %d", len(sinkWriter.all))
	}
	if string(sinkWriter.all[0].Value) != string(payload) {
		t.Fatalf("expected resolved payload")
	}
	if sinkWriter.all[0].Columns["lfs_key"] != "key" {
		t.Fatalf("expected lfs metadata")
	}
}
