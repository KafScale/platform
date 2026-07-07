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

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/KafScale/platform/internal/testutil"
	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/KafScale/platform/pkg/storage"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newTestEtcdStore(t *testing.T, ctx context.Context, initial metadata.ClusterMetadata, endpoints []string) *metadata.EtcdStore {
	t.Helper()
	store, err := metadata.NewEtcdStore(ctx, initial, metadata.EtcdStoreConfig{Endpoints: endpoints})
	if err != nil {
		t.Fatalf("NewEtcdStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

type failingListS3 struct {
	*storage.MemoryS3Client
	err error
}

func (f *failingListS3) ListSegments(context.Context, string) ([]storage.S3Object, error) {
	return nil, f.err
}

type failingUploadIndexS3 struct {
	*storage.MemoryS3Client
	targetPrefix string
	err          error
}

func (f *failingUploadIndexS3) UploadIndex(ctx context.Context, key string, body []byte) error {
	if strings.HasPrefix(key, f.targetPrefix) {
		return f.err
	}
	return f.MemoryS3Client.UploadIndex(ctx, key, body)
}

func TestRunHelpAndUnknownCommand(t *testing.T) {
	var stdout bytes.Buffer
	if err := run(context.Background(), []string{"help"}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("run help: %v", err)
	}
	if !strings.Contains(stdout.String(), "usage: kafscale-cli restore") {
		t.Fatalf("unexpected help output: %s", stdout.String())
	}

	err := run(context.Background(), []string{"wat"}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("expected unknown command error, got %v", err)
	}
}

func TestRunRestoreCommandUsesInjectedClients(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()

	store := newTestEtcdStore(t, ctx, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "broker-0", Port: 9092},
		},
		ControllerID: 1,
	}, endpoints)

	if _, err := store.CreateTopic(ctx, metadata.TopicSpec{
		Name:              "orders",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	s3 := storage.NewMemoryS3Client()
	artifact, err := storage.BuildSegment(storage.SegmentWriterConfig{IndexIntervalMessages: 1}, []storage.RecordBatch{
		makeStorageRecoveryBatch(0, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC).UnixMilli(), []int64{0}),
	}, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("BuildSegment: %v", err)
	}
	if err := s3.UploadSegment(ctx, "default/orders/0/segment-00000000000000000000.kfs", artifact.SegmentBytes); err != nil {
		t.Fatalf("UploadSegment: %v", err)
	}
	if err := s3.UploadIndex(ctx, "default/orders/0/segment-00000000000000000000.index", artifact.IndexBytes); err != nil {
		t.Fatalf("UploadIndex: %v", err)
	}

	prevS3 := newS3Client
	prevStore := newEtcdStore
	prevMemory := newMemoryS3
	t.Cleanup(func() {
		newS3Client = prevS3
		newEtcdStore = prevStore
		newMemoryS3 = prevMemory
	})
	newS3Client = func(context.Context, storage.S3Config) (storage.S3Client, error) {
		return s3, nil
	}
	newEtcdStore = func(ctx context.Context, _ metadata.ClusterMetadata, cfg metadata.EtcdStoreConfig) (*metadata.EtcdStore, error) {
		return metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{
			Brokers: []protocol.MetadataBroker{
				{NodeID: 1, Host: "broker-0", Port: 9092},
			},
			ControllerID: 1,
		}, cfg)
	}
	newMemoryS3 = func() storage.S3Client { return s3 }

	t.Setenv("KAFSCALE_S3_BUCKET", "bucket")
	t.Setenv("KAFSCALE_S3_REGION", "us-east-1")
	t.Setenv("KAFSCALE_ETCD_ENDPOINTS", strings.Join(endpoints, ","))

	var stdout bytes.Buffer
	if err := run(context.Background(), []string{
		"restore",
		"--topic", "orders",
		"--target-topic", "orders-wrapper",
		"--to", "2026-05-13T12:05:00Z",
	}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("run restore: %v", err)
	}
	if !strings.Contains(stdout.String(), "orders-wrapper") {
		t.Fatalf("unexpected restore output: %s", stdout.String())
	}
}

func TestRunRestoreCommandRequiresTopic(t *testing.T) {
	err := runRestoreCommand(context.Background(), []string{
		"--target-topic", "orders-restore",
		"--to", "2026-05-13T12:05:00Z",
	}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "--topic is required") {
		t.Fatalf("expected missing topic error, got %v", err)
	}
}

func TestS3ClientFromEnvUsesMemoryToggle(t *testing.T) {
	prevMemory := newMemoryS3
	t.Cleanup(func() { newMemoryS3 = prevMemory })

	mem := storage.NewMemoryS3Client()
	newMemoryS3 = func() storage.S3Client { return mem }
	t.Setenv("KAFSCALE_USE_MEMORY_S3", "true")

	client, err := s3ClientFromEnv(context.Background())
	if err != nil {
		t.Fatalf("s3ClientFromEnv: %v", err)
	}
	if client != mem {
		t.Fatal("expected injected memory s3 client")
	}
}

func TestMetadataStoreFromEnvPassesConfig(t *testing.T) {
	prevStore := newEtcdStore
	t.Cleanup(func() { newEtcdStore = prevStore })

	t.Setenv("KAFSCALE_ETCD_ENDPOINTS", "http://a:2379,http://b:2379")
	t.Setenv("KAFSCALE_ETCD_USERNAME", "user")
	t.Setenv("KAFSCALE_ETCD_PASSWORD", "pass")

	var got metadata.EtcdStoreConfig
	sentinel := errors.New("stop")
	newEtcdStore = func(_ context.Context, _ metadata.ClusterMetadata, cfg metadata.EtcdStoreConfig) (*metadata.EtcdStore, error) {
		got = cfg
		return nil, sentinel
	}

	err := func() error {
		_, err := metadataStoreFromEnv(context.Background())
		return err
	}()
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	if len(got.Endpoints) != 2 || got.Endpoints[0] != "http://a:2379" || got.Endpoints[1] != "http://b:2379" {
		t.Fatalf("unexpected endpoints: %+v", got.Endpoints)
	}
	if got.Username != "user" || got.Password != "pass" {
		t.Fatalf("unexpected auth config: %+v", got)
	}
}

func TestExecuteRestoreCreatesRecoveredTopic(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()

	store := newTestEtcdStore(t, ctx, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "broker-0", Port: 9092},
		},
		ControllerID: 1,
	}, endpoints)

	if _, err := store.CreateTopic(ctx, metadata.TopicSpec{
		Name:              "orders",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	cfg, err := store.FetchTopicConfig(ctx, "orders")
	if err != nil {
		t.Fatalf("FetchTopicConfig: %v", err)
	}
	cfg.RetentionMs = 60000
	cfg.Config = map[string]string{"cleanup.policy": "delete"}
	if err := store.UpdateTopicConfig(ctx, cfg); err != nil {
		t.Fatalf("UpdateTopicConfig: %v", err)
	}

	s3 := storage.NewMemoryS3Client()
	artifact, err := storage.BuildSegment(storage.SegmentWriterConfig{IndexIntervalMessages: 1}, []storage.RecordBatch{
		makeStorageRecoveryBatch(0, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC).UnixMilli(), []int64{0}),
	}, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("BuildSegment: %v", err)
	}
	if err := s3.UploadSegment(ctx, "default/orders/0/segment-00000000000000000000.kfs", artifact.SegmentBytes); err != nil {
		t.Fatalf("UploadSegment: %v", err)
	}
	if err := s3.UploadIndex(ctx, "default/orders/0/segment-00000000000000000000.index", artifact.IndexBytes); err != nil {
		t.Fatalf("UploadIndex: %v", err)
	}

	var stdout bytes.Buffer
	if err := executeRestore(ctx, &stdout, restoreConfig{
		SourceTopic:     "orders",
		SourceNamespace: "default",
		TargetTopic:     "orders-restored",
		TargetNamespace: "default",
		RestoreTo:       time.Date(2026, 5, 13, 12, 5, 0, 0, time.UTC),
	}, s3, store); err != nil {
		t.Fatalf("executeRestore: %v", err)
	}
	if !strings.Contains(stdout.String(), "restored orders to orders-restored") {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}

	meta, err := store.Metadata(ctx, []string{"orders-restored"})
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if len(meta.Topics) != 1 || meta.Topics[0].ErrorCode != 0 || meta.Topics[0].Topic == nil || *meta.Topics[0].Topic != "orders-restored" {
		t.Fatalf("unexpected target topic metadata: %+v", meta.Topics)
	}

	targetCfg, err := store.FetchTopicConfig(ctx, "orders-restored")
	if err != nil {
		t.Fatalf("FetchTopicConfig target: %v", err)
	}
	if targetCfg.RetentionMs != 60000 {
		t.Fatalf("expected retention to be copied, got %d", targetCfg.RetentionMs)
	}
	if targetCfg.Config["cleanup.policy"] != "delete" {
		t.Fatalf("expected config to be copied, got %+v", targetCfg.Config)
	}

	nextOffset, err := store.NextOffset(ctx, "orders-restored", 0)
	if err != nil {
		t.Fatalf("NextOffset: %v", err)
	}
	if nextOffset != 1 {
		t.Fatalf("expected next offset 1, got %d", nextOffset)
	}

	if _, err := s3.DownloadSegment(ctx, "default/orders-restored/0/segment-00000000000000000000.kfs", nil); err != nil {
		t.Fatalf("DownloadSegment restored: %v", err)
	}

	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("new etcd client: %v", err)
	}
	defer func() { _ = cli.Close() }()

	resp, err := cli.Get(ctx, metadata.PartitionStateKey("orders-restored", 0))
	if err != nil {
		t.Fatalf("get partition state: %v", err)
	}
	if len(resp.Kvs) != 1 {
		t.Fatalf("expected partition state, got %d", len(resp.Kvs))
	}
	state, err := metadata.DecodePartitionState(resp.Kvs[0].Value)
	if err != nil {
		t.Fatalf("DecodePartitionState: %v", err)
	}
	if state.LogEndOffset != 1 || state.HighWatermark != 1 {
		t.Fatalf("unexpected partition offsets: %+v", state)
	}
	if state.ActiveSegment != "segment-00000000000000000000.kfs" {
		t.Fatalf("unexpected active segment: %+v", state)
	}
}

func TestCloneTopicConfig(t *testing.T) {
	cfg := &metadatapb.TopicConfig{
		Name:              "orders",
		Partitions:        1,
		ReplicationFactor: 1,
		RetentionMs:       42,
		Config:            map[string]string{"a": "b"},
	}
	cloned := cloneTopicConfig(cfg)
	if cloned == cfg {
		t.Fatal("expected clone to allocate a new object")
	}
	cloned.Config["a"] = "c"
	if cfg.Config["a"] != "b" {
		t.Fatal("expected source config map to stay untouched")
	}
}

func TestParsePartitions(t *testing.T) {
	partitions, err := parsePartitions("2,0,2,1")
	if err != nil {
		t.Fatalf("parsePartitions: %v", err)
	}
	if want := []int32{0, 1, 2}; len(partitions) != len(want) || partitions[0] != want[0] || partitions[1] != want[1] || partitions[2] != want[2] {
		t.Fatalf("unexpected partitions: %+v", partitions)
	}
}

func TestExecuteRestoreRejectsUnknownPartition(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()

	store := newTestEtcdStore(t, ctx, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "broker-0", Port: 9092},
		},
		ControllerID: 1,
		Topics: []protocol.MetadataTopic{
			{
				Topic: kmsg.StringPtr("orders"),
				Partitions: []protocol.MetadataPartition{
					{Partition: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}},
				},
			},
		},
	}, endpoints)

	err := executeRestore(ctx, &bytes.Buffer{}, restoreConfig{
		SourceTopic:     "orders",
		SourceNamespace: "default",
		TargetTopic:     "orders-restored",
		TargetNamespace: "default",
		RestoreTo:       time.Now().UTC(),
		Partitions:      []int32{99},
	}, storage.NewMemoryS3Client(), store)
	if err == nil || !strings.Contains(err.Error(), "partition 99") {
		t.Fatalf("expected unknown partition error, got %v", err)
	}
}

func TestExecuteRestoreRollsBackTargetTopicOnRecoveryFailure(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()

	store := newTestEtcdStore(t, ctx, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "broker-0", Port: 9092},
		},
		ControllerID: 1,
	}, endpoints)

	if _, err := store.CreateTopic(ctx, metadata.TopicSpec{
		Name:              "orders",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	s3 := &failingListS3{
		MemoryS3Client: storage.NewMemoryS3Client(),
		err:            errors.New("list failed"),
	}
	err := executeRestore(ctx, &bytes.Buffer{}, restoreConfig{
		SourceTopic:     "orders",
		SourceNamespace: "default",
		TargetTopic:     "orders-restored",
		TargetNamespace: "default",
		RestoreTo:       time.Now().UTC(),
	}, s3, store)
	if err == nil || !strings.Contains(err.Error(), "list failed") {
		t.Fatalf("expected list failure, got %v", err)
	}

	meta, err := store.Metadata(ctx, []string{"orders-restored"})
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if len(meta.Topics) != 1 || meta.Topics[0].ErrorCode == 0 {
		t.Fatalf("expected rolled back target topic to be absent, got %+v", meta.Topics)
	}
}

func TestExecuteRestoreRollsBackCopiedS3ObjectsOnPartialFailure(t *testing.T) {
	endpoints := testutil.StartEmbeddedEtcd(t)
	ctx := context.Background()

	store := newTestEtcdStore(t, ctx, metadata.ClusterMetadata{
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "broker-0", Port: 9092},
		},
		ControllerID: 1,
	}, endpoints)

	if _, err := store.CreateTopic(ctx, metadata.TopicSpec{
		Name:              "orders",
		NumPartitions:     1,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	mem := storage.NewMemoryS3Client()
	artifact, err := storage.BuildSegment(storage.SegmentWriterConfig{IndexIntervalMessages: 1}, []storage.RecordBatch{
		makeStorageRecoveryBatch(0, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC).UnixMilli(), []int64{0}),
	}, time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("BuildSegment: %v", err)
	}
	if err := mem.UploadSegment(ctx, "default/orders/0/segment-00000000000000000000.kfs", artifact.SegmentBytes); err != nil {
		t.Fatalf("UploadSegment: %v", err)
	}
	if err := mem.UploadIndex(ctx, "default/orders/0/segment-00000000000000000000.index", artifact.IndexBytes); err != nil {
		t.Fatalf("UploadIndex: %v", err)
	}

	s3 := &failingUploadIndexS3{
		MemoryS3Client: mem,
		targetPrefix:   "default/orders-restored/",
		err:            errors.New("upload index failed"),
	}
	err = executeRestore(ctx, &bytes.Buffer{}, restoreConfig{
		SourceTopic:     "orders",
		SourceNamespace: "default",
		TargetTopic:     "orders-restored",
		TargetNamespace: "default",
		RestoreTo:       time.Now().UTC(),
	}, s3, store)
	if err == nil || !strings.Contains(err.Error(), "upload index failed") {
		t.Fatalf("expected upload index failure, got %v", err)
	}

	if _, err := mem.DownloadSegment(ctx, "default/orders-restored/0/segment-00000000000000000000.kfs", nil); err == nil {
		t.Fatal("expected restored segment to be cleaned up after failure")
	}
	if _, err := mem.DownloadIndex(ctx, "default/orders-restored/0/segment-00000000000000000000.index"); err == nil {
		t.Fatal("expected restored index to be cleaned up after failure")
	}

	meta, err := store.Metadata(ctx, []string{"orders-restored"})
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	if len(meta.Topics) != 1 || meta.Topics[0].ErrorCode == 0 {
		t.Fatalf("expected rolled back target topic to be absent, got %+v", meta.Topics)
	}
}

func makeStorageRecoveryBatch(baseOffset, firstTimestamp int64, timestampDeltas []int64) storage.RecordBatch {
	records := make([][]byte, 0, len(timestampDeltas))
	maxTimestamp := firstTimestamp
	for i, delta := range timestampDeltas {
		records = append(records, makeStorageRecoveryRecord(delta, int64(i)))
		if ts := firstTimestamp + delta; ts > maxTimestamp {
			maxTimestamp = ts
		}
	}

	bodyLen := 0
	for _, record := range records {
		bodyLen += len(record)
	}
	const recordBatchHeaderLen = 61
	const batchFrameHeaderLen = 12
	batch := make([]byte, recordBatchHeaderLen+bodyLen)
	binary.BigEndian.PutUint64(batch[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(batch[8:12], uint32(len(batch)-batchFrameHeaderLen))
	batch[16] = 2
	binary.BigEndian.PutUint64(batch[27:35], uint64(firstTimestamp))
	binary.BigEndian.PutUint64(batch[35:43], uint64(maxTimestamp))
	binary.BigEndian.PutUint64(batch[43:51], uint64(^uint64(0)))
	binary.BigEndian.PutUint16(batch[51:53], uint16(^uint16(0)))
	binary.BigEndian.PutUint32(batch[53:57], uint32(^uint32(0)))
	binary.BigEndian.PutUint32(batch[57:61], uint32(len(records)))
	offset := recordBatchHeaderLen
	for _, record := range records {
		copy(batch[offset:], record)
		offset += len(record)
	}
	binary.BigEndian.PutUint32(batch[23:27], uint32(len(records)-1))
	binary.BigEndian.PutUint32(batch[17:21], crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli)))

	return storage.RecordBatch{
		BaseOffset:      baseOffset,
		LastOffsetDelta: int32(len(records) - 1),
		MessageCount:    int32(len(records)),
		Bytes:           batch,
	}
}

func makeStorageRecoveryRecord(timestampDelta, offsetDelta int64) []byte {
	payload := bytes.NewBuffer(nil)
	payload.WriteByte(0)
	payload.Write(encodeStorageRecoveryVarint(timestampDelta))
	payload.Write(encodeStorageRecoveryVarint(offsetDelta))
	payload.Write(encodeStorageRecoveryVarint(-1))
	payload.Write(encodeStorageRecoveryVarint(-1))
	payload.Write(encodeStorageRecoveryVarint(0))

	record := bytes.NewBuffer(nil)
	record.Write(encodeStorageRecoveryVarint(int64(payload.Len())))
	record.Write(payload.Bytes())
	return record.Bytes()
}

func encodeStorageRecoveryVarint(value int64) []byte {
	zigzag := uint64(value<<1) ^ uint64(value>>63)
	out := make([]byte, 0, 10)
	for {
		b := byte(zigzag & 0x7f)
		zigzag >>= 7
		if zigzag != 0 {
			b |= 0x80
		}
		out = append(out, b)
		if zigzag == 0 {
			return out
		}
	}
}
