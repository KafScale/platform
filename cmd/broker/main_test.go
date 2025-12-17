package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/novatechflow/kafscale/pkg/broker"
	controlpb "github.com/novatechflow/kafscale/pkg/gen/control"
	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
	"github.com/novatechflow/kafscale/pkg/storage"
)

func TestHandleProduceAckAll(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	req := &protocol.ProduceRequest{
		Acks:      -1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceTopic{
			{
				Name: "orders",
				Partitions: []protocol.ProducePartition{
					{
						Partition: 0,
						Records:   testBatchBytes(0, 0, 1),
					},
				},
			},
		},
	}

	resp, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 1}, req)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected response for acks=-1")
	}

	offset, err := store.NextOffset(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("NextOffset: %v", err)
	}
	if offset != 1 {
		t.Fatalf("expected offset advanced to 1 got %d", offset)
	}
}

func TestHandleProduceAckZero(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	req := &protocol.ProduceRequest{
		Acks:      0,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceTopic{
			{
				Name: "orders",
				Partitions: []protocol.ProducePartition{
					{
						Partition: 0,
						Records:   testBatchBytes(0, 0, 1),
					},
				},
			},
		},
	}

	resp, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 1}, req)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	if resp != nil {
		t.Fatalf("expected no response for acks=0")
	}
}

func TestHandlerApiVersionsUnsupported(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	header := &protocol.RequestHeader{
		APIKey:        protocol.APIKeyApiVersion,
		APIVersion:    1,
		CorrelationID: 42,
	}
	payload, err := handler.Handle(context.Background(), header, &protocol.ApiVersionsRequest{})
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	reader := bytes.NewReader(payload)
	var (
		corr      int32
		errorCode int16
	)
	if err := binary.Read(reader, binary.BigEndian, &corr); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	if err := binary.Read(reader, binary.BigEndian, &errorCode); err != nil {
		t.Fatalf("read error code: %v", err)
	}
	if corr != 42 {
		t.Fatalf("expected correlation id 42 got %d", corr)
	}
	if errorCode != protocol.UNSUPPORTED_VERSION {
		t.Fatalf("expected UNSUPPORTED_VERSION (%d) got %d", protocol.UNSUPPORTED_VERSION, errorCode)
	}
}

func TestHandleFetch(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	produceReq := &protocol.ProduceRequest{
		Acks:      -1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceTopic{
			{
				Name: "orders",
				Partitions: []protocol.ProducePartition{
					{
						Partition: 0,
						Records:   testBatchBytes(0, 0, 1),
					},
				},
			},
		},
	}
	if _, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 1}, produceReq); err != nil {
		t.Fatalf("handleProduce: %v", err)
	}

	fetchReq := &protocol.FetchRequest{
		Topics: []protocol.FetchTopicRequest{
			{
				Name: "orders",
				Partitions: []protocol.FetchPartitionRequest{
					{
						Partition:   0,
						FetchOffset: 0,
						MaxBytes:    1024,
					},
				},
			},
		},
	}

	resp, err := handler.handleFetch(context.Background(), &protocol.RequestHeader{CorrelationID: 2}, fetchReq)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	if len(resp) == 0 {
		t.Fatalf("expected non-empty response for fetch")
	}
}

func TestAutoCreateTopicOnProduce(t *testing.T) {
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		ControllerID: 1,
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "localhost", Port: 19092},
		},
	})
	handler := newTestHandler(store)

	req := &protocol.ProduceRequest{
		Acks:      -1,
		TimeoutMs: 1000,
		Topics: []protocol.ProduceTopic{
			{
				Name: "auto-created",
				Partitions: []protocol.ProducePartition{
					{
						Partition: 0,
						Records:   testBatchBytes(0, 0, 1),
					},
				},
			},
		},
	}
	if _, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 99}, req); err != nil {
		t.Fatalf("handleProduce auto-create: %v", err)
	}
	meta, err := store.Metadata(context.Background(), []string{"auto-created"})
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if len(meta.Topics) == 0 || meta.Topics[0].ErrorCode != protocol.NONE {
		t.Fatalf("expected topic metadata, got %+v", meta.Topics)
	}
	offset, err := store.NextOffset(context.Background(), "auto-created", 0)
	if err != nil {
		t.Fatalf("NextOffset: %v", err)
	}
	if offset != 1 {
		t.Fatalf("expected offset advanced to 1 got %d", offset)
	}
}

func TestHandleCreateDeleteTopics(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	createReq := &protocol.CreateTopicsRequest{
		Topics: []protocol.CreateTopicConfig{
			{Name: "payments", NumPartitions: 1, ReplicationFactor: 1},
		},
	}
	respBytes, err := handler.handleCreateTopics(context.Background(), &protocol.RequestHeader{CorrelationID: 42}, createReq)
	if err != nil {
		t.Fatalf("handleCreateTopics: %v", err)
	}
	resp := decodeCreateTopicsResponse(t, respBytes)
	if len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != protocol.NONE {
		t.Fatalf("expected topic creation success: %#v", resp)
	}
	dupRespBytes, _ := handler.handleCreateTopics(context.Background(), &protocol.RequestHeader{CorrelationID: 43}, createReq)
	dupResp := decodeCreateTopicsResponse(t, dupRespBytes)
	if dupResp.Topics[0].ErrorCode != protocol.TOPIC_ALREADY_EXISTS {
		t.Fatalf("expected duplicate error got %d", dupResp.Topics[0].ErrorCode)
	}
	deleteReq := &protocol.DeleteTopicsRequest{TopicNames: []string{"payments", "missing"}}
	delBytes, err := handler.handleDeleteTopics(context.Background(), &protocol.RequestHeader{CorrelationID: 44}, deleteReq)
	if err != nil {
		t.Fatalf("handleDeleteTopics: %v", err)
	}
	delResp := decodeDeleteTopicsResponse(t, delBytes)
	if len(delResp.Topics) != 2 || delResp.Topics[0].ErrorCode != protocol.NONE {
		t.Fatalf("expected delete success, got %#v", delResp)
	}
	if delResp.Topics[1].ErrorCode != protocol.UNKNOWN_TOPIC_OR_PARTITION {
		t.Fatalf("expected unknown topic error got %d", delResp.Topics[1].ErrorCode)
	}
}

func TestHandleListOffsets(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	if err := store.UpdateOffsets(context.Background(), "orders", 0, 9); err != nil {
		t.Fatalf("UpdateOffsets: %v", err)
	}
	req := &protocol.ListOffsetsRequest{
		Topics: []protocol.ListOffsetsTopic{
			{
				Name:       "orders",
				Partitions: []protocol.ListOffsetsPartition{{Partition: 0, Timestamp: -1}},
			},
		},
	}
	respBytes, err := handler.handleListOffsets(context.Background(), &protocol.RequestHeader{CorrelationID: 55}, req)
	if err != nil {
		t.Fatalf("handleListOffsets: %v", err)
	}
	resp := decodeListOffsetsResponse(t, respBytes)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected list offsets response: %#v", resp)
	}
	if resp.Topics[0].Partitions[0].Offset != 10 {
		t.Fatalf("expected offset 10 got %d", resp.Topics[0].Partitions[0].Offset)
	}
}

func TestConsumerGroupLifecycle(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)

	joinReq := &protocol.JoinGroupRequest{
		GroupID:            "group-1",
		SessionTimeoutMs:   10000,
		RebalanceTimeoutMs: 10000,
		MemberID:           "",
		ProtocolType:       "consumer",
		Protocols: []protocol.JoinGroupProtocol{
			{Name: "range", Metadata: encodeJoinMetadata([]string{"orders"})},
		},
	}
	joinResp, err := handler.coordinator.JoinGroup(context.Background(), joinReq, 1)
	if err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}
	if joinResp.MemberID == "" {
		t.Fatalf("expected member id")
	}

	syncReq := &protocol.SyncGroupRequest{
		GroupID:      "group-1",
		GenerationID: joinResp.GenerationID,
		MemberID:     joinResp.MemberID,
	}
	syncResp, err := handler.coordinator.SyncGroup(context.Background(), syncReq, 2)
	if err != nil {
		t.Fatalf("SyncGroup: %v", err)
	}
	if len(syncResp.Assignment) == 0 {
		t.Fatalf("expected assignment bytes")
	}

	hbResp := handler.coordinator.Heartbeat(context.Background(), &protocol.HeartbeatRequest{
		GroupID:      "group-1",
		GenerationID: joinResp.GenerationID,
		MemberID:     joinResp.MemberID,
	}, 3)
	if hbResp.ErrorCode != protocol.NONE {
		t.Fatalf("heartbeat error: %d", hbResp.ErrorCode)
	}

	commitResp, err := handler.coordinator.OffsetCommit(context.Background(), &protocol.OffsetCommitRequest{
		GroupID:      "group-1",
		GenerationID: joinResp.GenerationID,
		MemberID:     joinResp.MemberID,
		Topics: []protocol.OffsetCommitTopic{
			{
				Name: "orders",
				Partitions: []protocol.OffsetCommitPartition{
					{Partition: 0, Offset: 5, Metadata: ""},
				},
			},
		},
	}, 4)
	if err != nil || len(commitResp.Topics) == 0 {
		t.Fatalf("offset commit failed: %v", err)
	}

	fetchResp, err := handler.coordinator.OffsetFetch(context.Background(), &protocol.OffsetFetchRequest{
		GroupID: "group-1",
		Topics: []protocol.OffsetFetchTopic{
			{
				Name: "orders",
				Partitions: []protocol.OffsetFetchPartition{
					{Partition: 0},
				},
			},
		},
	}, 5)
	if err != nil {
		t.Fatalf("OffsetFetch: %v", err)
	}
	if len(fetchResp.Topics) == 0 || len(fetchResp.Topics[0].Partitions) == 0 {
		t.Fatalf("missing offset fetch data")
	}
	if fetchResp.Topics[0].Partitions[0].Offset != 5 {
		t.Fatalf("offset mismatch, got %d", fetchResp.Topics[0].Partitions[0].Offset)
	}
}

func TestProduceBackpressureDegraded(t *testing.T) {
	t.Setenv("KAFSCALE_S3_LATENCY_WARN_MS", "1")
	t.Setenv("KAFSCALE_S3_LATENCY_CRIT_MS", "60000")
	t.Setenv("KAFSCALE_S3_ERROR_RATE_CRIT", "2.0")
	store := metadata.NewInMemoryStore(defaultMetadata())
	brokerInfo := protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}
	handler := newHandler(store, &failingS3Client{}, brokerInfo, testLogger())
	handler.s3Health.RecordUpload(2*time.Millisecond, nil)

	req := &protocol.ProduceRequest{
		Acks:      -1,
		TimeoutMs: 100,
		Topics: []protocol.ProduceTopic{
			{
				Name: "orders",
				Partitions: []protocol.ProducePartition{
					{Partition: 0, Records: testBatchBytes(0, 0, 1)},
				},
			},
		},
	}
	resp, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 9}, req)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	produceResp := decodeProduceResponse(t, resp)
	code := produceResp.Topics[0].Partitions[0].ErrorCode
	if code != protocol.REQUEST_TIMED_OUT {
		t.Fatalf("expected request timed out error, got %d", code)
	}
}

func TestProduceBackpressureUnavailable(t *testing.T) {
	t.Setenv("KAFSCALE_S3_ERROR_RATE_CRIT", "0.1")
	store := metadata.NewInMemoryStore(defaultMetadata())
	brokerInfo := protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}
	handler := newHandler(store, &failingS3Client{}, brokerInfo, testLogger())

	req := &protocol.ProduceRequest{
		Acks:      -1,
		TimeoutMs: 100,
		Topics: []protocol.ProduceTopic{
			{
				Name: "orders",
				Partitions: []protocol.ProducePartition{
					{Partition: 0, Records: testBatchBytes(0, 0, 1)},
				},
			},
		},
	}
	resp, err := handler.handleProduce(context.Background(), &protocol.RequestHeader{CorrelationID: 10}, req)
	if err != nil {
		t.Fatalf("handleProduce: %v", err)
	}
	produceResp := decodeProduceResponse(t, resp)
	code := produceResp.Topics[0].Partitions[0].ErrorCode
	if code != protocol.UNKNOWN_SERVER_ERROR {
		t.Fatalf("expected unknown server error, got %d", code)
	}
}

func TestFetchBackpressureDegraded(t *testing.T) {
	t.Setenv("KAFSCALE_S3_LATENCY_WARN_MS", "1")
	t.Setenv("KAFSCALE_S3_LATENCY_CRIT_MS", "60000")
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	handler.s3Health.RecordOperation("download", 2*time.Millisecond, nil)

	req := &protocol.FetchRequest{
		Topics: []protocol.FetchTopicRequest{
			{
				Name:       "orders",
				Partitions: []protocol.FetchPartitionRequest{{Partition: 0, FetchOffset: 0, MaxBytes: 1024}},
			},
		},
	}
	resp, err := handler.handleFetch(context.Background(), &protocol.RequestHeader{CorrelationID: 11}, req)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	fetchResp := decodeFetchResponse(t, resp)
	code := fetchResp.Topics[0].Partitions[0].ErrorCode
	if code != protocol.REQUEST_TIMED_OUT {
		t.Fatalf("expected request timed out error, got %d", code)
	}
}

func TestFetchBackpressureUnavailable(t *testing.T) {
	t.Setenv("KAFSCALE_S3_ERROR_RATE_CRIT", "0.1")
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	for i := 0; i < 2; i++ {
		handler.s3Health.RecordOperation("download", time.Millisecond, errors.New("boom"))
	}

	req := &protocol.FetchRequest{
		Topics: []protocol.FetchTopicRequest{
			{
				Name:       "orders",
				Partitions: []protocol.FetchPartitionRequest{{Partition: 0, FetchOffset: 0, MaxBytes: 1024}},
			},
		},
	}
	resp, err := handler.handleFetch(context.Background(), &protocol.RequestHeader{CorrelationID: 12}, req)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	fetchResp := decodeFetchResponse(t, resp)
	code := fetchResp.Topics[0].Partitions[0].ErrorCode
	if code != protocol.UNKNOWN_SERVER_ERROR {
		t.Fatalf("expected unknown server error, got %d", code)
	}
}

func TestStartupChecksSuccess(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}, testLogger())
	if err := handler.runStartupChecks(context.Background()); err != nil {
		t.Fatalf("expected startup checks to pass: %v", err)
	}
}

func TestStartupChecksMetadataFailure(t *testing.T) {
	store := failingMetadataStore{
		Store: metadata.NewInMemoryStore(defaultMetadata()),
		err:   errors.New("metadata offline"),
	}
	handler := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}, testLogger())
	err := handler.runStartupChecks(context.Background())
	if err == nil || !strings.Contains(err.Error(), "metadata") {
		t.Fatalf("expected metadata failure, got %v", err)
	}
}

func TestStartupChecksS3Failure(t *testing.T) {
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newHandler(store, &failingS3Client{}, protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}, testLogger())
	err := handler.runStartupChecks(context.Background())
	if err == nil || !strings.Contains(err.Error(), "s3 readiness") {
		t.Fatalf("expected s3 failure, got %v", err)
	}
}

func encodeJoinMetadata(topics []string) []byte {
	buf := make([]byte, 0)
	writeInt16 := func(v int16) {
		tmp := make([]byte, 2)
		binary.BigEndian.PutUint16(tmp, uint16(v))
		buf = append(buf, tmp...)
	}
	writeInt32 := func(v int32) {
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(v))
		buf = append(buf, tmp...)
	}
	writeInt16(0) // version
	writeInt32(int32(len(topics)))
	for _, topic := range topics {
		writeInt16(int16(len(topic)))
		buf = append(buf, []byte(topic)...)
	}
	writeInt32(0) // user data length
	return buf
}

func testBatchBytes(baseOffset int64, lastOffsetDelta int32, messageCount int32) []byte {
	data := make([]byte, 70)
	binary.BigEndian.PutUint64(data[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(data[23:27], uint32(lastOffsetDelta))
	binary.BigEndian.PutUint32(data[57:61], uint32(messageCount))
	return data
}

func newTestHandler(store metadata.Store) *handler {
	brokerInfo := protocol.MetadataBroker{NodeID: 1, Host: "localhost", Port: 19092}
	return newHandler(store, storage.NewMemoryS3Client(), brokerInfo, testLogger())
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
}

type failingS3Client struct{}

func (f *failingS3Client) UploadSegment(ctx context.Context, key string, body []byte) error {
	return errors.New("s3 unavailable")
}

func (f *failingS3Client) UploadIndex(ctx context.Context, key string, body []byte) error {
	return errors.New("s3 unavailable")
}

func (f *failingS3Client) DownloadSegment(ctx context.Context, key string, rng *storage.ByteRange) ([]byte, error) {
	return nil, errors.New("unsupported")
}

type failingMetadataStore struct {
	metadata.Store
	err error
}

func (f failingMetadataStore) Metadata(ctx context.Context, topics []string) (*metadata.ClusterMetadata, error) {
	return nil, f.err
}

func TestFranzGoProduceConsumeLocal(t *testing.T) {
	if os.Getenv("KAFSCALE_LOCAL_FRANZ") != "1" {
		t.Skip("set KAFSCALE_LOCAL_FRANZ=1 to run the local franz-go test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	clusterID := "franz-local"
	store := metadata.NewInMemoryStore(metadata.ClusterMetadata{
		ControllerID: 1,
		ClusterID:    &clusterID,
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "127.0.0.1", Port: 39092},
		},
		Topics: []protocol.MetadataTopic{
			{
				ErrorCode: 0,
				Name:      "orders",
				Partitions: []protocol.MetadataPartition{
					{
						ErrorCode:      0,
						PartitionIndex: 0,
						LeaderID:       1,
						ReplicaNodes:   []int32{1},
						ISRNodes:       []int32{1},
					},
				},
			},
		},
	})
	handler := newHandler(store, storage.NewMemoryS3Client(), protocol.MetadataBroker{NodeID: 1, Host: "127.0.0.1", Port: 39092}, testLogger())
	server := &broker.Server{Addr: "127.0.0.1:39092", Handler: handler}

	go func() {
		if err := server.ListenAndServe(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("broker listen failed: %v", err)
		}
	}()

	time.Sleep(150 * time.Millisecond)

	topic := "orders"
	producer, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:39092"),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableIdempotentWrite(),
		kgo.WithLogger(kgo.BasicLogger(io.Discard, kgo.LogLevelWarn, nil)),
	)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < 5; i++ {
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("message-%d", i))}).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:39092"),
		kgo.ConsumerGroup("franz-local-consumer"),
		kgo.ConsumeTopics(topic),
		kgo.WithLogger(kgo.BasicLogger(io.Discard, kgo.LogLevelWarn, nil)),
	)
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	received := 0
	deadline := time.Now().Add(5 * time.Second)
	for received < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for records (got %d)", received)
		}
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %+v", errs)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			received++
		})
	}
}

func decodeProduceResponse(t *testing.T, payload []byte) *protocol.ProduceResponse {
	t.Helper()
	reader := bytes.NewReader(payload)
	resp := &protocol.ProduceResponse{}
	if err := binary.Read(reader, binary.BigEndian, &resp.CorrelationID); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	var topicCount int32
	if err := binary.Read(reader, binary.BigEndian, &topicCount); err != nil {
		t.Fatalf("read topic count: %v", err)
	}
	resp.Topics = make([]protocol.ProduceTopicResponse, 0, topicCount)
	for i := 0; i < int(topicCount); i++ {
		name := readKafkaString(t, reader)
		var partCount int32
		if err := binary.Read(reader, binary.BigEndian, &partCount); err != nil {
			t.Fatalf("read partition count: %v", err)
		}
		topicResp := protocol.ProduceTopicResponse{
			Name:       name,
			Partitions: make([]protocol.ProducePartitionResponse, 0, partCount),
		}
		for j := 0; j < int(partCount); j++ {
			var part protocol.ProducePartitionResponse
			if err := binary.Read(reader, binary.BigEndian, &part.Partition); err != nil {
				t.Fatalf("read partition id: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &part.ErrorCode); err != nil {
				t.Fatalf("read error code: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &part.BaseOffset); err != nil {
				t.Fatalf("read base offset: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &part.LogAppendTimeMs); err != nil {
				t.Fatalf("read append time: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &part.LogStartOffset); err != nil {
				t.Fatalf("read start offset: %v", err)
			}
			var skip int32
			if err := binary.Read(reader, binary.BigEndian, &skip); err != nil {
				t.Fatalf("read delta: %v", err)
			}
			topicResp.Partitions = append(topicResp.Partitions, part)
		}
		resp.Topics = append(resp.Topics, topicResp)
	}
	if err := binary.Read(reader, binary.BigEndian, &resp.ThrottleMs); err != nil {
		t.Fatalf("read throttle: %v", err)
	}
	return resp
}

func readKafkaString(t *testing.T, reader *bytes.Reader) string {
	t.Helper()
	var length int16
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		t.Fatalf("read string length: %v", err)
	}
	if length < 0 {
		return ""
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		t.Fatalf("read string bytes: %v", err)
	}
	return string(buf)
}

func decodeCreateTopicsResponse(t *testing.T, payload []byte) *protocol.CreateTopicsResponse {
	t.Helper()
	reader := bytes.NewReader(payload)
	resp := &protocol.CreateTopicsResponse{}
	if err := binary.Read(reader, binary.BigEndian, &resp.CorrelationID); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	var topicCount int32
	if err := binary.Read(reader, binary.BigEndian, &topicCount); err != nil {
		t.Fatalf("read topic count: %v", err)
	}
	resp.Topics = make([]protocol.CreateTopicResult, 0, topicCount)
	for i := 0; i < int(topicCount); i++ {
		name := readKafkaString(t, reader)
		var code int16
		if err := binary.Read(reader, binary.BigEndian, &code); err != nil {
			t.Fatalf("read error code: %v", err)
		}
		msg := readKafkaString(t, reader)
		resp.Topics = append(resp.Topics, protocol.CreateTopicResult{Name: name, ErrorCode: code, ErrorMessage: msg})
	}
	return resp
}

func decodeDeleteTopicsResponse(t *testing.T, payload []byte) *protocol.DeleteTopicsResponse {
	t.Helper()
	reader := bytes.NewReader(payload)
	resp := &protocol.DeleteTopicsResponse{}
	if err := binary.Read(reader, binary.BigEndian, &resp.CorrelationID); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	var topicCount int32
	if err := binary.Read(reader, binary.BigEndian, &topicCount); err != nil {
		t.Fatalf("read topic count: %v", err)
	}
	resp.Topics = make([]protocol.DeleteTopicResult, 0, topicCount)
	for i := 0; i < int(topicCount); i++ {
		name := readKafkaString(t, reader)
		var code int16
		if err := binary.Read(reader, binary.BigEndian, &code); err != nil {
			t.Fatalf("read error code: %v", err)
		}
		msg := readKafkaString(t, reader)
		resp.Topics = append(resp.Topics, protocol.DeleteTopicResult{Name: name, ErrorCode: code, ErrorMessage: msg})
	}
	return resp
}

func decodeListOffsetsResponse(t *testing.T, payload []byte) *protocol.ListOffsetsResponse {
	t.Helper()
	reader := bytes.NewReader(payload)
	resp := &protocol.ListOffsetsResponse{}
	if err := binary.Read(reader, binary.BigEndian, &resp.CorrelationID); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	var topicCount int32
	if err := binary.Read(reader, binary.BigEndian, &topicCount); err != nil {
		t.Fatalf("read topic count: %v", err)
	}
	resp.Topics = make([]protocol.ListOffsetsTopicResponse, 0, topicCount)
	for i := 0; i < int(topicCount); i++ {
		topic := protocol.ListOffsetsTopicResponse{}
		topic.Name = readKafkaString(t, reader)
		var partCount int32
		if err := binary.Read(reader, binary.BigEndian, &partCount); err != nil {
			t.Fatalf("read partition count: %v", err)
		}
		topic.Partitions = make([]protocol.ListOffsetsPartitionResponse, 0, partCount)
		for j := 0; j < int(partCount); j++ {
			var part protocol.ListOffsetsPartitionResponse
			if err := binary.Read(reader, binary.BigEndian, &part.Partition); err != nil {
				t.Fatalf("read partition: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &part.ErrorCode); err != nil {
				t.Fatalf("read error code: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &part.Timestamp); err != nil {
				t.Fatalf("read timestamp: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &part.Offset); err != nil {
				t.Fatalf("read offset: %v", err)
			}
			topic.Partitions = append(topic.Partitions, part)
		}
		resp.Topics = append(resp.Topics, topic)
	}
	return resp
}

func decodeFetchResponse(t *testing.T, payload []byte) *protocol.FetchResponse {
	t.Helper()
	reader := bytes.NewReader(payload)
	resp := &protocol.FetchResponse{}
	if err := binary.Read(reader, binary.BigEndian, &resp.CorrelationID); err != nil {
		t.Fatalf("read correlation id: %v", err)
	}
	if err := binary.Read(reader, binary.BigEndian, &resp.ThrottleMs); err != nil {
		t.Fatalf("read throttle: %v", err)
	}
	var topicCount int32
	if err := binary.Read(reader, binary.BigEndian, &topicCount); err != nil {
		t.Fatalf("read topic count: %v", err)
	}
	resp.Topics = make([]protocol.FetchTopicResponse, 0, topicCount)
	for i := 0; i < int(topicCount); i++ {
		topic := protocol.FetchTopicResponse{}
		topic.Name = readKafkaString(t, reader)
		var partitionCount int32
		if err := binary.Read(reader, binary.BigEndian, &partitionCount); err != nil {
			t.Fatalf("read partition count: %v", err)
		}
		topic.Partitions = make([]protocol.FetchPartitionResponse, 0, partitionCount)
		for j := 0; j < int(partitionCount); j++ {
			part := protocol.FetchPartitionResponse{}
			if err := binary.Read(reader, binary.BigEndian, &part.Partition); err != nil {
				t.Fatalf("read partition: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &part.ErrorCode); err != nil {
				t.Fatalf("read error code: %v", err)
			}
			if err := binary.Read(reader, binary.BigEndian, &part.HighWatermark); err != nil {
				t.Fatalf("read watermark: %v", err)
			}
			var lastStable int64
			if err := binary.Read(reader, binary.BigEndian, &lastStable); err != nil {
				t.Fatalf("read last stable offset: %v", err)
			}
			var logStart int32
			if err := binary.Read(reader, binary.BigEndian, &logStart); err != nil {
				t.Fatalf("read log start offset: %v", err)
			}
			var recordLen int32
			if err := binary.Read(reader, binary.BigEndian, &recordLen); err != nil {
				t.Fatalf("read record length: %v", err)
			}
			if recordLen > 0 {
				buf := make([]byte, recordLen)
				if _, err := io.ReadFull(reader, buf); err != nil {
					t.Fatalf("read record bytes: %v", err)
				}
				part.RecordSet = buf
			}
			topic.Partitions = append(topic.Partitions, part)
		}
		resp.Topics = append(resp.Topics, topic)
	}
	return resp
}

func TestMetricsHandlerExposesS3Health(t *testing.T) {
	t.Setenv("KAFSCALE_S3_LATENCY_WARN_MS", "1")
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	handler.s3Health.RecordOperation("upload", 2*time.Millisecond, nil)
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.metricsHandler(rec, req)
	body := rec.Body.String()
	if !strings.Contains(body, `kafscale_s3_health_state{state="degraded"} 1`) {
		t.Fatalf("expected degraded metric, got:\n%s", body)
	}
}

func TestControlServerReportsHealth(t *testing.T) {
	t.Setenv("KAFSCALE_S3_ERROR_RATE_CRIT", "0.01")
	store := metadata.NewInMemoryStore(defaultMetadata())
	handler := newTestHandler(store)
	handler.s3Health.RecordOperation("upload", time.Millisecond, errors.New("boom"))
	srv := &controlServer{handler: handler}
	resp, err := srv.GetStatus(context.Background(), &controlpb.BrokerStatusRequest{})
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if resp.Ready {
		t.Fatalf("expected broker not ready while S3 unavailable")
	}
	if len(resp.Partitions) == 0 || resp.Partitions[0].GetState() != string(broker.S3StateUnavailable) {
		t.Fatalf("unexpected partition state: %+v", resp.Partitions)
	}
}
