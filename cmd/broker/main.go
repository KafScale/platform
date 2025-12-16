package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/alo/kafscale/pkg/broker"
	"github.com/alo/kafscale/pkg/cache"
	"github.com/alo/kafscale/pkg/metadata"
	"github.com/alo/kafscale/pkg/protocol"
	"github.com/alo/kafscale/pkg/storage"
)

type handler struct {
	apiVersions []protocol.ApiVersion
	store       metadata.Store
	s3          storage.S3Client
	cache       *cache.SegmentCache
	logs        map[string]map[int32]*storage.PartitionLog
	logMu       sync.Mutex
	logConfig   storage.PartitionLogConfig
}

func (h *handler) Handle(ctx context.Context, header *protocol.RequestHeader, req protocol.Request) ([]byte, error) {
	switch req.(type) {
	case *protocol.ApiVersionsRequest:
		return protocol.EncodeApiVersionsResponse(&protocol.ApiVersionsResponse{
			CorrelationID: header.CorrelationID,
			ErrorCode:     0,
			Versions:      h.apiVersions,
		})
	case *protocol.MetadataRequest:
		metaReq := req.(*protocol.MetadataRequest)
		meta, err := h.store.Metadata(ctx, metaReq.Topics)
		if err != nil {
			return nil, fmt.Errorf("load metadata: %w", err)
		}
		return protocol.EncodeMetadataResponse(&protocol.MetadataResponse{
			CorrelationID: header.CorrelationID,
			Brokers:       meta.Brokers,
			ClusterID:     meta.ClusterID,
			ControllerID:  meta.ControllerID,
			Topics:        meta.Topics,
		})
	case *protocol.ProduceRequest:
		return h.handleProduce(ctx, header, req.(*protocol.ProduceRequest))
	default:
		return nil, ErrUnsupportedAPI
	}
}

var ErrUnsupportedAPI = fmt.Errorf("unsupported api")

func (h *handler) handleProduce(ctx context.Context, header *protocol.RequestHeader, req *protocol.ProduceRequest) ([]byte, error) {
	topicResponses := make([]protocol.ProduceTopicResponse, 0, len(req.Topics))
	now := time.Now().UnixMilli()

	for _, topic := range req.Topics {
		partitionResponses := make([]protocol.ProducePartitionResponse, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			log := h.getPartitionLog(topic.Name, part.Partition)
			batch, err := storage.NewRecordBatchFromBytes(part.Records)
			if err != nil {
				partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
					Partition: part.Partition,
					ErrorCode: -1,
				})
				continue
			}
			result, err := log.AppendBatch(ctx, batch)
			if err != nil {
				partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
					Partition: part.Partition,
					ErrorCode: -1,
				})
				continue
			}
			partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
				Partition:       part.Partition,
				ErrorCode:       0,
				BaseOffset:      result.BaseOffset,
				LogAppendTimeMs: now,
				LogStartOffset:  0,
			})
		}
		topicResponses = append(topicResponses, protocol.ProduceTopicResponse{
			Name:       topic.Name,
			Partitions: partitionResponses,
		})
	}

	return protocol.EncodeProduceResponse(&protocol.ProduceResponse{
		CorrelationID: header.CorrelationID,
		Topics:        topicResponses,
		ThrottleMs:    0,
	})
}

func (h *handler) getPartitionLog(topic string, partition int32) *storage.PartitionLog {
	h.logMu.Lock()
	defer h.logMu.Unlock()
	partitions := h.logs[topic]
	if partitions == nil {
		partitions = make(map[int32]*storage.PartitionLog)
		h.logs[topic] = partitions
	}
	if log, ok := partitions[partition]; ok {
		return log
	}
	log := storage.NewPartitionLog(topic, partition, h.s3, h.cache, h.logConfig)
	partitions[partition] = log
	return log
}

func newHandler(store metadata.Store, s3Client storage.S3Client) *handler {
	return &handler{
		apiVersions: []protocol.ApiVersion{
			{APIKey: protocol.APIKeyApiVersion, MinVersion: 0, MaxVersion: 0},
			{APIKey: protocol.APIKeyMetadata, MinVersion: 0, MaxVersion: 0},
			{APIKey: protocol.APIKeyProduce, MinVersion: 9, MaxVersion: 9},
		},
		store: store,
		s3:    s3Client,
		cache: cache.NewSegmentCache(32 << 20),
		logs:  make(map[string]map[int32]*storage.PartitionLog),
		logConfig: storage.PartitionLogConfig{
			Buffer: storage.WriteBufferConfig{
				MaxBytes:      4 << 20,
				FlushInterval: 500 * time.Millisecond,
			},
			Segment: storage.SegmentWriterConfig{
				IndexIntervalMessages: 100,
			},
		},
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store := metadata.NewInMemoryStore(defaultMetadata())
	s3Client := buildS3Client(ctx)

	srv := &broker.Server{
		Addr:    ":19092",
		Handler: newHandler(store, s3Client),
	}
	if err := srv.ListenAndServe(ctx); err != nil {
		log.Fatalf("broker server error: %v", err)
	}
	srv.Wait()
}

func buildS3Client(ctx context.Context) storage.S3Client {
	bucket := os.Getenv("KAFSCALE_S3_BUCKET")
	region := os.Getenv("KAFSCALE_S3_REGION")
	if bucket == "" || region == "" {
		log.Printf("KAFSCALE_S3_BUCKET or region not set, using in-memory S3")
		return storage.NewMemoryS3Client()
	}
	client, err := storage.NewS3Client(ctx, storage.S3Config{
		Bucket:         bucket,
		Region:         region,
		Endpoint:       os.Getenv("KAFSCALE_S3_ENDPOINT"),
		ForcePathStyle: os.Getenv("KAFSCALE_S3_PATH_STYLE") == "true",
		KMSKeyARN:      os.Getenv("KAFSCALE_S3_KMS_ARN"),
	})
	if err != nil {
		log.Printf("failed to create S3 client (%v), falling back to in-memory", err)
		return storage.NewMemoryS3Client()
	}
	return client
}

func defaultMetadata() metadata.ClusterMetadata {
	clusterID := "kafscale-cluster"
	return metadata.ClusterMetadata{
		ControllerID: 1,
		ClusterID:    &clusterID,
		Brokers: []protocol.MetadataBroker{
			{NodeID: 1, Host: "localhost", Port: 19092},
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
	}
}
