package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/novatechflow/kafscale/pkg/broker"
	"github.com/novatechflow/kafscale/pkg/cache"
	controlpb "github.com/novatechflow/kafscale/pkg/gen/control"
	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
	"github.com/novatechflow/kafscale/pkg/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultKafkaAddr      = ":19092"
	defaultKafkaPort      = 19092
	defaultMetricsAddr    = ":19093"
	defaultControlAddr    = ":19094"
	defaultMinioBucket    = "kafscale"
	defaultMinioRegion    = "us-east-1"
	defaultMinioEndpoint  = "http://127.0.0.1:9000"
	defaultMinioAccessKey = "minioadmin"
	defaultMinioSecretKey = "minioadmin"
	brokerVersion         = "dev"
)

type handler struct {
	apiVersions          []protocol.ApiVersion
	store                metadata.Store
	s3                   storage.S3Client
	cache                *cache.SegmentCache
	logs                 map[string]map[int32]*storage.PartitionLog
	logMu                sync.Mutex
	logConfig            storage.PartitionLogConfig
	coordinator          *broker.GroupCoordinator
	s3Health             *broker.S3HealthMonitor
	brokerInfo           protocol.MetadataBroker
	logger               *slog.Logger
	autoCreateTopics     bool
	autoCreatePartitions int32
	traceKafka           bool
}

func (h *handler) Handle(ctx context.Context, header *protocol.RequestHeader, req protocol.Request) ([]byte, error) {
	if h.traceKafka {
		h.logger.Debug("received request", "api_key", header.APIKey, "api_version", header.APIVersion, "correlation", header.CorrelationID, "client_id", header.ClientID)
	}
	switch req.(type) {
	case *protocol.ApiVersionsRequest:
		errorCode := protocol.NONE
		if header.APIVersion != 0 {
			errorCode = protocol.UNSUPPORTED_VERSION
		}
		resp := &protocol.ApiVersionsResponse{
			CorrelationID: header.CorrelationID,
			ErrorCode:     errorCode,
			Versions:      h.apiVersions,
		}
		if h.traceKafka {
			h.logger.Debug("api versions response", "versions", resp.Versions)
		}
		return protocol.EncodeApiVersionsResponse(resp)
	case *protocol.MetadataRequest:
		metaReq := req.(*protocol.MetadataRequest)
		if h.traceKafka {
			h.logger.Debug("metadata request", "topics", metaReq.Topics)
		}
		meta, err := h.store.Metadata(ctx, metaReq.Topics)
		if err != nil {
			return nil, fmt.Errorf("load metadata: %w", err)
		}
		resp := &protocol.MetadataResponse{
			CorrelationID: header.CorrelationID,
			Brokers:       meta.Brokers,
			ClusterID:     meta.ClusterID,
			ControllerID:  meta.ControllerID,
			Topics:        meta.Topics,
		}
		if h.traceKafka {
			topicSummaries := make([]string, 0, len(meta.Topics))
			for _, topic := range meta.Topics {
				topicSummaries = append(topicSummaries, fmt.Sprintf("%s(error=%d partitions=%d)", topic.Name, topic.ErrorCode, len(topic.Partitions)))
			}
			brokerAddrs := make([]string, 0, len(meta.Brokers))
			for _, broker := range meta.Brokers {
				brokerAddrs = append(brokerAddrs, fmt.Sprintf("%s:%d", broker.Host, broker.Port))
			}
			h.logger.Debug("metadata response", "topics", topicSummaries, "brokers", brokerAddrs)
		}
		return protocol.EncodeMetadataResponse(resp, header.APIVersion)
	case *protocol.ProduceRequest:
		return h.handleProduce(ctx, header, req.(*protocol.ProduceRequest))
	case *protocol.FetchRequest:
		return h.handleFetch(ctx, header, req.(*protocol.FetchRequest))
	case *protocol.FindCoordinatorRequest:
		resp := h.coordinator.FindCoordinatorResponse(header.CorrelationID, protocol.NONE)
		return protocol.EncodeFindCoordinatorResponse(resp)
	case *protocol.JoinGroupRequest:
		resp, err := h.coordinator.JoinGroup(ctx, req.(*protocol.JoinGroupRequest), header.CorrelationID)
		if err != nil {
			return nil, err
		}
		return protocol.EncodeJoinGroupResponse(resp)
	case *protocol.SyncGroupRequest:
		resp, err := h.coordinator.SyncGroup(ctx, req.(*protocol.SyncGroupRequest), header.CorrelationID)
		if err != nil {
			return nil, err
		}
		return protocol.EncodeSyncGroupResponse(resp)
	case *protocol.HeartbeatRequest:
		resp := h.coordinator.Heartbeat(ctx, req.(*protocol.HeartbeatRequest), header.CorrelationID)
		return protocol.EncodeHeartbeatResponse(resp)
	case *protocol.LeaveGroupRequest:
		resp := h.coordinator.LeaveGroup(ctx, req.(*protocol.LeaveGroupRequest), header.CorrelationID)
		return protocol.EncodeLeaveGroupResponse(resp)
	case *protocol.OffsetCommitRequest:
		resp, err := h.coordinator.OffsetCommit(ctx, req.(*protocol.OffsetCommitRequest), header.CorrelationID)
		if err != nil {
			return nil, err
		}
		return protocol.EncodeOffsetCommitResponse(resp)
	case *protocol.OffsetFetchRequest:
		resp, err := h.coordinator.OffsetFetch(ctx, req.(*protocol.OffsetFetchRequest), header.CorrelationID)
		if err != nil {
			return nil, err
		}
		return protocol.EncodeOffsetFetchResponse(resp)
	case *protocol.CreateTopicsRequest:
		return h.handleCreateTopics(ctx, header, req.(*protocol.CreateTopicsRequest))
	case *protocol.DeleteTopicsRequest:
		return h.handleDeleteTopics(ctx, header, req.(*protocol.DeleteTopicsRequest))
	case *protocol.ListOffsetsRequest:
		return h.handleListOffsets(ctx, header, req.(*protocol.ListOffsetsRequest))
	default:
		return nil, ErrUnsupportedAPI
	}
}

var ErrUnsupportedAPI = fmt.Errorf("unsupported api")

func (h *handler) backpressureErrorCode() int16 {
	switch h.s3Health.State() {
	case broker.S3StateDegraded:
		return protocol.REQUEST_TIMED_OUT
	case broker.S3StateUnavailable:
		return protocol.UNKNOWN_SERVER_ERROR
	default:
		return protocol.UNKNOWN_SERVER_ERROR
	}
}

func (h *handler) metricsHandler(w http.ResponseWriter, r *http.Request) {
	snap := h.s3Health.Snapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, "# HELP kafscale_s3_health_state Current broker view of S3 health.")
	fmt.Fprintln(w, "# TYPE kafscale_s3_health_state gauge")
	for _, state := range []broker.S3HealthState{broker.S3StateHealthy, broker.S3StateDegraded, broker.S3StateUnavailable} {
		value := 0
		if snap.State == state {
			value = 1
		}
		fmt.Fprintf(w, "kafscale_s3_health_state{state=%q} %d\n", state, value)
	}
	fmt.Fprintln(w, "# HELP kafscale_s3_latency_ms_avg Average S3 latency in the sliding window.")
	fmt.Fprintln(w, "# TYPE kafscale_s3_latency_ms_avg gauge")
	fmt.Fprintf(w, "kafscale_s3_latency_ms_avg %f\n", float64(snap.AvgLatency)/float64(time.Millisecond))
	fmt.Fprintln(w, "# HELP kafscale_s3_error_rate Fraction of S3 operations that failed in the sliding window.")
	fmt.Fprintln(w, "# TYPE kafscale_s3_error_rate gauge")
	fmt.Fprintf(w, "kafscale_s3_error_rate %f\n", snap.ErrorRate)
	if !snap.Since.IsZero() {
		fmt.Fprintln(w, "# HELP kafscale_s3_state_duration_seconds Seconds spent in the current S3 state.")
		fmt.Fprintln(w, "# TYPE kafscale_s3_state_duration_seconds gauge")
		fmt.Fprintf(w, "kafscale_s3_state_duration_seconds %f\n", time.Since(snap.Since).Seconds())
	}
}

func (h *handler) recordS3Op(op string, latency time.Duration, err error) {
	if h.s3Health == nil {
		return
	}
	h.s3Health.RecordOperation(op, latency, err)
}

func (h *handler) handleProduce(ctx context.Context, header *protocol.RequestHeader, req *protocol.ProduceRequest) ([]byte, error) {
	topicResponses := make([]protocol.ProduceTopicResponse, 0, len(req.Topics))
	now := time.Now().UnixMilli()

	for _, topic := range req.Topics {
		if h.traceKafka {
			h.logger.Debug("produce request received", "topic", topic.Name, "partitions", len(topic.Partitions), "acks", req.Acks, "timeout_ms", req.TimeoutMs)
		}
		partitionResponses := make([]protocol.ProducePartitionResponse, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			if h.s3Health.State() == broker.S3StateUnavailable {
				partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
					Partition: part.Partition,
					ErrorCode: protocol.UNKNOWN_SERVER_ERROR,
				})
				if h.traceKafka {
					h.logger.Debug("produce rejected due to unavailable S3", "topic", topic.Name, "partition", part.Partition)
				}
				continue
			}
			plog, err := h.getPartitionLog(ctx, topic.Name, part.Partition)
			if err != nil {
				h.logger.Error("partition log init failed", "error", err, "topic", topic.Name, "partition", part.Partition)
				partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
					Partition: part.Partition,
					ErrorCode: protocol.UNKNOWN_SERVER_ERROR,
				})
				continue
			}
			batch, err := storage.NewRecordBatchFromBytes(part.Records)
			if err != nil {
				partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
					Partition: part.Partition,
					ErrorCode: protocol.UNKNOWN_SERVER_ERROR,
				})
				if h.traceKafka {
					h.logger.Debug("produce record batch decode failed", "topic", topic.Name, "partition", part.Partition, "error", err)
				}
				continue
			}
			result, err := plog.AppendBatch(ctx, batch)
			if err != nil {
				partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
					Partition: part.Partition,
					ErrorCode: h.backpressureErrorCode(),
				})
				if h.traceKafka {
					h.logger.Debug("produce append failed", "topic", topic.Name, "partition", part.Partition, "error", err)
				}
				continue
			}
			if req.Acks == -1 {
				if err := plog.Flush(ctx); err != nil {
					h.logger.Error("flush failed", "error", err, "topic", topic.Name, "partition", part.Partition)
					partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
						Partition: part.Partition,
						ErrorCode: h.backpressureErrorCode(),
					})
					continue
				}
			}
			partitionResponses = append(partitionResponses, protocol.ProducePartitionResponse{
				Partition:       part.Partition,
				ErrorCode:       0,
				BaseOffset:      result.BaseOffset,
				LogAppendTimeMs: now,
				LogStartOffset:  0,
			})
			if h.traceKafka {
				h.logger.Debug("produce append success", "topic", topic.Name, "partition", part.Partition, "base_offset", result.BaseOffset, "last_offset", result.LastOffset)
			}
		}
		topicResponses = append(topicResponses, protocol.ProduceTopicResponse{
			Name:       topic.Name,
			Partitions: partitionResponses,
		})
	}

	if req.Acks == 0 {
		return nil, nil
	}

	return protocol.EncodeProduceResponse(&protocol.ProduceResponse{
		CorrelationID: header.CorrelationID,
		Topics:        topicResponses,
		ThrottleMs:    0,
	})
}

func (h *handler) handleCreateTopics(ctx context.Context, header *protocol.RequestHeader, req *protocol.CreateTopicsRequest) ([]byte, error) {
	results := make([]protocol.CreateTopicResult, 0, len(req.Topics))
	for _, topic := range req.Topics {
		_, err := h.store.CreateTopic(ctx, metadata.TopicSpec{
			Name:              topic.Name,
			NumPartitions:     topic.NumPartitions,
			ReplicationFactor: topic.ReplicationFactor,
		})
		result := protocol.CreateTopicResult{Name: topic.Name}
		if err != nil {
			switch {
			case errors.Is(err, metadata.ErrTopicExists):
				result.ErrorCode = protocol.TOPIC_ALREADY_EXISTS
			case errors.Is(err, metadata.ErrInvalidTopic):
				result.ErrorCode = protocol.INVALID_TOPIC_EXCEPTION
			default:
				result.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			}
			result.ErrorMessage = err.Error()
		}
		results = append(results, result)
	}
	return protocol.EncodeCreateTopicsResponse(&protocol.CreateTopicsResponse{
		CorrelationID: header.CorrelationID,
		Topics:        results,
	})
}

func (h *handler) handleDeleteTopics(ctx context.Context, header *protocol.RequestHeader, req *protocol.DeleteTopicsRequest) ([]byte, error) {
	results := make([]protocol.DeleteTopicResult, 0, len(req.TopicNames))
	for _, name := range req.TopicNames {
		result := protocol.DeleteTopicResult{Name: name}
		if err := h.store.DeleteTopic(ctx, name); err != nil {
			switch {
			case errors.Is(err, metadata.ErrUnknownTopic):
				result.ErrorCode = protocol.UNKNOWN_TOPIC_OR_PARTITION
			default:
				result.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			}
			result.ErrorMessage = err.Error()
		}
		results = append(results, result)
	}
	return protocol.EncodeDeleteTopicsResponse(&protocol.DeleteTopicsResponse{
		CorrelationID: header.CorrelationID,
		Topics:        results,
	})
}

func (h *handler) handleListOffsets(ctx context.Context, header *protocol.RequestHeader, req *protocol.ListOffsetsRequest) ([]byte, error) {
	topicResponses := make([]protocol.ListOffsetsTopicResponse, 0, len(req.Topics))
	for _, topic := range req.Topics {
		partitions := make([]protocol.ListOffsetsPartitionResponse, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			offset, err := h.store.NextOffset(ctx, topic.Name, part.Partition)
			resp := protocol.ListOffsetsPartitionResponse{Partition: part.Partition}
			if err != nil {
				resp.ErrorCode = protocol.UNKNOWN_SERVER_ERROR
			} else {
				resp.Timestamp = part.Timestamp
				resp.Offset = offset
			}
			partitions = append(partitions, resp)
		}
		topicResponses = append(topicResponses, protocol.ListOffsetsTopicResponse{
			Name:       topic.Name,
			Partitions: partitions,
		})
	}
	return protocol.EncodeListOffsetsResponse(&protocol.ListOffsetsResponse{
		CorrelationID: header.CorrelationID,
		Topics:        topicResponses,
	})
}

func (h *handler) handleFetch(ctx context.Context, header *protocol.RequestHeader, req *protocol.FetchRequest) ([]byte, error) {
	topicResponses := make([]protocol.FetchTopicResponse, 0, len(req.Topics))

	for _, topic := range req.Topics {
		partitionResponses := make([]protocol.FetchPartitionResponse, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			switch h.s3Health.State() {
			case broker.S3StateDegraded, broker.S3StateUnavailable:
				partitionResponses = append(partitionResponses, protocol.FetchPartitionResponse{
					Partition: part.Partition,
					ErrorCode: h.backpressureErrorCode(),
				})
				continue
			}
			plog, err := h.getPartitionLog(ctx, topic.Name, part.Partition)
			if err != nil {
				partitionResponses = append(partitionResponses, protocol.FetchPartitionResponse{
					Partition: part.Partition,
					ErrorCode: protocol.UNKNOWN_SERVER_ERROR,
				})
				continue
			}
			records, err := plog.Read(ctx, part.FetchOffset, part.MaxBytes)
			errorCode := int16(0)
			if err != nil {
				if errors.Is(err, storage.ErrOffsetOutOfRange) {
					errorCode = protocol.OFFSET_OUT_OF_RANGE
				} else {
					errorCode = h.backpressureErrorCode()
				}
			}
			nextOffset, offsetErr := h.store.NextOffset(ctx, topic.Name, part.Partition)
			if offsetErr != nil {
				nextOffset = 0
			}
			highWatermark := nextOffset
			if highWatermark > 0 {
				highWatermark--
			}
			var recordSet []byte
			if errorCode == 0 {
				recordSet = records
			}
			partitionResponses = append(partitionResponses, protocol.FetchPartitionResponse{
				Partition:     part.Partition,
				ErrorCode:     errorCode,
				HighWatermark: highWatermark,
				RecordSet:     recordSet,
			})
		}
		topicResponses = append(topicResponses, protocol.FetchTopicResponse{
			Name:       topic.Name,
			Partitions: partitionResponses,
		})
	}

	return protocol.EncodeFetchResponse(&protocol.FetchResponse{
		CorrelationID: header.CorrelationID,
		Topics:        topicResponses,
		ThrottleMs:    0,
	})
}

func (h *handler) ensureTopic(ctx context.Context, topic string, partition int32) error {
	desired := int32(h.autoCreatePartitions)
	if desired < partition+1 {
		desired = partition + 1
	}
	spec := metadata.TopicSpec{
		Name:              topic,
		NumPartitions:     desired,
		ReplicationFactor: 1,
	}
	_, err := h.store.CreateTopic(ctx, spec)
	if err != nil && !errors.Is(err, metadata.ErrTopicExists) {
		return err
	}
	h.logger.Info("auto-created topic", "topic", topic, "partitions", desired)
	return nil
}

func (h *handler) getPartitionLog(ctx context.Context, topic string, partition int32) (*storage.PartitionLog, error) {
	for {
		h.logMu.Lock()
		partitions := h.logs[topic]
		if partitions == nil {
			partitions = make(map[int32]*storage.PartitionLog)
			h.logs[topic] = partitions
		}
		if log, ok := partitions[partition]; ok {
			h.logMu.Unlock()
			return log, nil
		}
		nextOffset, err := h.store.NextOffset(ctx, topic, partition)
		if err != nil {
			h.logMu.Unlock()
			if errors.Is(err, metadata.ErrUnknownTopic) && h.autoCreateTopics {
				if err := h.ensureTopic(ctx, topic, partition); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		plog := storage.NewPartitionLog(topic, partition, nextOffset, h.s3, h.cache, h.logConfig, func(cbCtx context.Context, artifact *storage.SegmentArtifact) {
			if err := h.store.UpdateOffsets(cbCtx, topic, partition, artifact.LastOffset); err != nil {
				h.logger.Error("update offsets failed", "error", err, "topic", topic, "partition", partition)
			}
		}, h.recordS3Op)
		partitions[partition] = plog
		h.logMu.Unlock()
		return plog, nil
	}
}

func newHandler(store metadata.Store, s3Client storage.S3Client, brokerInfo protocol.MetadataBroker, logger *slog.Logger) *handler {
	readAhead := parseEnvInt("KAFSCALE_READAHEAD_SEGMENTS", 2)
	cacheSize := parseEnvInt("KAFSCALE_CACHE_BYTES", 32<<20)
	autoCreate := parseEnvBool("KAFSCALE_AUTO_CREATE_TOPICS", true)
	autoPartitions := int32(parseEnvInt("KAFSCALE_AUTO_CREATE_PARTITIONS", 1))
	traceKafka := parseEnvBool("KAFSCALE_TRACE_KAFKA", false)
	if autoPartitions < 1 {
		autoPartitions = 1
	}
	health := broker.NewS3HealthMonitor(broker.S3HealthConfig{
		Window:      time.Duration(parseEnvInt("KAFSCALE_S3_HEALTH_WINDOW_SEC", 60)) * time.Second,
		LatencyWarn: time.Duration(parseEnvInt("KAFSCALE_S3_LATENCY_WARN_MS", 500)) * time.Millisecond,
		LatencyCrit: time.Duration(parseEnvInt("KAFSCALE_S3_LATENCY_CRIT_MS", 3000)) * time.Millisecond,
		ErrorWarn:   parseEnvFloat("KAFSCALE_S3_ERROR_RATE_WARN", 0.2),
		ErrorCrit:   parseEnvFloat("KAFSCALE_S3_ERROR_RATE_CRIT", 0.6),
	})
	return &handler{
		apiVersions: generateApiVersions(),
		store:       store,
		s3:          s3Client,
		cache:       cache.NewSegmentCache(cacheSize),
		logs:        make(map[string]map[int32]*storage.PartitionLog),
		logConfig: storage.PartitionLogConfig{
			Buffer: storage.WriteBufferConfig{
				MaxBytes:      4 << 20,
				FlushInterval: 500 * time.Millisecond,
			},
			Segment: storage.SegmentWriterConfig{
				IndexIntervalMessages: 100,
			},
			ReadAheadSegments: readAhead,
			CacheEnabled:      true,
		},
		coordinator:          broker.NewGroupCoordinator(store, brokerInfo, nil),
		s3Health:             health,
		brokerInfo:           brokerInfo,
		logger:               logger.With("component", "handler"),
		autoCreateTopics:     autoCreate,
		autoCreatePartitions: autoPartitions,
		traceKafka:           traceKafka,
	}
}

func (h *handler) runStartupChecks(parent context.Context) error {
	timeout := time.Duration(parseEnvInt("KAFSCALE_STARTUP_TIMEOUT_SEC", 30)) * time.Second
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	h.logger.Info("running startup checks", "timeout", timeout)
	if err := h.verifyMetadata(ctx); err != nil {
		return err
	}
	if err := h.verifyS3(ctx); err != nil {
		return err
	}
	h.logger.Info("startup checks passed")
	return nil
}

func (h *handler) verifyMetadata(ctx context.Context) error {
	if _, err := h.store.Metadata(ctx, nil); err != nil {
		return fmt.Errorf("metadata readiness check failed: %w", err)
	}
	return nil
}

func (h *handler) verifyS3(ctx context.Context) error {
	payload := []byte("kafscale-startup-probe")
	probeKey := fmt.Sprintf("__health/startup_probe_%d", time.Now().UnixNano())
	backoff := 500 * time.Millisecond

	for {
		start := time.Now()
		err := h.s3.UploadSegment(ctx, probeKey, payload)
		h.recordS3Op("startup_probe", time.Since(start), err)
		if err == nil {
			return nil
		}
		h.logger.Warn("startup S3 probe failed, retrying", "error", err, "key", probeKey)
		select {
		case <-ctx.Done():
			return fmt.Errorf("s3 readiness check failed: %w", err)
		case <-time.After(backoff):
		}
	}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := newLogger()
	brokerInfo := buildBrokerInfo()
	store := buildStore(ctx, brokerInfo, logger)
	s3Client := buildS3Client(ctx, logger)
	handler := newHandler(store, s3Client, brokerInfo, logger)
	if err := handler.runStartupChecks(ctx); err != nil {
		logger.Error("startup checks failed", "error", err)
		os.Exit(1)
	}
	metricsAddr := envOrDefault("KAFSCALE_METRICS_ADDR", defaultMetricsAddr)
	controlAddr := envOrDefault("KAFSCALE_CONTROL_ADDR", defaultControlAddr)
	startMetricsServer(ctx, metricsAddr, handler, logger)
	startControlServer(ctx, controlAddr, handler, logger)
	kafkaAddr := envOrDefault("KAFSCALE_BROKER_ADDR", defaultKafkaAddr)
	srv := &broker.Server{
		Addr:    kafkaAddr,
		Handler: handler,
	}
	if err := srv.ListenAndServe(ctx); err != nil {
		logger.Error("broker server error", "error", err)
		os.Exit(1)
	}
	srv.Wait()
}

func buildS3Client(ctx context.Context, logger *slog.Logger) storage.S3Client {
	if parseEnvBool("KAFSCALE_USE_MEMORY_S3", false) {
		logger.Info("using in-memory S3 client", "env", "KAFSCALE_USE_MEMORY_S3=1")
		return storage.NewMemoryS3Client()
	}

	bucket := envOrDefault("KAFSCALE_S3_BUCKET", defaultMinioBucket)
	region := envOrDefault("KAFSCALE_S3_REGION", defaultMinioRegion)
	endpoint := envOrDefault("KAFSCALE_S3_ENDPOINT", defaultMinioEndpoint)
	forcePathStyle := parseEnvBool("KAFSCALE_S3_PATH_STYLE", true)
	kmsARN := os.Getenv("KAFSCALE_S3_KMS_ARN")
	usingDefaultMinio := bucket == defaultMinioBucket && region == defaultMinioRegion && endpoint == defaultMinioEndpoint
	accessKey := os.Getenv("KAFSCALE_S3_ACCESS_KEY")
	secretKey := os.Getenv("KAFSCALE_S3_SECRET_KEY")
	sessionToken := os.Getenv("KAFSCALE_S3_SESSION_TOKEN")
	if accessKey == "" && secretKey == "" && usingDefaultMinio {
		accessKey = defaultMinioAccessKey
		secretKey = defaultMinioSecretKey
	}
	credsProvided := accessKey != "" && secretKey != ""

	client, err := storage.NewS3Client(ctx, storage.S3Config{
		Bucket:          bucket,
		Region:          region,
		Endpoint:        endpoint,
		ForcePathStyle:  forcePathStyle,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
		KMSKeyARN:       kmsARN,
	})
	if err != nil {
		logger.Error("failed to create S3 client; using in-memory", "error", err, "bucket", bucket, "region", region, "endpoint", endpoint)
		return storage.NewMemoryS3Client()
	}

	logger.Info("using AWS-compatible S3 client", "bucket", bucket, "region", region, "endpoint", endpoint, "force_path_style", forcePathStyle, "kms_configured", kmsARN != "", "default_minio", usingDefaultMinio, "credentials_provided", credsProvided)
	return client
}

func metadataForBroker(broker protocol.MetadataBroker) metadata.ClusterMetadata {
	clusterID := "kafscale-cluster"
	return metadata.ClusterMetadata{
		ControllerID: broker.NodeID,
		ClusterID:    &clusterID,
		Brokers: []protocol.MetadataBroker{
			broker,
		},
		Topics: []protocol.MetadataTopic{
			{
				ErrorCode: 0,
				Name:      "orders",
				Partitions: []protocol.MetadataPartition{
					{
						ErrorCode:      0,
						PartitionIndex: 0,
						LeaderID:       broker.NodeID,
						ReplicaNodes:   []int32{broker.NodeID},
						ISRNodes:       []int32{broker.NodeID},
					},
				},
			},
		},
	}
}

func defaultMetadata() metadata.ClusterMetadata {
	return metadataForBroker(buildBrokerInfo())
}

func buildStore(ctx context.Context, brokerInfo protocol.MetadataBroker, logger *slog.Logger) metadata.Store {
	meta := metadataForBroker(brokerInfo)
	endpoints := strings.TrimSpace(os.Getenv("KAFSCALE_ETCD_ENDPOINTS"))
	if endpoints == "" {
		return metadata.NewInMemoryStore(meta)
	}
	cfg := metadata.EtcdStoreConfig{
		Endpoints: strings.Split(endpoints, ","),
		Username:  os.Getenv("KAFSCALE_ETCD_USERNAME"),
		Password:  os.Getenv("KAFSCALE_ETCD_PASSWORD"),
	}
	store, err := metadata.NewEtcdStore(ctx, meta, cfg)
	if err != nil {
		logger.Error("failed to initialize etcd store; using in-memory", "error", err)
		return metadata.NewInMemoryStore(meta)
	}
	logger.Info("using etcd-backed metadata store", "endpoints", cfg.Endpoints)
	return store
}

func startMetricsServer(ctx context.Context, addr string, h *handler, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", h.metricsHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "ok state=%s\n", h.s3Health.State())
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if ready, state := h.readiness(); !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "not ready state=%s\n", state)
		} else {
			fmt.Fprintf(w, "ready state=%s\n", state)
		}
	})
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server error", "error", err)
		}
	}()
}

func startControlServer(ctx context.Context, addr string, h *handler, logger *slog.Logger) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("control server listen error", "error", err)
		return
	}
	server := grpc.NewServer()
	controlpb.RegisterBrokerControlServer(server, &controlServer{handler: h})
	go func() {
		<-ctx.Done()
		done := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			server.Stop()
		}
	}()
	go func() {
		if err := server.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("control server error", "error", err)
		}
	}()
}

type controlServer struct {
	controlpb.UnimplementedBrokerControlServer
	handler *handler
}

func (s *controlServer) GetStatus(ctx context.Context, req *controlpb.BrokerStatusRequest) (*controlpb.BrokerStatusResponse, error) {
	snap := s.handler.s3Health.Snapshot()
	state := string(snap.State)
	status := &controlpb.PartitionStatus{
		Topic:          "__s3_health",
		Partition:      0,
		Leader:         true,
		State:          state,
		LogStartOffset: int64(snap.AvgLatency / time.Millisecond),
		LogEndOffset:   int64(snap.AvgLatency / time.Millisecond),
		HighWatermark:  int64(snap.ErrorRate * 1000),
	}
	ready := snap.State == broker.S3StateHealthy
	return &controlpb.BrokerStatusResponse{
		BrokerId: fmt.Sprintf("%d", s.handler.brokerInfo.NodeID),
		Version:  brokerVersion,
		Ready:    ready,
		Partitions: []*controlpb.PartitionStatus{
			status,
		},
	}, nil
}

func (s *controlServer) DrainPartitions(ctx context.Context, req *controlpb.DrainPartitionsRequest) (*controlpb.DrainPartitionsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "DrainPartitions not implemented")
}

func (s *controlServer) TriggerFlush(ctx context.Context, req *controlpb.TriggerFlushRequest) (*controlpb.TriggerFlushResponse, error) {
	return nil, status.Error(codes.Unimplemented, "TriggerFlush not implemented")
}

func (s *controlServer) StreamMetrics(stream grpc.ClientStreamingServer[controlpb.MetricsSample, controlpb.Ack]) error {
	return status.Error(codes.Unimplemented, "StreamMetrics not implemented")
}

func (h *handler) readiness() (bool, string) {
	snap := h.s3Health.Snapshot()
	return snap.State != broker.S3StateUnavailable, string(snap.State)
}

func newLogger() *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("KAFSCALE_LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	})
	return slog.New(handler).With("component", "broker")
}
func parseEnvInt(name string, fallback int) int {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		if parsed, err := strconv.Atoi(val); err == nil {
			return parsed
		}
	}
	return fallback
}

func envOrDefault(name, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		return val
	}
	return fallback
}

func parseEnvFloat(name string, fallback float64) float64 {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func parseEnvBool(name string, fallback bool) bool {
	if val := strings.TrimSpace(os.Getenv(name)); val != "" {
		switch strings.ToLower(val) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return fallback
}

type apiVersionSupport struct {
	key                    int16
	minVersion, maxVersion int16
}

func generateApiVersions() []protocol.ApiVersion {
	supported := []apiVersionSupport{
		{key: protocol.APIKeyApiVersion, minVersion: 0, maxVersion: 0},
		{key: protocol.APIKeyMetadata, minVersion: 0, maxVersion: 0},
		{key: protocol.APIKeyProduce, minVersion: 9, maxVersion: 9},
		{key: protocol.APIKeyFetch, minVersion: 13, maxVersion: 13},
		{key: protocol.APIKeyFindCoordinator, minVersion: 3, maxVersion: 3},
		{key: protocol.APIKeyListOffsets, minVersion: 0, maxVersion: 0},
		{key: protocol.APIKeyJoinGroup, minVersion: 4, maxVersion: 4},
		{key: protocol.APIKeySyncGroup, minVersion: 4, maxVersion: 4},
		{key: protocol.APIKeyHeartbeat, minVersion: 4, maxVersion: 4},
		{key: protocol.APIKeyLeaveGroup, minVersion: 4, maxVersion: 4},
		{key: protocol.APIKeyOffsetCommit, minVersion: 3, maxVersion: 3},
		{key: protocol.APIKeyOffsetFetch, minVersion: 5, maxVersion: 5},
		{key: protocol.APIKeyCreateTopics, minVersion: 0, maxVersion: 0},
		{key: protocol.APIKeyDeleteTopics, minVersion: 0, maxVersion: 0},
	}
	unsupported := []int16{
		4, 5, 6, 7,
		15, 16,
		21, 22,
		24, 25, 26,
		37,
		protocol.APIKeyDescribeConfigs,
		protocol.APIKeyDeleteGroups,
	}

	entries := make([]protocol.ApiVersion, 0, len(supported)+len(unsupported))
	for _, entry := range supported {
		entries = append(entries, protocol.ApiVersion{
			APIKey:     entry.key,
			MinVersion: entry.minVersion,
			MaxVersion: entry.maxVersion,
		})
	}
	for _, key := range unsupported {
		entries = append(entries, protocol.ApiVersion{
			APIKey:     key,
			MinVersion: -1,
			MaxVersion: -1,
		})
	}
	return entries
}

func buildBrokerInfo() protocol.MetadataBroker {
	id := parseEnvInt("KAFSCALE_BROKER_ID", 1)
	host := os.Getenv("KAFSCALE_BROKER_HOST")
	port := parseEnvInt("KAFSCALE_BROKER_PORT", defaultKafkaPort)
	if addr := strings.TrimSpace(os.Getenv("KAFSCALE_BROKER_ADDR")); addr != "" {
		parsedHost, parsedPort := parseBrokerAddr(addr)
		if parsedHost != "" {
			host = parsedHost
		}
		port = parsedPort
	}
	if host == "" {
		host = "localhost"
	}
	return protocol.MetadataBroker{
		NodeID: int32(id),
		Host:   host,
		Port:   int32(port),
	}
}

func parseBrokerAddr(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return strings.TrimSpace(addr), defaultKafkaPort
	}
	port := defaultKafkaPort
	if parsedPort, err := strconv.Atoi(portStr); err == nil {
		port = parsedPort
	}
	return host, port
}
