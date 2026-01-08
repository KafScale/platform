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

package console

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/ui"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type MetricsSnapshot struct {
	S3State                 string
	S3LatencyMS             int
	S3ErrorRate             float64
	ProduceRPS              float64
	FetchRPS                float64
	AdminRequestsTotal      float64
	AdminRequestErrorsTotal float64
	AdminRequestLatencyMS   float64
	BrokerCPUPercent        float64
	BrokerMemBytes          int64
	BrokerRuntime           map[string]BrokerRuntime
	OperatorClusters                        float64
	OperatorEtcdSnapshotAgeSeconds          float64
	OperatorEtcdSnapshotLastSuccessTS       float64
	OperatorEtcdSnapshotLastScheduleTS      float64
	OperatorEtcdSnapshotStale               float64
	OperatorEtcdSnapshotAccessOK            float64
	OperatorMetricsAvailable                bool
}

type MetricsProvider interface {
	Snapshot(ctx context.Context) (*MetricsSnapshot, error)
}

type ServerOptions struct {
	Store   metadata.Store
	Metrics MetricsProvider
	Logger  *log.Logger
	Auth    AuthConfig
}

// StartServer launches the HTTP console on the provided address. When store is
// nil, the server falls back to mock responses so the UI still functions.
func StartServer(ctx context.Context, addr string, opts ServerOptions) error {
	mux, err := NewMux(opts)
	if err != nil {
		return err
	}
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	logger := opts.Logger
	if logger == nil {
		logger = log.Default()
	}
	go func() {
		logger.Printf("console listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Printf("console server error: %v", err)
		}
	}()
	return nil
}

// NewMux constructs the console HTTP mux with the supplied dependencies.
func NewMux(opts ServerOptions) (http.Handler, error) {
	mux := http.NewServeMux()
	staticHandler, err := ui.StaticHandler()
	if err != nil {
		return nil, err
	}
	auth := newAuthManager(opts.Auth)
	mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
	mux.Handle("/ui/", http.StripPrefix("/ui/", staticHandler))

	handlers := &consoleHandlers{store: opts.Store, metrics: opts.Metrics}
	mux.HandleFunc("/ui/api/auth/config", auth.handleConfig)
	mux.HandleFunc("/ui/api/auth/session", auth.handleSession)
	mux.HandleFunc("/ui/api/auth/login", auth.handleLogin)
	mux.HandleFunc("/ui/api/auth/logout", auth.handleLogout)
	mux.HandleFunc("/ui/api/status", auth.requireAuth(handlers.handleStatus))
	mux.HandleFunc("/ui/api/status/topics", auth.requireAuth(handlers.handleCreateTopic))
	mux.HandleFunc("/ui/api/status/topics/", auth.requireAuth(handlers.handleDeleteTopic))
	mux.HandleFunc("/ui/api/metrics", auth.requireAuth(handlers.handleMetrics))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux, nil
}

type consoleHandlers struct {
	store   metadata.Store
	metrics MetricsProvider
}

type statusResponse struct {
	Cluster string       `json:"cluster"`
	ClusterID string     `json:"cluster_id,omitempty"`
	Version string       `json:"version"`
	Brokers brokerStatus `json:"brokers"`
	S3      s3Status     `json:"s3"`
	Etcd    component    `json:"etcd"`
	Alerts  []alert      `json:"alerts"`
	Topics  []topicInfo  `json:"topics"`
}

type brokerStatus struct {
	Ready   int          `json:"ready"`
	Desired int          `json:"desired"`
	Nodes   []brokerNode `json:"nodes"`
}

type s3Status struct {
	State        string `json:"state"`
	LatencyMS    int    `json:"latency_ms"`
	Backpressure string `json:"backpressure"`
}

type brokerNode struct {
	ID           int32  `json:"id"`
	Name         string `json:"name"`
	State        string `json:"state"`
	Partitions   int    `json:"partitions"`
	CPU          int    `json:"cpu"`
	Memory       int    `json:"memory"`
	Backpressure string `json:"backpressure"`
}

type BrokerRuntime struct {
	CPUPercent float64
	MemBytes   int64
}

type component struct {
	State string `json:"state"`
}

type alert struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

type topicInfo struct {
	Name              string             `json:"name"`
	Partitions        int                `json:"partitions"`
	State             string             `json:"state"`
	PartitionsDetails []partitionDetails `json:"partitions_details"`
}

type partitionDetails struct {
	ID       int32 `json:"id"`
	Leader   int32 `json:"leader"`
	Replicas int   `json:"replicas"`
	ISR      int   `json:"isr"`
}

func (h *consoleHandlers) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.store == nil {
		writeJSON(w, mockClusterStatus())
		return
	}
	meta, err := h.store.Metadata(r.Context(), nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("metadata error: %v", err), http.StatusInternalServerError)
		return
	}
	var metrics *MetricsSnapshot
	if h.metrics != nil {
		if snap, snapErr := h.metrics.Snapshot(r.Context()); snapErr == nil {
			metrics = snap
		}
	}
	writeJSON(w, statusFromMetadata(meta, metrics))
}

func (h *consoleHandlers) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Future work will invoke the operator APIs; for now this is an ack.
	w.WriteHeader(http.StatusAccepted)
}

func (h *consoleHandlers) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (h *consoleHandlers) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sample := map[string]any{"timestamp": time.Now().UnixMilli()}
			metrics := map[string]any{}
			if h.metrics != nil {
				if snap, err := h.metrics.Snapshot(ctx); err == nil && snap != nil {
					metrics["s3_latency_ms"] = snap.S3LatencyMS
					metrics["s3_error_rate"] = snap.S3ErrorRate
					metrics["produce_rps"] = snap.ProduceRPS
					metrics["fetch_rps"] = snap.FetchRPS
					metrics["admin_requests_total"] = snap.AdminRequestsTotal
					metrics["admin_errors_total"] = snap.AdminRequestErrorsTotal
					metrics["admin_latency_ms_avg"] = snap.AdminRequestLatencyMS
					if snap.OperatorMetricsAvailable {
						metrics["etcd_snapshot_age_s"] = snap.OperatorEtcdSnapshotAgeSeconds
						metrics["etcd_snapshot_last_success_ts"] = snap.OperatorEtcdSnapshotLastSuccessTS
						metrics["etcd_snapshot_last_schedule_ts"] = snap.OperatorEtcdSnapshotLastScheduleTS
						metrics["etcd_snapshot_stale"] = snap.OperatorEtcdSnapshotStale
						metrics["etcd_snapshot_access_ok"] = snap.OperatorEtcdSnapshotAccessOK
						metrics["operator_clusters"] = snap.OperatorClusters
					}
				}
			}
			if len(metrics) == 0 {
				metrics["s3_latency_ms"] = 40 + rand.Intn(60)
				metrics["s3_error_rate"] = 0.0
				metrics["produce_rps"] = 2000 + rand.Intn(500)
				metrics["fetch_rps"] = 1800 + rand.Intn(600)
				metrics["admin_requests_total"] = 0
				metrics["admin_errors_total"] = 0
				metrics["admin_latency_ms_avg"] = 0
			}
			sample["metrics"] = metrics
			b, _ := json.Marshal(sample)
			_, _ = w.Write([]byte("data: "))
			_, _ = w.Write(b)
			_, _ = w.Write([]byte("\n\n"))
			flusher.Flush()
		}
	}
}

func statusFromMetadata(meta *metadata.ClusterMetadata, metrics *MetricsSnapshot) statusResponse {
	resp := statusResponse{
		Cluster: "kafscale",
		Version: "dev",
		Brokers: brokerStatus{
			Ready:   len(meta.Brokers),
			Desired: len(meta.Brokers),
			Nodes:   make([]brokerNode, 0, len(meta.Brokers)),
		},
		S3: s3Status{
			State:        "unknown",
			LatencyMS:    0,
			Backpressure: "unknown",
		},
		Etcd: component{State: "connected"},
	}
	if metrics != nil {
		if metrics.S3State != "" {
			resp.S3.State = metrics.S3State
			resp.S3.Backpressure = metrics.S3State
		}
		if metrics.S3LatencyMS > 0 {
			resp.S3.LatencyMS = metrics.S3LatencyMS
		}
	}
	if meta.ClusterName != nil && *meta.ClusterName != "" {
		resp.Cluster = *meta.ClusterName
	} else if meta.ClusterID != nil && *meta.ClusterID != "" {
		resp.Cluster = *meta.ClusterID
	}
	if meta.ClusterID != nil && *meta.ClusterID != "" {
		resp.ClusterID = *meta.ClusterID
	}
	for _, broker := range meta.Brokers {
		var cpu int
		var memMB int
		if metrics != nil {
			if metrics.BrokerRuntime != nil {
				if runtime, ok := metrics.BrokerRuntime[broker.Host]; ok {
					if runtime.CPUPercent > 0 {
						cpu = int(runtime.CPUPercent + 0.5)
					}
					if runtime.MemBytes > 0 {
						memMB = int(runtime.MemBytes / (1024 * 1024))
					}
				}
			} else if len(meta.Brokers) == 1 {
				if metrics.BrokerCPUPercent > 0 {
					cpu = int(metrics.BrokerCPUPercent + 0.5)
				}
				if metrics.BrokerMemBytes > 0 {
					memMB = int(metrics.BrokerMemBytes / (1024 * 1024))
				}
			}
		}
		resp.Brokers.Nodes = append(resp.Brokers.Nodes, brokerNode{
			ID:           broker.NodeID,
			Name:         broker.Host,
			State:        "ready",
			Partitions:   0,
			CPU:          cpu,
			Memory:       memMB,
			Backpressure: "healthy",
		})
	}
	totalPartitions := 0
	for _, topic := range meta.Topics {
		state := "ready"
		if topic.ErrorCode != 0 {
			state = "error"
		}
		partitions := make([]partitionDetails, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			partitions = append(partitions, partitionDetails{
				ID:       part.PartitionIndex,
				Leader:   part.LeaderID,
				Replicas: len(part.ReplicaNodes),
				ISR:      len(part.ISRNodes),
			})
		}
		resp.Topics = append(resp.Topics, topicInfo{
			Name:              topic.Name,
			Partitions:        len(topic.Partitions),
			State:             state,
			PartitionsDetails: partitions,
		})
		totalPartitions += len(topic.Partitions)
	}
	if len(resp.Brokers.Nodes) > 0 {
		partitionsPer := totalPartitions / len(resp.Brokers.Nodes)
		for i := range resp.Brokers.Nodes {
			resp.Brokers.Nodes[i].Partitions = partitionsPer
		}
	}
	return resp
}

func mockClusterStatus() statusResponse {
	s3States := []string{"healthy", "degraded", "healthy", "healthy"}
	state := s3States[rand.Intn(len(s3States))]
	alerts := []alert{}
	if state != "healthy" {
		level := "warning"
		if state == "unavailable" {
			level = "critical"
		}
		alerts = append(alerts, alert{
			Level:   level,
			Message: "S3 " + state + " · operator pausing rollouts",
		})
	}
	return statusResponse{
		Cluster: "kafscale-dev",
		ClusterID: "cluster-dev-1",
		Version: "0.2.0",
		Brokers: brokerStatus{
			Ready:   2 + rand.Intn(2),
			Desired: 3,
			Nodes:   mockBrokerNodes(),
		},
		S3: s3Status{
			State:        state,
			LatencyMS:    40 + rand.Intn(60),
			Backpressure: []string{"healthy", "degraded"}[rand.Intn(2)],
		},
		Etcd:   component{State: "connected"},
		Alerts: alerts,
		Topics: []topicInfo{
			{Name: "orders", Partitions: 3, State: "ready"},
			{Name: "payments", Partitions: 2, State: "ready"},
			{Name: "audit", Partitions: 1, State: "initializing"},
		},
	}
}

func mockBrokerNodes() []brokerNode {
	nodes := []brokerNode{
		{Name: "broker-0", Partitions: 12},
		{Name: "broker-1", Partitions: 11},
		{Name: "broker-2", Partitions: 10},
	}
	states := []string{"healthy", "healthy", "degraded"}
	for i := range nodes {
		nodes[i].State = states[i%len(states)]
		nodes[i].CPU = 30 + rand.Intn(40)
		nodes[i].Memory = 40 + rand.Intn(30)
		nodes[i].Backpressure = []string{"healthy", "degraded"}[rand.Intn(2)]
	}
	return nodes
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "encode error", http.StatusInternalServerError)
	}
}
