package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/novatechflow/kafscale/ui"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type consoleServer struct {
	server *http.Server
}

func startConsoleServer(ctx context.Context, addr string) error {
	mux, err := newConsoleMux()
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
	go func() {
		log.Printf("console listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("console server error: %v", err)
		}
	}()
	return nil
}

func newConsoleMux() (http.Handler, error) {
	mux := http.NewServeMux()
	staticHandler, err := ui.StaticHandler()
	if err != nil {
		return nil, err
	}
	mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
	mux.Handle("/ui/", http.StripPrefix("/ui/", staticHandler))
	mux.HandleFunc("/ui/api/status", handleStatus)
	mux.HandleFunc("/ui/api/status/topics", handleCreateTopic)
	mux.HandleFunc("/ui/api/status/topics/", handleDeleteTopic)
	mux.HandleFunc("/ui/api/metrics", handleMetrics)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux, nil
}

type statusResponse struct {
	Cluster string       `json:"cluster"`
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
	Name         string `json:"name"`
	State        string `json:"state"`
	Partitions   int    `json:"partitions"`
	CPU          int    `json:"cpu"`
	Memory       int    `json:"memory"`
	Backpressure string `json:"backpressure"`
}

type component struct {
	State string `json:"state"`
}

type alert struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

type topicInfo struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	State      string `json:"state"`
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, mockClusterStatus())
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

func handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
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
			sample := map[string]any{
				"timestamp": time.Now().UnixMilli(),
				"metrics": map[string]any{
					"s3_latency_ms": 40 + rand.Intn(60),
					"produce_rps":   2000 + rand.Intn(500),
					"fetch_rps":     1800 + rand.Intn(600),
				},
			}
			b, _ := json.Marshal(sample)
			_, _ = w.Write([]byte("data: "))
			_, _ = w.Write(b)
			_, _ = w.Write([]byte("\n\n"))
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "encode error", http.StatusInternalServerError)
	}
}
