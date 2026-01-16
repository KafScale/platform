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
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/KafScale/platform/pkg/metadata"
)

type promMetricsClient struct {
	url    string
	client *http.Client
}

func NewPromMetricsClient(url string) MetricsProvider {
	return &promMetricsClient{
		url: url,
		client: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

func (c *promMetricsClient) Snapshot(ctx context.Context) (*MetricsSnapshot, error) {
	return fetchPromSnapshot(ctx, c.client, c.url)
}

type aggregatedPromMetricsClient struct {
	store       metadata.Store
	client      *http.Client
	scheme      string
	metricsPort string
	metricsPath string
	fallback    *promMetricsClient
}

type compositeMetricsProvider struct {
	broker      MetricsProvider
	operatorURL string
	client      *http.Client
}

func NewAggregatedPromMetricsClient(store metadata.Store, metricsURL string) MetricsProvider {
	client := &http.Client{Timeout: 3 * time.Second}
	scheme := "http"
	metricsPort := "9093"
	metricsPath := "/metrics"
	if metricsURL != "" {
		if parsed, err := url.Parse(metricsURL); err == nil {
			if parsed.Scheme != "" {
				scheme = parsed.Scheme
			}
			if parsed.Port() != "" {
				metricsPort = parsed.Port()
			}
			if parsed.Path != "" && parsed.Path != "/" {
				metricsPath = parsed.Path
			}
		}
	}
	var fallback *promMetricsClient
	if metricsURL != "" {
		fallback = &promMetricsClient{url: metricsURL, client: client}
	}
	return &aggregatedPromMetricsClient{
		store:       store,
		client:      client,
		scheme:      scheme,
		metricsPort: metricsPort,
		metricsPath: metricsPath,
		fallback:    fallback,
	}
}

func NewCompositeMetricsProvider(broker MetricsProvider, operatorURL string) MetricsProvider {
	return &compositeMetricsProvider{
		broker:      broker,
		operatorURL: operatorURL,
		client:      &http.Client{Timeout: 3 * time.Second},
	}
}

func (c *compositeMetricsProvider) Snapshot(ctx context.Context) (*MetricsSnapshot, error) {
	var snap *MetricsSnapshot
	if c.broker != nil {
		var err error
		snap, err = c.broker.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
	}
	if snap == nil {
		snap = &MetricsSnapshot{}
	}
	if c.operatorURL != "" {
		if op, err := fetchOperatorSnapshot(ctx, c.client, c.operatorURL); err == nil && op != nil {
			snap.OperatorClusters = op.Clusters
			snap.OperatorEtcdSnapshotAgeSeconds = op.EtcdSnapshotAgeSeconds
			snap.OperatorEtcdSnapshotLastSuccessTS = op.EtcdSnapshotLastSuccessTS
			snap.OperatorEtcdSnapshotLastScheduleTS = op.EtcdSnapshotLastScheduleTS
			snap.OperatorEtcdSnapshotStale = op.EtcdSnapshotStale
			snap.OperatorEtcdSnapshotAccessOK = op.EtcdSnapshotAccessOK
			snap.OperatorMetricsAvailable = true
		}
	}
	return snap, nil
}

func (c *aggregatedPromMetricsClient) Snapshot(ctx context.Context) (*MetricsSnapshot, error) {
	meta, err := c.store.Metadata(ctx, nil)
	if err != nil || len(meta.Brokers) == 0 {
		if c.fallback != nil {
			return c.fallback.Snapshot(ctx)
		}
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("no brokers available for metrics")
	}
	var (
		state                 string
		latencySum            int
		latencyCount          int
		errorRateSum          float64
		errorRateCount        int
		produceRPS            float64
		fetchRPS              float64
		adminReqTotal         float64
		adminErrTotal         float64
		adminLatencySum       float64
		adminLatencySamples   int
		healthyRank           = map[string]int{"healthy": 0, "degraded": 1, "unavailable": 2}
		selectedStateRank     = -1
		successfulBrokerCount int
		runtimeByHost         = make(map[string]BrokerRuntime)
	)
	for _, broker := range meta.Brokers {
		host := broker.Host
		if strings.Contains(host, ":") {
			if splitHost, _, err := net.SplitHostPort(host); err == nil && splitHost != "" {
				host = splitHost
			} else if split := strings.SplitN(host, ":", 2); len(split) > 0 && split[0] != "" {
				host = split[0]
			}
		}
		metricsURL := url.URL{
			Scheme: c.scheme,
			Host:   fmt.Sprintf("%s:%s", host, c.metricsPort),
			Path:   path.Clean(c.metricsPath),
		}
		snap, snapErr := fetchPromSnapshot(ctx, c.client, metricsURL.String())
		if snapErr != nil || snap == nil {
			continue
		}
		successfulBrokerCount++
		if snap.S3LatencyMS > 0 {
			latencySum += snap.S3LatencyMS
			latencyCount++
		}
		if snap.S3ErrorRate > 0 {
			errorRateSum += snap.S3ErrorRate
			errorRateCount++
		}
		if snap.S3State != "" {
			if rank, ok := healthyRank[snap.S3State]; ok && rank > selectedStateRank {
				selectedStateRank = rank
				state = snap.S3State
			}
		}
		if snap.BrokerCPUPercent > 0 || snap.BrokerMemBytes > 0 {
			runtimeByHost[broker.Host] = BrokerRuntime{
				CPUPercent: snap.BrokerCPUPercent,
				MemBytes:   snap.BrokerMemBytes,
			}
			if host != broker.Host {
				runtimeByHost[host] = BrokerRuntime{
					CPUPercent: snap.BrokerCPUPercent,
					MemBytes:   snap.BrokerMemBytes,
				}
			}
		}
		produceRPS += snap.ProduceRPS
		fetchRPS += snap.FetchRPS
		adminReqTotal += snap.AdminRequestsTotal
		adminErrTotal += snap.AdminRequestErrorsTotal
		if snap.AdminRequestLatencyMS > 0 {
			adminLatencySum += snap.AdminRequestLatencyMS
			adminLatencySamples++
		}
	}
	if successfulBrokerCount == 0 {
		if c.fallback != nil {
			return c.fallback.Snapshot(ctx)
		}
		return nil, fmt.Errorf("no broker metrics available")
	}
	latencyAvg := 0
	if latencyCount > 0 {
		latencyAvg = latencySum / latencyCount
	}
	adminLatencyAvg := 0.0
	if adminLatencySamples > 0 {
		adminLatencyAvg = adminLatencySum / float64(adminLatencySamples)
	}
	errorRateAvg := 0.0
	if errorRateCount > 0 {
		errorRateAvg = errorRateSum / float64(errorRateCount)
	}
	return &MetricsSnapshot{
		S3State:                 state,
		S3LatencyMS:             latencyAvg,
		S3ErrorRate:             errorRateAvg,
		ProduceRPS:              produceRPS,
		FetchRPS:                fetchRPS,
		AdminRequestsTotal:      adminReqTotal,
		AdminRequestErrorsTotal: adminErrTotal,
		AdminRequestLatencyMS:   adminLatencyAvg,
		BrokerRuntime:           runtimeByHost,
	}, nil
}

type operatorSnapshot struct {
	Clusters                     float64
	EtcdSnapshotAgeSeconds       float64
	EtcdSnapshotLastSuccessTS    float64
	EtcdSnapshotLastScheduleTS   float64
	EtcdSnapshotStale            float64
	EtcdSnapshotAccessOK         float64
}

func fetchOperatorSnapshot(ctx context.Context, client *http.Client, metricsURL string) (*operatorSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics request failed: %s", resp.Status)
	}
	var (
		clusters           float64
		ageMax             float64
		lastSuccessMax     float64
		lastScheduleMax    float64
		staleMax           float64
		accessMin          = 1.0
		accessSeen         bool
	)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "kafscale_operator_clusters"):
			if val, ok := parsePromSample(line); ok {
				clusters = val
			}
		case strings.HasPrefix(line, "kafscale_operator_etcd_snapshot_age_seconds"):
			if val, ok := parsePromSample(line); ok && val > ageMax {
				ageMax = val
			}
		case strings.HasPrefix(line, "kafscale_operator_etcd_snapshot_last_success_timestamp"):
			if val, ok := parsePromSample(line); ok && val > lastSuccessMax {
				lastSuccessMax = val
			}
		case strings.HasPrefix(line, "kafscale_operator_etcd_snapshot_last_schedule_timestamp"):
			if val, ok := parsePromSample(line); ok && val > lastScheduleMax {
				lastScheduleMax = val
			}
		case strings.HasPrefix(line, "kafscale_operator_etcd_snapshot_stale"):
			if val, ok := parsePromSample(line); ok && val > staleMax {
				staleMax = val
			}
		case strings.HasPrefix(line, "kafscale_operator_etcd_snapshot_access_ok"):
			if val, ok := parsePromSample(line); ok {
				if !accessSeen || val < accessMin {
					accessMin = val
				}
				accessSeen = true
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if !accessSeen {
		accessMin = 0
	}
	return &operatorSnapshot{
		Clusters:                   clusters,
		EtcdSnapshotAgeSeconds:     ageMax,
		EtcdSnapshotLastSuccessTS:  lastSuccessMax,
		EtcdSnapshotLastScheduleTS: lastScheduleMax,
		EtcdSnapshotStale:          staleMax,
		EtcdSnapshotAccessOK:       accessMin,
	}, nil
}

func fetchPromSnapshot(ctx context.Context, client *http.Client, metricsURL string) (*MetricsSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics request failed: %s", resp.Status)
	}
	var (
		state             string
		latencyMS         int
		errorRate         float64
		produceRPS        float64
		fetchRPS          float64
		adminReqTotal     float64
		adminErrTotal     float64
		adminLatencySum   float64
		adminLatencyCount int
		brokerCPU         float64
		memAllocBytes     int64
		memHeapBytes      int64
	)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "kafscale_s3_health_state"):
			if val, ok := parsePromSample(line); ok && val == 1 {
				if parsedState, ok := parseStateLabel(line); ok {
					state = parsedState
				}
			}
		case strings.HasPrefix(line, "kafscale_s3_latency_ms_avg"):
			if val, ok := parsePromSample(line); ok {
				latencyMS = int(val)
			}
		case strings.HasPrefix(line, "kafscale_s3_error_rate"):
			if val, ok := parsePromSample(line); ok {
				errorRate = val
			}
		case strings.HasPrefix(line, "kafscale_produce_rps"):
			if val, ok := parsePromSample(line); ok {
				produceRPS = val
			}
		case strings.HasPrefix(line, "kafscale_fetch_rps"):
			if val, ok := parsePromSample(line); ok {
				fetchRPS = val
			}
		case strings.HasPrefix(line, "kafscale_admin_requests_total"):
			if val, ok := parsePromSample(line); ok {
				adminReqTotal += val
			}
		case strings.HasPrefix(line, "kafscale_admin_request_errors_total"):
			if val, ok := parsePromSample(line); ok {
				adminErrTotal += val
			}
		case strings.HasPrefix(line, "kafscale_admin_request_latency_ms_avg"):
			if val, ok := parsePromSample(line); ok {
				adminLatencySum += val
				adminLatencyCount++
			}
		case strings.HasPrefix(line, "kafscale_broker_cpu_percent"):
			if val, ok := parsePromSample(line); ok {
				brokerCPU = val
			}
		case strings.HasPrefix(line, "kafscale_broker_mem_alloc_bytes"):
			if val, ok := parsePromSample(line); ok {
				memAllocBytes = int64(val)
			}
		case strings.HasPrefix(line, "kafscale_broker_heap_inuse_bytes"):
			if val, ok := parsePromSample(line); ok {
				memHeapBytes = int64(val)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	adminLatencyAvg := 0.0
	if adminLatencyCount > 0 {
		adminLatencyAvg = adminLatencySum / float64(adminLatencyCount)
	}
	return &MetricsSnapshot{
		S3State:                 state,
		S3LatencyMS:             latencyMS,
		S3ErrorRate:             errorRate,
		ProduceRPS:              produceRPS,
		FetchRPS:                fetchRPS,
		AdminRequestsTotal:      adminReqTotal,
		AdminRequestErrorsTotal: adminErrTotal,
		AdminRequestLatencyMS:   adminLatencyAvg,
		BrokerCPUPercent:        brokerCPU,
		BrokerMemBytes:          pickMemBytes(memHeapBytes, memAllocBytes),
	}, nil
}

func pickMemBytes(heapBytes, allocBytes int64) int64 {
	if heapBytes > 0 {
		return heapBytes
	}
	return allocBytes
}

func parsePromSample(line string) (float64, bool) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return 0, false
	}
	last := parts[len(parts)-1]
	val, err := strconv.ParseFloat(last, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

func parseStateLabel(line string) (string, bool) {
	start := strings.Index(line, `state="`)
	if start == -1 {
		return "", false
	}
	start += len(`state="`)
	end := strings.Index(line[start:], `"`)
	if end == -1 {
		return "", false
	}
	return line[start : start+end], true
}
