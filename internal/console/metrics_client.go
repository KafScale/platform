// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

	"github.com/novatechflow/kafscale/pkg/metadata"
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
		produceRPS            float64
		fetchRPS              float64
		adminReqTotal         float64
		adminErrTotal         float64
		adminLatencySum       float64
		adminLatencySamples   int
		healthyRank           = map[string]int{"healthy": 0, "degraded": 1, "unavailable": 2}
		selectedStateRank     = -1
		successfulBrokerCount int
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
		if snap.S3State != "" {
			if rank, ok := healthyRank[snap.S3State]; ok && rank > selectedStateRank {
				selectedStateRank = rank
				state = snap.S3State
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
	return &MetricsSnapshot{
		S3State:                 state,
		S3LatencyMS:             latencyAvg,
		ProduceRPS:              produceRPS,
		FetchRPS:                fetchRPS,
		AdminRequestsTotal:      adminReqTotal,
		AdminRequestErrorsTotal: adminErrTotal,
		AdminRequestLatencyMS:   adminLatencyAvg,
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
		produceRPS        float64
		fetchRPS          float64
		adminReqTotal     float64
		adminErrTotal     float64
		adminLatencySum   float64
		adminLatencyCount int
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
		ProduceRPS:              produceRPS,
		FetchRPS:                fetchRPS,
		AdminRequestsTotal:      adminReqTotal,
		AdminRequestErrorsTotal: adminErrTotal,
		AdminRequestLatencyMS:   adminLatencyAvg,
	}, nil
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
