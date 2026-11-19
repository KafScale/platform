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
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"sync"
)

// NewDiscoveredCompositeMetricsProvider scrapes each address of a headless
// operator metrics Service. Direct endpoint overrides use NewCompositeMetricsProvider.
func NewDiscoveredCompositeMetricsProvider(broker MetricsProvider, metricsURL string) MetricsProvider {
	provider := NewCompositeMetricsProvider(broker, metricsURL).(*compositeMetricsProvider)
	provider.resolveOperator = net.DefaultResolver.LookupHost
	// Connections are pooled per pod IP, while HTTPS still verifies the Service
	// hostname and sends it as SNI. Never disable certificate verification.
	if parsed, err := url.Parse(metricsURL); err == nil && parsed.Scheme == "https" {
		if base, ok := http.DefaultTransport.(*http.Transport); ok {
			provider.client.Transport = operatorDiscoveryTransport(base, parsed.Hostname())
		}
	}
	return provider
}

func operatorDiscoveryTransport(base *http.Transport, hostname string) *http.Transport {
	transport := base.Clone()
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}
	transport.TLSClientConfig.ServerName = hostname
	return transport
}

func (c *compositeMetricsProvider) fetchOperatorSnapshots(ctx context.Context) (*operatorSnapshot, error) {
	if c.resolveOperator == nil {
		return fetchOperatorSnapshot(ctx, c.client, c.operatorURL)
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout)
	defer cancel()
	base, err := url.Parse(c.operatorURL)
	if err != nil {
		return nil, err
	}
	if base.Hostname() == "" || (base.Scheme != "http" && base.Scheme != "https") {
		return nil, fmt.Errorf("invalid operator discovery URL")
	}
	addresses, err := c.resolveOperator(ctx, base.Hostname())
	if err != nil {
		return nil, err
	}
	port := base.Port()
	if port == "" {
		port = "80"
		if base.Scheme == "https" {
			port = "443"
		}
	}
	// Resolver answers can repeat addresses or change order between scrapes.
	addresses = uniqueOperatorAddresses(addresses)
	samples := make([]*operatorSnapshot, len(addresses))
	var wg sync.WaitGroup
	for i, address := range addresses {
		wg.Add(1)
		go func() {
			defer wg.Done()
			endpoint := *base
			endpoint.Host = net.JoinHostPort(address, port)
			sample, err := fetchOperatorSnapshotHost(ctx, c.client, endpoint.String(), base.Host)
			if err == nil {
				samples[i] = sample
			}
		}()
	}
	wg.Wait()
	var selected *operatorSnapshot
	for _, sample := range samples {
		if sample == nil {
			continue
		}
		if selected == nil || operatorSampleNewer(sample, selected) {
			selected = sample
		}
	}
	return selected, nil
}

func uniqueOperatorAddresses(addresses []string) []string {
	seen := make(map[string]bool, len(addresses))
	out := make([]string, 0, len(addresses))
	for _, address := range addresses {
		if !seen[address] {
			seen[address] = true
			out = append(out, address)
		}
	}
	sort.Strings(out)
	return out
}

func operatorSampleNewer(a, b *operatorSnapshot) bool {
	if a.EtcdSnapshotReported != b.EtcdSnapshotReported {
		return a.EtcdSnapshotReported
	}
	if a.EtcdSnapshotLastScheduleTS != b.EtcdSnapshotLastScheduleTS {
		return a.EtcdSnapshotLastScheduleTS > b.EtcdSnapshotLastScheduleTS
	}
	if a.EtcdSnapshotLastSuccessTS != b.EtcdSnapshotLastSuccessTS {
		return a.EtcdSnapshotLastSuccessTS > b.EtcdSnapshotLastSuccessTS
	}
	// During handover, prefer an unhealthy sample when timestamps are tied.
	if a.EtcdSnapshotStale != b.EtcdSnapshotStale {
		return a.EtcdSnapshotStale > b.EtcdSnapshotStale
	}
	return a.EtcdSnapshotAccessReported && (!b.EtcdSnapshotAccessReported || a.EtcdSnapshotAccessOK < b.EtcdSnapshotAccessOK)
}
