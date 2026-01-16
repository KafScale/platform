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

package metrics

import "github.com/prometheus/client_golang/prometheus"

const namespace = "kafsql"

var (
	QueriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "queries_total",
			Help:      "Total queries by type and status.",
		},
		[]string{"type", "status"},
	)
	QueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "query_duration_ms",
			Help:      "Query duration in milliseconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"type", "status"},
	)
	QueryRows = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "query_rows_total",
			Help:      "Total rows returned by query type.",
		},
		[]string{"type"},
	)
	QueryBytes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "query_bytes_scanned_total",
			Help:      "Total bytes scanned by queries.",
		},
	)
	QuerySegments = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "query_segments_scanned_total",
			Help:      "Total segments scanned by queries.",
		},
	)
	QueryUnboundedRejected = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "query_unbounded_rejected_total",
			Help:      "Total unbounded queries rejected.",
		},
	)
	S3Requests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "s3_requests_total",
			Help:      "S3 requests by operation.",
		},
		[]string{"operation"},
	)
	S3Bytes = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "s3_bytes_downloaded_total",
			Help:      "Total bytes downloaded from S3.",
		},
	)
	S3Duration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "s3_request_duration_ms",
			Help:      "S3 request duration in milliseconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"operation"},
	)
	S3Errors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "s3_errors_total",
			Help:      "S3 errors by operation.",
		},
		[]string{"operation"},
	)
	ConnectionsActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "connections_active",
			Help:      "Active client connections.",
		},
	)
	ConnectionsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connections_total",
			Help:      "Total client connections accepted.",
		},
	)
	ActiveQueries = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_queries",
			Help:      "Queries currently executing.",
		},
	)
	DecodeErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "decode_errors_total",
			Help:      "Total decode errors.",
		},
	)
	InvalidJSON = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "invalid_json_total",
			Help:      "Total invalid JSON payloads.",
		},
	)
	SchemaMiss = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "schema_miss_total",
			Help:      "Total schema path misses by topic.",
		},
		[]string{"topic"},
	)
	DiscoveryCacheHits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "discovery_cache_hits_total",
			Help:      "Total discovery cache hits.",
		},
	)
	DiscoveryCacheMisses = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "discovery_cache_misses_total",
			Help:      "Total discovery cache misses.",
		},
	)
	QueryQueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "query_queue_depth",
			Help:      "Current depth of the query queue.",
		},
	)
	QueryQueueRejected = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "query_queue_rejected_total",
			Help:      "Queries rejected due to full queue.",
		},
	)
	QueryQueueTimeout = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "query_queue_timeout_total",
			Help:      "Queries that timed out waiting in the queue.",
		},
	)
	QueryQueueWait = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "query_queue_wait_ms",
			Help:      "Time spent waiting in the query queue in milliseconds.",
			Buckets:   prometheus.DefBuckets,
		},
	)
)

func init() {
	prometheus.MustRegister(
		QueriesTotal,
		QueryDuration,
		QueryRows,
		QueryBytes,
		QuerySegments,
		QueryUnboundedRejected,
		S3Requests,
		S3Bytes,
		S3Duration,
		S3Errors,
		ConnectionsActive,
		ConnectionsTotal,
		ActiveQueries,
		DecodeErrors,
		InvalidJSON,
		SchemaMiss,
		DiscoveryCacheHits,
		DiscoveryCacheMisses,
		QueryQueueDepth,
		QueryQueueRejected,
		QueryQueueTimeout,
		QueryQueueWait,
	)
}
