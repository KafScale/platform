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

package metrics

import "github.com/prometheus/client_golang/prometheus"

const namespace = "kafscale_processor"

var (
	RecordsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "records_total",
			Help:      "Total records processed by result.",
		},
		[]string{"topic", "result"},
	)
	BatchesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "batches_total",
			Help:      "Total batches written per topic.",
		},
		[]string{"topic"},
	)
	WriteLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "write_latency_ms",
			Help:      "Write latency in milliseconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"topic"},
	)
	ErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "errors_total",
			Help:      "Total errors by stage.",
		},
		[]string{"stage"},
	)
	LastOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "last_offset",
			Help:      "Last committed offset per topic/partition.",
		},
		[]string{"topic", "partition"},
	)
	WatermarkOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "watermark_offset",
			Help:      "Watermark offset per topic/partition.",
		},
		[]string{"topic", "partition"},
	)
	WatermarkTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "watermark_timestamp_ms",
			Help:      "Watermark timestamp (ms) per topic/partition.",
		},
		[]string{"topic", "partition"},
	)
)

func init() {
	prometheus.MustRegister(
		RecordsTotal,
		BatchesTotal,
		WriteLatency,
		ErrorsTotal,
		LastOffset,
		WatermarkOffset,
		WatermarkTimestamp,
	)
}
