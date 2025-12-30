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
