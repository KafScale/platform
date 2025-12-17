package operator

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	kafscalev1alpha1 "github.com/novatechflow/kafscale/api/v1alpha1"
)

var (
	operatorClusters = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kafscale_operator_clusters",
		Help: "Number of KafscaleCluster resources currently managed.",
	})
	operatorSnapshotResults = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafscale_operator_snapshot_publish_total",
		Help: "Count of metadata snapshot publish attempts labeled by result.",
	}, []string{"result"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(operatorClusters, operatorSnapshotResults)
}

func recordClusterCount(ctx context.Context, c client.Client) {
	var clusters kafscalev1alpha1.KafscaleClusterList
	if err := c.List(ctx, &clusters); err != nil {
		return
	}
	operatorClusters.Set(float64(len(clusters.Items)))
}
