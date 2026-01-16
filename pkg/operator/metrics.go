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
	operatorEtcdSnapshotAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafscale_operator_etcd_snapshot_age_seconds",
		Help: "Seconds since the last successful etcd snapshot upload.",
	}, []string{"cluster"})
	operatorEtcdSnapshotLastSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafscale_operator_etcd_snapshot_last_success_timestamp",
		Help: "Unix timestamp of the last successful etcd snapshot upload.",
	}, []string{"cluster"})
	operatorEtcdSnapshotLastSchedule = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafscale_operator_etcd_snapshot_last_schedule_timestamp",
		Help: "Unix timestamp of the last scheduled etcd snapshot job.",
	}, []string{"cluster"})
	operatorEtcdSnapshotStale = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafscale_operator_etcd_snapshot_stale",
		Help: "1 if the last snapshot is older than the staleness threshold.",
	}, []string{"cluster"})
	operatorEtcdSnapshotSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafscale_operator_etcd_snapshot_success",
		Help: "1 if at least one successful etcd snapshot has been recorded.",
	}, []string{"cluster"})
	operatorEtcdSnapshotAccessOK = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafscale_operator_etcd_snapshot_access_ok",
		Help: "1 if the operator can write to the snapshot bucket.",
	}, []string{"cluster"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		operatorClusters,
		operatorSnapshotResults,
		operatorEtcdSnapshotAge,
		operatorEtcdSnapshotLastSuccess,
		operatorEtcdSnapshotLastSchedule,
		operatorEtcdSnapshotStale,
		operatorEtcdSnapshotSuccess,
		operatorEtcdSnapshotAccessOK,
	)
}

func recordClusterCount(ctx context.Context, c client.Client) {
	var clusters kafscalev1alpha1.KafscaleClusterList
	if err := c.List(ctx, &clusters); err != nil {
		return
	}
	operatorClusters.Set(float64(len(clusters.Items)))
}
