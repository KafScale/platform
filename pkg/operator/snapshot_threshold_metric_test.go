// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"testing"

	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

func TestOperatorExportsSnapshotStaleThresholdOnEveryReplica(t *testing.T) {
	families, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "kafscale_operator_etcd_snapshot_threshold_seconds" {
			continue
		}
		metrics := family.GetMetric()
		if len(metrics) != 1 || len(metrics[0].GetLabel()) != 0 {
			t.Fatalf("want one unlabeled sample, got %v", metrics)
		}
		if got, want := metrics[0].GetGauge().GetValue(), float64(snapshotStaleAfterSeconds()); got != want {
			t.Fatalf("threshold gauge = %v, want %v", got, want)
		}
		return
	}
	t.Fatal("kafscale_operator_etcd_snapshot_threshold_seconds is not exported before any reconcile")
}
