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
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kafscalev1alpha1 "github.com/novatechflow/kafscale/api/v1alpha1"
)

func TestVerifySnapshotS3AccessSkipPreflight(t *testing.T) {
	t.Setenv(operatorEtcdSnapshotSkipPreflightEnv, "1")
	scheme := testScheme(t)
	cluster := &kafscalev1alpha1.KafscaleCluster{}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	r := &ClusterReconciler{
		Client: c,
		Scheme: scheme,
	}

	if err := r.verifySnapshotS3Access(context.Background(), cluster, EtcdResolution{Managed: true}); err != nil {
		t.Fatalf("expected skip preflight to bypass snapshot checks: %v", err)
	}
}
