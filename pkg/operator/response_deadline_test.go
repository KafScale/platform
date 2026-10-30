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
	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
	"testing"
)

func TestBrokerResponseWriteTimeoutConfig(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{}
	cluster.Spec.Config.ResponseWriteTimeoutMs = 100
	container := (&ClusterReconciler{}).brokerContainer(cluster, []string{"http://etcd:2379"})
	for _, env := range container.Env {
		if env.Name == "KAFSCALE_RESPONSE_WRITE_TIMEOUT_MS" {
			if env.Value != "100" {
				t.Fatal(env.Value)
			}
			return
		}
	}
	t.Fatal("operator omitted the response deadline")
}
