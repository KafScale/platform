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
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
)

func TestWhitespaceCredentialsSecretRefIsNoReference(t *testing.T) {
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			S3:       kafscalev1alpha1.S3Spec{Bucket: "bucket", Region: "us-east-1", CredentialsSecretRef: " \t "},
			LfsProxy: kafscalev1alpha1.LfsProxySpec{Enabled: true},
		},
	}
	scheme := testScheme(t)
	r := &ClusterReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build(), Scheme: scheme}
	if broker := r.brokerContainer(cluster, []string{"http://etcd:2379"}); len(broker.EnvFrom) != 0 {
		t.Fatalf("broker container references secret %+v, want no reference", broker.EnvFrom)
	}
	for _, env := range r.lfsProxyContainer(cluster, []string{"http://etcd:2379"}).Env {
		if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
			t.Fatalf("LFS proxy env %s references secret %q, want no reference", env.Name, env.ValueFrom.SecretKeyRef.Name)
		}
	}
	if checksum, ok := r.s3CredentialsChecksum(context.Background(), cluster); checksum != "" || !ok {
		t.Fatalf("s3 credentials checksum = %q, %v, want no checksum", checksum, ok)
	}
}
