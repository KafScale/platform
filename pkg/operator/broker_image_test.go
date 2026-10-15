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
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	kafscalev1alpha1 "github.com/KafScale/platform/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestBrokerContainerImageOverride(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, tc := range []struct{ name, defaultImage, repository, tag, want string }{
		{name: "operator_default", defaultImage: "ghcr.io/kafscale/kafscale-broker:v1.6.1", want: "ghcr.io/kafscale/kafscale-broker:v1.6.1"},
		{name: "repository_and_tag", defaultImage: "default/broker:v1", repository: "registry.example.com/team/broker", tag: "v2", want: "registry.example.com/team/broker:v2"},
		{name: "repository_only", defaultImage: "default/broker:v1", repository: "registry.example.com/team/broker", want: "registry.example.com/team/broker"},
		{name: "complete_reference", defaultImage: "default/broker:v1", repository: "registry.example.com:5000/team/broker:v2", want: "registry.example.com:5000/team/broker:v2"},
		{name: "digest_pin", defaultImage: "default/broker:v1", repository: "registry.example.com:5000/team/broker@" + digest, want: "registry.example.com:5000/team/broker@" + digest},
		{name: "tag_only", defaultImage: "registry.example.com:5000/team/broker:v1", tag: "v2", want: "registry.example.com:5000/team/broker:v2"},
		{name: "tag_only_untagged_default", defaultImage: "registry.example.com:5000/team/broker", tag: "v2", want: "registry.example.com:5000/team/broker:v2"},
		{name: "tag_only_digest_default", defaultImage: "registry.example.com/team/broker@" + digest, tag: "v2", want: "registry.example.com/team/broker:v2"},
		{name: "tag_only_tagged_digest_default", defaultImage: "registry.example.com/team/broker:v1@" + digest, tag: "v2", want: "registry.example.com/team/broker:v2"},
		{name: "tag_replaces_repository_tag", defaultImage: "default/broker:v1", repository: "registry.example.com:5000/team/broker:old", tag: "v2", want: "registry.example.com:5000/team/broker:v2"},
		{name: "tag_replaces_repository_digest", defaultImage: "default/broker:v1", repository: "registry.example.com/team/broker@" + digest, tag: "v2", want: "registry.example.com/team/broker:v2"},
		{name: "trim_override", defaultImage: "default/broker:v1", repository: " custom/broker ", tag: " v2 ", want: "custom/broker:v2"},
		{name: "blank_override", defaultImage: "default/broker:v1", repository: " ", tag: " ", want: "default/broker:v1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saved := brokerImage
			brokerImage = tc.defaultImage
			t.Cleanup(func() { brokerImage = saved })
			cluster := testCluster("images", []string{"http://etcd:2379"})
			raw := fmt.Sprintf(`{"spec":{"brokers":{"image":{"repository":%q,"tag":%q}}}}`, tc.repository, tc.tag)
			if err := json.Unmarshal([]byte(raw), cluster); err != nil {
				t.Fatal(err)
			}
			r := &ClusterReconciler{}
			container := r.brokerContainer(cluster, cluster.Spec.Etcd.Endpoints)
			if container.Image != tc.want {
				t.Fatalf("image=%q, want %q", container.Image, tc.want)
			}
			if container.ImagePullPolicy != parsePullPolicy(brokerImagePullPolicy) {
				t.Fatal("image override changed pull policy")
			}
		})
	}
}

func TestReconcileBrokerImageChangesPodTemplate(t *testing.T) {
	ctx := context.Background()
	cluster := testCluster("image-pin", []string{"http://etcd:2379"})
	cluster.Spec.Brokers.Image = kafscalev1alpha1.BrokerImageSpec{Repository: "registry.example.com/team/broker", Tag: "v1"}
	other := testCluster("default-image", []string{"http://etcd:2379"})
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, other).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	reconcile := func(name, want string) {
		t.Helper()
		var current kafscalev1alpha1.KafscaleCluster
		if err := c.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: name}, &current); err != nil {
			t.Fatal(err)
		}
		if err := r.reconcileBrokerDeployment(ctx, &current, current.Spec.Etcd.Endpoints); err != nil {
			t.Fatal(err)
		}
		sts := &appsv1.StatefulSet{}
		assertFound(t, c, sts, cluster.Namespace, name+"-broker")
		if len(sts.Spec.Template.Spec.Containers) != 1 || sts.Spec.Template.Spec.Containers[0].Image != want {
			t.Fatalf("StatefulSet image: %+v, want %q", sts.Spec.Template.Spec.Containers, want)
		}
	}
	reconcile(cluster.Name, "registry.example.com/team/broker:v1")
	reconcile(other.Name, brokerImage)
	var current kafscalev1alpha1.KafscaleCluster
	if err := c.Get(ctx, client.ObjectKeyFromObject(cluster), &current); err != nil {
		t.Fatal(err)
	}
	current.Spec.Brokers.Image.Tag = "v2"
	if err := c.Update(ctx, &current); err != nil {
		t.Fatal(err)
	}
	reconcile(cluster.Name, "registry.example.com/team/broker:v2")
	reconcile(other.Name, brokerImage)
	if err := c.Get(ctx, client.ObjectKeyFromObject(cluster), &current); err != nil {
		t.Fatal(err)
	}
	current.Spec.Brokers.Image = kafscalev1alpha1.BrokerImageSpec{}
	if err := c.Update(ctx, &current); err != nil {
		t.Fatal(err)
	}
	reconcile(cluster.Name, brokerImage)
}

func TestBrokerImageSurvivesJSONAndDeepCopy(t *testing.T) {
	original := testCluster("image", nil)
	original.Spec.Brokers.Image = kafscalev1alpha1.BrokerImageSpec{Repository: "custom/broker", Tag: "v2"}
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded kafscalev1alpha1.KafscaleCluster
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Spec.Brokers.Image != original.Spec.Brokers.Image {
		t.Fatal("image lost during JSON round-trip")
	}
	copied := decoded.DeepCopy()
	if copied.Spec.Brokers.Image != decoded.Spec.Brokers.Image {
		t.Fatal("image lost during deep copy")
	}
	copied.Spec.Brokers.Image.Tag = "changed"
	if decoded.Spec.Brokers.Image.Tag != "v2" {
		t.Fatal("copy mutation changed the source")
	}
}
