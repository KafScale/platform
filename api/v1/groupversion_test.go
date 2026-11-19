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

package v1

import (
	"testing"

	"github.com/KafScale/platform/api/v1alpha1"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestAddToSchemeRegistersV1Kinds(t *testing.T) {
	if GroupVersion.String() != "kafscale.io/v1" {
		t.Fatalf("GroupVersion %s, want kafscale.io/v1", GroupVersion)
	}
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"KafscaleCluster", "KafscaleClusterList", "KafscaleTopic", "KafscaleTopicList"} {
		if !scheme.Recognizes(GroupVersion.WithKind(kind)) {
			t.Errorf("scheme does not recognize %s", GroupVersion.WithKind(kind))
		}
	}
	gvks, _, err := scheme.ObjectKinds(&KafscaleCluster{})
	if err != nil {
		t.Fatal(err)
	}
	if len(gvks) != 1 || gvks[0] != GroupVersion.WithKind("KafscaleCluster") {
		t.Fatalf("KafscaleCluster kinds %v, want only %s", gvks, GroupVersion.WithKind("KafscaleCluster"))
	}
}

func TestV1AndV1alpha1ShareTypes(t *testing.T) {
	var cluster *v1alpha1.KafscaleCluster = &KafscaleCluster{}
	var topic *v1alpha1.KafscaleTopic = &KafscaleTopic{}
	if cluster.DeepCopy() == nil || topic.DeepCopy() == nil {
		t.Fatal("DeepCopy returned nil")
	}
	alpha := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(alpha); err != nil {
		t.Fatal(err)
	}
	gvks, _, err := alpha.ObjectKinds(&KafscaleTopic{})
	if err != nil {
		t.Fatal(err)
	}
	if len(gvks) != 1 || gvks[0] != v1alpha1.GroupVersion.WithKind("KafscaleTopic") {
		t.Fatalf("v1alpha1 scheme kinds %v, want only %s", gvks, v1alpha1.GroupVersion.WithKind("KafscaleTopic"))
	}
}
