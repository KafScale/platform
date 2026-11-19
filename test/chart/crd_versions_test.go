// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").

package chart

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

type crdVersion struct {
	Name               string         `json:"name"`
	Served             bool           `json:"served"`
	Storage            bool           `json:"storage"`
	Deprecated         bool           `json:"deprecated"`
	DeprecationWarning string         `json:"deprecationWarning"`
	Schema             map[string]any `json:"schema"`
	Subresources       map[string]any `json:"subresources"`
}

func TestCRDsServeV1AndDeprecatedV1alpha1(t *testing.T) {
	for file, kind := range map[string]string{"kafscaleclusters.yaml": "KafscaleCluster", "kafscaletopics.yaml": "KafscaleTopic"} {
		data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "helm", "kafscale", "crds", file))
		if err != nil {
			t.Fatal(err)
		}
		var crd struct {
			Spec struct {
				Group      string `json:"group"`
				Conversion struct {
					Strategy string `json:"strategy"`
				} `json:"conversion"`
				Versions []crdVersion `json:"versions"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal(data, &crd); err != nil {
			t.Fatal(err)
		}
		if crd.Spec.Group != "kafscale.io" {
			t.Errorf("%s group %q, want kafscale.io", file, crd.Spec.Group)
		}
		if crd.Spec.Conversion.Strategy != "None" {
			t.Errorf("%s conversion strategy %q, want None", file, crd.Spec.Conversion.Strategy)
		}
		if len(crd.Spec.Versions) != 2 {
			t.Fatalf("%s has %d versions, want v1 and v1alpha1", file, len(crd.Spec.Versions))
		}
		v1, alpha := crd.Spec.Versions[0], crd.Spec.Versions[1]
		if v1.Name != "v1" || !v1.Served || !v1.Storage || v1.Deprecated {
			t.Errorf("%s first version %q must be v1, served, stored and not deprecated", file, v1.Name)
		}
		if alpha.Name != "v1alpha1" || !alpha.Served || alpha.Storage || !alpha.Deprecated {
			t.Errorf("%s v1alpha1 must be served, deprecated and not stored", file)
		}
		for _, want := range []string{"kafscale.io/v1alpha1 " + kind + " is deprecated", "kafscale.io/v1 " + kind, "future release"} {
			if !strings.Contains(alpha.DeprecationWarning, want) {
				t.Errorf("%s deprecationWarning %q does not contain %q", file, alpha.DeprecationWarning, want)
			}
		}
		if !reflect.DeepEqual(v1.Schema, alpha.Schema) {
			t.Errorf("%s v1 and v1alpha1 schemas differ; conversion strategy None needs identical schemas", file)
		}
		if !reflect.DeepEqual(v1.Subresources, alpha.Subresources) {
			t.Errorf("%s v1 and v1alpha1 subresources differ", file)
		}
	}
}
