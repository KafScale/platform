// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").

package chart

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestClusterConfigMinimums(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "helm", "kafscale", "crds", "kafscaleclusters.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var crd struct {
		Spec struct {
			Versions []struct {
				Schema struct {
					OpenAPIV3Schema struct {
						Properties struct {
							Spec struct {
								Properties struct {
									Config struct {
										Properties map[string]struct {
											Minimum *float64 `json:"minimum"`
										} `json:"properties"`
									} `json:"config"`
								} `json:"properties"`
							} `json:"spec"`
						} `json:"properties"`
					} `json:"openAPIV3Schema"`
				} `json:"schema"`
			} `json:"versions"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	config := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties.Spec.Properties.Config.Properties
	for field, want := range map[string]float64{"offsetsRetentionMinutes": 0, "responseWriteTimeoutMs": 1} {
		if got := config[field].Minimum; got == nil || *got != want {
			t.Fatalf("%s minimum %v, want %v", field, got, want)
		}
	}
}
