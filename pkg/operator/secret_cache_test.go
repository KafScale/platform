// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").
package operator

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSecretCacheContainsNoValuesOrAnnotations(t *testing.T) {
	options := SecretClientOptions()
	if options.Cache == nil || len(options.Cache.DisableFor) != 1 {
		t.Fatal("Secret caching not disabled")
	}
	if _, ok := options.Cache.DisableFor[0].(*corev1.Secret); !ok {
		t.Fatal("wrong uncached type")
	}
	optionsCache := SecretCacheOptions()
	if len(optionsCache.ByObject) != 1 {
		t.Fatal("missing Secret cache transform")
	}
	original := &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}, ObjectMeta: metav1.ObjectMeta{Name: "acl", Namespace: "default", ResourceVersion: "2", Annotations: map[string]string{"kubectl.kubernetes.io/last-applied-configuration": `{"data":{"password":"secret"}}`}, Labels: map[string]string{"private": "value"}, ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "private"}}}}
	transformed, err := SecretWatchTransform(original)
	if err != nil {
		t.Fatal(err)
	}
	metadata := transformed.(*metav1.PartialObjectMetadata)
	if metadata.Name != "acl" || metadata.Namespace != "default" || metadata.ResourceVersion != "2" {
		t.Fatal("lost watch routing information")
	}
	if len(metadata.Annotations) != 0 || len(metadata.Labels) != 0 || len(metadata.ManagedFields) != 0 {
		t.Fatal("Secret metadata retains sensitive fields")
	}
	if len(original.Annotations) != 1 {
		t.Fatal("mutated input object")
	}
	other := &corev1.Pod{}
	transformed, err = SecretWatchTransform(other)
	if err != nil || transformed != other {
		t.Fatal("changed other resources")
	}
}
