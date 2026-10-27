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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SecretClientOptions prevents typed Secret reads from creating a second,
// namespace-wide cache alongside the metadata-only event watch.
func SecretClientOptions() client.Options {
	return client.Options{Cache: &client.CacheOptions{DisableFor: []client.Object{&corev1.Secret{}}}}
}

// SecretWatchTransform keeps names and resource versions for ACL event routing.
// Annotations can contain a full last-applied Secret, so discard them too.
func SecretWatchTransform(object interface{}) (interface{}, error) {
	if metadata, ok := object.(*metav1.PartialObjectMetadata); ok {
		copy := metadata.DeepCopy()
		copy.Annotations = nil
		copy.Labels = nil
		copy.ManagedFields = nil
		return copy, nil
	}
	return object, nil
}

// SecretCacheOptions applies metadata stripping only to Secret watches.
func SecretCacheOptions() cache.Options {
	return cache.Options{ByObject: map[client.Object]cache.ByObject{&corev1.Secret{}: {Transform: SecretWatchTransform}}}
}
