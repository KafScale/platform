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

import corev1 "k8s.io/api/core/v1"

const workloadUID int64 = 10001

// restrictedPodSecurityContext matches the shipped broker/tools UID. Volume
// ownership must also be applied to existing PVC contents when upgrading from
// root-running etcd; OnRootMismatch can skip inaccessible descendants.
func restrictedPodSecurityContext(writableVolumes bool) *corev1.PodSecurityContext {
	uid, gid, nonRoot := workloadUID, workloadUID, true
	ctx := &corev1.PodSecurityContext{
		RunAsNonRoot: &nonRoot, RunAsUser: &uid, RunAsGroup: &gid,
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	if writableVolumes {
		group, policy := workloadUID, corev1.FSGroupChangeAlways
		ctx.FSGroup = &group
		ctx.FSGroupChangePolicy = &policy
	}
	return ctx
}

func restrictedContainerSecurityContext() *corev1.SecurityContext {
	no, yes := false, true
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: &no,
		ReadOnlyRootFilesystem:   &yes,
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// AWS CLI/Python may write temporary files and user caches. Keep those writes
// in an explicit volume rather than the image's root-owned home directory.
func snapshotTempVolume() corev1.Volume {
	return corev1.Volume{Name: "snapshot-tmp", VolumeSource: corev1.VolumeSource{
		EmptyDir: &corev1.EmptyDirVolumeSource{},
	}}
}
