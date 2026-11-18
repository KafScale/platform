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

//go:build e2e

package e2e

import (
	"reflect"
	"strings"
	"testing"
)

func TestEtcdMemberPodSelectorExcludesJobNameKey(t *testing.T) {
	sel := etcdMemberPodSelector()
	if sel != "app=kafscale-etcd,cluster=kafscale" {
		t.Fatalf("etcdMemberPodSelector() = %q", sel)
	}
	if strings.Contains(sel, "job-name") {
		t.Fatalf("selector %q still uses a job-name workaround", sel)
	}
}

func TestFilterEtcdMemberPodsExcludesSnapshotAndMaintenanceJobs(t *testing.T) {
	names := []string{
		"kafscale-etcd-0",
		"kafscale-etcd-1",
		"kafscale-etcd-2",
		"kafscale-etcd-maintenance-check-29899845-rzrj9",
		"etcd-snapshot-manual-ln6qp",
		"kafscale-etcd-maintenance-29899800-xyz",
	}
	labels := map[string]map[string]string{
		"kafscale-etcd-0": {"app": "kafscale-etcd", "cluster": "kafscale"},
		"kafscale-etcd-1": {"app": "kafscale-etcd", "cluster": "kafscale"},
		"kafscale-etcd-2": {"app": "kafscale-etcd", "cluster": "kafscale"},
		"kafscale-etcd-maintenance-check-29899845-rzrj9": {
			"app": "kafscale-etcd-job", "cluster": "kafscale", "job-name": "kafscale-etcd-maintenance-check-29899845",
		},
		"etcd-snapshot-manual-ln6qp": {
			"app": "kafscale-etcd-job", "cluster": "kafscale", "job-name": "etcd-snapshot-manual",
		},
		"kafscale-etcd-maintenance-29899800-xyz": {
			"app": "kafscale-etcd-job", "cluster": "kafscale", "job-name": "kafscale-etcd-maintenance-29899800",
		},
	}
	got := filterEtcdMemberPods(names, labels)
	want := []string{"kafscale-etcd-0", "kafscale-etcd-1", "kafscale-etcd-2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("filterEtcdMemberPods = %v, want %v", got, want)
	}
}
