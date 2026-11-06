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
	if sel != "app=kafscale-etcd,cluster=kafscale,!job-name" {
		t.Fatalf("etcdMemberPodSelector() = %q", sel)
	}
	if strings.Contains(sel, "job-name!=") {
		t.Fatalf("selector %q uses job-name inequality and would include other Jobs", sel)
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
			"app": "kafscale-etcd", "job-name": "kafscale-etcd-maintenance-check-29899845",
		},
		"etcd-snapshot-manual-ln6qp": {
			"app": "kafscale-etcd", "job-name": "etcd-snapshot-manual",
		},
		"kafscale-etcd-maintenance-29899800-xyz": {
			"app": "kafscale-etcd", "job-name": "kafscale-etcd-maintenance-29899800",
		},
	}
	got := filterEtcdMemberPods(names, labels)
	want := []string{"kafscale-etcd-0", "kafscale-etcd-1", "kafscale-etcd-2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("filterEtcdMemberPods = %v, want %v", got, want)
	}
}

func TestJobNameInequalityIncludesOtherEtcdJobs(t *testing.T) {
	manual := map[string]string{"app": "kafscale-etcd", "job-name": "etcd-snapshot-manual"}
	check := map[string]string{"app": "kafscale-etcd", "job-name": "kafscale-etcd-maintenance-check-29899845"}
	member := map[string]string{"app": "kafscale-etcd", "cluster": "kafscale"}
	if matchesJobNameNotEquals(manual, "etcd-snapshot-manual") {
		t.Fatal("job-name!=etcd-snapshot-manual unexpectedly matched the manual snapshot Job")
	}
	if !matchesJobNameNotEquals(check, "etcd-snapshot-manual") {
		t.Fatal("job-name!=etcd-snapshot-manual must match maintenance-check Jobs")
	}
	if !matchesJobNameNotEquals(member, "etcd-snapshot-manual") {
		t.Fatal("job-name!=etcd-snapshot-manual must match StatefulSet members")
	}
	if _, ok := check["job-name"]; !ok {
		t.Fatal("maintenance-check fixture missing job-name")
	}
}
