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

func etcdMemberPodSelector() string {
	return "app=kafscale-etcd,cluster=kafscale,!job-name"
}

func filterEtcdMemberPods(names []string, labelsByName map[string]map[string]string) []string {
	members := make([]string, 0, len(names))
	for _, name := range names {
		labels := labelsByName[name]
		if _, isJob := labels["job-name"]; isJob {
			continue
		}
		members = append(members, name)
	}
	return members
}

func matchesJobNameNotEquals(labels map[string]string, value string) bool {
	got, ok := labels["job-name"]
	if !ok {
		return true
	}
	return got != value
}
