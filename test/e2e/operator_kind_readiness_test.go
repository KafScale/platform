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
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBrokerScaleRequiresRequestedCountAndCompletedRollout(t *testing.T) {
	for _, mode := range []string{"ready", "previous_count", "unobserved", "old_revision", "not_updated", "extra_pod", "terminating", "unready", "get_error", "bad_json", "pod_error", "bad_pods"} {
		t.Run(mode, func(t *testing.T) {
			replicas := int32(1)
			sts := appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Generation: 2}, Spec: appsv1.StatefulSetSpec{Replicas: &replicas},
				Status: appsv1.StatefulSetStatus{ObservedGeneration: 2, Replicas: 1, ReadyReplicas: 1, UpdatedReplicas: 1, CurrentRevision: "new", UpdateRevision: "new"}}
			pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "demo-broker-0"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
			pods := corev1.PodList{Items: []corev1.Pod{pod}}
			switch mode {
			case "previous_count":
				replicas, sts.Status.Replicas, sts.Status.ReadyReplicas, sts.Status.UpdatedReplicas = 3, 3, 3, 3
			case "unobserved":
				sts.Status.ObservedGeneration = 1
			case "old_revision":
				sts.Status.CurrentRevision = "old"
			case "not_updated":
				sts.Status.UpdatedReplicas = 0
			case "extra_pod":
				pods.Items = append(pods.Items, pod)
			case "terminating":
				now := metav1.Now()
				pods.Items[0].DeletionTimestamp = &now
			case "unready":
				pods.Items[0].Status.Conditions = nil
			}
			calls := 0
			run := func(_ context.Context, name string, args ...string) ([]byte, error) {
				calls++
				want := []string{"-n", "test-ns", "get", "statefulset", "demo-broker", "-o", "json", "--request-timeout=5s"}
				if calls == 2 {
					want = []string{"-n", "test-ns", "get", "pods", "-l", "app=kafscale-broker,cluster=demo", "-o", "json", "--request-timeout=5s"}
				}
				if name != "kubectl" || !reflect.DeepEqual(args, want) {
					t.Fatalf("unexpected command %s %v", name, args)
				}
				if (calls == 1 && mode == "get_error") || (calls == 2 && mode == "pod_error") {
					return nil, errors.New("API unavailable")
				}
				if (calls == 1 && mode == "bad_json") || (calls == 2 && mode == "bad_pods") {
					return []byte("invalid JSON"), nil
				}
				if calls == 1 {
					return json.Marshal(sts)
				}
				return json.Marshal(pods)
			}
			err := checkBrokerReplicas(context.Background(), run, "test-ns", "demo", 1)
			if (err == nil) != (mode == "ready") {
				t.Fatalf("mode %s: readiness result %v", mode, err)
			}
		})
	}
}

func TestSingleBrokerEndpointRejectsStaleMetadata(t *testing.T) {
	for _, tc := range []struct {
		data  string
		ready bool
	}{
		{`{"Brokers":[{"Host":"127.0.0.1","Port":19092}]}`, true},
		{`{"Brokers":[{"Host":"127.0.0.1","Port":19092},{"Host":"internal","Port":9092}]}`, false},
		{`{"Brokers":[{"Host":"internal","Port":19092}]}`, false},
		{`{"Brokers":[{"Host":"127.0.0.1","Port":9092}]}`, false},
		{`{"Brokers":[]}`, false},
		{`invalid JSON`, false},
	} {
		if err := checkSingleBrokerEndpoint([]byte(tc.data), "127.0.0.1", 19092); (err == nil) != tc.ready {
			t.Fatalf("snapshot %s: %v", tc.data, err)
		}
	}
}

func TestOperatorBackendChecks(t *testing.T) {
	failure := errors.New("not healthy")
	for _, tc := range []struct {
		name             string
		replicas, failAt int
		pods, want       string
	}{
		{name: "all members and brokers", replicas: 3, pods: "demo-broker-0 demo-broker-1"},
		{name: "no etcd members", want: "no expected etcd members"},
		{name: "unhealthy etcd member", replicas: 3, failAt: 1, want: "etcd member health"},
		{name: "cannot list brokers", replicas: 3, failAt: 2, want: "list brokers"},
		{name: "no brokers", replicas: 3, want: "no broker pods"},
		{name: "second broker unready", replicas: 3, pods: "demo-broker-0 demo-broker-1", failAt: 4, want: "broker demo-broker-1 readiness"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			run := func(ctx context.Context, name string, args ...string) ([]byte, error) {
				calls++
				if name != "kubectl" {
					t.Fatal(name)
				}
				if calls == 1 {
					want := []string{"-n", "test-ns", "exec", "statefulset/demo-etcd", "--", "etcdctl",
						"--endpoints=http://demo-etcd-0.demo-etcd.test-ns.svc.cluster.local:2379,http://demo-etcd-1.demo-etcd.test-ns.svc.cluster.local:2379,http://demo-etcd-2.demo-etcd.test-ns.svc.cluster.local:2379",
						"--dial-timeout=3s", "--command-timeout=5s", "endpoint", "health"}
					if !reflect.DeepEqual(args, want) {
						t.Fatalf("etcd health command: %v", args)
					}
				} else if calls == 2 {
					if !strings.Contains(strings.Join(args, " "), "app=kafscale-broker,cluster=demo") {
						t.Fatal(args)
					}
				} else {
					pod := "demo-broker-0"
					if calls == 4 {
						pod = "demo-broker-1"
					}
					want := []string{"get", "--raw=/api/v1/namespaces/test-ns/pods/" + pod + ":9093/proxy/readyz", "--request-timeout=5s"}
					if !reflect.DeepEqual(args, want) {
						t.Fatalf("broker readiness command: %v", args)
					}
				}
				if calls == tc.failAt {
					return []byte("dependency detail"), failure
				}
				if calls == 2 {
					return []byte(tc.pods), nil
				}
				return nil, nil
			}
			err := checkOperatorBackends(context.Background(), run, "test-ns", "demo", tc.replicas)
			if tc.want == "" {
				if err != nil || calls != 4 {
					t.Fatalf("calls=%d err=%v", calls, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
			if tc.failAt > 0 && (!errors.Is(err, failure) || !strings.Contains(err.Error(), "dependency detail")) {
				t.Fatal(err)
			}
		})
	}
}

func TestWaitForOperatorBackends(t *testing.T) {
	t.Run("recovers", func(t *testing.T) {
		calls := 0
		run := func(context.Context, string, ...string) ([]byte, error) {
			calls++
			if calls == 1 {
				return nil, errors.New("member joining")
			}
			return []byte("demo-broker-0"), nil
		}
		if err := waitForOperatorBackends(context.Background(), run, "ns", "demo", 3, 5*time.Second); err != nil || calls != 4 {
			t.Fatalf("calls=%d err=%v", calls, err)
		}
	})
	t.Run("bounded and canceled", func(t *testing.T) {
		for _, cancelEarly := range []bool{false, true} {
			ctx, cancel := context.WithCancel(context.Background())
			run := func(ctx context.Context, _ string, _ ...string) ([]byte, error) {
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("no probe deadline")
				}
				if cancelEarly {
					cancel()
				}
				return nil, errors.New("still forming")
			}
			err := waitForOperatorBackends(ctx, run, "ns", "demo", 3, 20*time.Millisecond)
			cancel()
			if err == nil || !strings.Contains(err.Error(), "still forming") {
				t.Fatal(err)
			}
			if cancelEarly && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if !cancelEarly && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
		}
	})
}

func TestProduceAfterReadiness(t *testing.T) {
	failure := errors.New("unavailable")
	for _, tc := range []struct {
		name                                                 string
		readyFailAt, produceFailures, wantReady, wantProduce int
		wantError                                            bool
	}{
		{name: "success", wantReady: 1, wantProduce: 1},
		{name: "not ready", readyFailAt: 1, wantReady: 1, wantError: true},
		{name: "retry succeeds", produceFailures: 1, wantReady: 2, wantProduce: 2},
		{name: "readiness lost", readyFailAt: 2, produceFailures: 1, wantReady: 2, wantProduce: 1, wantError: true},
		{name: "only one retry", produceFailures: 2, wantReady: 2, wantProduce: 2, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readyCalls, produceCalls := 0, 0
			ready := func(context.Context) error {
				readyCalls++
				if readyCalls == tc.readyFailAt {
					return failure
				}
				return nil
			}
			produce := func(context.Context) error {
				produceCalls++
				if readyCalls != produceCalls {
					t.Fatal("produce before readiness")
				}
				if produceCalls <= tc.produceFailures {
					return failure
				}
				return nil
			}
			err := produceAfterReadiness(context.Background(), ready, produce)
			if readyCalls != tc.wantReady || produceCalls != tc.wantProduce || (err != nil) != tc.wantError {
				t.Fatalf("ready=%d produce=%d err=%v", readyCalls, produceCalls, err)
			}
			if err != nil && !errors.Is(err, failure) {
				t.Fatal(err)
			}
		})
	}
}

func TestWaitForStatefulSetPodReady(t *testing.T) {
	notFound := errors.New("exit status 1")
	t.Run("pod created after the wait starts", func(t *testing.T) {
		calls := 0
		run := func(_ context.Context, name string, args ...string) ([]byte, error) {
			calls++
			want := []string{"-n", "ns", "get", "pod", "demo-etcd-1", "-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`, "--request-timeout=10s"}
			if name != "kubectl" || !reflect.DeepEqual(args, want) {
				t.Fatalf("command: %s %v", name, args)
			}
			switch calls {
			case 1:
				return []byte(`Error from server (NotFound): pods "demo-etcd-1" not found`), notFound
			case 2:
				return nil, nil
			case 3:
				return []byte("False"), nil
			}
			return []byte("True"), nil
		}
		if err := waitForStatefulSetPodReady(context.Background(), run, "ns", "demo-etcd-1", 5*time.Second, time.Millisecond); err != nil || calls != 4 {
			t.Fatalf("calls=%d err=%v", calls, err)
		}
	})
	t.Run("bounded with last check", func(t *testing.T) {
		run := func(context.Context, string, ...string) ([]byte, error) {
			return []byte(`Error from server (NotFound): pods "demo-etcd-2" not found`), notFound
		}
		err := waitForStatefulSetPodReady(context.Background(), run, "ns", "demo-etcd-2", 20*time.Millisecond, time.Millisecond)
		if err == nil || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), `pods "demo-etcd-2" not found`) || !strings.Contains(err.Error(), "pod/demo-etcd-2 not Ready within 20ms") {
			t.Fatal(err)
		}
	})
	t.Run("not ready reports status", func(t *testing.T) {
		run := func(context.Context, string, ...string) ([]byte, error) { return []byte("False"), nil }
		err := waitForStatefulSetPodReady(context.Background(), run, "ns", "demo-etcd-0", 20*time.Millisecond, time.Millisecond)
		if err == nil || !strings.Contains(err.Error(), "last check: Ready=False") {
			t.Fatal(err)
		}
	})
}
