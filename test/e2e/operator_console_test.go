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
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	stdruntime "runtime"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kafscalev1alpha1 "github.com/novatechflow/kafscale/api/v1alpha1"
	consolepkg "github.com/novatechflow/kafscale/internal/console"
	"github.com/novatechflow/kafscale/pkg/metadata"
	"github.com/novatechflow/kafscale/pkg/operator"
)

func TestOperatorConsoleEndToEnd(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	e, endpoints := startEmbeddedEtcd(t)
	defer e.Close()

	replicas := int32(2)
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "production",
			Namespace: "default",
			UID:       "cluster-uid-console",
		},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{
				Replicas: &replicas,
			},
			S3: kafscalev1alpha1.S3Spec{
				Bucket: "test",
				Region: "us-east-1",
			},
			Etcd: kafscalev1alpha1.EtcdSpec{
				Endpoints: endpoints,
			},
		},
	}
	topic := &kafscalev1alpha1.KafscaleTopic{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orders",
			Namespace: "default",
		},
		Spec: kafscalev1alpha1.KafscaleTopicSpec{
			ClusterRef: cluster.Name,
			Partitions: 3,
		},
	}

	scheme := k8sruntime.NewScheme()
	utilruntime.Must(kafscalev1alpha1.AddToScheme(scheme))
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, topic).
		Build()

	publisher := operator.NewSnapshotPublisher(fakeClient)
	if err := publisher.Publish(ctx, cluster, endpoints); err != nil {
		t.Fatalf("publish snapshot: %v", err)
	}

	store, err := metadata.NewEtcdStore(ctx, metadata.ClusterMetadata{}, metadata.EtcdStoreConfig{
		Endpoints: endpoints,
	})
	if err != nil {
		t.Fatalf("create etcd store: %v", err)
	}

	consoleAddr := "127.0.0.1:48080"
	releasePort(t, consoleAddr)
	consoleCtx, consoleCancel := context.WithCancel(ctx)
	defer consoleCancel()
	logger := log.New(io.Discard, "", 0)
	authUser := "demo"
	authPass := "demo"
	if err := consolepkg.StartServer(consoleCtx, consoleAddr, consolepkg.ServerOptions{
		Store:  store,
		Logger: logger,
		Auth: consolepkg.AuthConfig{
			Username: authUser,
			Password: authPass,
		},
	}); err != nil {
		t.Fatalf("start console: %v", err)
	}
	consoleURL := fmt.Sprintf("http://%s", consoleAddr)

	waitForHTTP(t, consoleURL+"/healthz", 5*time.Second)

	resp, err := http.Get(consoleURL + "/ui/")
	if err != nil {
		t.Fatalf("GET /ui/: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected ui status: %d", resp.StatusCode)
	}

	sessionCookie := loginConsole(t, consoleURL, authUser, authPass)
	statusReq, err := http.NewRequest(http.MethodGet, consoleURL+"/ui/api/status", nil)
	if err != nil {
		t.Fatalf("build status request: %v", err)
	}
	statusReq.AddCookie(sessionCookie)
	statusResp, err := http.DefaultClient.Do(statusReq)
	if err != nil {
		t.Fatalf("GET status: %v", err)
	}
	defer statusResp.Body.Close()
	if statusResp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %d", statusResp.StatusCode)
	}
	var status consoleStatusPayload
	if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.Cluster != string(cluster.UID) {
		t.Fatalf("expected cluster %s, got %s", cluster.UID, status.Cluster)
	}
	if status.Brokers.Ready != int(replicas) {
		t.Fatalf("expected %d brokers ready, got %d", replicas, status.Brokers.Ready)
	}
	if len(status.Topics) != 1 || status.Topics[0].Name != topic.Name {
		t.Fatalf("unexpected topics: %+v", status.Topics)
	}
	if status.Topics[0].Partitions != int(topic.Spec.Partitions) {
		t.Fatalf("expected %d partitions, got %d", topic.Spec.Partitions, status.Topics[0].Partitions)
	}

	maybeOpenConsoleUI(t, consoleURL+"/ui/")
}

type consoleStatusPayload struct {
	Cluster string `json:"cluster"`
	Brokers struct {
		Ready int `json:"ready"`
	} `json:"brokers"`
	Topics []struct {
		Name       string `json:"name"`
		Partitions int    `json:"partitions"`
		State      string `json:"state"`
	} `json:"topics"`
}

func waitForHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s: %v", url, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func releasePort(t *testing.T, addr string) {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("port %s unavailable: %v", addr, err)
	}
	_ = ln.Close()
}

func maybeOpenConsoleUI(t *testing.T, url string) {
	if !parseBoolEnv("KAFSCALE_E2E_OPEN_UI") {
		return
	}
	var cmd *exec.Cmd
	switch stdruntime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "linux":
		cmd = exec.Command("xdg-open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		t.Logf("auto-open UI not supported on %s", stdruntime.GOOS)
		return
	}
	if err := cmd.Start(); err != nil {
		t.Logf("open UI failed: %v", err)
	}
}

func loginConsole(t *testing.T, baseURL, username, password string) *http.Cookie {
	t.Helper()
	body := fmt.Sprintf(`{"username":"%s","password":"%s"}`, username, password)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/ui/api/auth/login", strings.NewReader(body))
	if err != nil {
		t.Fatalf("build login request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("login request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected login status: %d", resp.StatusCode)
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "kafscale_ui_session" {
			return cookie
		}
	}
	t.Fatalf("login did not return session cookie")
	return nil
}
