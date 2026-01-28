// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestACLsE2E(t *testing.T) {
	const enableEnv = "KAFSCALE_E2E"
	if os.Getenv(enableEnv) != "1" {
		t.Skipf("set %s=1 to run integration harness", enableEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	etcd, endpoints := startEmbeddedEtcd(t)
	defer etcd.Close()

	brokerAddr := freeAddr(t)
	metricsAddr := freeAddr(t)
	controlAddr := freeAddr(t)

	aclJSON := `{"default_policy":"deny","principals":[` +
		`{"name":"allowed","allow":[` +
		`{"action":"produce","resource":"topic","name":"orders-*" },` +
		`{"action":"fetch","resource":"topic","name":"orders-*" },` +
		`{"action":"group_read","resource":"group","name":"group-allowed"},` +
		`{"action":"group_write","resource":"group","name":"group-allowed"}` +
		`]},` +
		`{"name":"denied","allow":[]}` +
		`]}`

	brokerCmd, brokerLogs := startBrokerWithEtcdACL(t, ctx, brokerAddr, metricsAddr, controlAddr, endpoints, aclJSON)
	t.Cleanup(func() { stopBroker(t, brokerCmd) })
	waitForBroker(t, brokerLogs, brokerAddr)

	allowedClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableIdempotentWrite(),
		kgo.ClientID("allowed"),
	)
	if err != nil {
		t.Fatalf("create allowed client: %v\nbroker logs:\n%s", err, brokerLogs.String())
	}
	defer allowedClient.Close()

	deniedClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.AllowAutoTopicCreation(),
		kgo.DisableIdempotentWrite(),
		kgo.ClientID("denied"),
	)
	if err != nil {
		t.Fatalf("create denied client: %v\nbroker logs:\n%s", err, brokerLogs.String())
	}
	defer deniedClient.Close()

	topic := fmt.Sprintf("orders-%d", time.Now().UnixNano())
	produceErr := allowedClient.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("ok")}).FirstErr()
	if produceErr != nil {
		t.Fatalf("allowed produce failed: %v\nbroker logs:\n%s", produceErr, brokerLogs.String())
	}

	denyErr := deniedClient.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("deny")}).FirstErr()
	if denyErr == nil {
		t.Fatalf("expected denied produce error")
	}
	var kerrVal *kerr.Error
	if !errors.As(denyErr, &kerrVal) || kerrVal.Code != kerr.TopicAuthorizationFailed.Code {
		t.Fatalf("expected topic authorization failed, got: %v", denyErr)
	}

	listReq := kmsg.NewPtrListGroupsRequest()
	listReq.Version = 5
	listReq.StatesFilter = []string{"Stable"}
	listReq.TypesFilter = []string{"classic"}
	listResp, err := listReq.RequestWith(ctx, deniedClient)
	if err != nil {
		t.Fatalf("list groups request failed: %v\nbroker logs:\n%s", err, brokerLogs.String())
	}
	if listResp.ErrorCode != kerr.GroupAuthorizationFailed.Code {
		t.Fatalf("expected group authorization failed, got %d", listResp.ErrorCode)
	}
}

func startBrokerWithEtcdACL(t *testing.T, ctx context.Context, brokerAddr, metricsAddr, controlAddr string, endpoints []string, aclJSON string) (*exec.Cmd, *bytes.Buffer) {
	t.Helper()
	brokerCmd := exec.CommandContext(ctx, "go", "run", filepath.Join(repoRoot(t), "cmd", "broker"))
	configureProcessGroup(brokerCmd)
	brokerCmd.Env = append(os.Environ(),
		"KAFSCALE_AUTO_CREATE_TOPICS=true",
		"KAFSCALE_AUTO_CREATE_PARTITIONS=1",
		"KAFSCALE_USE_MEMORY_S3=1",
		"KAFSCALE_ACL_ENABLED=true",
		fmt.Sprintf("KAFSCALE_ACL_JSON=%s", aclJSON),
		fmt.Sprintf("KAFSCALE_BROKER_ADDR=%s", brokerAddr),
		fmt.Sprintf("KAFSCALE_METRICS_ADDR=%s", metricsAddr),
		fmt.Sprintf("KAFSCALE_CONTROL_ADDR=%s", controlAddr),
		fmt.Sprintf("KAFSCALE_ETCD_ENDPOINTS=%s", strings.Join(endpoints, ",")),
	)
	var brokerLogs bytes.Buffer
	logWriter := io.MultiWriter(&brokerLogs, mustLogFile(t, "broker-acl.log"))
	brokerCmd.Stdout = logWriter
	brokerCmd.Stderr = logWriter
	if err := brokerCmd.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	return brokerCmd, &brokerLogs
}
