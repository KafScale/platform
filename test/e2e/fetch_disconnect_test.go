// Copyright 2026 KafScale team.
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
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func testFetchDisconnect(t *testing.T, addr, topic string) {
	t.Helper()
	address, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTCP("tcp", nil, address)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	request := kmsg.NewPtrFetchRequest()
	request.Version, request.ReplicaID, request.MinBytes, request.MaxWaitMillis = 4, -1, 1, 30000
	request.MaxBytes = 1 << 20
	// The shared topic contains three messages; wait beyond its end.
	request.Topics = []kmsg.FetchRequestTopic{{Topic: topic, Partitions: []kmsg.FetchRequestTopicPartition{{Partition: 0, FetchOffset: 3, PartitionMaxBytes: 1 << 20}}}}
	if _, err := conn.Write(kmsg.NewRequestFormatter().AppendRequest(nil, request, 84)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	// Half-close gives the server EOF but lets the test observe when the server
	// finishes its waiting handler and closes its side of the connection.
	if err := conn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.ReadFrame(conn); !errors.Is(err, io.EOF) {
		t.Fatalf("30-second Fetch did not finish promptly on client EOF: %v", err)
	}
}
