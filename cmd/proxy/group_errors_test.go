// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").
package main

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/v2/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProxyFailureLogsAndResponses(t *testing.T) {
	const private = "private-request-marker"
	heartbeat := kmsg.NewPtrHeartbeatRequest()
	heartbeat.Group = private
	offsets := kmsg.NewPtrListOffsetsRequest()
	offsets.Version = 1
	offsets.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: private, Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0}}}}
	for _, tc := range []struct {
		name    string
		request kmsg.Request
		log     string
	}{
		{"group", heartbeat, "group routing failed"},
		{"backend", offsets, "backend connect failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			p := lfsTestProxy(nil, time.Minute)
			p.backends = []string{closedBackendAddr(t)}
			p.logger = slog.New(slog.NewTextHandler(&logs, nil))
			p.setReady(true)
			server, client := net.Pipe()
			defer func() { _ = client.Close() }()
			_ = client.SetDeadline(time.Now().Add(3 * time.Second))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			done := make(chan struct{})
			go func() { defer close(done); p.handleConnection(ctx, server) }()
			payload := kmsg.NewRequestFormatter().AppendRequest(nil, tc.request, 1)
			if _, err := client.Write(payload); err != nil {
				t.Fatal(err)
			}
			frame, err := protocol.ReadFrame(client)
			if err != nil {
				t.Fatal(err)
			}
			body, ok := protocol.SkipResponseHeader(tc.request.Key(), tc.request.GetVersion(), frame.Payload)
			if !ok {
				t.Fatal("invalid response header")
			}
			response := tc.request.ResponseKind()
			response.SetVersion(tc.request.GetVersion())
			if err := response.ReadFrom(body); err != nil {
				t.Fatal(err)
			}
			switch response := response.(type) {
			case *kmsg.HeartbeatResponse:
				if response.ErrorCode != protocol.REQUEST_TIMED_OUT {
					t.Fatal(response)
				}
			case *kmsg.ListOffsetsResponse:
				if len(response.Topics) != 1 || len(response.Topics[0].Partitions) != 1 || response.Topics[0].Partitions[0].ErrorCode != protocol.REQUEST_TIMED_OUT {
					t.Fatal(response)
				}
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("proxy did not close failed request")
			}
			if got := logs.String(); strings.Contains(got, private) || !strings.Contains(got, tc.log) {
				t.Fatalf("unsafe or missing failure log: %s", got)
			}
		})
	}
}
