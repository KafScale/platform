// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// Licensed under the Apache License, Version 2.0 (the "License").
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"github.com/KafScale/platform/pkg/acl"
	"github.com/KafScale/platform/pkg/authn"
	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLFSHTTPProduceReplies(t *testing.T) {
	for _, test := range []struct {
		name   string
		code   int16
		status int
	}{{"accepted", 0, 200}, {"denied", 29, 403}, {"unknown_topic", 3, 502}} {
		t.Run(test.name, func(t *testing.T) {
			m := testHTTPModule(t)
			m.httpAPIKey = ""
			m.dialTimeout = time.Second
			m.backendRetries = 1
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			m.backends = []string{listener.Addr().String()}
			done := make(chan struct{})
			go func() {
				defer close(done)
				c, err := listener.Accept()
				if err != nil {
					return
				}
				defer c.Close()
				frame, err := protocol.ReadFrame(c)
				if err != nil {
					return
				}
				header, _, _ := protocol.ParseRequestHeader(frame.Payload)
				response := kmsg.NewPtrProduceResponse()
				response.Version = 9
				response.Topics = []kmsg.ProduceResponseTopic{{Topic: "allowed", Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0, ErrorCode: test.code, BaseOffset: 42}}}}
				_ = protocol.WriteFrame(c, protocol.EncodeResponse(header.CorrelationID, 9, response))
			}()
			request := httptest.NewRequest("POST", "/lfs/produce", bytes.NewReader([]byte("payload")))
			request.Header.Set(lfsHeaderTopic, "allowed")
			request.Header.Set("Content-Type", "application/octet-stream")
			recorder := httptest.NewRecorder()
			m.handleHTTPProduce(recorder, request)
			<-done
			if recorder.Code != test.status {
				t.Fatalf("status %d: %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestLFSHTTPAuthorizationBeforeUpload(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.authorizer = acl.NewAuthorizer(acl.Config{Enabled: true, DefaultPolicy: "deny"})
	request := httptest.NewRequest("POST", "/lfs/produce", bytes.NewReader([]byte("payload")))
	request.Header.Set(lfsHeaderTopic, "denied")
	recorder := httptest.NewRecorder()
	m.handleHTTPProduce(recorder, request)
	if recorder.Code != 403 || len(m.s3Uploader.api.(*fakeS3).objects) != 0 {
		t.Fatal("denied upload had side effects", recorder.Code)
	}
	request = httptest.NewRequest("POST", "/lfs/uploads", bytes.NewBufferString(`{"topic":"denied","content_type":"application/octet-stream","size_bytes":7}`))
	recorder = httptest.NewRecorder()
	m.handleHTTPUploadInit(recorder, request)
	if recorder.Code != 403 || len(m.uploadSessions) != 0 {
		t.Fatal(recorder.Code)
	}
	// The service identity is fixed; a client-supplied ID does not participate.
	m.authorizer = acl.NewAuthorizer(acl.Config{Enabled: true, Principals: []acl.PrincipalRules{{Name: authn.LFSPrincipal, Allow: []acl.Rule{{Action: acl.ActionProduce, Resource: acl.ResourceTopic, Name: "allowed"}}}}})
	if !m.authorizeHTTPUpload(httptest.NewRecorder(), "id", "allowed") {
		t.Fatal("service grant ignored")
	}
}

func TestLFSHTTPMalformedAcknowledgements(t *testing.T) {
	for _, kind := range []string{"transport", "short", "correlation", "missing", "wrong_partition", "wrong_topic"} {
		t.Run(kind, func(t *testing.T) {
			server, client := net.Pipe()
			defer client.Close()
			m := testHTTPModule(t)
			m.dialTimeout = time.Second
			go func() {
				defer server.Close()
				_, _ = protocol.ReadFrame(server)
				if kind == "transport" {
					return
				}
				data := []byte{0}
				if kind != "short" {
					r := kmsg.NewPtrProduceResponse()
					r.Version = 9
					if kind != "missing" {
						name := "allowed"
						part := int32(0)
						if kind == "wrong_topic" {
							name = "other"
						}
						if kind == "wrong_partition" {
							part = 1
						}
						r.Topics = []kmsg.ProduceResponseTopic{{Topic: name, Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: part}}}}
					}
					id := int32(7)
					if kind == "correlation" {
						id = 8
					}
					data = protocol.EncodeResponse(id, 9, r)
				}
				_ = protocol.WriteFrame(server, data)
			}()
			if _, err := m.produceHTTPRecord(context.Background(), client, []byte{1}, "allowed", 0, 7); err == nil {
				t.Fatal("invalid acknowledgement accepted")
			}
		})
	}
	if produceHTTPStatus(kerr.ClusterAuthorizationFailed) != http.StatusForbidden || produceHTTPStatus(errors.New("other")) != http.StatusBadGateway {
		t.Fatal("incorrect error status")
	}
}

func TestLFSHTTPMultipartBrokerRejection(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.dialTimeout = time.Second
	m.backendRetries = 1
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	m.backends = []string{listener.Addr().String()}
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := listener.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		frame, err := protocol.ReadFrame(c)
		if err != nil {
			return
		}
		header, _, _ := protocol.ParseRequestHeader(frame.Payload)
		r := kmsg.NewPtrProduceResponse()
		r.Version = 9
		r.Topics = []kmsg.ProduceResponseTopic{{Topic: "allowed", Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0, ErrorCode: 29}}}}
		_ = protocol.WriteFrame(c, protocol.EncodeResponse(header.CorrelationID, 9, r))
	}()
	session := &uploadSession{ID: "session", Topic: "allowed", S3Key: "object", UploadID: "upload", SizeBytes: 1, TotalUploaded: 1, ExpiresAt: time.Now().Add(time.Hour), Parts: map[int32]string{1: "etag"}, sha256Hasher: sha256.New(), ChecksumAlg: lfs.ChecksumNone}
	m.lfsStoreUploadSession(session)
	request := httptest.NewRequest("POST", "/lfs/uploads/session/complete", bytes.NewBufferString(`{"parts":[{"part_number":1,"etag":"etag"}]}`))
	recorder := httptest.NewRecorder()
	m.handleHTTPUploadComplete(recorder, request, "request", "session")
	<-done
	if recorder.Code != 403 {
		t.Fatal("multipart reported success for rejected produce", recorder.Code, recorder.Body.String())
	}
	m.authorizer = acl.NewAuthorizer(acl.Config{Enabled: true, DefaultPolicy: "deny"})
	recorder = httptest.NewRecorder()
	m.handleHTTPUploadComplete(recorder, httptest.NewRequest("POST", "/", nil), "request", "session")
	if recorder.Code != 403 {
		t.Fatal(recorder.Code)
	}
	recorder = httptest.NewRecorder()
	m.handleHTTPUploadPart(recorder, httptest.NewRequest("PUT", "/", bytes.NewReader([]byte("x"))), "request", "session", 2)
	if recorder.Code != 403 {
		t.Fatal(recorder.Code)
	}
}
