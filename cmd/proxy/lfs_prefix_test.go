// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestBuildObjectKeyUsesOwnPrefix(t *testing.T) {
	m, _ := testLFSModule(t)
	key, err := m.buildObjectKey(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^test-ns/@lfs/orders/\d{4}/\d{2}/\d{2}/obj-.+$`).MatchString(key) {
		t.Fatalf("unexpected key %s", key)
	}
	m.topicGeneration = func(ctx context.Context, topic string) (int64, error) { return 2, nil }
	key, err = m.buildObjectKey(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^test-ns/@lfs/orders@2/\d{4}/\d{2}/\d{2}/obj-.+$`).MatchString(key) {
		t.Fatalf("unexpected key %s", key)
	}
	if err := m.lfsValidateObjectKey(key); err != nil {
		t.Fatalf("new key rejected: %v", err)
	}
	m.topicGeneration = func(ctx context.Context, topic string) (int64, error) { return 0, errors.New("etcd down") }
	if _, err := m.buildObjectKey(context.Background(), "orders"); err == nil {
		t.Fatal("expected error when generation lookup fails")
	}
}

func TestRewriteProduceWritesOwnPrefix(t *testing.T) {
	m, fs3 := testLFSModule(t)
	m.topicGeneration = func(ctx context.Context, topic string) (int64, error) { return 1, nil }
	payload := []byte("blob for new prefix")
	sum := sha256.Sum256(payload)
	req := &kmsg.ProduceRequest{
		Acks:          1,
		TimeoutMillis: 5000,
		Topics: []kmsg.ProduceRequestTopic{{
			Topic: "orders",
			Partitions: []kmsg.ProduceRequestTopicPartition{{
				Records: buildTestBatch([]kmsg.Record{{Value: payload, Headers: []kmsg.Header{{Key: "LFS_BLOB", Value: []byte(hex.EncodeToString(sum[:]))}}}}),
			}},
		}},
	}
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9}
	if _, err := m.rewriteProduceRecords(context.Background(), header, req); err != nil {
		t.Fatal(err)
	}
	batches, err := lfsDecodeRecordBatches(req.Topics[0].Partitions[0].Records)
	if err != nil {
		t.Fatal(err)
	}
	recs, _, err := lfsDecodeBatchRecords(&batches[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	env, err := lfs.DecodeEnvelope(recs[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^test-ns/@lfs/orders@1/`).MatchString(env.Key) {
		t.Fatalf("envelope key %s not under own prefix", env.Key)
	}
	if _, ok := fs3.objects[env.Key]; !ok || len(fs3.objects) != 1 {
		t.Fatalf("object not stored at envelope key: %v", fs3.objects)
	}
}

func TestDownloadOldPathStillReadable(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	fs3 := m.s3Uploader.api.(*fakeS3)
	oldKey := "test-ns/orders/lfs/2026/01/01/obj-old"
	payload := []byte("written before the prefix move")
	fs3.objects[oldKey] = payload
	sum := sha256.Sum256(payload)
	body, _ := json.Marshal(lfsDownloadRequest{
		Bucket:    "test-bucket",
		Key:       oldKey,
		Mode:      "stream",
		Integrity: &lfsIntegrityRequest{SHA256: hex.EncodeToString(sum[:]), ChecksumAlg: "sha256", Size: int64(len(payload))},
	})
	rr := httptest.NewRecorder()
	m.handleHTTPDownload(rr, httptest.NewRequest(http.MethodPost, "/lfs/download", bytes.NewReader(body)))
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d: %s", rr.Code, rr.Body.String())
	}
	if !bytes.Equal(rr.Body.Bytes(), payload) {
		t.Fatalf("unexpected body %q", rr.Body.String())
	}
}

func blockingGeneration(ctx context.Context, topic string) (int64, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestHTTPGenerationLookupTimesOut(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.metadataTimeout = 100 * time.Millisecond
	m.topicGeneration = blockingGeneration

	produce := httptest.NewRequest(http.MethodPost, "/lfs/produce", bytes.NewReader([]byte("payload")))
	produce.Header.Set(lfsHeaderTopic, "orders")
	produce.Header.Set("Content-Type", "application/octet-stream")
	uploads := httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewBufferString(`{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`))
	cases := []struct {
		name    string
		req     *http.Request
		handler func(http.ResponseWriter, *http.Request)
	}{
		{"produce", produce, m.handleHTTPProduce},
		{"uploads", uploads, m.handleHTTPUploadInit},
	}
	for _, c := range cases {
		start := time.Now()
		rr := httptest.NewRecorder()
		c.handler(rr, c.req)
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Fatalf("%s took %s", c.name, elapsed)
		}
		if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "metadata_unavailable") {
			t.Fatalf("%s: got %d %s", c.name, rr.Code, rr.Body.String())
		}
	}
	if len(m.s3Uploader.api.(*fakeS3).objects) != 0 || len(m.uploadSessions) != 0 {
		t.Fatal("timed out request had side effects")
	}
}

func lfsProduceRequest(t *testing.T, topics map[string]int) *kmsg.ProduceRequest {
	t.Helper()
	req := &kmsg.ProduceRequest{Acks: 1, TimeoutMillis: 5000}
	for _, name := range []string{"orders", "payments", "plain"} {
		count, ok := topics[name]
		if !ok {
			continue
		}
		topic := kmsg.ProduceRequestTopic{Topic: name}
		for p := 0; p < count; p++ {
			payload := []byte(fmt.Sprintf("%s-%d", name, p))
			sum := sha256.Sum256(payload)
			rec := kmsg.Record{Value: payload}
			if name != "plain" {
				rec.Headers = []kmsg.Header{{Key: "LFS_BLOB", Value: []byte(hex.EncodeToString(sum[:]))}}
			}
			topic.Partitions = append(topic.Partitions, kmsg.ProduceRequestTopicPartition{Partition: int32(p), Records: buildTestBatch([]kmsg.Record{rec, rec})})
		}
		req.Topics = append(req.Topics, topic)
	}
	return req
}

func TestRewriteProduceGenerationLookupTimesOut(t *testing.T) {
	m, fs3 := testLFSModule(t)
	m.metadataTimeout = 100 * time.Millisecond
	m.topicGeneration = blockingGeneration
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9}
	start := time.Now()
	_, err := m.rewriteProduceRecords(context.Background(), header, lfsProduceRequest(t, map[string]int{"orders": 1}))
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("took %s", elapsed)
	}
	if len(fs3.objects) != 0 {
		t.Fatal("blob uploaded after lookup failure")
	}
}

func TestRewriteProduceResolvesGenerationsBeforeUpload(t *testing.T) {
	m, fs3 := testLFSModule(t)
	var calls atomic.Int32
	m.topicGeneration = func(ctx context.Context, topic string) (int64, error) {
		calls.Add(1)
		return 3, nil
	}
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9}
	if _, err := m.rewriteProduceRecords(context.Background(), header, lfsProduceRequest(t, map[string]int{"orders": 2, "payments": 1, "plain": 1})); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 4 {
		t.Fatalf("expected 2 lookups and 2 rechecks, got %d", calls.Load())
	}
	if len(fs3.objects) != 6 {
		t.Fatalf("expected 6 uploads, got %d", len(fs3.objects))
	}
	for key := range fs3.objects {
		if !strings.HasPrefix(key, "test-ns/@lfs/orders@3/") && !strings.HasPrefix(key, "test-ns/@lfs/payments@3/") {
			t.Fatalf("unexpected key %s", key)
		}
	}

	m, fs3 = testLFSModule(t)
	m.topicGeneration = func(ctx context.Context, topic string) (int64, error) {
		if topic == "payments" {
			return 0, errors.New("etcd down")
		}
		return 0, nil
	}
	if _, err := m.rewriteProduceRecords(context.Background(), header, lfsProduceRequest(t, map[string]int{"orders": 2, "payments": 1})); err == nil {
		t.Fatal("expected error")
	}
	if len(fs3.objects) != 0 {
		t.Fatalf("uploads before lookup failure: %v", fs3.objects)
	}
}

func TestValidateObjectKeyShapes(t *testing.T) {
	m := testHTTPModule(t)
	for _, key := range []string{
		"test-ns/@lfs/orders/2026/10/30/obj-a",
		"test-ns/@lfs/orders@2/2026/10/30/obj-a",
		"test-ns/orders/lfs/2026/10/30/obj-a",
	} {
		if err := m.lfsValidateObjectKey(key); err != nil {
			t.Fatalf("%s rejected: %v", key, err)
		}
	}
	for _, key := range []string{
		"test-ns/anything/@lfs/x",
		"test-ns/a/b/lfs/x",
		"test-ns/orders/0/segment-00000000000000000000.kfs",
		"test-ns/@lfs/",
		"test-ns/orders/lfs/",
		"test-ns/bad topic/lfs/x",
		"test-ns/@lfsx/orders/obj",
		"other/@lfs/orders/obj",
	} {
		if err := m.lfsValidateObjectKey(key); err == nil {
			t.Fatalf("%s accepted", key)
		}
	}
}

func bumpingGeneration() func(context.Context, string) (int64, error) {
	var mu sync.Mutex
	calls := map[string]int64{}
	return func(ctx context.Context, topic string) (int64, error) {
		mu.Lock()
		defer mu.Unlock()
		gen := calls[topic]
		calls[topic]++
		return gen, nil
	}
}

func countingBackend(t *testing.T, m *lfsModule) *atomic.Int32 {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var accepted atomic.Int32
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			_ = c.Close()
		}
	}()
	m.backends = []string{listener.Addr().String()}
	m.dialTimeout = time.Second
	m.backendRetries = 1
	return &accepted
}

func TestRewriteProduceGenerationChangedDiscardsUploads(t *testing.T) {
	m, fs3 := testLFSModule(t)
	m.topicGeneration = bumpingGeneration()
	header := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9}
	req := lfsProduceRequest(t, map[string]int{"orders": 1})
	_, err := m.rewriteProduceRecords(context.Background(), header, req)
	if !errors.Is(err, errLFSGenerationChanged) {
		t.Fatalf("expected generation changed error, got %v", err)
	}
	if len(fs3.objects) != 0 || len(fs3.deleted) != 2 {
		t.Fatalf("uploads not discarded: objects=%v deleted=%v", fs3.objects, fs3.deleted)
	}
	for _, key := range fs3.deleted {
		if !strings.HasPrefix(key, "test-ns/@lfs/orders/") {
			t.Fatalf("unexpected deleted key %s", key)
		}
	}
}

func TestHTTPProduceGenerationChangedDiscardsUpload(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.topicGeneration = bumpingGeneration()
	accepted := countingBackend(t, m)
	fs3 := m.s3Uploader.api.(*fakeS3)
	req := httptest.NewRequest(http.MethodPost, "/lfs/produce", bytes.NewReader([]byte("payload")))
	req.Header.Set(lfsHeaderTopic, "orders")
	req.Header.Set("Content-Type", "application/octet-stream")
	rr := httptest.NewRecorder()
	m.handleHTTPProduce(rr, req)
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "metadata_unavailable") {
		t.Fatalf("got %d %s", rr.Code, rr.Body.String())
	}
	if len(fs3.objects) != 0 || len(fs3.deleted) != 1 || !strings.HasPrefix(fs3.deleted[0], "test-ns/@lfs/orders/") {
		t.Fatalf("upload not discarded: objects=%v deleted=%v", fs3.objects, fs3.deleted)
	}
	if accepted.Load() != 0 {
		t.Fatal("record was produced")
	}
}

func TestHTTPMultipartGenerationChangedDiscardsUpload(t *testing.T) {
	m := testHTTPModule(t)
	m.httpAPIKey = ""
	m.topicGeneration = bumpingGeneration()
	accepted := countingBackend(t, m)
	fs3 := m.s3Uploader.api.(*fakeS3)
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewBufferString(`{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)))
	if rr.Code != http.StatusOK {
		t.Fatalf("init: %d %s", rr.Code, rr.Body.String())
	}
	var session *uploadSession
	for _, s := range m.uploadSessions {
		session = s
	}
	if session == nil || !strings.HasPrefix(session.S3Key, "test-ns/@lfs/orders/") {
		t.Fatalf("unexpected session %+v", session)
	}
	rr = httptest.NewRecorder()
	m.handleHTTPUploadPart(rr, httptest.NewRequest(http.MethodPut, "/", bytes.NewReader([]byte("payload"))), "part", session.ID, 1)
	if rr.Code != http.StatusOK {
		t.Fatalf("part: %d %s", rr.Code, rr.Body.String())
	}
	fs3.objects[session.S3Key] = []byte("payload")
	rr = httptest.NewRecorder()
	m.handleHTTPUploadComplete(rr, httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"parts":[{"part_number":1,"etag":"test-etag"}]}`)), "complete", session.ID)
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "metadata_unavailable") {
		t.Fatalf("got %d %s", rr.Code, rr.Body.String())
	}
	if len(fs3.objects) != 0 || len(fs3.deleted) != 1 || fs3.deleted[0] != session.S3Key {
		t.Fatalf("upload not discarded: objects=%v deleted=%v", fs3.objects, fs3.deleted)
	}
	if _, ok := m.lfsGetUploadSession(session.ID); ok {
		t.Fatal("session kept")
	}
	if accepted.Load() != 0 {
		t.Fatal("record was produced")
	}
}
