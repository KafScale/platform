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
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

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
