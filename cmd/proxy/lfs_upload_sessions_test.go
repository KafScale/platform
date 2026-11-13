// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func sessionAckBackend(t *testing.T, topic string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				for {
					frame, err := protocol.ReadFrame(c)
					if err != nil {
						return
					}
					header, _, _ := protocol.ParseRequestHeader(frame.Payload)
					response := kmsg.NewPtrProduceResponse()
					response.Version = 9
					response.Topics = []kmsg.ProduceResponseTopic{{Topic: topic, Partitions: []kmsg.ProduceResponseTopicPartition{{Partition: 0, BaseOffset: 7}}}}
					if err := protocol.WriteFrame(c, protocol.EncodeResponse(header.CorrelationID, 9, response)); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return listener.Addr().String()
}

func sharedSessionModules(t *testing.T) (*lfsModule, *lfsModule, *fakeS3) {
	t.Helper()
	backend := sessionAckBackend(t, "orders")
	a := testHTTPModule(t)
	b := testHTTPModule(t)
	b.s3Uploader = a.s3Uploader
	for _, m := range []*lfsModule{a, b} {
		m.httpAPIKey = ""
		m.backends = []string{backend}
		m.dialTimeout = time.Second
		m.backendRetries = 1
		m.backendBackoff = time.Millisecond
		m.useSharedUploadSessions()
	}
	return a, b, a.s3Uploader.api.(*fakeS3)
}

func sessionInit(t *testing.T, m *lfsModule, body string) lfsUploadInitResponse {
	t.Helper()
	rr := httptest.NewRecorder()
	m.handleHTTPUploadInit(rr, httptest.NewRequest(http.MethodPost, "/lfs/uploads", bytes.NewBufferString(body)))
	if rr.Code != http.StatusOK {
		t.Fatalf("init: %d %s", rr.Code, rr.Body.String())
	}
	var resp lfsUploadInitResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func sessionPart(t *testing.T, m *lfsModule, id string, n int, data []byte) (int, string) {
	t.Helper()
	rr := httptest.NewRecorder()
	m.handleHTTPUploadSession(rr, httptest.NewRequest(http.MethodPut, fmt.Sprintf("/lfs/uploads/%s/parts/%d", id, n), bytes.NewReader(data)))
	if rr.Code != http.StatusOK {
		return rr.Code, rr.Body.String()
	}
	var resp lfsUploadPartResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	return rr.Code, resp.ETag
}

func sessionComplete(m *lfsModule, id string, etags []string) *httptest.ResponseRecorder {
	parts := make([]string, 0, len(etags))
	for i, etag := range etags {
		parts = append(parts, fmt.Sprintf(`{"part_number":%d,"etag":%q}`, i+1, etag))
	}
	rr := httptest.NewRecorder()
	m.handleHTTPUploadSession(rr, httptest.NewRequest(http.MethodPost, "/lfs/uploads/"+id+"/complete", bytes.NewBufferString(`{"parts":[`+strings.Join(parts, ",")+`]}`)))
	return rr
}

func sessionRecords(fs3 *fakeS3) []string {
	var keys []string
	for key := range fs3.objects {
		if strings.HasPrefix(key, "test-ns/"+lfsUploadSessionPrefix+"/") {
			keys = append(keys, key)
		}
	}
	return keys
}

func runCrossPodUpload(t *testing.T, alg string, checksum func([]byte) string) {
	a, b, fs3 := sharedSessionModules(t)
	payload := bytes.Repeat([]byte("kafscale"), (5<<20)/8)
	payload = append(payload, []byte("tail-bytes")...)
	init := fmt.Sprintf(`{"topic":"orders","content_type":"application/octet-stream","size_bytes":%d,"checksum_alg":%q,"checksum":%q}`, len(payload), alg, checksum(payload))
	session := sessionInit(t, a, init)
	code, etag1 := sessionPart(t, b, session.UploadID, 1, payload[:5<<20])
	if code != http.StatusOK {
		t.Fatalf("part 1 on the other proxy: %d %s", code, etag1)
	}
	code, etag2 := sessionPart(t, a, session.UploadID, 2, payload[5<<20:])
	if code != http.StatusOK {
		t.Fatalf("part 2 back on the first proxy: %d %s", code, etag2)
	}
	rr := sessionComplete(b, session.UploadID, []string{etag1, etag2})
	if rr.Code != http.StatusOK {
		t.Fatalf("complete on the other proxy: %d %s", rr.Code, rr.Body.String())
	}
	var env lfs.Envelope
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	if env.SHA256 != hex.EncodeToString(sum[:]) || env.Size != int64(len(payload)) || env.Key != session.S3Key {
		t.Fatalf("envelope does not match the payload: %+v", env)
	}
	if env.Checksum != checksum(payload) || env.ChecksumAlg != alg {
		t.Fatalf("checksum %s/%s, want %s/%s", env.ChecksumAlg, env.Checksum, alg, checksum(payload))
	}
	if keys := sessionRecords(fs3); len(keys) != 0 {
		t.Fatalf("session records left: %v", keys)
	}
	if len(fs3.multipartUploads) != 0 {
		t.Fatalf("multipart uploads left in flight: %v", fs3.multipartUploads)
	}
	if code, body := sessionPart(t, a, session.UploadID, 3, []byte("x")); code != http.StatusNotFound {
		t.Fatalf("finished session still found: %d %s", code, body)
	}
}

func TestLFSMultipartAcrossProxiesSHA256(t *testing.T) {
	runCrossPodUpload(t, "sha256", func(data []byte) string {
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:])
	})
}

func TestLFSMultipartAcrossProxiesMD5(t *testing.T) {
	runCrossPodUpload(t, "md5", func(data []byte) string {
		sum := md5.Sum(data)
		return hex.EncodeToString(sum[:])
	})
}

func TestLFSMultipartExpiredOnOtherProxyAbortsUpload(t *testing.T) {
	a, b, fs3 := sharedSessionModules(t)
	a.uploadSessionTTL = -time.Second
	session := sessionInit(t, a, `{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)
	code, body := sessionPart(t, b, session.UploadID, 1, []byte("payload"))
	if code != http.StatusGone || !strings.Contains(body, "upload_expired") {
		t.Fatalf("part on an expired session: %d %s", code, body)
	}
	if len(fs3.aborted) != 1 || len(fs3.multipartUploads) != 0 {
		t.Fatalf("expired multipart upload not aborted: aborted=%v in flight=%v", fs3.aborted, fs3.multipartUploads)
	}
	if keys := sessionRecords(fs3); len(keys) != 0 {
		t.Fatalf("expired session record left: %v", keys)
	}
}

func TestLFSMultipartAbortOnOtherProxy(t *testing.T) {
	a, b, fs3 := sharedSessionModules(t)
	session := sessionInit(t, a, `{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)
	rr := httptest.NewRecorder()
	b.handleHTTPUploadSession(rr, httptest.NewRequest(http.MethodDelete, "/lfs/uploads/"+session.UploadID, nil))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("abort on the other proxy: %d %s", rr.Code, rr.Body.String())
	}
	if len(fs3.multipartUploads) != 0 || len(sessionRecords(fs3)) != 0 {
		t.Fatalf("abort left state: in flight=%v records=%v", fs3.multipartUploads, sessionRecords(fs3))
	}
}

func TestLFSUploadSessionSweepRemovesExpiredAndLostUploads(t *testing.T) {
	m, _, fs3 := sharedSessionModules(t)
	now := time.Now().UTC()
	for _, s := range []*uploadSession{
		{ID: "0000000a-0000-0000-0000-000000000001", Topic: "orders", S3Key: "test-ns/@lfs/orders/expired", UploadID: "expired-upload", ExpiresAt: now.Add(-time.Minute), sha256Hasher: sha256.New()},
		{ID: "0000000a-0000-0000-0000-000000000002", Topic: "orders", S3Key: "test-ns/@lfs/orders/active", UploadID: "active-upload", ExpiresAt: now.Add(time.Minute), sha256Hasher: sha256.New()},
	} {
		if err := m.saveUploadSession(context.Background(), s); err != nil {
			t.Fatal(err)
		}
	}
	old := now.Add(-(m.uploadSessionTTL + lfsLostMultipartUploadGrace + time.Minute))
	young := now.Add(-time.Minute)
	fs3.multipartUploads = []types.MultipartUpload{
		{Key: aws.String("test-ns/@lfs/orders/expired"), UploadId: aws.String("expired-upload"), Initiated: &young},
		{Key: aws.String("test-ns/@lfs/orders/active"), UploadId: aws.String("active-upload"), Initiated: &young},
		{Key: aws.String("test-ns/@lfs/orders/lost"), UploadId: aws.String("lost-upload"), Initiated: &old},
		{Key: aws.String("other-ns/@lfs/orders/lost"), UploadId: aws.String("other-namespace-upload"), Initiated: &old},
	}
	m.sweepUploadSessions(context.Background())
	if got := strings.Join(fs3.aborted, ","); got != "expired-upload,lost-upload" {
		t.Fatalf("aborted %q", got)
	}
	records := sessionRecords(fs3)
	if len(records) != 1 || !strings.Contains(records[0], "000000000002") {
		t.Fatalf("session records after sweep: %v", records)
	}
	if len(fs3.multipartUploads) != 2 {
		t.Fatalf("multipart uploads after sweep: %v", fs3.multipartUploads)
	}
}

func TestLFSMultipartAcrossProxiesCRC32(t *testing.T) {
	runCrossPodUpload(t, "crc32", func(data []byte) string {
		sum, err := lfs.ComputeChecksum(lfs.ChecksumCRC32, data)
		if err != nil {
			t.Fatal(err)
		}
		return sum
	})
}

func TestLFSSharedSessionRemovedWhenBackendUnavailableAtComplete(t *testing.T) {
	a, b, fs3 := sharedSessionModules(t)
	session := sessionInit(t, a, `{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)
	code, etag := sessionPart(t, b, session.UploadID, 1, []byte("payload"))
	if code != http.StatusOK {
		t.Fatalf("part: %d %s", code, etag)
	}
	b.backends = nil
	b.backendsUsable = func(context.Context) bool { return false }
	rr := sessionComplete(b, session.UploadID, []string{etag})
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("complete: %d %s", rr.Code, rr.Body.String())
	}
	if len(sessionRecords(fs3)) != 0 {
		t.Fatal("shared session remains after completed object was discarded")
	}
}

type failSessionSaveS3 struct{ s3API }

func (s *failSessionSaveS3) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, fmt.Errorf("session storage unavailable")
}

func TestLFSFailedSharedSessionSaveAbortsMultipart(t *testing.T) {
	a, _, fs3 := sharedSessionModules(t)
	a.s3Uploader.api = &failSessionSaveS3{s3API: a.s3Uploader.api}
	rr := httptest.NewRecorder()
	a.handleHTTPUploadInit(rr, httptest.NewRequest(http.MethodPost, "/lfs/uploads", strings.NewReader(`{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)))
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("init: %d %s", rr.Code, rr.Body.String())
	}
	if len(fs3.multipartUploads) != 0 || len(fs3.aborted) != 1 {
		t.Fatalf("session save failure leaked multipart upload: %+v", fs3.multipartUploads)
	}
	if !fs3.abortDeadline {
		t.Fatal("cleanup has no deadline")
	}
}
