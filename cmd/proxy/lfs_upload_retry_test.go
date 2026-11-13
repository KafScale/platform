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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type latePart struct {
	body []byte
	etag string
}
type latePartS3 struct {
	s3API
	delay         bool
	pendingNumber int32
	pending       latePart
	calls         int
	parts         map[int32]latePart
}

func (s *latePartS3) UploadPart(ctx context.Context, p *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	s.calls++
	body, err := io.ReadAll(p.Body)
	if err != nil {
		return nil, err
	}
	sum := md5.Sum(body)
	part := latePart{body: body, etag: fmt.Sprintf("%x", sum)}
	if s.delay {
		s.delay = false
		s.pendingNumber = aws.ToInt32(p.PartNumber)
		s.pending = part
		return nil, context.DeadlineExceeded
	}
	s.parts[aws.ToInt32(p.PartNumber)] = part
	return &s3.UploadPartOutput{ETag: aws.String(part.etag)}, nil
}
func (s *latePartS3) CompleteMultipartUpload(ctx context.Context, p *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	for _, part := range p.MultipartUpload.Parts {
		if s.parts[aws.ToInt32(part.PartNumber)].etag != aws.ToString(part.ETag) {
			return nil, &smithy.GenericAPIError{Code: "InvalidPart", Message: "part ETag no longer matches"}
		}
	}
	return s.s3API.CompleteMultipartUpload(ctx, p, opts...)
}

func TestLFSPartRetryContentIsBoundBeforeS3Write(t *testing.T) {
	for _, shared := range []bool{true, false} {
		t.Run(fmt.Sprintf("shared=%t", shared), func(t *testing.T) {
			a, b, _ := sharedSessionModules(t)
			if !shared {
				a.sharedUploadSessions = false
				b = a
			}
			init := sessionInit(t, a, `{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)
			store := &latePartS3{s3API: a.s3Uploader.api, delay: true, parts: map[int32]latePart{}}
			a.s3Uploader.api = store
			if code, _ := sessionPart(t, a, init.UploadID, 1, []byte("payload")); code != http.StatusBadGateway {
				t.Fatal("expected S3 timeout", code)
			}
			code, body := sessionPart(t, b, init.UploadID, 1, []byte("changed"))
			if code != http.StatusConflict || !strings.Contains(body, "part_conflict") {
				t.Fatalf("changed retry: %d %s", code, body)
			}
			if store.calls != 1 {
				t.Fatal("conflicting retry reached S3")
			}
			code, etag := sessionPart(t, b, init.UploadID, 1, []byte("payload"))
			if code != http.StatusOK {
				t.Fatal("same-body retry failed", code, etag)
			}
			store.parts[store.pendingNumber] = store.pending
			if code, body := sessionPart(t, b, init.UploadID, 1, []byte("changed")); code != http.StatusConflict {
				t.Fatalf("changed acknowledged part: %d %s", code, body)
			}
			rr := sessionComplete(b, init.UploadID, []string{etag})
			if rr.Code != http.StatusOK {
				t.Fatalf("completion after late request: %d %s", rr.Code, rr.Body.String())
			}
			var env lfs.Envelope
			if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256([]byte("payload"))
			if env.SHA256 != hex.EncodeToString(sum[:]) {
				t.Fatal("failed attempt advanced the checksum")
			}
		})
	}
}

type unavailableLockDeleteKV struct {
	clientv3.KV
	contexts  chan context.Context
	recovered chan struct{}
}

func (k *unavailableLockDeleteKV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	k.contexts <- ctx
	select {
	case <-k.recovered:
		return k.KV.Delete(ctx, key, opts...)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func TestLFSCanceledLockWaitHasBoundedCleanup(t *testing.T) {
	a, b, _ := sharedSessionModules(t)
	init := sessionInit(t, a, `{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)
	_, unlock, err := a.acquireUploadSession(context.Background(), init.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	b.dialTimeout = 50 * time.Millisecond
	blocked := &unavailableLockDeleteKV{KV: b.uploadSessionClient.KV, contexts: make(chan context.Context, 4), recovered: make(chan struct{})}
	b.uploadSessionClient.KV = blocked
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, release, err := b.acquireUploadSession(ctx, init.UploadID)
		if release != nil {
			release()
		}
		done <- err
	}()
	select {
	case err := <-done:
		close(blocked.recovered)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waiter: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(blocked.recovered)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("waiter failed to recover")
		}
		t.Fatal("canceled request remained blocked in unbounded cleanup")
	}
	if len(blocked.contexts) == 0 {
		t.Fatal("did not exercise lock cleanup failure")
	}
	for len(blocked.contexts) > 0 {
		if _, ok := (<-blocked.contexts).Deadline(); !ok {
			t.Fatal("lock cleanup used a context without a deadline")
		}
	}
}

func TestLFSLegacySessionMustRestartBeforeUploading(t *testing.T) {
	a, b, fs3 := sharedSessionModules(t)
	init := sessionInit(t, a, `{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)
	resp, err := a.uploadSessionClient.Get(context.Background(), a.uploadSessionKey(init.UploadID))
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatal("read session", err)
	}
	var record uploadSessionRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &record); err != nil {
		t.Fatal(err)
	}
	record.Version = 0
	record.PartHashes = nil
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.uploadSessionClient.Put(context.Background(), a.uploadSessionKey(init.UploadID), string(data)); err != nil {
		t.Fatal(err)
	}
	code, body := sessionPart(t, b, init.UploadID, 1, []byte("payload"))
	if code != http.StatusGone || !strings.Contains(body, "upload_restart_required") {
		t.Fatalf("legacy part: %d %s", code, body)
	}
	if len(fs3.multipartUploads) != 0 || len(sessionRecords(t, a)) != 0 {
		t.Fatal("legacy upload was not cleaned up")
	}
}

func TestLFSFullMultipartSessionFitsEtcdLimit(t *testing.T) {
	a, _, _ := sharedSessionModules(t)
	session := &uploadSession{
		ID: "0000000a-0000-0000-0000-000000000100", Topic: "orders", S3Key: "test-ns/@lfs/orders/full", UploadID: "full-upload",
		Parts: make(map[int32]string), PartHashes: make(map[int32]string), PartSizes: make(map[int32]int64), sha256Hasher: sha256.New(), ExpiresAt: time.Now().Add(time.Hour),
	}
	for part := int32(1); part <= 10000; part++ {
		session.Parts[part] = `"0123456789abcdef0123456789abcdef"`
		session.PartHashes[part] = strings.Repeat("a", 64)
		session.PartSizes[part] = 5 << 20
	}
	if err := a.saveUploadSession(context.Background(), session); err != nil {
		t.Fatalf("supported 10,000-part session cannot be persisted: %v", err)
	}
	loaded, err := a.loadUploadSession(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Parts) != 10000 || len(loaded.PartHashes) != 10000 {
		t.Fatal("large session lost part metadata")
	}
}

type completionCountingS3 struct {
	s3API
	completions int
}

func (s *completionCountingS3) CompleteMultipartUpload(ctx context.Context, p *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	s.completions++
	return s.s3API.CompleteMultipartUpload(ctx, p, opts...)
}

func TestLFSCompleteRequiresAllPartsBeforeS3Finalization(t *testing.T) {
	a, b, fs3 := sharedSessionModules(t)
	payload := bytes.Repeat([]byte("a"), 5<<20)
	init := sessionInit(t, a, fmt.Sprintf(`{"topic":"orders","content_type":"application/octet-stream","size_bytes":%d}`, len(payload)+4))
	code, first := sessionPart(t, a, init.UploadID, 1, payload)
	if code != http.StatusOK {
		t.Fatal(code, first)
	}
	code, last := sessionPart(t, b, init.UploadID, 2, []byte("tail"))
	if code != http.StatusOK {
		t.Fatal(code, last)
	}
	counter := &completionCountingS3{s3API: a.s3Uploader.api}
	a.s3Uploader.api = counter
	for _, parts := range []string{
		fmt.Sprintf(`[{"part_number":1,"etag":%q}]`, first),
		fmt.Sprintf(`[{"part_number":2,"etag":%q}]`, last),
		fmt.Sprintf(`[{"part_number":1,"etag":%q},{"part_number":1,"etag":%q}]`, first, first),
		fmt.Sprintf(`[{"part_number":2,"etag":%q},{"part_number":1,"etag":%q}]`, last, first),
	} {
		rr := httptest.NewRecorder()
		b.handleHTTPUploadSession(rr, httptest.NewRequest(http.MethodPost, "/lfs/uploads/"+init.UploadID+"/complete", strings.NewReader(`{"parts":`+parts+`}`)))
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("invalid part list: %d %s", rr.Code, rr.Body.String())
		}
	}
	if counter.completions != 0 || len(fs3.multipartUploads) != 1 {
		t.Fatal("invalid request finalized the S3 object")
	}
	if rr := sessionComplete(b, init.UploadID, []string{first, last}); rr.Code != http.StatusOK {
		t.Fatalf("valid completion: %d %s", rr.Code, rr.Body.String())
	}
	if counter.completions != 1 {
		t.Fatal("valid completion did not finalize exactly once")
	}
}

func TestLFSChecksumMismatchAbortsBeforeS3Finalization(t *testing.T) {
	a, b, fs3 := sharedSessionModules(t)
	init := sessionInit(t, a, fmt.Sprintf(`{"topic":"orders","content_type":"application/octet-stream","size_bytes":7,"checksum_alg":"sha256","checksum":%q}`, strings.Repeat("0", 64)))
	code, etag := sessionPart(t, a, init.UploadID, 1, []byte("payload"))
	if code != http.StatusOK {
		t.Fatal(code, etag)
	}
	counter := &completionCountingS3{s3API: a.s3Uploader.api}
	a.s3Uploader.api = counter
	rr := sessionComplete(b, init.UploadID, []string{etag})
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "checksum_mismatch") {
		t.Fatalf("completion: %d %s", rr.Code, rr.Body.String())
	}
	if counter.completions != 0 {
		t.Fatal("checksum mismatch left a completed orphan")
	}
	if len(fs3.aborted) != 1 || len(fs3.multipartUploads) != 0 || len(sessionRecords(t, a)) != 0 {
		t.Fatal("checksum mismatch did not clean up the multipart session")
	}
}

type completedResponseLostS3 struct {
	s3API
	fake         *fakeS3
	payload      []byte
	completions  int
	loseResponse bool
}

func (s *completedResponseLostS3) CompleteMultipartUpload(ctx context.Context, p *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	s.completions++
	found := false
	for _, u := range s.fake.multipartUploads {
		if aws.ToString(u.Key) == aws.ToString(p.Key) && aws.ToString(u.UploadId) == aws.ToString(p.UploadId) {
			found = true
		}
	}
	if !found {
		return nil, &smithy.GenericAPIError{Code: "NoSuchUpload", Message: "multipart upload has already completed"}
	}
	out, err := s.s3API.CompleteMultipartUpload(ctx, p, opts...)
	if err != nil {
		return nil, err
	}
	s.fake.objects[aws.ToString(p.Key)] = append([]byte(nil), s.payload...)
	if s.loseResponse {
		s.loseResponse = false
		return nil, context.DeadlineExceeded
	}
	return out, nil
}
func TestLFSCompleteRecoversLostS3CompletionResponse(t *testing.T) {
	a, b, fs3 := sharedSessionModules(t)
	init := sessionInit(t, a, `{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)
	code, etag := sessionPart(t, a, init.UploadID, 1, []byte("payload"))
	if code != http.StatusOK {
		t.Fatal(code, etag)
	}
	store := &completedResponseLostS3{s3API: a.s3Uploader.api, fake: fs3, payload: []byte("payload"), loseResponse: true}
	a.s3Uploader.api = store
	if rr := sessionComplete(a, init.UploadID, []string{etag}); rr.Code != http.StatusBadGateway {
		t.Fatal("expected lost response", rr.Code)
	}
	rr := sessionComplete(b, init.UploadID, []string{etag})
	if rr.Code != http.StatusOK {
		t.Fatalf("completed object cannot resume on another replica: %d %s", rr.Code, rr.Body.String())
	}
	var env lfs.Envelope
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("payload"))
	if env.SHA256 != hex.EncodeToString(sum[:]) || len(sessionRecords(t, a)) != 0 {
		t.Fatal("recovered completion lost its authoritative metadata")
	}
}

type deleteObjectResponseLostS3 struct {
	s3API
	failOnce bool
}

func (s *deleteObjectResponseLostS3) DeleteObject(ctx context.Context, p *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if s.failOnce {
		s.failOnce = false
		return nil, context.DeadlineExceeded
	}
	return s.s3API.DeleteObject(ctx, p, opts...)
}
func TestLFSDiscardIntentPreventsRecoveryFromPublishing(t *testing.T) {
	a, b, fs3 := sharedSessionModules(t)
	init := sessionInit(t, a, `{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)
	code, etag := sessionPart(t, a, init.UploadID, 1, []byte("payload"))
	if code != http.StatusOK {
		t.Fatal(code, etag)
	}
	completed := &completedResponseLostS3{s3API: a.s3Uploader.api, fake: fs3, payload: []byte("payload")}
	a.s3Uploader.api = &deleteObjectResponseLostS3{s3API: completed, failOnce: true}
	a.backends = nil
	a.backendsUsable = func(context.Context) bool { return false }
	rr := sessionComplete(a, init.UploadID, []string{etag})
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("backend failure: %d %s", rr.Code, rr.Body.String())
	}
	pending, err := b.loadUploadSession(context.Background(), init.UploadID)
	if err != nil || !pending.Discarding {
		t.Fatalf("cleanup intent not persisted: %v", err)
	}
	rr = sessionComplete(b, init.UploadID, []string{etag})
	if rr.Code != http.StatusGone || !strings.Contains(rr.Body.String(), "upload_aborted") {
		t.Fatalf("discarded blob was published: %d %s", rr.Code, rr.Body.String())
	}
	if _, ok := fs3.objects[init.S3Key]; ok {
		t.Fatal("failed cleanup was not retried")
	}
	if len(sessionRecords(t, a)) != 0 {
		t.Fatal("discarded session remains")
	}
}

func TestLFSAcknowledgedBlobSurvivesAbortAfterSessionDeleteFailure(t *testing.T) {
	a, b, fs3 := sharedSessionModules(t)
	init := sessionInit(t, a, `{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)
	code, etag := sessionPart(t, a, init.UploadID, 1, []byte("payload"))
	if code != http.StatusOK {
		t.Fatal(code, etag)
	}
	a.s3Uploader.api = &completedResponseLostS3{s3API: a.s3Uploader.api, fake: fs3, payload: []byte("payload")}
	kv := &deferredSessionKV{KV: a.uploadSessionClient.KV, prefix: a.uploadSessionPrefix(), deferNext: true, deferDelete: true}
	a.uploadSessionClient.KV = kv
	rr := sessionComplete(a, init.UploadID, []string{etag})
	if rr.Code != http.StatusOK {
		t.Fatalf("Kafka acknowledgement: %d %s", rr.Code, rr.Body.String())
	}
	saved, err := b.loadUploadSession(context.Background(), init.UploadID)
	if err != nil || !saved.Publishing {
		t.Fatalf("publication marker missing after successful Kafka produce: %v", err)
	}
	abort := httptest.NewRecorder()
	b.handleHTTPUploadSession(abort, httptest.NewRequest(http.MethodDelete, "/lfs/uploads/"+init.UploadID, nil))
	if abort.Code != http.StatusNoContent {
		t.Fatalf("abort: %d %s", abort.Code, abort.Body.String())
	}
	if string(fs3.objects[init.S3Key]) != "payload" {
		t.Fatal("cleanup deleted a Kafka-acknowledged blob")
	}
	if len(fs3.deleted) != 0 {
		t.Fatal("cleanup issued an unsafe object DELETE")
	}
	late, err := b.uploadSessionClient.Txn(context.Background()).If(kv.checks...).Then(kv.writes...).Commit()
	if err != nil || late.Succeeded {
		t.Fatalf("old metadata delete was not fenced: %v", err)
	}
}

func TestLFSCompletionRecoveryRejectsMissingOrWrongSizeObject(t *testing.T) {
	for _, missing := range []bool{true, false} {
		t.Run(fmt.Sprintf("missing=%t", missing), func(t *testing.T) {
			a, b, fs3 := sharedSessionModules(t)
			init := sessionInit(t, a, `{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)
			code, etag := sessionPart(t, a, init.UploadID, 1, []byte("payload"))
			if code != http.StatusOK {
				t.Fatal(code, etag)
			}
			a.s3Uploader.api = &completedResponseLostS3{s3API: a.s3Uploader.api, fake: fs3, payload: []byte("payload"), loseResponse: true}
			if rr := sessionComplete(a, init.UploadID, []string{etag}); rr.Code != http.StatusBadGateway {
				t.Fatal(rr.Code)
			}
			if missing {
				delete(fs3.objects, init.S3Key)
			} else {
				fs3.objects[init.S3Key] = []byte("wrong size")
			}
			if rr := sessionComplete(b, init.UploadID, []string{etag}); rr.Code != http.StatusBadGateway {
				t.Fatalf("invalid object was referenced: %d %s", rr.Code, rr.Body.String())
			}
			fs3.objects[init.S3Key] = []byte("payload")
			if rr := sessionComplete(b, init.UploadID, []string{etag}); rr.Code != http.StatusOK {
				t.Fatalf("completion failed after object recovered: %d %s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestLFSFutureSessionVersionIsNotModifiedOrAborted(t *testing.T) {
	a, b, fs3 := sharedSessionModules(t)
	init := sessionInit(t, a, `{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)
	resp, err := a.uploadSessionClient.Get(context.Background(), a.uploadSessionKey(init.UploadID))
	if err != nil || len(resp.Kvs) != 1 {
		t.Fatal("read session", err)
	}
	var record uploadSessionRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &record); err != nil {
		t.Fatal(err)
	}
	record.Version = lfsUploadSessionVersion + 1
	record.ExpiresAt = time.Now().Add(-time.Hour)
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.uploadSessionClient.Put(context.Background(), a.uploadSessionKey(init.UploadID), string(data)); err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	b.handleHTTPUploadSession(rr, httptest.NewRequest(http.MethodDelete, "/lfs/uploads/"+init.UploadID, nil))
	if rr.Code != http.StatusServiceUnavailable || !strings.Contains(rr.Body.String(), "upload_version_unsupported") {
		t.Fatalf("future version abort: %d %s", rr.Code, rr.Body.String())
	}
	b.sweepUploadSessions(context.Background())
	if len(fs3.aborted) != 0 || len(fs3.deleted) != 0 || len(sessionRecords(t, a)) != 1 {
		t.Fatal("older proxy modified a session version it cannot interpret")
	}
}
