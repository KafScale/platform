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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Capture a session write as though the request timed out after dispatch. The
// server may still evaluate that transaction after another replica advances it.
type deferredSessionKV struct {
	clientv3.KV
	prefix            string
	deferNext         bool
	deferDelete       bool
	skipSessionWrites int
	checks            []clientv3.Cmp
	writes            []clientv3.Op
}
type deferredSessionTxn struct {
	clientv3.Txn
	parent *deferredSessionKV
	checks []clientv3.Cmp
	writes []clientv3.Op
}

func (k *deferredSessionKV) Txn(ctx context.Context) clientv3.Txn {
	return &deferredSessionTxn{Txn: k.KV.Txn(ctx), parent: k}
}
func (t *deferredSessionTxn) If(c ...clientv3.Cmp) clientv3.Txn {
	t.checks = append(t.checks, c...)
	t.Txn = t.Txn.If(c...)
	return t
}
func (t *deferredSessionTxn) Then(o ...clientv3.Op) clientv3.Txn {
	t.writes = append(t.writes, o...)
	t.Txn = t.Txn.Then(o...)
	return t
}
func (t *deferredSessionTxn) Else(o ...clientv3.Op) clientv3.Txn { t.Txn = t.Txn.Else(o...); return t }
func (t *deferredSessionTxn) Commit() (*clientv3.TxnResponse, error) {
	if t.parent.deferNext {
		for _, op := range t.writes {
			if ((op.IsPut() && !t.parent.deferDelete) || (op.IsDelete() && t.parent.deferDelete)) && strings.HasPrefix(string(op.KeyBytes()), t.parent.prefix) {
				if t.parent.skipSessionWrites > 0 {
					t.parent.skipSessionWrites--
					return t.Txn.Commit()
				}
				t.parent.deferNext = false
				t.parent.checks = t.checks
				t.parent.writes = t.writes
				return nil, context.DeadlineExceeded
			}
		}
	}
	return t.Txn.Commit()
}

func TestLFSDeferredSessionWriteCannotLoseAcknowledgedPart(t *testing.T) {
	a, b, _ := sharedSessionModules(t)
	first := bytes.Repeat([]byte("a"), 5<<20)
	tail := []byte("tail")
	init := sessionInit(t, a, fmt.Sprintf(`{"topic":"orders","content_type":"application/octet-stream","size_bytes":%d}`, len(first)+len(tail)))
	kv := &deferredSessionKV{KV: a.uploadSessionClient.KV, prefix: a.uploadSessionPrefix(), deferNext: true, skipSessionWrites: 1}
	a.uploadSessionClient.KV = kv
	code, _ := sessionPart(t, a, init.UploadID, 1, first)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("lost session save response: %d", code)
	}
	if len(kv.writes) == 0 {
		t.Fatal("did not intercept a session write")
	}
	code, etag1 := sessionPart(t, b, init.UploadID, 1, first)
	if code != http.StatusOK {
		t.Fatalf("retry part 1: %d %s", code, etag1)
	}
	code, etag2 := sessionPart(t, b, init.UploadID, 2, tail)
	if code != http.StatusOK {
		t.Fatalf("part 2: %d %s", code, etag2)
	}
	late, err := b.uploadSessionClient.Txn(context.Background()).If(kv.checks...).Then(kv.writes...).Commit()
	if err != nil {
		t.Fatal(err)
	}
	if late.Succeeded {
		t.Fatal("delayed old request rolled back acknowledged progress")
	}
	rr := sessionComplete(b, init.UploadID, []string{etag1, etag2})
	if rr.Code != http.StatusOK {
		t.Fatalf("complete: %d %s", rr.Code, rr.Body.String())
	}
	var env lfs.Envelope
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(append(first, tail...))
	if env.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("lost part from authoritative checksum")
	}
}

func TestLFSS3SessionTamperingCannotForgeAuthoritativeChecksum(t *testing.T) {
	a, b, fs3 := sharedSessionModules(t)
	original := []byte("payload")
	replacement := []byte("tamper!")
	originalSHA := sha256.Sum256(original)
	badSHA := sha256.Sum256(replacement)
	init := sessionInit(t, a, fmt.Sprintf(`{"topic":"orders","content_type":"application/octet-stream","size_bytes":7,"checksum_alg":"sha256","checksum":%q}`, hex.EncodeToString(originalSHA[:])))
	code, etag := sessionPart(t, a, init.UploadID, 1, original)
	if code != http.StatusOK {
		t.Fatal(code, etag)
	}
	saved, err := a.loadUploadSession(context.Background(), init.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	record, err := uploadSessionToRecord(saved)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	_, _ = h.Write(replacement)
	record.SHA256State, err = marshalHashState(h)
	if err != nil {
		t.Fatal(err)
	}
	record.Checksum = hex.EncodeToString(badSHA[:])
	// Forged legacy session JSON and blob bytes in S3 must not become trusted state.
	fs3.objects["test-ns/@lfs-uploads/"+init.UploadID+".json"], err = json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	fs3.objects[init.S3Key] = replacement
	rr := sessionComplete(b, init.UploadID, []string{etag})
	if rr.Code != http.StatusOK {
		t.Fatalf("complete: %d %s", rr.Code, rr.Body.String())
	}
	var env lfs.Envelope
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.SHA256 != hex.EncodeToString(originalSHA[:]) || env.Checksum != hex.EncodeToString(originalSHA[:]) {
		t.Fatalf("published forged checksum: %+v", env)
	}
	// The consumer detects the replacement using the trusted envelope.
	actual := sha256.Sum256(fs3.objects[env.Key])
	if env.SHA256 == hex.EncodeToString(actual[:]) {
		t.Fatal("tampered S3 bytes passed the envelope integrity check")
	}
}

func TestLFSLostSweepPreservesUnexpiredSessionWithLongerTTL(t *testing.T) {
	a, _, fs3 := sharedSessionModules(t)
	a.uploadSessionTTL = time.Hour
	now := time.Now().UTC()
	started := now.Add(-3 * time.Hour)
	id := "0000000a-0000-0000-0000-000000000099"
	key := "test-ns/@lfs/orders/active-long-session"
	session := &uploadSession{ID: id, Topic: "orders", S3Key: key, UploadID: "still-active", CreatedAt: started, ExpiresAt: now.Add(time.Hour), SizeBytes: 7, PartSize: 5 << 20, NextPart: 1, sha256Hasher: sha256.New()}
	if err := a.saveUploadSession(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	fs3.multipartUploads = []types.MultipartUpload{{Key: aws.String(key), UploadId: aws.String("still-active"), Initiated: &started}}
	a.sweepUploadSessions(context.Background())
	if len(fs3.aborted) != 0 || len(fs3.multipartUploads) != 1 {
		t.Fatalf("aborted active upload: %v", fs3.aborted)
	}
	if _, err := a.loadUploadSession(context.Background(), id); err != nil {
		t.Fatalf("lost live session: %v", err)
	}
}

func TestLFSOldLockCannotSaveOrDeleteSession(t *testing.T) {
	a, b, _ := sharedSessionModules(t)
	init := sessionInit(t, a, `{"topic":"orders","content_type":"application/octet-stream","size_bytes":7}`)
	old, unlock, err := a.acquireUploadSession(context.Background(), init.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	leaseKey, err := a.uploadSessionClient.Get(context.Background(), string(old.storeGuard.Key))
	if err != nil || len(leaseKey.Kvs) != 1 {
		t.Fatalf("read lock lease: %v", err)
	}
	if _, err := a.uploadSessionClient.Revoke(context.Background(), clientv3.LeaseID(leaseKey.Kvs[0].Lease)); err != nil {
		t.Fatal(err)
	}
	current, release, err := b.acquireUploadSession(context.Background(), init.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// The revision has not changed: the ownership comparison alone must fence it.
	if current.storeRevision != old.storeRevision {
		t.Fatal("test unexpectedly changed revision")
	}
	if err := a.saveUploadSession(context.Background(), old); !errors.Is(err, errUploadSessionConflict) {
		t.Fatalf("stale save: %v", err)
	}
	if err := a.deleteUploadSession(context.Background(), old); !errors.Is(err, errUploadSessionConflict) {
		t.Fatalf("stale delete: %v", err)
	}
	if _, err := b.loadUploadSession(context.Background(), init.UploadID); err != nil {
		t.Fatal(err)
	}
	// Revocation is reported by the next keepalive response (30s lease / 3).
	// The transaction fences above must hold before that local notification.
	select {
	case <-old.operationContext.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("lost lease did not cancel the request")
	}
}

type unavailableUploadIndexKV struct{ clientv3.KV }

func (k unavailableUploadIndexKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if strings.HasPrefix(key, "/kafscale/lfs-upload-index/") {
		return nil, errors.New("session store unavailable")
	}
	return k.KV.Get(ctx, key, opts...)
}
func TestLFSLostSweepDoesNotAbortWithoutTrustedOwnershipRead(t *testing.T) {
	a, _, fs3 := sharedSessionModules(t)
	a.uploadSessionClient.KV = unavailableUploadIndexKV{KV: a.uploadSessionClient.KV}
	started := time.Now().Add(-3 * time.Hour)
	fs3.multipartUploads = []types.MultipartUpload{{Key: aws.String("test-ns/@lfs/orders/unknown"), UploadId: aws.String("unknown"), Initiated: &started}}
	a.sweepUploadSessions(context.Background())
	if len(fs3.aborted) != 0 || len(fs3.multipartUploads) != 1 {
		t.Fatal("aborted an upload without an authoritative ownership check")
	}
}
