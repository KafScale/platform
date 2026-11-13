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
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	lfsUploadSessionVersion     = 1
	lfsUploadLockRetryInterval  = 50 * time.Millisecond
	lfsLostMultipartUploadGrace = time.Hour
	lfsUploadSessionSweepMax    = 10 * time.Minute
	lfsUploadSessionSweepMin    = 30 * time.Second
	// Leave 64 KiB for keys and transaction framing under etcd's default limit.
	lfsUploadSessionMaxRecordLen = (3 << 19) - (64 << 10)
)

var (
	errUploadSessionStoreUnavailable = errors.New("multipart uploads require an available etcd session store")
	errUploadSessionVersion          = errors.New("unsupported upload session version")
	errUploadSessionConflict         = errors.New("upload session changed or request lost its lock")
	errUploadSessionNotFound         = errors.New("upload session not found")
	errUploadHashState               = errors.New("upload hash state cannot be saved")
)

type uploadSessionRecord struct {
	Version       int              `json:"version"`
	Publishing    bool             `json:"publishing,omitempty"`
	Discarding    bool             `json:"discarding,omitempty"`
	ID            string           `json:"id"`
	Topic         string           `json:"topic"`
	S3Key         string           `json:"s3_key"`
	StorageName   string           `json:"storage_name"`
	UploadID      string           `json:"upload_id"`
	ContentType   string           `json:"content_type"`
	SizeBytes     int64            `json:"size_bytes"`
	KeyBytes      []byte           `json:"key_bytes,omitempty"`
	Partition     int32            `json:"partition"`
	Checksum      string           `json:"checksum,omitempty"`
	ChecksumAlg   string           `json:"checksum_alg"`
	CreatedAt     time.Time        `json:"created_at"`
	ExpiresAt     time.Time        `json:"expires_at"`
	PartSize      int64            `json:"part_size"`
	NextPart      int32            `json:"next_part"`
	TotalUploaded int64            `json:"total_uploaded"`
	Parts         map[int32]string `json:"parts"`
	PartHashes    map[int32]string `json:"part_sha256"`
	PartSizes     map[int32]int64  `json:"part_sizes"`
	SHA256State   []byte           `json:"sha256_state"`
	ChecksumState []byte           `json:"checksum_state,omitempty"`
}

func (m *lfsModule) useSharedUploadSessions(clients ...*clientv3.Client) {
	m.sharedUploadSessions = true
	if len(clients) > 0 {
		m.uploadSessionClient = clients[0]
	}
}

func (m *lfsModule) lfsNamespace() string {
	ns := strings.TrimSpace(m.s3Namespace)
	if ns == "" {
		ns = "default"
	}
	return ns
}

func (m *lfsModule) uploadSessionScope() string {
	return url.PathEscape(m.s3Bucket) + "/" + url.PathEscape(m.lfsNamespace()) + "/"
}

func (m *lfsModule) uploadSessionPrefix() string {
	return "/kafscale/lfs-upload-sessions/" + m.uploadSessionScope()
}

func (m *lfsModule) uploadSessionKey(id string) string { return m.uploadSessionPrefix() + id }

func (m *lfsModule) uploadSessionIndexKey(key, uploadID string) string {
	sum := sha256.Sum256([]byte(key + "\x00" + uploadID))
	return fmt.Sprintf("/kafscale/lfs-upload-index/%s%x", m.uploadSessionScope(), sum)
}

func validUploadSessionID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, r := range id {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f' || r >= 'A' && r <= 'F' || r == '-') {
			return false
		}
	}
	return true
}

// Each request gets a unique leased mutex key. Delayed saves and deletes are
// fenced by both the record revision and the continued ownership of that key.
func (m *lfsModule) acquireUploadSession(ctx context.Context, id string) (*uploadSession, func(), error) {
	if !m.sharedUploadSessions {
		session, ok := m.lfsGetUploadSession(id)
		if !ok {
			return nil, nil, errUploadSessionNotFound
		}
		session.mu.Lock()
		return session, session.mu.Unlock, nil
	}
	if !validUploadSessionID(id) {
		return nil, nil, errUploadSessionNotFound
	}
	if m.uploadSessionClient == nil {
		return nil, nil, errUploadSessionStoreUnavailable
	}
	lease, err := concurrency.NewSession(m.uploadSessionClient, concurrency.WithTTL(30), concurrency.WithContext(ctx))
	if err != nil {
		return nil, nil, fmt.Errorf("open upload session lock: %w", err)
	}
	mutex := concurrency.NewMutex(lease, "/kafscale/lfs-upload-locks/"+m.uploadSessionScope()+id)
	if err := m.lockUploadSession(ctx, mutex); err != nil {
		m.releaseUploadSessionLock(ctx, mutex, lease, id)
		return nil, nil, err
	}
	opCtx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-lease.Done():
			cancel()
		case <-opCtx.Done():
		}
	}()
	unlock := func() {
		cancel()
		m.releaseUploadSessionLock(ctx, mutex, lease, id)
	}
	session, err := m.loadUploadSession(opCtx, id)
	if err != nil {
		unlock()
		return nil, nil, err
	}
	guard := mutex.IsOwner()
	session.storeGuard = &guard
	session.operationContext = opCtx
	return session, unlock, nil
}

// TryLock never switches cleanup to the long-lived client context. Contended
// attempts and failed acquisition cleanup remain bounded by explicit contexts.
func (m *lfsModule) lockUploadSession(ctx context.Context, mutex *concurrency.Mutex) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := mutex.TryLock(ctx)
		if !errors.Is(err, concurrency.ErrLocked) {
			return err
		}
		timer := time.NewTimer(lfsUploadLockRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *lfsModule) releaseUploadSessionLock(ctx context.Context, mutex *concurrency.Mutex, lease *concurrency.Session, id string) {
	lease.Orphan()
	cleanupCtx, cancel := m.uploadSessionCleanupContext(ctx)
	defer cancel()
	if err := mutex.Unlock(cleanupCtx); err != nil && !errors.Is(err, concurrency.ErrLockReleased) {
		m.logger.Warn("upload session unlock failed", "uploadId", logSafe(id), "error", err)
	}
	if _, err := m.uploadSessionClient.Revoke(cleanupCtx, lease.Lease()); err != nil && !errors.Is(err, rpctypes.ErrLeaseNotFound) {
		m.logger.Warn("upload session lease cleanup failed", "uploadId", logSafe(id), "error", err)
	}
}

func (m *lfsModule) saveUploadSession(ctx context.Context, session *uploadSession) error {
	if !m.sharedUploadSessions {
		m.lfsStoreUploadSession(session)
		return nil
	}
	if m.uploadSessionClient == nil {
		return errUploadSessionStoreUnavailable
	}
	if session.storeRevision == 0 {
		session.storeVersion = lfsUploadSessionVersion
	}
	if session.PartHashes == nil {
		session.PartHashes = make(map[int32]string)
	}
	record, err := uploadSessionToRecord(session)
	if err != nil {
		return err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(data) > lfsUploadSessionMaxRecordLen {
		return errors.New("upload session record exceeds maximum size")
	}
	key := m.uploadSessionKey(session.ID)
	checks := []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(key), "=", session.storeRevision)}
	if session.storeRevision > 0 {
		if session.storeGuard == nil {
			return errUploadSessionConflict
		}
		checks = append(checks, *session.storeGuard)
	}
	resp, err := m.uploadSessionClient.Txn(ctx).If(checks...).Then(
		clientv3.OpPut(key, string(data)),
		clientv3.OpPut(m.uploadSessionIndexKey(session.S3Key, session.UploadID), session.ID),
	).Commit()
	if err != nil {
		return fmt.Errorf("save upload session: %w", err)
	}
	if !resp.Succeeded {
		return errUploadSessionConflict
	}
	session.storeRevision = resp.Header.Revision
	return nil
}

func (m *lfsModule) deleteUploadSession(ctx context.Context, session *uploadSession) error {
	if !m.sharedUploadSessions {
		m.lfsDeleteUploadSession(session.ID)
		return nil
	}
	if m.uploadSessionClient == nil {
		return errUploadSessionStoreUnavailable
	}
	if session.storeGuard == nil {
		return errUploadSessionConflict
	}
	resp, err := m.uploadSessionClient.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(m.uploadSessionKey(session.ID)), "=", session.storeRevision),
		*session.storeGuard,
	).Then(
		clientv3.OpDelete(m.uploadSessionKey(session.ID)),
		clientv3.OpDelete(m.uploadSessionIndexKey(session.S3Key, session.UploadID)),
	).Commit()
	if err != nil {
		return fmt.Errorf("delete upload session: %w", err)
	}
	if !resp.Succeeded {
		return errUploadSessionConflict
	}
	return nil
}

func (m *lfsModule) loadUploadSession(ctx context.Context, id string) (*uploadSession, error) {
	if m.uploadSessionClient == nil {
		return nil, errUploadSessionStoreUnavailable
	}
	resp, err := m.uploadSessionClient.Get(ctx, m.uploadSessionKey(id))
	if err != nil {
		return nil, fmt.Errorf("read upload session: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return nil, errUploadSessionNotFound
	}
	if len(resp.Kvs[0].Value) > lfsUploadSessionMaxRecordLen {
		return nil, errors.New("upload session record exceeds maximum size")
	}
	var record uploadSessionRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &record); err != nil {
		return nil, fmt.Errorf("decode upload session: %w", err)
	}
	if record.ID != id {
		return nil, errUploadSessionNotFound
	}
	if record.Version < 0 || record.Version > lfsUploadSessionVersion {
		return nil, fmt.Errorf("%w: %d", errUploadSessionVersion, record.Version)
	}
	session, err := uploadSessionFromRecord(&record)
	if err != nil {
		return nil, err
	}
	session.storeRevision = resp.Kvs[0].ModRevision
	return session, nil
}

func marshalHashState(h lfsHashWriter) ([]byte, error) {
	marshaler, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		return nil, errUploadHashState
	}
	return marshaler.MarshalBinary()
}

func restoreHash(alg lfs.ChecksumAlg, state []byte) (lfsHashWriter, error) {
	var h lfsHashWriter
	if alg == lfs.ChecksumSHA256 {
		h = sha256.New()
	} else {
		created, err := lfs.NewChecksumHasher(alg)
		if err != nil {
			return nil, err
		}
		if created == nil {
			return nil, nil
		}
		h = created
	}
	unmarshaler, ok := h.(encoding.BinaryUnmarshaler)
	if !ok {
		return nil, errUploadHashState
	}
	if err := unmarshaler.UnmarshalBinary(state); err != nil {
		return nil, fmt.Errorf("restore upload hash state: %w", err)
	}
	return h, nil
}

func uploadSessionToRecord(session *uploadSession) (*uploadSessionRecord, error) {
	shaState, err := marshalHashState(session.sha256Hasher)
	if err != nil {
		return nil, err
	}
	record := &uploadSessionRecord{
		Version:       session.storeVersion,
		Publishing:    session.Publishing,
		Discarding:    session.Discarding,
		PartHashes:    session.PartHashes,
		ID:            session.ID,
		Topic:         session.Topic,
		S3Key:         session.S3Key,
		StorageName:   session.StorageName,
		UploadID:      session.UploadID,
		ContentType:   session.ContentType,
		SizeBytes:     session.SizeBytes,
		KeyBytes:      session.KeyBytes,
		Partition:     session.Partition,
		Checksum:      session.Checksum,
		ChecksumAlg:   string(session.ChecksumAlg),
		CreatedAt:     session.CreatedAt,
		ExpiresAt:     session.ExpiresAt,
		PartSize:      session.PartSize,
		NextPart:      session.NextPart,
		TotalUploaded: session.TotalUploaded,
		Parts:         session.Parts,
		PartSizes:     session.PartSizes,
		SHA256State:   shaState,
	}
	if session.checksumHasher != nil && session.checksumHasher != session.sha256Hasher {
		state, err := marshalHashState(session.checksumHasher)
		if err != nil {
			return nil, err
		}
		record.ChecksumState = state
	}
	return record, nil
}

func uploadSessionFromRecord(record *uploadSessionRecord) (*uploadSession, error) {
	alg := lfs.ChecksumAlg(record.ChecksumAlg)
	shaHasher, err := restoreHash(lfs.ChecksumSHA256, record.SHA256State)
	if err != nil {
		return nil, err
	}
	session := &uploadSession{
		storeVersion:  record.Version,
		Publishing:    record.Publishing,
		Discarding:    record.Discarding,
		PartHashes:    record.PartHashes,
		ID:            record.ID,
		Topic:         record.Topic,
		S3Key:         record.S3Key,
		StorageName:   record.StorageName,
		UploadID:      record.UploadID,
		ContentType:   record.ContentType,
		SizeBytes:     record.SizeBytes,
		KeyBytes:      record.KeyBytes,
		Partition:     record.Partition,
		Checksum:      record.Checksum,
		ChecksumAlg:   alg,
		CreatedAt:     record.CreatedAt,
		ExpiresAt:     record.ExpiresAt,
		PartSize:      record.PartSize,
		NextPart:      record.NextPart,
		TotalUploaded: record.TotalUploaded,
		Parts:         record.Parts,
		PartSizes:     record.PartSizes,
		sha256Hasher:  shaHasher,
	}
	if session.PartHashes == nil {
		session.PartHashes = make(map[int32]string)
	}
	if session.Parts == nil {
		session.Parts = make(map[int32]string)
	}
	if session.PartSizes == nil {
		session.PartSizes = make(map[int32]int64)
	}
	switch alg {
	case lfs.ChecksumNone, "":
	case lfs.ChecksumSHA256:
		session.checksumHasher = shaHasher
	default:
		h, err := restoreHash(alg, record.ChecksumState)
		if err != nil {
			return nil, err
		}
		session.checksumHasher = h
	}
	return session, nil
}

func uploadSessionSweepInterval(ttl time.Duration) time.Duration {
	interval := ttl / 2
	if interval > lfsUploadSessionSweepMax {
		interval = lfsUploadSessionSweepMax
	}
	if interval < lfsUploadSessionSweepMin {
		interval = lfsUploadSessionSweepMin
	}
	return interval
}

func (m *lfsModule) runUploadSessionSweeper(ctx context.Context) {
	ticker := time.NewTicker(uploadSessionSweepInterval(m.uploadSessionTTL))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sweepUploadSessions(ctx)
		}
	}
}

func (m *lfsModule) sweepUploadSessions(ctx context.Context) {
	now := time.Now().UTC()
	m.sweepExpiredUploadSessions(ctx, now)
	m.sweepLostMultipartUploads(ctx, now)
}

// S3 can complete an upload even when its response is lost. A later retry
// gets NoSuchUpload. The per-upload object key is unique: recognize the existing
// object by key and size, keeping the checksum from trusted etcd for consumers.
func (m *lfsModule) recoverCompletedMultipart(ctx context.Context, session *uploadSession) error {
	out, err := m.s3Uploader.api.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(m.s3Bucket), Key: aws.String(session.S3Key)})
	if err != nil {
		return fmt.Errorf("read previously completed multipart object: %w", err)
	}
	if out.Body != nil {
		defer func() { _ = out.Body.Close() }()
	}
	if out.ContentLength == nil || *out.ContentLength != session.SizeBytes {
		return errors.New("previously completed multipart object has unexpected size")
	}
	return nil
}

func isMissingMultipartUpload(err error) bool {
	var apiError smithy.APIError
	return errors.As(err, &apiError) && apiError.ErrorCode() == "NoSuchUpload"
}

func (m *lfsModule) abortMultipartIgnoringMissing(ctx context.Context, key, uploadID string) error {
	err := m.s3Uploader.AbortMultipartUpload(ctx, key, uploadID)
	if isMissingMultipartUpload(err) {
		return nil
	}
	return err
}

func (m *lfsModule) sweepExpiredUploadSessions(ctx context.Context, now time.Time) {
	if m.uploadSessionClient == nil {
		return
	}
	resp, err := m.uploadSessionClient.Get(ctx, m.uploadSessionPrefix(), clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		m.logger.Warn("lfs upload session sweep: list failed", "error", err)
		return
	}
	for _, kv := range resp.Kvs {
		id := strings.TrimPrefix(string(kv.Key), m.uploadSessionPrefix())
		if !validUploadSessionID(id) {
			continue
		}
		m.sweepUploadSession(ctx, now, id)
	}
}

func (m *lfsModule) sweepUploadSession(ctx context.Context, now time.Time, id string) {
	cleanupCtx, cancel := m.uploadSessionCleanupContext(ctx)
	defer cancel()
	session, unlock, err := m.acquireUploadSession(cleanupCtx, id)
	if errors.Is(err, errUploadSessionNotFound) {
		return
	}
	if err != nil {
		m.logger.Warn("lfs upload session sweep: read failed", "uploadId", logSafe(id), "error", err)
		return
	}
	defer unlock()
	if now.Before(session.ExpiresAt) {
		return
	}
	if err := m.discardMultipartUpload(cleanupCtx, session); err != nil {
		m.logger.Warn("lfs upload session sweep: cleanup failed", "uploadId", logSafe(id), "error", err)
		return
	}
	m.logger.Info("lfs upload session expired and removed", "uploadId", logSafe(id))
}

func (m *lfsModule) sweepLostMultipartUploads(ctx context.Context, now time.Time) {
	if m.uploadSessionClient == nil {
		return
	}
	cutoff := now.Add(-(m.uploadSessionTTL + lfsLostMultipartUploadGrace))
	input := &s3.ListMultipartUploadsInput{
		Bucket: aws.String(m.s3Uploader.bucket),
		Prefix: aws.String(m.lfsNamespace() + "/" + lfsPrefix + "/"),
	}
	for {
		page, err := m.s3Uploader.api.ListMultipartUploads(ctx, input)
		if err != nil {
			m.logger.Warn("lfs upload session sweep: list multipart uploads failed", "error", err)
			return
		}
		for _, upload := range page.Uploads {
			initiated := aws.ToTime(upload.Initiated)
			if initiated.IsZero() || initiated.After(cutoff) {
				continue
			}
			key := aws.ToString(upload.Key)
			uploadID := aws.ToString(upload.UploadId)
			// Session TTLs may differ during a rollout. A trusted session, including
			// one with a later persisted expiry, must never be treated as lost by age.
			current, err := m.uploadSessionClient.Get(ctx, m.uploadSessionIndexKey(key, uploadID))
			if err != nil {
				m.logger.Warn("lfs upload session sweep: ownership read failed", "error", err)
				return
			}
			if len(current.Kvs) > 0 {
				continue
			}
			if err := m.abortMultipartIgnoringMissing(ctx, key, uploadID); err != nil {
				m.logger.Warn("lfs upload session sweep: abort of lost multipart upload failed", "s3Key", logSafe(key), "error", err)
				continue
			}
			m.logger.Info("lfs upload session sweep: aborted lost multipart upload", "s3Key", logSafe(key), "initiated", initiated.Format(time.RFC3339))
		}
		if !aws.ToBool(page.IsTruncated) {
			return
		}
		input.KeyMarker = page.NextKeyMarker
		input.UploadIdMarker = page.NextUploadIdMarker
	}
}

func (m *lfsModule) checkUploadSessionActive(ctx context.Context, w http.ResponseWriter, requestID string, session *uploadSession) bool {
	if !session.Discarding {
		return true
	}
	if err := m.discardMultipartUpload(ctx, session); err != nil {
		m.logger.Warn("upload discard retry failed", "uploadId", logSafe(session.ID), "error", err)
	}
	m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusGone, "upload_aborted", "upload cleanup has begun; start a new upload")
	return false
}

func (m *lfsModule) checkUploadSessionVersion(ctx context.Context, w http.ResponseWriter, requestID string, session *uploadSession) bool {
	if !m.sharedUploadSessions || session.storeVersion == lfsUploadSessionVersion {
		return true
	}
	if session.storeVersion != 0 {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusServiceUnavailable, "upload_version_unsupported", "upload session version is not supported by this proxy")
		return false
	}
	if err := m.discardMultipartUpload(ctx, session); err != nil {
		m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusBadGateway, "abort_failed", "legacy upload cleanup failed; retry the request")
		return false
	}
	m.lfsWriteHTTPError(w, requestID, session.Topic, http.StatusGone, "upload_restart_required", "restart uploads created before durable part identities were introduced")
	return false
}

func (m *lfsModule) uploadSessionCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := m.dialTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

func (m *lfsModule) dropUploadSession(ctx context.Context, requestID string, session *uploadSession) {
	cleanupCtx, cancel := m.uploadSessionCleanupContext(ctx)
	defer cancel()
	if err := m.deleteUploadSession(cleanupCtx, session); err != nil {
		m.logger.Warn("multipart upload session delete failed", "requestId", logSafe(requestID), "uploadId", logSafe(session.ID), "error", err)
	}
}

func (m *lfsModule) writeUploadSessionError(w http.ResponseWriter, requestID string, err error) {
	if errors.Is(err, errUploadSessionVersion) {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusServiceUnavailable, "upload_version_unsupported", "upload session version is not supported by this proxy")
		return
	}
	if errors.Is(err, errUploadSessionNotFound) {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusNotFound, "upload_not_found", "upload session not found")
		return
	}
	if errors.Is(err, errUploadSessionConflict) {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusConflict, "upload_conflict", "upload session changed; retry the request")
		return
	}
	m.logger.Warn("multipart upload session store failed", "requestId", logSafe(requestID), "error", err)
	m.lfsWriteHTTPError(w, requestID, "", http.StatusServiceUnavailable, "session_store_unavailable", "multipart upload session store unavailable")
}
