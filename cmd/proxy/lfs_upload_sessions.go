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
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const (
	lfsUploadSessionPrefix       = "@lfs-uploads"
	lfsUploadSessionLockStripes  = 64
	lfsLostMultipartUploadGrace  = time.Hour
	lfsUploadSessionSweepMax     = 10 * time.Minute
	lfsUploadSessionSweepMin     = 30 * time.Second
	lfsUploadSessionMaxRecordLen = 1 << 20
)

var (
	errUploadSessionNotFound = errors.New("upload session not found")
	errUploadHashState       = errors.New("upload hash state cannot be saved")
)

type uploadSessionRecord struct {
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
	PartSizes     map[int32]int64  `json:"part_sizes"`
	SHA256State   []byte           `json:"sha256_state"`
	ChecksumState []byte           `json:"checksum_state,omitempty"`
}

func (m *lfsModule) useSharedUploadSessions() {
	m.sharedUploadSessions = true
}

func (m *lfsModule) lfsNamespace() string {
	ns := strings.TrimSpace(m.s3Namespace)
	if ns == "" {
		ns = "default"
	}
	return ns
}

func (m *lfsModule) uploadSessionPrefix() string {
	return m.lfsNamespace() + "/" + lfsUploadSessionPrefix + "/"
}

func (m *lfsModule) uploadSessionKey(id string) string {
	return m.uploadSessionPrefix() + id + ".json"
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

func (m *lfsModule) uploadSessionLock(id string) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	mu := &m.uploadLocks[h.Sum32()%lfsUploadSessionLockStripes]
	mu.Lock()
	return mu.Unlock
}

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
	unlock := m.uploadSessionLock(id)
	session, err := m.loadUploadSession(ctx, id)
	if err != nil {
		unlock()
		return nil, nil, err
	}
	return session, unlock, nil
}

func (m *lfsModule) saveUploadSession(ctx context.Context, session *uploadSession) error {
	if !m.sharedUploadSessions {
		m.lfsStoreUploadSession(session)
		return nil
	}
	record, err := uploadSessionToRecord(session)
	if err != nil {
		return err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = m.s3Uploader.api.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(m.s3Uploader.bucket),
		Key:           aws.String(m.uploadSessionKey(session.ID)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("save upload session: %w", err)
	}
	return nil
}

func (m *lfsModule) deleteUploadSession(ctx context.Context, id string) error {
	m.lfsDeleteUploadSession(id)
	if !m.sharedUploadSessions {
		return nil
	}
	_, err := m.s3Uploader.api.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(m.s3Uploader.bucket),
		Key:    aws.String(m.uploadSessionKey(id)),
	})
	if err != nil && !isS3NotFound(err) {
		return fmt.Errorf("delete upload session: %w", err)
	}
	return nil
}

func (m *lfsModule) loadUploadSession(ctx context.Context, id string) (*uploadSession, error) {
	record, err := m.readUploadSessionRecord(ctx, m.uploadSessionKey(id))
	if err != nil {
		return nil, err
	}
	if record.ID != id {
		return nil, errUploadSessionNotFound
	}
	return uploadSessionFromRecord(record)
}

func (m *lfsModule) readUploadSessionRecord(ctx context.Context, key string) (*uploadSessionRecord, error) {
	out, err := m.s3Uploader.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(m.s3Uploader.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, errUploadSessionNotFound
		}
		return nil, fmt.Errorf("read upload session: %w", err)
	}
	defer func() { _ = out.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(out.Body, lfsUploadSessionMaxRecordLen))
	if err != nil {
		return nil, fmt.Errorf("read upload session: %w", err)
	}
	var record uploadSessionRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("decode upload session: %w", err)
	}
	return &record, nil
}

func isS3NotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
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

func (m *lfsModule) abortMultipartIgnoringMissing(ctx context.Context, key, uploadID string) error {
	err := m.s3Uploader.AbortMultipartUpload(ctx, key, uploadID)
	if err == nil {
		return nil
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) && apiError.ErrorCode() == "NoSuchUpload" {
		return nil
	}
	return err
}

func (m *lfsModule) sweepExpiredUploadSessions(ctx context.Context, now time.Time) {
	paginator := s3.NewListObjectsV2Paginator(m.s3Uploader.api, &s3.ListObjectsV2Input{
		Bucket: aws.String(m.s3Uploader.bucket),
		Prefix: aws.String(m.uploadSessionPrefix()),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			m.logger.Warn("lfs upload session sweep: list failed", "error", err)
			return
		}
		for _, object := range page.Contents {
			key := aws.ToString(object.Key)
			id := strings.TrimSuffix(strings.TrimPrefix(key, m.uploadSessionPrefix()), ".json")
			if !validUploadSessionID(id) {
				continue
			}
			m.sweepUploadSession(ctx, now, id, key, aws.ToTime(object.LastModified))
		}
	}
}

func (m *lfsModule) sweepUploadSession(ctx context.Context, now time.Time, id, key string, modified time.Time) {
	unlock := m.uploadSessionLock(id)
	defer unlock()
	record, err := m.readUploadSessionRecord(ctx, key)
	if errors.Is(err, errUploadSessionNotFound) {
		return
	}
	if err != nil {
		if modified.IsZero() || now.Sub(modified) < m.uploadSessionTTL+lfsLostMultipartUploadGrace {
			return
		}
		m.logger.Warn("lfs upload session sweep: removing unreadable session", "key", logSafe(key), "error", err)
	} else {
		if now.Before(record.ExpiresAt) {
			return
		}
		if err := m.abortMultipartIgnoringMissing(ctx, record.S3Key, record.UploadID); err != nil {
			m.logger.Warn("lfs upload session sweep: abort failed", "uploadId", logSafe(id), "error", err)
			return
		}
	}
	if err := m.deleteUploadSession(ctx, id); err != nil {
		m.logger.Warn("lfs upload session sweep: delete failed", "uploadId", logSafe(id), "error", err)
		return
	}
	m.logger.Info("lfs upload session expired and removed", "uploadId", logSafe(id))
}

func (m *lfsModule) sweepLostMultipartUploads(ctx context.Context, now time.Time) {
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

func (m *lfsModule) dropUploadSession(ctx context.Context, requestID, id string) {
	if err := m.deleteUploadSession(context.WithoutCancel(ctx), id); err != nil {
		m.logger.Warn("multipart upload session delete failed", "requestId", logSafe(requestID), "uploadId", logSafe(id), "error", err)
	}
}

func (m *lfsModule) writeUploadSessionError(w http.ResponseWriter, requestID string, err error) {
	if errors.Is(err, errUploadSessionNotFound) {
		m.lfsWriteHTTPError(w, requestID, "", http.StatusNotFound, "upload_not_found", "upload session not found")
		return
	}
	m.metrics.IncS3Errors()
	m.logger.Warn("multipart upload session read failed", "requestId", logSafe(requestID), "error", err)
	m.lfsWriteHTTPError(w, requestID, "", http.StatusBadGateway, "s3_upload_failed", "upload session read failed")
}
