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
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const minMultipartChunkSize int64 = 5 * 1024 * 1024

type s3Config struct {
	Bucket          string
	Region          string
	Endpoint        string
	PublicEndpoint  string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	ForcePathStyle  bool
	ChunkSize       int64
}

type s3API interface {
	CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
}

type s3PresignAPI interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}

type s3Uploader struct {
	bucket    string
	region    string
	chunkSize int64
	api       s3API
	presign   s3PresignAPI
}

func normalizeChunkSize(chunk int64) int64 {
	if chunk <= 0 {
		chunk = defaultChunkSize
	}
	if chunk < minMultipartChunkSize {
		chunk = minMultipartChunkSize
	}
	return chunk
}

func newS3Uploader(ctx context.Context, cfg s3Config) (*s3Uploader, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3 bucket required")
	}
	if cfg.Region == "" {
		return nil, errors.New("s3 region required")
	}
	cfg.ChunkSize = normalizeChunkSize(cfg.ChunkSize)

	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)))
	}
	if cfg.Endpoint != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					URL:           cfg.Endpoint,
					PartitionID:   "aws",
					SigningRegion: cfg.Region,
				}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
		loadOpts = append(loadOpts, config.WithEndpointResolverWithOptions(customResolver))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
	})
	presignCfg := awsCfg
	if cfg.PublicEndpoint != "" {
		publicResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					URL:           cfg.PublicEndpoint,
					PartitionID:   "aws",
					SigningRegion: cfg.Region,
				}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
		presignCfg.EndpointResolverWithOptions = publicResolver
	}
	presignClient := s3.NewFromConfig(presignCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
	})
	presigner := s3.NewPresignClient(presignClient)

	return &s3Uploader{
		bucket:    cfg.Bucket,
		region:    cfg.Region,
		chunkSize: cfg.ChunkSize,
		api:       client,
		presign:   presigner,
	}, nil
}

func (u *s3Uploader) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (string, error) {
	if key == "" {
		return "", errors.New("s3 key required")
	}
	if u.presign == nil {
		return "", errors.New("presign client not configured")
	}
	out, err := u.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = ttl
	})
	if err != nil {
		return "", err
	}
	return out.URL, nil
}

func (u *s3Uploader) GetObject(ctx context.Context, key string) (*s3.GetObjectOutput, error) {
	if key == "" {
		return nil, errors.New("s3 key required")
	}
	return u.api.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	})
}

func (u *s3Uploader) HeadBucket(ctx context.Context) error {
	_, err := u.api.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(u.bucket)})
	if err == nil {
		return nil
	}
	return err
}

func (u *s3Uploader) EnsureBucket(ctx context.Context) error {
	if err := u.HeadBucket(ctx); err == nil {
		return nil
	}
	input := &s3.CreateBucketInput{Bucket: aws.String(u.bucket)}
	if u.region != "" && u.region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{LocationConstraint: types.BucketLocationConstraint(u.region)}
	}
	_, err := u.api.CreateBucket(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
				return nil
			}
		}
		return fmt.Errorf("create bucket %s: %w", u.bucket, err)
	}
	return nil
}

func (u *s3Uploader) Upload(ctx context.Context, key string, payload []byte, alg lfs.ChecksumAlg) (string, string, string, error) {
	if key == "" {
		return "", "", "", errors.New("s3 key required")
	}
	shaHasher := sha256.New()
	if _, err := shaHasher.Write(payload); err != nil {
		return "", "", "", err
	}
	shaHex := hex.EncodeToString(shaHasher.Sum(nil))

	checksumAlg := alg
	if checksumAlg == "" {
		checksumAlg = lfs.ChecksumSHA256
	}
	var checksum string
	if checksumAlg != lfs.ChecksumNone {
		if checksumAlg == lfs.ChecksumSHA256 {
			checksum = shaHex
		} else {
			computed, err := lfs.ComputeChecksum(checksumAlg, payload)
			if err != nil {
				return "", "", "", err
			}
			checksum = computed
		}
	}

	size := int64(len(payload))
	if size <= u.chunkSize {
		_, err := u.api.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(u.bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(payload),
			ContentLength: aws.Int64(size),
		})
		return shaHex, checksum, string(checksumAlg), err
	}
	return shaHex, checksum, string(checksumAlg), u.multipartUpload(ctx, key, payload)
}

func (u *s3Uploader) UploadStream(ctx context.Context, key string, reader io.Reader, maxSize int64, alg lfs.ChecksumAlg) (string, string, string, int64, error) {
	if key == "" {
		return "", "", "", 0, errors.New("s3 key required")
	}
	if reader == nil {
		return "", "", "", 0, errors.New("reader required")
	}
	u.chunkSize = normalizeChunkSize(u.chunkSize)

	checksumAlg := alg
	if checksumAlg == "" {
		checksumAlg = lfs.ChecksumSHA256
	}

	// Read first chunk to determine if we need multipart upload
	firstBuf := make([]byte, u.chunkSize)
	firstN, firstErr := io.ReadFull(reader, firstBuf)
	if firstErr != nil && firstErr != io.EOF && firstErr != io.ErrUnexpectedEOF {
		return "", "", "", 0, firstErr
	}
	if firstN == 0 {
		return "", "", "", 0, errors.New("empty upload")
	}

	firstReadHitEOF := firstErr == io.EOF || firstErr == io.ErrUnexpectedEOF

	// If data fits in one chunk and is smaller than minMultipartChunkSize, use PutObject
	if firstReadHitEOF && int64(firstN) < minMultipartChunkSize {
		data := firstBuf[:firstN]
		shaHasher := sha256.New()
		shaHasher.Write(data)
		shaHex := hex.EncodeToString(shaHasher.Sum(nil))

		checksum := ""
		if checksumAlg != lfs.ChecksumNone {
			if checksumAlg == lfs.ChecksumSHA256 {
				checksum = shaHex
			} else {
				computed, err := lfs.ComputeChecksum(checksumAlg, data)
				if err != nil {
					return "", "", "", 0, err
				}
				checksum = computed
			}
		}

		_, err := u.api.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(u.bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(data),
			ContentLength: aws.Int64(int64(firstN)),
		})
		if err != nil {
			return "", "", "", 0, fmt.Errorf("put object: %w", err)
		}
		return shaHex, checksum, string(checksumAlg), int64(firstN), nil
	}

	// Use multipart upload for larger files
	createResp, err := u.api.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", "", "", 0, fmt.Errorf("create multipart upload: %w", err)
	}
	uploadID := createResp.UploadId
	if uploadID == nil {
		return "", "", "", 0, errors.New("missing upload id")
	}

	shaHasher := sha256.New()
	var checksumHasher interface {
		Write([]byte) (int, error)
		Sum([]byte) []byte
	}
	if checksumAlg != lfs.ChecksumNone {
		if checksumAlg == lfs.ChecksumSHA256 {
			checksumHasher = shaHasher
		} else {
			h, err := lfs.NewChecksumHasher(checksumAlg)
			if err != nil {
				_ = u.abortUpload(ctx, key, *uploadID)
				return "", "", "", 0, err
			}
			checksumHasher = h
		}
	}
	parts := make([]types.CompletedPart, 0, 4)
	partNum := int32(1)
	var total int64

	// Upload first chunk
	total += int64(firstN)
	if maxSize > 0 && total > maxSize {
		_ = u.abortUpload(ctx, key, *uploadID)
		return "", "", "", total, fmt.Errorf("blob size %d exceeds max %d", total, maxSize)
	}
	shaHasher.Write(firstBuf[:firstN])
	if checksumHasher != nil && checksumHasher != shaHasher {
		checksumHasher.Write(firstBuf[:firstN])
	}
	partResp, err := u.api.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(u.bucket),
		Key:        aws.String(key),
		UploadId:   uploadID,
		PartNumber: aws.Int32(partNum),
		Body:       bytes.NewReader(firstBuf[:firstN]),
	})
	if err != nil {
		_ = u.abortUpload(ctx, key, *uploadID)
		return "", "", "", total, fmt.Errorf("upload part %d: %w", partNum, err)
	}
	parts = append(parts, types.CompletedPart{ETag: partResp.ETag, PartNumber: aws.Int32(partNum)})
	partNum++

	// Continue reading remaining chunks
	buf := make([]byte, u.chunkSize)
	for {
		n, readErr := io.ReadFull(reader, buf)
		if n > 0 {
			total += int64(n)
			if maxSize > 0 && total > maxSize {
				_ = u.abortUpload(ctx, key, *uploadID)
				return "", "", "", total, fmt.Errorf("blob size %d exceeds max %d", total, maxSize)
			}
			if _, err := shaHasher.Write(buf[:n]); err != nil {
				_ = u.abortUpload(ctx, key, *uploadID)
				return "", "", "", total, err
			}
			if checksumHasher != nil && checksumHasher != shaHasher {
				if _, err := checksumHasher.Write(buf[:n]); err != nil {
					_ = u.abortUpload(ctx, key, *uploadID)
					return "", "", "", total, err
				}
			}
			partResp, err := u.api.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(u.bucket),
				Key:        aws.String(key),
				UploadId:   uploadID,
				PartNumber: aws.Int32(partNum),
				Body:       bytes.NewReader(buf[:n]),
			})
			if err != nil {
				_ = u.abortUpload(ctx, key, *uploadID)
				return "", "", "", total, fmt.Errorf("upload part %d: %w", partNum, err)
			}
			parts = append(parts, types.CompletedPart{ETag: partResp.ETag, PartNumber: aws.Int32(partNum)})
			partNum++
		}
		if readErr == io.EOF {
			break
		}
		if readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			_ = u.abortUpload(ctx, key, *uploadID)
			return "", "", "", total, readErr
		}
	}

	_, err = u.api.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		_ = u.abortUpload(ctx, key, *uploadID)
		return "", "", "", total, fmt.Errorf("complete multipart upload: %w", err)
	}
	shaHex := hex.EncodeToString(shaHasher.Sum(nil))
	checksum := ""
	if checksumAlg != lfs.ChecksumNone {
		if checksumAlg == lfs.ChecksumSHA256 {
			checksum = shaHex
		} else if checksumHasher != nil {
			checksum = hex.EncodeToString(checksumHasher.Sum(nil))
		}
	}
	return shaHex, checksum, string(checksumAlg), total, nil
}

func (u *s3Uploader) StartMultipartUpload(ctx context.Context, key, contentType string) (string, error) {
	if key == "" {
		return "", errors.New("s3 key required")
	}
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	}
	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}
	resp, err := u.api.CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", fmt.Errorf("create multipart upload: %w", err)
	}
	if resp.UploadId == nil || *resp.UploadId == "" {
		return "", errors.New("missing upload id")
	}
	return *resp.UploadId, nil
}

func (u *s3Uploader) UploadPart(ctx context.Context, key, uploadID string, partNumber int32, payload []byte) (string, error) {
	if key == "" {
		return "", errors.New("s3 key required")
	}
	if uploadID == "" {
		return "", errors.New("upload id required")
	}
	resp, err := u.api.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(u.bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(partNumber),
		Body:       bytes.NewReader(payload),
	})
	if err != nil {
		return "", fmt.Errorf("upload part %d: %w", partNumber, err)
	}
	if resp.ETag == nil || *resp.ETag == "" {
		return "", errors.New("missing etag")
	}
	return *resp.ETag, nil
}

func (u *s3Uploader) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []types.CompletedPart) error {
	if key == "" {
		return errors.New("s3 key required")
	}
	if uploadID == "" {
		return errors.New("upload id required")
	}
	_, err := u.api.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}

func (u *s3Uploader) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	if key == "" {
		return errors.New("s3 key required")
	}
	if uploadID == "" {
		return errors.New("upload id required")
	}
	_, err := u.api.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return err
}

func (u *s3Uploader) multipartUpload(ctx context.Context, key string, payload []byte) error {
	createResp, err := u.api.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	uploadID := createResp.UploadId
	if uploadID == nil {
		return errors.New("missing upload id")
	}

	parts := make([]types.CompletedPart, 0, (len(payload)/int(u.chunkSize))+1)
	reader := bytes.NewReader(payload)
	partNum := int32(1)
	buf := make([]byte, u.chunkSize)
	for {
		n, readErr := io.ReadFull(reader, buf)
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			if n == 0 {
				break
			}
		}
		if n > 0 {
			partResp, err := u.api.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(u.bucket),
				Key:        aws.String(key),
				UploadId:   uploadID,
				PartNumber: aws.Int32(partNum),
				Body:       bytes.NewReader(buf[:n]),
			})
			if err != nil {
				_ = u.abortUpload(ctx, key, *uploadID)
				return fmt.Errorf("upload part %d: %w", partNum, err)
			}
			parts = append(parts, types.CompletedPart{ETag: partResp.ETag, PartNumber: aws.Int32(partNum)})
			partNum++
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			_ = u.abortUpload(ctx, key, *uploadID)
			return fmt.Errorf("read payload: %w", readErr)
		}
		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	_, err = u.api.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		_ = u.abortUpload(ctx, key, *uploadID)
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}

func (u *s3Uploader) abortUpload(ctx context.Context, key, uploadID string) error {
	_, err := u.api.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return err
}

func (u *s3Uploader) DeleteObject(ctx context.Context, key string) error {
	if key == "" {
		return errors.New("s3 key required")
	}
	_, err := u.api.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	})
	return err
}
