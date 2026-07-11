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

package console

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// LFSS3Client provides S3 operations for the LFS admin console
type LFSS3Client struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	logger  *log.Logger
}

// LFSS3Config holds configuration for the S3 client
type LFSS3Config struct {
	Bucket         string
	Region         string
	Endpoint       string
	AccessKey      string
	SecretKey      string
	ForcePathStyle bool
}

// NewLFSS3Client creates a new S3 client for LFS admin operations
func NewLFSS3Client(ctx context.Context, cfg LFSS3Config, logger *log.Logger) (*LFSS3Client, error) {
	if logger == nil {
		logger = log.Default()
	}

	if cfg.Bucket == "" {
		logger.Println("lfs s3 client: no bucket configured")
		return nil, nil
	}

	// Build AWS config
	var opts []func(*config.LoadOptions) error

	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	} else {
		opts = append(opts, config.WithRegion("us-east-1"))
	}

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	// Build S3 client options
	var s3Opts []func(*s3.Options)

	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	if cfg.ForcePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)
	presign := s3.NewPresignClient(client)

	return &LFSS3Client{
		client:  client,
		presign: presign,
		bucket:  cfg.Bucket,
		logger:  logger,
	}, nil
}

// ListObjects lists objects in S3 with the given prefix
func (c *LFSS3Client) ListObjects(ctx context.Context, prefix, delimiter string, maxKeys int) ([]S3Object, []string, bool, error) {
	if maxKeys <= 0 {
		maxKeys = 100
	}
	if maxKeys > 1000 {
		maxKeys = 1000
	}

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(c.bucket),
		MaxKeys: aws.Int32(int32(maxKeys)),
	}

	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	if delimiter != "" {
		input.Delimiter = aws.String(delimiter)
	}

	output, err := c.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, nil, false, err
	}

	objects := make([]S3Object, 0, len(output.Contents))
	for _, obj := range output.Contents {
		s3Obj := S3Object{
			Key:  aws.ToString(obj.Key),
			Size: aws.ToInt64(obj.Size),
		}
		if obj.LastModified != nil {
			s3Obj.LastModified = obj.LastModified.Format(time.RFC3339)
		}
		if obj.ETag != nil {
			s3Obj.ETag = aws.ToString(obj.ETag)
		}
		objects = append(objects, s3Obj)
	}

	prefixes := make([]string, 0, len(output.CommonPrefixes))
	for _, p := range output.CommonPrefixes {
		if p.Prefix != nil {
			prefixes = append(prefixes, aws.ToString(p.Prefix))
		}
	}

	truncated := aws.ToBool(output.IsTruncated)

	return objects, prefixes, truncated, nil
}

// PresignGetObject generates a presigned URL for downloading an object
func (c *LFSS3Client) PresignGetObject(ctx context.Context, key string, ttl time.Duration) (string, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}

	presignOpts := func(opts *s3.PresignOptions) {
		opts.Expires = ttl
	}

	result, err := c.presign.PresignGetObject(ctx, input, presignOpts)
	if err != nil {
		return "", err
	}

	return result.URL, nil
}

// HeadObject checks if an object exists and returns its metadata
func (c *LFSS3Client) HeadObject(ctx context.Context, key string) (*S3Object, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}

	output, err := c.client.HeadObject(ctx, input)
	if err != nil {
		return nil, err
	}

	obj := &S3Object{
		Key:  key,
		Size: aws.ToInt64(output.ContentLength),
	}

	if output.LastModified != nil {
		obj.LastModified = output.LastModified.Format(time.RFC3339)
	}

	if output.ETag != nil {
		obj.ETag = aws.ToString(output.ETag)
	}

	return obj, nil
}
