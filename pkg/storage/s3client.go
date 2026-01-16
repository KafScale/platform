// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package storage

import (
	"context"
	"fmt"
)

// ByteRange represents an inclusive byte range for reads.
type ByteRange struct {
	Start int64
	End   int64
}

func (br *ByteRange) headerValue() *string {
	if br == nil {
		return nil
	}
	val := fmt.Sprintf("bytes=%d-%d", br.Start, br.End)
	return &val
}

// S3Client is the abstraction used by storage to read/write segments.
type S3Client interface {
	UploadSegment(ctx context.Context, key string, body []byte) error
	UploadIndex(ctx context.Context, key string, body []byte) error
	DownloadSegment(ctx context.Context, key string, rng *ByteRange) ([]byte, error)
	DownloadIndex(ctx context.Context, key string) ([]byte, error)
	ListSegments(ctx context.Context, prefix string) ([]S3Object, error)
	EnsureBucket(ctx context.Context) error
}

// S3Object describes a stored segment object.
type S3Object struct {
	Key  string
	Size int64
}

// S3Config describes connection details for AWS S3 or compatible endpoints.
type S3Config struct {
	Bucket          string
	Region          string
	Endpoint        string
	ForcePathStyle  bool
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	KMSKeyARN       string
}
