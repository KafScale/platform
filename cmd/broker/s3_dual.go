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

package main

import (
	"context"

	"github.com/novatechflow/kafscale/pkg/storage"
)

type dualS3Client struct {
	write storage.S3Client
	read  storage.S3Client
}

func newDualS3Client(writeClient, readClient storage.S3Client) storage.S3Client {
	return &dualS3Client{
		write: writeClient,
		read:  readClient,
	}
}

func (d *dualS3Client) UploadSegment(ctx context.Context, key string, body []byte) error {
	return d.write.UploadSegment(ctx, key, body)
}

func (d *dualS3Client) UploadIndex(ctx context.Context, key string, body []byte) error {
	return d.write.UploadIndex(ctx, key, body)
}

func (d *dualS3Client) DownloadSegment(ctx context.Context, key string, rng *storage.ByteRange) ([]byte, error) {
	data, err := d.read.DownloadSegment(ctx, key, rng)
	if err == nil {
		return data, nil
	}
	return d.write.DownloadSegment(ctx, key, rng)
}

func (d *dualS3Client) DownloadIndex(ctx context.Context, key string) ([]byte, error) {
	data, err := d.read.DownloadIndex(ctx, key)
	if err == nil {
		return data, nil
	}
	return d.write.DownloadIndex(ctx, key)
}

func (d *dualS3Client) ListSegments(ctx context.Context, prefix string) ([]storage.S3Object, error) {
	return d.write.ListSegments(ctx, prefix)
}

func (d *dualS3Client) EnsureBucket(ctx context.Context) error {
	return d.write.EnsureBucket(ctx)
}
