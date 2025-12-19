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
