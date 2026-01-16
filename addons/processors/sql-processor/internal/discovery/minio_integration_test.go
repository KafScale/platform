// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package discovery

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/decoder"
)

func TestMinioDiscoveryAndDecode(t *testing.T) {
	endpoint := os.Getenv("KAFSQL_MINIO_ENDPOINT")
	accessKey := os.Getenv("KAFSQL_MINIO_ACCESS_KEY")
	secretKey := os.Getenv("KAFSQL_MINIO_SECRET_KEY")
	bucket := os.Getenv("KAFSQL_MINIO_BUCKET")
	if endpoint == "" || accessKey == "" || secretKey == "" || bucket == "" {
		t.Skip("minio env vars not set")
	}

	t.Setenv("AWS_ACCESS_KEY_ID", accessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)
	t.Setenv("AWS_REGION", "us-east-1")

	client, err := newMinioS3Client(endpoint)
	if err != nil {
		t.Fatalf("s3 client: %v", err)
	}

	ctx := context.Background()
	if err := ensureBucket(ctx, client, bucket); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}

	namespace := "kafsql-test-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	topic := "orders"
	partition := "0"
	base := "00000000000000000000"
	segmentKey := fmt.Sprintf("%s/%s/%s/segment-%s.kfs", namespace, topic, partition, base)
	indexKey := fmt.Sprintf("%s/%s/%s/segment-%s.index", namespace, topic, partition, base)

	segmentBytes := buildSegment(buildBatch(buildRecord(0, 0, []byte("k"), []byte("v"))))
	indexBytes := buildIndex()

	if err := putObject(ctx, client, bucket, segmentKey, segmentBytes); err != nil {
		t.Fatalf("put segment: %v", err)
	}
	if err := putObject(ctx, client, bucket, indexKey, indexBytes); err != nil {
		t.Fatalf("put index: %v", err)
	}

	cfg := config.Config{
		S3: config.S3Config{
			Bucket:    bucket,
			Namespace: namespace,
			Endpoint:  endpoint,
			Region:    "us-east-1",
			PathStyle: true,
		},
	}
	lister, err := New(cfg)
	if err != nil {
		t.Fatalf("lister: %v", err)
	}
	segments, err := lister.ListCompleted(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segments))
	}

	dec, err := decoder.New(cfg)
	if err != nil {
		t.Fatalf("decoder: %v", err)
	}
	records, err := dec.Decode(ctx, segmentKey, indexKey, topic, 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(records) != 1 || string(records[0].Key) != "k" || string(records[0].Value) != "v" {
		t.Fatalf("unexpected records: %+v", records)
	}
}

const (
	minioSegmentHeaderLen     = 32
	minioSegmentFooterLen     = 16
	minioSegmentMagic         = "KAFS"
	minioRecordBatchHeaderLen = 61
)

func newMinioS3Client(endpoint string) (*s3.Client, error) {
	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
		if service == s3.ServiceID {
			return aws.Endpoint{URL: endpoint, SigningRegion: region}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithEndpointResolverWithOptions(resolver),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(awsCfg, func(opts *s3.Options) {
		opts.UsePathStyle = true
	}), nil
}

func ensureBucket(ctx context.Context, client *s3.Client, bucket string) error {
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil
	}
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		if !errorsAs(err, &owned) {
			return err
		}
	}
	return nil
}

func putObject(ctx context.Context, client *s3.Client, bucket, key string, data []byte) error {
	reader := bytes.NewReader(data)
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          reader,
		ContentLength: aws.Int64(int64(len(data))),
	})
	return err
}

func buildIndex() []byte {
	data := make([]byte, 16+12)
	copy(data[0:4], []byte("IDX\x00"))
	binary.BigEndian.PutUint16(data[4:6], 1)
	binary.BigEndian.PutUint32(data[6:10], 1)
	binary.BigEndian.PutUint32(data[10:14], 1)
	binary.BigEndian.PutUint64(data[16:24], 0)
	binary.BigEndian.PutUint32(data[24:28], 0)
	return data
}

func buildSegment(batch []byte) []byte {
	segment := make([]byte, 0, minioSegmentHeaderLen+len(batch)+minioSegmentFooterLen)
	header := make([]byte, minioSegmentHeaderLen)
	copy(header[0:4], []byte(minioSegmentMagic))
	segment = append(segment, header...)
	segment = append(segment, batch...)
	footer := make([]byte, minioSegmentFooterLen)
	copy(footer[12:16], []byte("END!"))
	segment = append(segment, footer...)
	return segment
}

func buildBatch(record []byte) []byte {
	headerRest := make([]byte, minioRecordBatchHeaderLen-12)
	binary.BigEndian.PutUint32(headerRest[57-12:61-12], 1)
	batchLen := len(headerRest) + len(record)

	frame := make([]byte, 12+batchLen)
	binary.BigEndian.PutUint32(frame[8:12], uint32(batchLen))
	copy(frame[12:], headerRest)
	copy(frame[12+len(headerRest):], record)
	return frame
}

func buildRecord(tsDelta int32, offsetDelta int32, key []byte, value []byte) []byte {
	payload := makeRecordPayload(tsDelta, offsetDelta, key, value)
	encoded := encodeVarint(int32(len(payload)))
	out := make([]byte, 0, len(encoded)+len(payload))
	out = append(out, encoded...)
	out = append(out, payload...)
	return out
}

func makeRecordPayload(tsDelta int32, offsetDelta int32, key []byte, value []byte) []byte {
	var body bytes.Buffer
	body.WriteByte(0)
	writeVarint(&body, tsDelta)
	writeVarint(&body, offsetDelta)
	writeVarint(&body, int32(len(key)))
	body.Write(key)
	writeVarint(&body, int32(len(value)))
	body.Write(value)
	writeVarint(&body, 0)
	return body.Bytes()
}

func writeVarint(buf *bytes.Buffer, value int32) {
	encoded := encodeVarint(value)
	buf.Write(encoded)
}

func encodeVarint(value int32) []byte {
	zigzag := uint32((value << 1) ^ (value >> 31))
	out := make([]byte, 0, 5)
	for {
		b := byte(zigzag & 0x7f)
		zigzag >>= 7
		if zigzag != 0 {
			b |= 0x80
		}
		out = append(out, b)
		if zigzag == 0 {
			break
		}
	}
	return out
}

func errorsAs(err error, target interface{}) bool {
	for err != nil {
		if ok := errors.As(err, target); ok {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}
