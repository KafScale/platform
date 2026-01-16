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

package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jackc/pgproto3/v2"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
)

const (
	e2eSegmentHeaderLen     = 32
	e2eSegmentFooterLen     = 16
	e2eSegmentMagic         = "KAFS"
	e2eRecordBatchHeaderLen = 61
)

func TestMinioE2ESQL(t *testing.T) {
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

	client, err := newMinioClient(endpoint)
	if err != nil {
		t.Fatalf("s3 client: %v", err)
	}
	ctx := context.Background()
	if err := ensureBucket(ctx, client, bucket); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}

	now := time.Now().UTC()
	baseTimestamp := now.Add(-5 * time.Minute).UnixMilli()
	namespace := "kafsql-e2e-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	if err := seedTopic(ctx, client, bucket, namespace, "orders", "order", 0, baseTimestamp, 200); err != nil {
		t.Fatalf("seed orders: %v", err)
	}
	if err := seedTopic(ctx, client, bucket, namespace, "payments", "order", 0, baseTimestamp, 50); err != nil {
		t.Fatalf("seed payments: %v", err)
	}

	cfg := config.Config{
		S3: config.S3Config{
			Bucket:    bucket,
			Namespace: namespace,
			Endpoint:  endpoint,
			Region:    "us-east-1",
			PathStyle: true,
		},
		Query: config.QueryConfig{
			DefaultLimit:     1000,
			MaxUnbounded:     10000,
			RequireTimeBound: true,
		},
	}
	srv := New(cfg, nil)

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()
	go srv.handleConnection(ctx, serverConn)

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)
	sendStartupMessage(t, clientConn)
	readUntilReady(t, frontend)

	countRows := runQueryRows(t, frontend, "SELECT count(*) FROM orders LAST 1h;")
	if len(countRows) != 1 || string(countRows[0][0]) != "200" {
		t.Fatalf("unexpected count result: %+v", countRows)
	}

	offsetRows := runQueryRows(t, frontend, "SELECT _offset FROM orders TAIL 1;")
	if len(offsetRows) != 1 || string(offsetRows[0][0]) != "199" {
		t.Fatalf("unexpected offset result: %+v", offsetRows)
	}

	joinRows := runQueryRows(t, frontend, "SELECT o._key, p._key FROM orders o JOIN payments p WITHIN 10m LAST 1h;")
	if len(joinRows) != 50 {
		t.Fatalf("expected 50 join rows, got %d", len(joinRows))
	}
}

func runQueryRows(t *testing.T, frontend *pgproto3.Frontend, query string) [][][]byte {
	t.Helper()
	if err := frontend.Send(&pgproto3.Query{String: query}); err != nil {
		t.Fatalf("send query: %v", err)
	}
	rows := collectRows(t, frontend)
	readUntilReady(t, frontend)
	return rows
}

type recordSpec struct {
	offsetDelta int32
	tsDelta     int32
	key         []byte
	value       []byte
}

func seedTopic(ctx context.Context, client *s3.Client, bucket, namespace, topic, keyPrefix string, partition int32, baseTimestamp int64, total int) error {
	baseOffset := int64(0)
	baseStr := fmt.Sprintf("%020d", baseOffset)
	segmentKey := fmt.Sprintf("%s/%s/%d/segment-%s.kfs", namespace, topic, partition, baseStr)
	indexKey := fmt.Sprintf("%s/%s/%d/segment-%s.index", namespace, topic, partition, baseStr)

	records := make([]recordSpec, 0, total)
	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("%s-%03d", keyPrefix, i))
		value := []byte(fmt.Sprintf(`{"id":%d,"amount":%d}`, i, i*10))
		records = append(records, recordSpec{
			offsetDelta: int32(i),
			tsDelta:     int32(i * 1000),
			key:         key,
			value:       value,
		})
	}

	segment := buildSegment(buildBatch(baseOffset, baseTimestamp, records))
	index := buildIndex()

	if err := putObject(ctx, client, bucket, segmentKey, segment); err != nil {
		return err
	}
	if err := putObject(ctx, client, bucket, indexKey, index); err != nil {
		return err
	}
	return nil
}

func newMinioClient(endpoint string) (*s3.Client, error) {
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
	segment := make([]byte, 0, e2eSegmentHeaderLen+len(batch)+e2eSegmentFooterLen)
	header := make([]byte, e2eSegmentHeaderLen)
	copy(header[0:4], []byte(e2eSegmentMagic))
	segment = append(segment, header...)
	segment = append(segment, batch...)
	footer := make([]byte, e2eSegmentFooterLen)
	copy(footer[12:16], []byte("END!"))
	segment = append(segment, footer...)
	return segment
}

func buildBatch(baseOffset int64, baseTimestamp int64, records []recordSpec) []byte {
	headerRest := make([]byte, e2eRecordBatchHeaderLen-12)
	binary.BigEndian.PutUint64(headerRest[15:23], uint64(baseTimestamp))
	binary.BigEndian.PutUint32(headerRest[45:49], uint32(len(records)))

	var recordData []byte
	for _, record := range records {
		recordData = append(recordData, buildRecord(record.tsDelta, record.offsetDelta, record.key, record.value)...)
	}

	batchLen := len(headerRest) + len(recordData)
	frame := make([]byte, 12+batchLen)
	binary.BigEndian.PutUint64(frame[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(frame[8:12], uint32(batchLen))
	copy(frame[12:], headerRest)
	copy(frame[12+len(headerRest):], recordData)
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
	buf.Write(encodeVarint(value))
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
