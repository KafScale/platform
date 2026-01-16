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

package decoder

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	segmentHeaderLen     = 32
	segmentFooterLen     = 16
	segmentMagic         = "KAFS"
	indexMagic           = "IDX\x00"
	recordBatchHeaderLen = 61
)

type Header struct {
	Key   string
	Value []byte
}

type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

type Decoder interface {
	Decode(ctx context.Context, segmentKey, indexKey string, topic string, partition int32) ([]Record, error)
}

func New(cfg config.Config) (Decoder, error) {
	loadOptions := []func(*awsconfig.LoadOptions) error{}
	if cfg.S3.Region != "" {
		loadOptions = append(loadOptions, awsconfig.WithRegion(cfg.S3.Region))
	}
	if cfg.S3.Endpoint != "" {
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{URL: cfg.S3.Endpoint, SigningRegion: region}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
		loadOptions = append(loadOptions, awsconfig.WithEndpointResolverWithOptions(resolver))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(opts *s3.Options) {
		if cfg.S3.PathStyle {
			opts.UsePathStyle = true
		}
	})

	return &s3Decoder{
		client:  client,
		bucket:  cfg.S3.Bucket,
		metrics: newS3Metrics(),
	}, nil
}

type s3Decoder struct {
	client  *s3.Client
	bucket  string
	metrics s3Metrics
}

type s3Metrics struct {
	requests     *prometheus.CounterVec
	errors       *prometheus.CounterVec
	duration     *prometheus.HistogramVec
	bytes        prometheus.Counter
	decodeErrors prometheus.Counter
}

func newS3Metrics() s3Metrics {
	return s3Metrics{
		requests:     metrics.S3Requests,
		errors:       metrics.S3Errors,
		duration:     metrics.S3Duration,
		bytes:        metrics.S3Bytes,
		decodeErrors: metrics.DecodeErrors,
	}
}

func (d *s3Decoder) Decode(ctx context.Context, segmentKey, indexKey string, topic string, partition int32) ([]Record, error) {
	indexBytes, err := d.getObject(ctx, "get", indexKey)
	if err != nil {
		return nil, fmt.Errorf("download index: %w", err)
	}
	if _, err := parseIndex(indexBytes); err != nil {
		d.metrics.decodeErrors.Inc()
		return nil, err
	}

	segmentBytes, err := d.getObject(ctx, "get", segmentKey)
	if err != nil {
		return nil, fmt.Errorf("download segment: %w", err)
	}

	return decodeSegment(segmentBytes, topic, partition)
}

func (d *s3Decoder) getObject(ctx context.Context, op, key string) ([]byte, error) {
	start := time.Now()
	d.metrics.requests.WithLabelValues(op).Inc()
	resp, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		d.metrics.errors.WithLabelValues(op).Inc()
		d.metrics.duration.WithLabelValues(op).Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	d.metrics.duration.WithLabelValues(op).Observe(float64(time.Since(start).Milliseconds()))
	if err != nil {
		d.metrics.errors.WithLabelValues(op).Inc()
		return nil, err
	}
	d.metrics.bytes.Add(float64(len(data)))
	return data, nil
}

func decodeSegment(segment []byte, topic string, partition int32) ([]Record, error) {
	if len(segment) < segmentHeaderLen+segmentFooterLen {
		return nil, fmt.Errorf("segment too small")
	}
	if string(segment[:4]) != segmentMagic {
		return nil, fmt.Errorf("invalid segment magic")
	}

	body := segment[segmentHeaderLen : len(segment)-segmentFooterLen]
	return decodeRecordBatches(body, topic, partition)
}

func decodeRecordBatches(data []byte, topic string, partition int32) ([]Record, error) {
	const frameHeaderLen = 12
	var records []Record
	offset := 0
	for offset+frameHeaderLen <= len(data) {
		batchLen := int(binary.BigEndian.Uint32(data[offset+8 : offset+12]))
		if batchLen <= 0 {
			break
		}
		frameLen := frameHeaderLen + batchLen
		if offset+frameLen > len(data) {
			break
		}
		batch := data[offset : offset+frameLen]
		batchRecords, err := decodeBatchRecords(batch, topic, partition)
		if err != nil {
			return nil, err
		}
		records = append(records, batchRecords...)
		offset += frameLen
	}
	return records, nil
}

func decodeBatchRecords(batch []byte, topic string, partition int32) ([]Record, error) {
	if len(batch) < recordBatchHeaderLen {
		return nil, fmt.Errorf("record batch too small")
	}

	attributes := int16(binary.BigEndian.Uint16(batch[21:23]))
	if compressionType(attributes) != 0 {
		return nil, fmt.Errorf("compressed batches are not supported")
	}

	baseOffset := int64(binary.BigEndian.Uint64(batch[0:8]))
	firstTimestamp := int64(binary.BigEndian.Uint64(batch[27:35]))
	recordCount := int32(binary.BigEndian.Uint32(batch[57:61]))
	if recordCount <= 0 {
		return nil, nil
	}

	recordsData := batch[recordBatchHeaderLen:]
	reader := bytes.NewReader(recordsData)
	records := make([]Record, 0, recordCount)
	for i := int32(0); i < recordCount; i++ {
		record, err := decodeRecord(reader, baseOffset, firstTimestamp, topic, partition)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func decodeRecord(reader *bytes.Reader, baseOffset int64, baseTimestamp int64, topic string, partition int32) (Record, error) {
	length, err := readVarint(reader)
	if err != nil {
		return Record{}, err
	}
	if length < 0 {
		return Record{}, fmt.Errorf("invalid record length")
	}

	recordData := make([]byte, length)
	if _, err := io.ReadFull(reader, recordData); err != nil {
		return Record{}, err
	}
	buf := bytes.NewReader(recordData)

	_, err = buf.ReadByte()
	if err != nil {
		return Record{}, err
	}

	timestampDelta, err := readVarint(buf)
	if err != nil {
		return Record{}, err
	}
	offsetDelta, err := readVarint(buf)
	if err != nil {
		return Record{}, err
	}

	keyLen, err := readVarint(buf)
	if err != nil {
		return Record{}, err
	}
	key, err := readNullableBytes(buf, keyLen)
	if err != nil {
		return Record{}, err
	}

	valueLen, err := readVarint(buf)
	if err != nil {
		return Record{}, err
	}
	value, err := readNullableBytes(buf, valueLen)
	if err != nil {
		return Record{}, err
	}

	headerCount, err := readVarint(buf)
	if err != nil {
		return Record{}, err
	}
	headers := make([]Header, 0, headerCount)
	for i := int32(0); i < headerCount; i++ {
		headerKeyLen, err := readVarint(buf)
		if err != nil {
			return Record{}, err
		}
		headerKey, err := readString(buf, headerKeyLen)
		if err != nil {
			return Record{}, err
		}
		headerValLen, err := readVarint(buf)
		if err != nil {
			return Record{}, err
		}
		headerVal, err := readNullableBytes(buf, headerValLen)
		if err != nil {
			return Record{}, err
		}
		headers = append(headers, Header{Key: headerKey, Value: headerVal})
	}

	return Record{
		Topic:     topic,
		Partition: partition,
		Offset:    baseOffset + int64(offsetDelta),
		Timestamp: baseTimestamp + int64(timestampDelta),
		Key:       key,
		Value:     value,
		Headers:   headers,
	}, nil
}

func parseIndex(data []byte) ([]indexEntry, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("index too small")
	}
	if string(data[:4]) != indexMagic {
		return nil, fmt.Errorf("invalid index magic")
	}
	entryCount := int(binary.BigEndian.Uint32(data[6:10]))
	entries := make([]indexEntry, 0, entryCount)
	offset := 16
	for i := 0; i < entryCount; i++ {
		if offset+12 > len(data) {
			return nil, fmt.Errorf("index entry out of bounds")
		}
		entry := indexEntry{
			Offset:   int64(binary.BigEndian.Uint64(data[offset : offset+8])),
			Position: int32(binary.BigEndian.Uint32(data[offset+8 : offset+12])),
		}
		entries = append(entries, entry)
		offset += 12
	}
	return entries, nil
}

type indexEntry struct {
	Offset   int64
	Position int32
}

func readVarint(reader *bytes.Reader) (int32, error) {
	var shift uint
	var value int32
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		value |= int32(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 28 {
			return 0, errors.New("varint too long")
		}
	}
	return zigZagDecode(value), nil
}

func zigZagDecode(value int32) int32 {
	return (value >> 1) ^ -(value & 1)
}

func readNullableBytes(reader *bytes.Reader, length int32) ([]byte, error) {
	if length < 0 {
		return nil, nil
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readString(reader *bytes.Reader, length int32) (string, error) {
	data, err := readNullableBytes(reader, length)
	if err != nil {
		return "", err
	}
	if data == nil {
		return "", nil
	}
	return string(data), nil
}

func compressionType(attributes int16) int8 {
	return int8(attributes & 0x07)
}
