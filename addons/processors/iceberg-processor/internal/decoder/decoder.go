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

package decoder

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/KafScale/platform/addons/processors/iceberg-processor/internal/config"
)

const (
	segmentHeaderLen     = 32
	segmentFooterLen     = 16
	segmentMagic         = "KAFS"
	indexMagic           = "IDX\x00"
	recordBatchHeaderLen = 61
)

// Header represents a Kafka record header.
type Header struct {
	Key   string
	Value []byte
}

// Record represents a decoded Kafka record.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

// Decoder parses Kafscale segments into records.
type Decoder interface {
	Decode(ctx context.Context, segmentKey, indexKey string, topic string, partition int32) ([]Record, error)
}

// New returns an S3-backed decoder.
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
		client: client,
		bucket: cfg.S3.Bucket,
	}, nil
}

type s3Decoder struct {
	client *s3.Client
	bucket string
}

func (d *s3Decoder) Decode(ctx context.Context, segmentKey, indexKey string, topic string, partition int32) ([]Record, error) {
	indexBytes, err := d.getObject(ctx, indexKey)
	if err != nil {
		return nil, fmt.Errorf("download index: %w", err)
	}
	if _, err := parseIndex(indexBytes); err != nil {
		return nil, err
	}

	segmentBytes, err := d.getObject(ctx, segmentKey)
	if err != nil {
		return nil, fmt.Errorf("download segment: %w", err)
	}

	return decodeSegment(segmentBytes, topic, partition)
}

func (d *s3Decoder) getObject(ctx context.Context, key string) ([]byte, error) {
	resp, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
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

	_, err = buf.ReadByte() // attributes
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
	for i := int64(0); i < headerCount; i++ {
		keyLen, err := readVarint(buf)
		if err != nil {
			return Record{}, err
		}
		keyBytes, err := readNullableBytes(buf, keyLen)
		if err != nil {
			return Record{}, err
		}
		valueLen, err := readVarint(buf)
		if err != nil {
			return Record{}, err
		}
		valueBytes, err := readNullableBytes(buf, valueLen)
		if err != nil {
			return Record{}, err
		}
		headers = append(headers, Header{Key: string(keyBytes), Value: valueBytes})
	}

	return Record{
		Topic:     topic,
		Partition: partition,
		Offset:    baseOffset + offsetDelta,
		Timestamp: baseTimestamp + timestampDelta,
		Key:       key,
		Value:     value,
		Headers:   headers,
	}, nil
}

func readNullableBytes(reader *bytes.Reader, length int64) ([]byte, error) {
	if length < 0 {
		return nil, nil
	}
	if length == 0 {
		return []byte{}, nil
	}
	out := make([]byte, length)
	if _, err := io.ReadFull(reader, out); err != nil {
		return nil, err
	}
	return out, nil
}

func readVarint(reader *bytes.Reader) (int64, error) {
	var value uint64
	var shift uint
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		value |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 63 {
			return 0, errors.New("varint overflow")
		}
	}
	return decodeZigZag(value), nil
}

func decodeZigZag(value uint64) int64 {
	if value&1 == 0 {
		return int64(value >> 1)
	}
	return -int64((value >> 1) + 1)
}

func compressionType(attributes int16) int16 {
	return attributes & 0x07
}

func parseIndex(data []byte) ([]IndexEntry, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("index too small")
	}
	if string(data[:4]) != indexMagic {
		return nil, fmt.Errorf("invalid index magic")
	}
	reader := bytes.NewReader(data[4:])
	var version uint16
	if err := binary.Read(reader, binary.BigEndian, &version); err != nil {
		return nil, err
	}
	if version != 1 {
		return nil, fmt.Errorf("unsupported index version %d", version)
	}
	var count int32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return nil, err
	}
	var interval int32
	if err := binary.Read(reader, binary.BigEndian, &interval); err != nil {
		return nil, err
	}
	var reserved uint16
	if err := binary.Read(reader, binary.BigEndian, &reserved); err != nil {
		return nil, err
	}
	entries := make([]IndexEntry, count)
	for i := int32(0); i < count; i++ {
		var offset int64
		var position int32
		if err := binary.Read(reader, binary.BigEndian, &offset); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.BigEndian, &position); err != nil {
			return nil, err
		}
		entries[i] = IndexEntry{Offset: offset, Position: position}
	}
	_ = interval
	return entries, nil
}

// IndexEntry captures an index offset/position pair.
type IndexEntry struct {
	Offset   int64
	Position int32
}
