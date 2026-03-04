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
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"strings"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type lfsRecordBatch struct {
	kmsg.RecordBatch
	Raw []byte
}

type lfsRewriteResult struct {
	modified    bool
	uploadBytes int64
	topics      map[string]struct{}
	orphans     []orphanInfo
	duration    float64
}

type orphanInfo struct {
	Topic     string
	Key       string
	RequestID string
	Reason    string
}

var lfsCRC32cTable = crc32.MakeTable(crc32.Castagnoli)

// safeHeaderAllowlist defines headers safe to include in the LFS envelope.
var lfsSafeHeaderAllowlist = map[string]bool{
	"content-type":     true,
	"content-encoding": true,
	"correlation-id":   true,
	"message-id":       true,
	"x-correlation-id": true,
	"x-request-id":     true,
	"traceparent":      true,
	"tracestate":       true,
}

// rewriteProduceRecords scans all records in a ProduceRequest for LFS_BLOB
// headers. For each such record, the payload is uploaded to S3 and the record
// value is replaced with an LFS envelope JSON. Batches are re-encoded in-place
// so the caller can pass the modified ProduceRequest to the existing fan-out.
func (m *lfsModule) rewriteProduceRecords(ctx context.Context, header *protocol.RequestHeader, req *protocol.ProduceRequest) (lfsRewriteResult, error) {
	if m.logger == nil {
		m.logger = slog.Default()
	}
	if req == nil {
		return lfsRewriteResult{}, errors.New("nil produce request")
	}

	start := time.Now()
	modified := false
	uploadBytes := int64(0)
	decompressor := kgo.DefaultDecompressor()
	topics := make(map[string]struct{})
	orphans := make([]orphanInfo, 0, 4)

	for ti := range req.Topics {
		topic := &req.Topics[ti]
		for pi := range topic.Partitions {
			partition := &topic.Partitions[pi]
			if len(partition.Records) == 0 {
				continue
			}
			batches, err := lfsDecodeRecordBatches(partition.Records)
			if err != nil {
				return lfsRewriteResult{}, err
			}
			batchModified := false
			for bi := range batches {
				batch := &batches[bi]
				records, codec, err := lfsDecodeBatchRecords(batch, decompressor)
				if err != nil {
					return lfsRewriteResult{}, err
				}
				if len(records) == 0 {
					continue
				}
				recordChanged := false
				for ri := range records {
					rec := &records[ri]
					headers := rec.Headers
					lfsValue, ok := lfsFindHeaderValue(headers, "LFS_BLOB")
					if !ok {
						continue
					}
					recordChanged = true
					modified = true
					topics[topic.Name] = struct{}{}
					checksumHeader := strings.TrimSpace(string(lfsValue))
					algHeader, _ := lfsFindHeaderValue(headers, "LFS_BLOB_ALG")
					alg, err := m.resolveChecksumAlg(string(algHeader))
					if err != nil {
						return lfsRewriteResult{}, err
					}
					if checksumHeader != "" && alg == lfs.ChecksumNone {
						return lfsRewriteResult{}, errors.New("checksum provided but checksum algorithm is none")
					}
					payload := rec.Value
					m.logger.Info("LFS blob detected", "topic", topic.Name, "size", len(payload))
					if int64(len(payload)) > m.maxBlob {
						m.logger.Error("blob exceeds max size", "size", len(payload), "max", m.maxBlob)
						return lfsRewriteResult{}, fmt.Errorf("blob size %d exceeds max %d", len(payload), m.maxBlob)
					}
					key := m.buildObjectKey(topic.Name)
					sha256Hex, checksum, checksumAlg, err := m.s3Uploader.Upload(ctx, key, payload, alg)
					if err != nil {
						m.metrics.IncS3Errors()
						return lfsRewriteResult{}, err
					}
					if checksumHeader != "" && checksum != "" && !strings.EqualFold(checksumHeader, checksum) {
						if err := m.s3Uploader.DeleteObject(ctx, key); err != nil {
							m.trackOrphans([]orphanInfo{{Topic: topic.Name, Key: key, RequestID: "", Reason: "checksum_mismatch_delete_failed"}})
							return lfsRewriteResult{}, fmt.Errorf("checksum mismatch; delete failed: %w", err)
						}
						return lfsRewriteResult{}, &lfs.ChecksumError{Expected: checksumHeader, Actual: checksum}
					}
					env := lfs.Envelope{
						Version:         1,
						Bucket:          m.s3Bucket,
						Key:             key,
						Size:            int64(len(payload)),
						SHA256:          sha256Hex,
						Checksum:        checksum,
						ChecksumAlg:     checksumAlg,
						ContentType:     lfsHeaderValue(headers, "content-type"),
						OriginalHeaders: lfsHeadersToMap(headers),
						CreatedAt:       time.Now().UTC().Format(time.RFC3339),
						ProxyID:         m.proxyID,
					}
					encoded, err := lfs.EncodeEnvelope(env)
					if err != nil {
						return lfsRewriteResult{}, err
					}
					rec.Value = encoded
					rec.Headers = lfsDropHeader(headers, "LFS_BLOB")
					uploadBytes += int64(len(payload))
					orphans = append(orphans, orphanInfo{Topic: topic.Name, Key: key, RequestID: "", Reason: "kafka_produce_failed"})
				}
				if !recordChanged {
					continue
				}
				newRecords := lfsEncodeRecords(records)
				compressedRecords, usedCodec, err := lfsCompressRecords(codec, newRecords)
				if err != nil {
					return lfsRewriteResult{}, err
				}
				batch.Records = compressedRecords
				batch.NumRecords = int32(len(records))
				batch.Attributes = (batch.Attributes &^ 0x0007) | int16(usedCodec)
				batch.Length = 0
				batch.CRC = 0
				batchBytes := batch.AppendTo(nil)
				batch.Length = int32(len(batchBytes) - 12)
				batchBytes = batch.AppendTo(nil)
				batch.CRC = int32(crc32.Checksum(batchBytes[21:], lfsCRC32cTable))
				batchBytes = batch.AppendTo(nil)
				batch.Raw = batchBytes
				batchModified = true
			}
			if !batchModified {
				continue
			}
			partition.Records = lfsJoinRecordBatches(batches)
		}
	}
	if !modified {
		return lfsRewriteResult{modified: false}, nil
	}

	// Records have been modified in-place on the parsed ProduceRequest.
	// The caller sets payload=nil which forces the proxy's fan-out to
	// re-encode via protocol.EncodeProduceRequest().
	return lfsRewriteResult{
		modified:    true,
		uploadBytes: uploadBytes,
		topics:      topics,
		orphans:     orphans,
		duration:    time.Since(start).Seconds(),
	}, nil
}

func lfsDecodeRecordBatches(records []byte) ([]lfsRecordBatch, error) {
	out := make([]lfsRecordBatch, 0, 4)
	buf := records
	for len(buf) > 0 {
		if len(buf) < 12 {
			return nil, fmt.Errorf("record batch too short: %d", len(buf))
		}
		length := int(lfsInt32FromBytes(buf[8:12]))
		total := 12 + length
		if length < 0 || len(buf) < total {
			return nil, fmt.Errorf("invalid record batch length %d", length)
		}
		batchBytes := buf[:total]
		var batch kmsg.RecordBatch
		if err := batch.ReadFrom(batchBytes); err != nil {
			return nil, err
		}
		out = append(out, lfsRecordBatch{RecordBatch: batch, Raw: batchBytes})
		buf = buf[total:]
	}
	return out, nil
}

func lfsJoinRecordBatches(batches []lfsRecordBatch) []byte {
	if len(batches) == 0 {
		return nil
	}
	size := 0
	for _, batch := range batches {
		size += len(batch.Raw)
	}
	out := make([]byte, 0, size)
	for _, batch := range batches {
		out = append(out, batch.Raw...)
	}
	return out
}

func lfsDecodeBatchRecords(batch *lfsRecordBatch, decompressor kgo.Decompressor) ([]kmsg.Record, kgo.CompressionCodecType, error) {
	codec := kgo.CompressionCodecType(batch.Attributes & 0x0007)
	rawRecords := batch.Records
	if codec != kgo.CodecNone {
		var err error
		rawRecords, err = decompressor.Decompress(rawRecords, codec)
		if err != nil {
			return nil, codec, err
		}
	}
	numRecords := int(batch.NumRecords)
	records := make([]kmsg.Record, numRecords)
	records = lfsReadRawRecordsInto(records, rawRecords)
	return records, codec, nil
}

func lfsReadRawRecordsInto(rs []kmsg.Record, in []byte) []kmsg.Record {
	for i := range rs {
		length, used := lfsVarint(in)
		total := used + int(length)
		if used == 0 || length < 0 || len(in) < total {
			return rs[:i]
		}
		if err := (&rs[i]).ReadFrom(in[:total]); err != nil {
			rs[i] = kmsg.Record{}
			return rs[:i]
		}
		in = in[total:]
	}
	return rs
}

func lfsCompressRecords(codec kgo.CompressionCodecType, raw []byte) ([]byte, kgo.CompressionCodecType, error) {
	if codec == kgo.CodecNone {
		return raw, kgo.CodecNone, nil
	}
	var comp kgo.Compressor
	var err error
	switch codec {
	case kgo.CodecGzip:
		comp, err = kgo.DefaultCompressor(kgo.GzipCompression())
	case kgo.CodecSnappy:
		comp, err = kgo.DefaultCompressor(kgo.SnappyCompression())
	case kgo.CodecLz4:
		comp, err = kgo.DefaultCompressor(kgo.Lz4Compression())
	case kgo.CodecZstd:
		comp, err = kgo.DefaultCompressor(kgo.ZstdCompression())
	default:
		return raw, kgo.CodecNone, nil
	}
	if err != nil || comp == nil {
		return raw, kgo.CodecNone, err
	}
	out, usedCodec := comp.Compress(bytes.NewBuffer(nil), raw)
	return out, usedCodec, nil
}

func lfsFindHeaderValue(headers []kmsg.Header, key string) ([]byte, bool) {
	for _, header := range headers {
		if header.Key == key {
			return header.Value, true
		}
	}
	return nil, false
}

func lfsHeaderValue(headers []kmsg.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func lfsHeadersToMap(headers []kmsg.Header) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string]string)
	for _, header := range headers {
		key := strings.ToLower(header.Key)
		if lfsSafeHeaderAllowlist[key] {
			out[header.Key] = string(header.Value)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func lfsDropHeader(headers []kmsg.Header, key string) []kmsg.Header {
	if len(headers) == 0 {
		return headers
	}
	out := make([]kmsg.Header, 0, len(headers))
	for _, header := range headers {
		if header.Key == key {
			continue
		}
		out = append(out, header)
	}
	return out
}

func lfsInt32FromBytes(b []byte) int32 {
	return int32(uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]))
}
