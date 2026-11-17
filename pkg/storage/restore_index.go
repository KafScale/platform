// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"time"
)

func (l *PartitionLog) restoreSegmentIndex(ctx context.Context, obj S3Object, base, last int64) ([]*IndexEntry, error) {
	if err := l.acquireS3(ctx); err != nil {
		return nil, err
	}
	started := time.Now()
	data, err := l.s3.DownloadSegment(ctx, obj.Key, nil)
	l.releaseS3()
	if l.onS3Op != nil {
		l.onS3Op("download_segment_rebuild_index", time.Since(started), err)
	}
	if err != nil {
		return nil, err
	}
	return rebuildSegmentIndex(data, base, last, l.cfg.Segment.IndexIntervalMessages)
}

// rebuildSegmentIndex validates the complete winning segment before using its
// offsets. The derived index stays local so a delayed writer can finish its
// sidecar without racing a recovery PUT.
func rebuildSegmentIndex(data []byte, base, last int64, interval int32) ([]*IndexEntry, error) {
	if len(data) < segmentHeaderLen+segmentFooterLen || base < 0 || last < base || last == math.MaxInt64 {
		return nil, fmt.Errorf("invalid segment bounds")
	}
	if _, err := parseSegmentHeaderCreatedAt(data[:segmentHeaderLen]); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint16(data[4:6]) != 1 || int64(binary.BigEndian.Uint64(data[8:16])) != base {
		return nil, fmt.Errorf("segment header does not match its key")
	}
	footer := data[len(data)-segmentFooterLen:]
	gotLast, err := parseSegmentFooter(footer)
	if err != nil {
		return nil, err
	}
	if gotLast != last {
		return nil, fmt.Errorf("segment footer changed during restore")
	}
	body := data[segmentHeaderLen : len(data)-segmentFooterLen]
	if crc32.Checksum(body, crcTable) != binary.BigEndian.Uint32(footer[:4]) {
		return nil, fmt.Errorf("segment checksum mismatch")
	}
	index := NewIndexBuilder(interval)
	next := base
	for position := 0; position < len(body); {
		if len(body)-position < batchFrameHeaderLen {
			return nil, fmt.Errorf("truncated record batch frame")
		}
		frameLen := uint64(binary.BigEndian.Uint32(body[position+8:position+12])) + batchFrameHeaderLen
		if frameLen > uint64(len(body)-position) {
			return nil, fmt.Errorf("record batch exceeds segment bounds")
		}
		batch, err := NewRecordBatchFromBytes(body[position : position+int(frameLen)])
		if err != nil {
			return nil, err
		}
		if batch.BaseOffset != next || batch.LastOffsetDelta < 0 || int64(batch.LastOffsetDelta) > last-next {
			return nil, fmt.Errorf("record batch offsets do not match segment bounds")
		}
		index.MaybeAdd(batch.BaseOffset, int32(segmentHeaderLen+position), batch.MessageCount)
		next += int64(batch.LastOffsetDelta) + 1
		position += int(frameLen)
	}
	if next != last+1 {
		return nil, fmt.Errorf("segment does not contain its advertised last offset")
	}
	return index.Entries(), nil
}
