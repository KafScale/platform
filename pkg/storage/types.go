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

import "time"

// RecordBatch carries a Kafka record batch blob plus metadata required for indexing.
type RecordBatch struct {
	BaseOffset      int64
	LastOffsetDelta int32
	MessageCount    int32
	Bytes           []byte
}

// SegmentWriterConfig controls serialization.
type SegmentWriterConfig struct {
	IndexIntervalMessages int32
}

// SegmentArtifact contains serialized segment + index bytes ready for upload.
type SegmentArtifact struct {
	BaseOffset    int64
	LastOffset    int64
	MessageCount  int32
	CreatedAt     time.Time
	SegmentBytes  []byte
	IndexBytes    []byte
	RelativeIndex []*IndexEntry
}

// IndexEntry mirrors a sparse index row.
type IndexEntry struct {
	Offset   int64
	Position int32
}
