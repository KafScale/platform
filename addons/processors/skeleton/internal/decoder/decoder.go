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

import "context"

// Batch represents a decoded record batch.
type Batch struct {
	Topic     string
	Partition int32
	Offset    int64
	Payload   []byte
}

// Decoder parses Kafscale segment data into batches.
type Decoder interface {
	Decode(ctx context.Context, segmentKey, indexKey string) ([]Batch, error)
}

// New returns a placeholder decoder.
func New() Decoder {
	return &noopDecoder{}
}

type noopDecoder struct{}

func (n *noopDecoder) Decode(ctx context.Context, segmentKey, indexKey string) ([]Batch, error) {
	return nil, nil
}
