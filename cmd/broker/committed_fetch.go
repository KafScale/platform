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

package main

import "encoding/binary"

// limitFetchToStableOffset fences a read at the durable offset captured for this
// response. A concurrent flush or a snap across a retained-data gap can make Read
// return newer batches. Never expose a batch that crosses the captured boundary.
func limitFetchToStableOffset(records []byte, stable int64) []byte {
	end := 0
	for len(records)-end >= 61 {
		batch := records[end:]
		length := int64(int32(binary.BigEndian.Uint32(batch[8:12]))) + 12
		if length < 61 || length > int64(len(batch)) {
			break
		}
		base := int64(binary.BigEndian.Uint64(batch[:8]))
		delta := int64(int32(binary.BigEndian.Uint32(batch[23:27])))
		if base < 0 || delta < 0 || base >= stable || delta >= stable-base {
			break
		}
		end += int(length)
	}
	return records[:end]
}
