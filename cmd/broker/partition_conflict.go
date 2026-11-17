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

import (
	"github.com/KafScale/platform/pkg/metadata"
	"github.com/KafScale/platform/pkg/storage"
)

// Concurrent requests may still hold the failed handle after another request
// has restored a new log under the same lease. Only evict that exact handle.
func (h *handler) dropConflictingPartitionLog(topic string, partition int32, failed *storage.PartitionLog) {
	h.logMu.Lock()
	defer h.logMu.Unlock()
	if h.logs[topic][partition] == failed {
		delete(h.logs[topic], partition)
		delete(h.logTokens, metadata.PartitionID{Topic: topic, Partition: partition})
	}
}
