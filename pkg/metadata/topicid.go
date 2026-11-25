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

package metadata

import (
	"crypto/sha1"

	"github.com/google/uuid"
)

// NewTopicID allocates a random UUID for a new topic incarnation.
func NewTopicID() [16]byte { return [16]byte(uuid.New()) }

// TopicIDForName returns the legacy name-derived ID. It is retained only for
// existing snapshots without an explicit ID; new topics use NewTopicID.
func TopicIDForName(name string) [16]byte {
	sum := sha1.Sum([]byte(name))
	var id [16]byte
	copy(id[:], sum[:16])
	return id
}
