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

import "context"

// GroupConsumerOffsetLister is an optional efficient group-scoped lookup.
// Implementations predating it remain valid Stores.
type GroupConsumerOffsetLister interface {
	ListGroupConsumerOffsets(context.Context, string) ([]ConsumerOffset, error)
}

// ListGroupConsumerOffsets uses a group-scoped lookup when available. Older
// Stores are adapted by filtering ListConsumerOffsets, preserving offset metadata
// and propagating errors. The fallback scans all groups and can be more expensive.
func ListGroupConsumerOffsets(ctx context.Context, store Store, group string) ([]ConsumerOffset, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lister, ok := store.(GroupConsumerOffsetLister); ok {
		return lister.ListGroupConsumerOffsets(ctx, group)
	}
	offsets, err := store.ListConsumerOffsets(ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var result []ConsumerOffset
	for _, offset := range offsets {
		if offset.Group == group {
			result = append(result, offset)
		}
	}
	return result, nil
}
