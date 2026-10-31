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

package broker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	metadatapb "github.com/KafScale/platform/pkg/gen/metadata"
	"github.com/KafScale/platform/pkg/metadata"
)

var errRetentionMembersRemain = errors.New("consumer group still has members")

func retentionCandidate(group *metadatapb.ConsumerGroup, now, cutoff time.Time) bool {
	if len(group.Members) == 0 {
		since, err := time.Parse(time.RFC3339Nano, group.EmptySince)
		return err != nil || !since.After(cutoff)
	}
	for _, member := range group.Members {
		heartbeat, err := time.Parse(time.RFC3339Nano, member.HeartbeatAt)
		timeout := defaultSessionTimeout
		if member.SessionTimeoutMs > 0 {
			timeout = time.Duration(member.SessionTimeoutMs) * time.Millisecond
		}
		if err != nil || now.Sub(heartbeat) > timeout+recoveredSessionGrace {
			return true
		}
	}
	return false
}

func (c *GroupCoordinator) cleanupRetainedGroups(now time.Time) {
	retainer, ok := c.store.(metadata.ConsumerGroupRetainer)
	if !ok || c.config.OffsetsRetention <= 0 {
		return
	}
	c.mu.Lock()
	if !c.lastRetentionSweep.IsZero() && now.Sub(c.lastRetentionSweep) < c.config.RetentionCheckInterval {
		c.mu.Unlock()
		return
	}
	c.lastRetentionSweep = now
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cutoff := now.Add(-c.config.OffsetsRetention)
	c.mu.Lock()
	var owned []string
	for id, state := range c.groups {
		if len(state.members) == 0 && c.ownsGroup(id) {
			owned = append(owned, id)
		}
	}
	c.mu.Unlock()
	seen := make(map[string]bool, len(owned))
	for _, id := range owned {
		if ctx.Err() != nil {
			return
		}
		seen[id] = true
		c.retainGroup(ctx, retainer, id, false, cutoff, now)
	}
	if c.config.AcquireRetentionSweep != nil && c.config.AcquireRetentionSweep(ctx) != nil {
		return
	}
	groups, err := c.store.ListConsumerGroups(ctx)
	if err != nil {
		slog.Warn("list groups for retention failed", "error", err)
		return
	}
	for _, group := range groups {
		if ctx.Err() != nil {
			return
		}
		if seen[group.GroupId] || !retentionCandidate(group, now, cutoff) {
			continue
		}
		c.retainGroup(ctx, retainer, group.GroupId, len(group.Members) > 0, cutoff, now)
	}
}

func (c *GroupCoordinator) retainGroup(ctx context.Context, retainer metadata.ConsumerGroupRetainer, id string, hasMembers bool, cutoff, now time.Time) {
	acquired := !c.ownsGroup(id)
	if acquired {
		if c.config.AcquireGroup == nil || c.config.AcquireGroup(ctx, id) != nil {
			return
		}
	}
	release := func() {
		if acquired && c.config.ReleaseGroup != nil {
			c.config.ReleaseGroup(id)
		}
	}
	c.mu.Lock()
	if !c.ownsGroup(id) {
		c.mu.Unlock()
		release()
		return
	}
	if hasMembers {
		state, loadErr := c.loadGroupIfMissing(ctx, id)
		if loadErr == nil && state != nil {
			loadErr = c.expireGroupMembersLocked(ctx, id, state, now)
			if loadErr == nil && len(state.members) > 0 {
				loadErr = errRetentionMembersRemain
			}
		}
		if loadErr != nil {
			c.mu.Unlock()
			release()
			return
		}
	}
	owned := c.ownsGroup(id)
	c.mu.Unlock()
	if !owned {
		release()
		return
	}
	deleted, expireErr := retainer.ExpireConsumerGroup(ctx, id, cutoff, now)
	if expireErr != nil && !errors.Is(expireErr, metadata.ErrConsumerGroupChanged) {
		slog.Warn("consumer group retention failed", "group", id, "error", expireErr)
	}
	if !deleted {
		release()
		return
	}
	c.mu.Lock()
	var released []string
	if state := c.groups[id]; state == nil || len(state.members) == 0 {
		delete(c.groups, id)
		released = c.markReleasing(released, id)
	}
	c.mu.Unlock()
	c.finishRelease(released)
}
