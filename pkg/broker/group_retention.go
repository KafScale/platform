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
	groups, err := c.store.ListConsumerGroups(ctx)
	if err != nil {
		slog.Warn("list groups for retention failed", "error", err)
		return
	}
	cutoff := now.Add(-c.config.OffsetsRetention)
	for _, group := range groups {
		if ctx.Err() != nil {
			return
		}
		if !retentionCandidate(group, now, cutoff) {
			continue
		}
		id := group.GroupId
		acquired := !c.ownsGroup(id)
		if acquired {
			if c.config.AcquireGroup == nil || c.config.AcquireGroup(ctx, id) != nil {
				continue
			}
		}
		c.mu.Lock()
		if !c.ownsGroup(id) {
			c.mu.Unlock()
			continue
		}
		// Recover stale sessions after a restart as well as cached groups. Missing
		// heartbeat timestamps receive the normal recovery grace before expiry.
		if len(group.Members) > 0 {
			state, loadErr := c.loadGroupIfMissing(ctx, id)
			if loadErr != nil {
				if acquired && c.config.ReleaseGroup != nil {
					c.config.ReleaseGroup(id)
				}
				c.mu.Unlock()
				continue
			}
			if state != nil {
				if expireErr := c.expireGroupMembersLocked(ctx, id, state, now); expireErr != nil || len(state.members) > 0 {
					c.mu.Unlock()
					continue
				}
			}
		}
		if !c.ownsGroup(id) {
			c.mu.Unlock()
			continue
		}
		deleted, expireErr := retainer.ExpireConsumerGroup(ctx, id, cutoff, now)
		if expireErr != nil && !errors.Is(expireErr, metadata.ErrConsumerGroupChanged) {
			slog.Warn("consumer group retention failed", "group", id, "error", expireErr)
		}
		if deleted {
			delete(c.groups, id)
			if c.config.GroupDeleted != nil {
				c.config.GroupDeleted(id)
			}
		} else if acquired && c.config.ReleaseGroup != nil {
			c.config.ReleaseGroup(id)
		}
		c.mu.Unlock()
	}
}
