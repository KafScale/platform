// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package discovery

import (
	"context"
	"errors"
	"sync"
	"time"
)

var errLeaseHeld = errors.New("build lease already held")

type buildLease struct {
	mu        sync.Mutex
	holder    string
	expiresAt time.Time
}

func newBuildLease() *buildLease {
	return &buildLease{}
}

func (l *buildLease) Acquire(ctx context.Context, holder string, ttl time.Duration) (func(), error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	now := time.Now()
	l.mu.Lock()
	if holder == "" {
		l.mu.Unlock()
		return nil, errors.New("lease holder required")
	}
	if l.holder != "" && now.Before(l.expiresAt) {
		l.mu.Unlock()
		return nil, errLeaseHeld
	}
	l.holder = holder
	l.expiresAt = now.Add(ttl)
	l.mu.Unlock()

	release := func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.holder == holder {
			l.holder = ""
			l.expiresAt = time.Time{}
		}
	}

	select {
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	default:
	}
	return release, nil
}

func (l *buildLease) Snapshot() (string, time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.holder, l.expiresAt
}
