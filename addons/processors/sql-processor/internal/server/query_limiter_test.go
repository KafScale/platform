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

package server

import (
	"testing"
	"time"
)

func TestQueryLimiterTimeout(t *testing.T) {
	limiter := newQueryLimiter(1, 1)
	release, err := limiter.acquire(50 * time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected acquire error: %v", err)
	}
	defer release()

	_, err = limiter.acquire(30 * time.Millisecond)
	if err != errQueueTimeout {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestQueryLimiterQueueFull(t *testing.T) {
	limiter := newQueryLimiter(1, 0)
	release, err := limiter.acquire(50 * time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected acquire error: %v", err)
	}
	defer release()

	_, err = limiter.acquire(10 * time.Millisecond)
	if err != errQueueFull {
		t.Fatalf("expected queue full error, got %v", err)
	}
}
