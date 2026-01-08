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

package main

import (
	"sync"
	"syscall"
	"time"
)

type cpuTracker struct {
	mu       sync.Mutex
	lastWall time.Time
	lastCPU  time.Duration
}

func newCPUTracker() *cpuTracker {
	now := time.Now()
	return &cpuTracker{
		lastWall: now,
		lastCPU:  readCPUTime(),
	}
}

func (t *cpuTracker) Percent() float64 {
	if t == nil {
		return 0
	}
	now := time.Now()
	cpu := readCPUTime()
	t.mu.Lock()
	defer t.mu.Unlock()
	wall := now.Sub(t.lastWall)
	cpuDelta := cpu - t.lastCPU
	t.lastWall = now
	t.lastCPU = cpu
	if wall <= 0 {
		return 0
	}
	if cpuDelta < 0 {
		return 0
	}
	return cpuDelta.Seconds() / wall.Seconds() * 100
}

func readCPUTime() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	utime := time.Duration(usage.Utime.Sec)*time.Second + time.Duration(usage.Utime.Usec)*time.Microsecond
	stime := time.Duration(usage.Stime.Sec)*time.Second + time.Duration(usage.Stime.Usec)*time.Microsecond
	return utime + stime
}
