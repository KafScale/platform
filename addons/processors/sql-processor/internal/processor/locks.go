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

package processor

import "sync"

type topicLocker struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newTopicLocker() *topicLocker {
	return &topicLocker{
		locks: make(map[string]*sync.Mutex),
	}
}

func (l *topicLocker) Lock(topic string) func() {
	l.mu.Lock()
	lock, ok := l.locks[topic]
	if !ok {
		lock = &sync.Mutex{}
		l.locks[topic] = lock
	}
	l.mu.Unlock()

	lock.Lock()
	return lock.Unlock
}
