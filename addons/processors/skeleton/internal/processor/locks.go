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
