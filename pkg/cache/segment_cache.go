package cache

import (
	"container/list"
	"fmt"
	"sync"
)

// SegmentCache provides an LRU cache keyed by topic/partition/baseOffset storing segment bytes.
type SegmentCache struct {
	mu       sync.Mutex
	capacity int
	size     int
	ll       *list.List
	items    map[string]*list.Element
}

type cacheEntry struct {
	key        string
	topic      string
	partition  int32
	baseOffset int64
	data       []byte
}

// NewSegmentCache creates a cache with capacity in bytes.
func NewSegmentCache(capacityBytes int) *SegmentCache {
	if capacityBytes <= 0 {
		capacityBytes = 1
	}
	return &SegmentCache{
		capacity: capacityBytes,
		ll:       list.New(),
		items:    make(map[string]*list.Element),
	}
}

func makeKey(topic string, partition int32, baseOffset int64) string {
	return fmt.Sprintf("%s:%d:%d", topic, partition, baseOffset)
}

// GetSegment returns cached data if present.
func (c *SegmentCache) GetSegment(topic string, partition int32, baseOffset int64) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[makeKey(topic, partition, baseOffset)]; ok {
		c.ll.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		return entry.data, true
	}
	return nil, false
}

// SetSegment adds or updates a cache entry.
func (c *SegmentCache) SetSegment(topic string, partition int32, baseOffset int64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := makeKey(topic, partition, baseOffset)
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*cacheEntry)
		c.size -= len(entry.data)
		entry.data = append(entry.data[:0], data...)
		c.size += len(entry.data)
		c.ll.MoveToFront(elem)
		c.evictIfNeeded()
		return
	}
	copyData := append([]byte(nil), data...)
	entry := &cacheEntry{
		key:        key,
		topic:      topic,
		partition:  partition,
		baseOffset: baseOffset,
		data:       copyData,
	}
	elem := c.ll.PushFront(entry)
	c.items[key] = elem
	c.size += len(copyData)
	c.evictIfNeeded()
}

func (c *SegmentCache) evictIfNeeded() {
	for c.size > c.capacity && c.ll.Len() > 0 {
		elem := c.ll.Back()
		entry := elem.Value.(*cacheEntry)
		delete(c.items, entry.key)
		c.ll.Remove(elem)
		c.size -= len(entry.data)
	}
}
