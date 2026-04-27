package cache

import (
	"container/list"
	"hash/fnv"
	"sync"
)

const totalMaxEntries = 100000
const numShards = 32

type Entry struct {
	Key   string
	Value any
}

type Cache interface {
	Get(key string) (any, bool)
	Set(key string, value any)
	Delete(key string)
}

type shard struct {
	mu         sync.RWMutex
	data       map[string]*list.Element
	ll         *list.List
	maxEntries int
}

type MemoryCache struct {
	shards   []*shard
	sketch   *FrequencySketch
	sketchMu sync.Mutex
}

func New() *MemoryCache {
	shardLimit := totalMaxEntries / numShards

	c := &MemoryCache{
		shards: make([]*shard, numShards),
		sketch: NewSketch(totalMaxEntries),
	}
	for i := 0; i < numShards; i++ {
		c.shards[i] = &shard{
			data:       make(map[string]*list.Element),
			ll:         list.New(),
			maxEntries: shardLimit,
		}
	}
	return c
}

func (c *MemoryCache) getShardIndex(key string) uint32 {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return hash.Sum32() % uint32(numShards)
}

func (c *MemoryCache) Get(key string) (any, bool) {
	shardIndex := c.getShardIndex(key)
	s := c.shards[shardIndex]

	s.mu.Lock()
	defer s.mu.Unlock()

	if ele, ok := s.data[key]; ok {
		s.ll.MoveToFront(ele)

		c.sketchMu.Lock()
		c.sketch.Increment(key)
		c.sketchMu.Unlock()

		entry := ele.Value.(*Entry)
		return entry.Value, true
	}
	return nil, false
}

func (c *MemoryCache) Set(key string, value any) {
	shardIndex := c.getShardIndex(key)
	s := c.shards[shardIndex]

	s.mu.Lock()
	defer s.mu.Unlock()

	if ele, ok := s.data[key]; ok {
		s.ll.MoveToFront(ele)
		ele.Value.(*Entry).Value = value
		return
	}

	c.sketchMu.Lock()
	c.sketch.Increment(key)
	c.sketchMu.Unlock()

	if s.maxEntries > 0 && s.ll.Len() >= s.maxEntries {
		victimEle := s.ll.Back()
		victim := victimEle.Value.(*Entry)

		c.sketchMu.Lock()
		victimFreq := c.sketch.Estimate(victim.Key)
		newFreq := c.sketch.Estimate(key)
		c.sketchMu.Unlock()

		if newFreq < victimFreq {
			return
		}

		s.removeOldest()
	}

	newEntry := &Entry{Key: key, Value: value}
	ele := s.ll.PushFront(newEntry)
	s.data[key] = ele
}

func (s *shard) removeOldest() {
	ele := s.ll.Back()
	if ele != nil {
		s.ll.Remove(ele)
		entry := ele.Value.(*Entry)
		delete(s.data, entry.Key)
	}
}

func (c *MemoryCache) Delete(key string) {
	shardIndex := c.getShardIndex(key)
	s := c.shards[shardIndex]

	s.mu.Lock()
	defer s.mu.Unlock()

	if ele, ok := s.data[key]; ok {
		s.ll.Remove(ele)
		delete(s.data, key)
	}
}
