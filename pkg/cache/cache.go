package cache

import (
	"hash/fnv"
	"sync"
)

const numShards = 32

type Entry struct {
	Value any
}

type Cache interface {
	Get(key string) (any, bool)
	Set(key string, value any)
	Delete(key string)
}

type shard struct {
	mu   sync.RWMutex
	data map[string]*Entry
}

type MemoryCache struct {
	shards []*shard
}

func New() *MemoryCache {
	c := &MemoryCache{
		shards: make([]*shard, numShards),
	}
	for i := 0; i < numShards; i++ {
		c.shards[i] = &shard{
			data: make(map[string]*Entry),
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

	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data[key]
	if !ok {
		return nil, false
	}
	return entry.Value, true
}

func (c *MemoryCache) Set(key string, value any) {
	shardIndex := c.getShardIndex(key)
	s := c.shards[shardIndex]

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = &Entry{Value: value}
}

func (c *MemoryCache) Delete(key string) {
	shardIndex := c.getShardIndex(key)
	s := c.shards[shardIndex]

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}
