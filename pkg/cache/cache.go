package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// totalMaxEntries is the aggregate capacity across all shards.
const totalMaxEntries = 100000

// numShards is the number of independent LRU shards the keyspace is
// partitioned into. Each shard has its own mutex, so unrelated keys never
// contend for the same lock.
const numShards = 32

// Entry is a single cached key/value pair, exposed mainly so callers
// walking a shard's list (e.g. in tests) can read what's stored.
type Entry struct {
	Key   string
	Value any
}

// Cache is the minimal interface MemoryCache implements; it exists so
// callers can depend on an interface rather than the concrete type.
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

// MemoryCache is a sharded, in-memory, concurrency-safe key-value store
// with LRU eviction gated by a TinyLFU admission policy (a Count-Min
// Sketch behind a Bloom-filter doorkeeper, see sketch.go and
// doorkeeper.go). It implements Cache.
type MemoryCache struct {
	shards           []*shard
	sketch           *FrequencySketch
	buffer           chan uint64
	stopChan         chan struct{}
	admissionEnabled bool

	hits      uint64
	misses    uint64
	evictions uint64
	rejected  uint64
}

// CacheStats is a point-in-time snapshot of cache activity, useful for
// measuring hit ratio and admission behavior (e.g. from a benchmark
// harness) without instrumenting call sites yourself.
type CacheStats struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
	// Rejected counts Set calls that were declined by the TinyLFU
	// admission policy because the incoming key was estimated to be
	// less frequent than the shard's current LRU victim. Always 0 when
	// admission is disabled.
	Rejected uint64
}

// HitRatio returns Hits / (Hits + Misses), or 0 if there have been no
// Get calls yet.
func (s CacheStats) HitRatio() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// Option configures a MemoryCache at construction time.
type Option func(*MemoryCache)

// WithAdmission toggles the TinyLFU admission policy. When disabled, the
// cache behaves like a plain sharded LRU: every new key is admitted and
// always evicts the shard's least-recently-used entry. This exists so a
// single binary (e.g. the benchmark harness in cmd/bench) can compare both
// policies head-to-head without switching git branches. Admission is
// enabled by default.
func WithAdmission(enabled bool) Option {
	return func(c *MemoryCache) {
		c.admissionEnabled = enabled
	}
}

// New creates a MemoryCache with totalMaxEntries capacity split evenly
// across numShards shards, and starts its background frequency-tracking
// goroutine (see processBuffer). There is currently no Close/Shutdown
// method; the goroutine is expected to live for the process's lifetime.
func New(opts ...Option) *MemoryCache {
	shardLimit := totalMaxEntries / numShards

	c := &MemoryCache{
		shards:           make([]*shard, numShards),
		sketch:           NewSketch(totalMaxEntries),
		buffer:           make(chan uint64, 1024),
		stopChan:         make(chan struct{}),
		admissionEnabled: true,
	}
	for _, opt := range opts {
		opt(c)
	}
	for i := 0; i < numShards; i++ {
		c.shards[i] = &shard{
			data:       make(map[string]*list.Element),
			ll:         list.New(),
			maxEntries: shardLimit,
		}
	}

	go c.processBuffer()

	return c
}

// Capacity returns the maximum number of entries the cache can hold across
// all shards.
func (c *MemoryCache) Capacity() int {
	total := 0
	for _, s := range c.shards {
		total += s.maxEntries
	}
	return total
}

// Stats returns a snapshot of the cache's hit/miss/eviction counters.
func (c *MemoryCache) Stats() CacheStats {
	return CacheStats{
		Hits:      atomic.LoadUint64(&c.hits),
		Misses:    atomic.LoadUint64(&c.misses),
		Evictions: atomic.LoadUint64(&c.evictions),
		Rejected:  atomic.LoadUint64(&c.rejected),
	}
}

func (c *MemoryCache) getShardIndex(key string) uint32 {
	h := c.sketch.Hash(key)
	return uint32(h) % uint32(numShards)
}

// Get returns the value stored under key and true if present, moving the
// entry to the front of its shard's LRU list. Every call also records an
// access for frequency-tracking purposes (see processBuffer), regardless
// of hit or miss.
func (c *MemoryCache) Get(key string) (any, bool) {
	h := c.sketch.Hash(key)

	select {
	case c.buffer <- h:
	default:
	}

	shardIndex := uint32(h) % uint32(numShards)
	s := c.shards[shardIndex]

	s.mu.Lock()
	defer s.mu.Unlock()

	if ele, ok := s.data[key]; ok {
		s.ll.MoveToFront(ele)

		entry := ele.Value.(*Entry)
		atomic.AddUint64(&c.hits, 1)
		return entry.Value, true
	}

	atomic.AddUint64(&c.misses, 1)
	return nil, false
}

// Set inserts or updates key. If its shard is full and admission is
// enabled, the incoming key is only admitted if the TinyLFU sketch
// estimates it to be at least as frequent as the shard's current LRU
// victim; otherwise the write is silently dropped (see CacheStats.Rejected).
// With admission disabled, Set always evicts the LRU victim and admits
// the new key, matching plain LRU behavior.
func (c *MemoryCache) Set(key string, value any) {
	h := c.sketch.Hash(key)

	shardIndex := uint32(h) % uint32(numShards)
	s := c.shards[shardIndex]

	s.mu.Lock()
	defer s.mu.Unlock()

	if ele, ok := s.data[key]; ok {
		s.ll.MoveToFront(ele)
		ele.Value.(*Entry).Value = value
		return
	}

	if s.maxEntries > 0 && s.ll.Len() >= s.maxEntries {
		if c.admissionEnabled {
			victimEle := s.ll.Back()
			victim := victimEle.Value.(*Entry)

			victimFreq := c.sketch.Estimate(c.sketch.Hash(victim.Key))
			newFreq := c.sketch.Estimate(h)

			if newFreq < victimFreq {
				atomic.AddUint64(&c.rejected, 1)
				return
			}
		}

		s.removeOldest()
		atomic.AddUint64(&c.evictions, 1)
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

// Delete removes key if present. It is a no-op if the key is not cached.
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

func (c *MemoryCache) processBuffer() {
	for {
		select {
		case h := <-c.buffer:
			c.sketch.Increment(h)
		case <-c.stopChan:
			return
		}
	}
}
