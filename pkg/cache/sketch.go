package cache

import (
	"hash/fnv"
	"sync/atomic"
)

// FrequencySketch approximates how often each key has been accessed
// recently, using a 4-way Count-Min Sketch behind a Bloom-filter
// doorkeeper (see doorkeeper.go). It is the "TinyLFU" in this cache: its
// estimates decide whether a new key deserves to evict the LRU victim.
// All operations are lock-free (atomics only), so it is safe to call
// concurrently from many goroutines.
type FrequencySketch struct {
	counters   []uint64
	door       *doorkeeper
	mask       uint32
	totalCount uint32
	sampleSize uint32
}

// doorkeeperBitsPerCounter controls the size of the Bloom filter relative
// to the Count-Min Sketch: more bits means fewer false positives (keys
// wrongly treated as "already seen"), at the cost of memory.
const doorkeeperBitsPerCounter = 8

// NewSketch builds a FrequencySketch sized for roughly size distinct
// keys. Counters age (halve) every time totalCount reaches size, keeping
// the sketch biased towards recent behavior rather than all-time totals.
func NewSketch(size uint32) *FrequencySketch {
	var counterSize uint32 = 1
	for counterSize < size {
		counterSize <<= 1
	}
	return &FrequencySketch{
		counters:   make([]uint64, counterSize),
		door:       newDoorkeeper(counterSize * doorkeeperBitsPerCounter),
		mask:       uint32(counterSize - 1),
		sampleSize: size,
	}
}

// Hash returns a 64-bit FNV-1a hash of key. Callers hash a key once and
// reuse the result across Increment/Estimate calls (and for shard
// selection) to avoid re-hashing the same string repeatedly.
func (s *FrequencySketch) Hash(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// Increment records one access for the key whose hash is h.
func (s *FrequencySketch) Increment(h uint64) {
	// A key must clear the doorkeeper (i.e. be observed at least twice
	// since the last reset) before it starts accumulating frequency in
	// the Count-Min Sketch. This keeps one-hit-wonders from polluting it.
	if s.door.addIfNotContains(h) {
		h1 := uint32(h)
		h2 := uint32(h >> 32)

		for i := uint32(0); i < 4; i++ {
			idx := (h1 + i*h2) & s.mask

			atomic.AddUint64(&s.counters[idx], 1)
		}
	}

	if atomic.AddUint32(&s.totalCount, 1) >= s.sampleSize {
		s.reset()
	}
}

// Estimate returns the approximate access frequency for the key whose
// hash is h: the minimum of its 4 Count-Min Sketch counters, which is
// always >= the true count and only overestimates on hash collisions.
func (s *FrequencySketch) Estimate(h uint64) uint64 {
	h1 := uint32(h)
	h2 := uint32(h >> 32)

	var min uint64 = 255
	for i := uint32(0); i < 4; i++ {
		idx := (h1 + i*h2) & s.mask

		count := atomic.LoadUint64(&s.counters[idx])

		if count < min {
			min = count
		}
	}
	return min
}

func (s *FrequencySketch) reset() {
	for i := range s.counters {
		v := atomic.LoadUint64(&s.counters[i])
		atomic.StoreUint64(&s.counters[i], v>>1)
	}
	atomic.StoreUint32(&s.totalCount, 0)
	s.door.reset()
}
