package cache

import (
	"hash/fnv"
	"sync/atomic"
)

type FrequencySketch struct {
	counters   []uint64
	mask       uint32
	totalCount uint32
	sampleSize uint32
}

func NewSketch(size uint32) *FrequencySketch {
	var sampleSize uint32 = 1
	for sampleSize < size {
		sampleSize <<= 1
	}
	return &FrequencySketch{
		counters:   make([]uint64, sampleSize),
		mask:       uint32(sampleSize - 1),
		sampleSize: size,
	}
}

func (s *FrequencySketch) hash(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func (s *FrequencySketch) Increment(key string) {
	h := s.hash(key)
	h1 := uint32(h)
	h2 := uint32(h >> 32)

	for i := uint32(0); i < 4; i++ {
		idx := (h1 + i*h2) & s.mask

		atomic.AddUint64(&s.counters[idx], 1)
	}

	if atomic.AddUint32(&s.totalCount, 1) >= s.sampleSize {
		s.reset()
	}
}

func (s *FrequencySketch) Estimate(key string) uint64 {
	h := s.hash(key)
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
}
