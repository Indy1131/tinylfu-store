package cache

import "hash/fnv"

type FrequencySketch struct {
	counters []uint8
	mask     uint32
}

func NewSketch(size int) *FrequencySketch {
	sampleSize := 1
	for sampleSize < size {
		sampleSize <<= 1
	}
	return &FrequencySketch{
		counters: make([]uint8, sampleSize),
		mask:     uint32(sampleSize - 1),
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
		if s.counters[idx] < 255 {
			s.counters[idx]++
		}
	}
}

func (s *FrequencySketch) Estimate(key string) uint8 {
	h := s.hash(key)
	h1 := uint32(h)
	h2 := uint32(h >> 32)

	var min uint8 = 255
	for i := uint32(0); i < 4; i++ {
		idx := (h1 + i*h2) & s.mask
		if s.counters[idx] < min {
			min = s.counters[idx]
		}
	}
	return min
}
