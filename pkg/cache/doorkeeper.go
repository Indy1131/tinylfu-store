package cache

import "sync/atomic"

// doorkeeper is a small lock-free Bloom filter that gates entry into the
// Count-Min Sketch. A key must be observed at least twice within a reset
// window before it starts accumulating frequency, so one-hit-wonders never
// pollute the sketch. This mirrors the doorkeeper used by Caffeine's
// TinyLFU implementation.
type doorkeeper struct {
	bits []uint64
	mask uint32
}

// newDoorkeeper builds a filter with at least numBits bits, rounded up to
// the next power of two so bit indexes can be masked instead of modded.
func newDoorkeeper(numBits uint32) *doorkeeper {
	var size uint32 = 1
	for size < numBits {
		size <<= 1
	}

	words := size / 64
	if words == 0 {
		words = 1
	}

	return &doorkeeper{
		bits: make([]uint64, words),
		mask: size - 1,
	}
}

// bitIndexes derives two bit positions from a single 64-bit hash via
// double hashing, the same trick used by FrequencySketch for its counters.
func (d *doorkeeper) bitIndexes(h uint64) (uint32, uint32) {
	h1 := uint32(h)
	h2 := uint32(h >> 32)
	return h1 & d.mask, (h1 + h2) & d.mask
}

func (d *doorkeeper) getBit(bitIdx uint32) bool {
	word := atomic.LoadUint64(&d.bits[bitIdx/64])
	return word&(uint64(1)<<(bitIdx%64)) != 0
}

func (d *doorkeeper) setBit(bitIdx uint32) {
	wordIdx := bitIdx / 64
	bit := uint64(1) << (bitIdx % 64)

	for {
		old := atomic.LoadUint64(&d.bits[wordIdx])
		if old&bit != 0 {
			return
		}
		if atomic.CompareAndSwapUint64(&d.bits[wordIdx], old, old|bit) {
			return
		}
	}
}

// addIfNotContains reports whether h was already present in the filter,
// then ensures it is present going forward. There is a benign race between
// the check and the set under concurrent callers (the same key could be
// reported as "new" by two goroutines at once), but that only ever causes
// one extra Count-Min Sketch increment, never a correctness problem - the
// same lossy trade-off already accepted elsewhere in this cache.
func (d *doorkeeper) addIfNotContains(h uint64) bool {
	i1, i2 := d.bitIndexes(h)
	alreadySeen := d.getBit(i1) && d.getBit(i2)
	d.setBit(i1)
	d.setBit(i2)
	return alreadySeen
}

func (d *doorkeeper) reset() {
	for i := range d.bits {
		atomic.StoreUint64(&d.bits[i], 0)
	}
}
