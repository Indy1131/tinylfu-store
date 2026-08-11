package cache

import "testing"

func TestSketch_Doorkeeper_FirstTouchNotCounted(t *testing.T) {
	s := NewSketch(1000)
	h := s.Hash("once")

	s.Increment(h)
	if got := s.Estimate(h); got != 0 {
		t.Errorf("expected estimate 0 after a single touch (absorbed by the doorkeeper), got %d", got)
	}

	s.Increment(h)
	if got := s.Estimate(h); got != 1 {
		t.Errorf("expected estimate 1 after a second touch, got %d", got)
	}

	s.Increment(h)
	if got := s.Estimate(h); got != 2 {
		t.Errorf("expected estimate 2 after a third touch, got %d", got)
	}
}

func TestSketch_Doorkeeper_DistinctKeysIndependent(t *testing.T) {
	s := NewSketch(1000)
	a := s.Hash("a")
	b := s.Hash("b")

	s.Increment(a)

	if got := s.Estimate(b); got != 0 {
		t.Errorf("expected key 'b' to be unaffected by 'a's first touch, got %d", got)
	}
}

func TestSketch_Doorkeeper_ResetOnAging(t *testing.T) {
	s := NewSketch(4)
	h := s.Hash("hot")

	// First touch is absorbed by the doorkeeper, second starts counting.
	s.Increment(h)
	s.Increment(h)
	if got := s.Estimate(h); got != 1 {
		t.Errorf("expected estimate 1 before aging, got %d", got)
	}

	// Push totalCount past sampleSize (4) with unrelated keys to force a
	// reset, which should clear the doorkeeper as well as halve counters.
	s.Increment(s.Hash("noise-1"))
	s.Increment(s.Hash("noise-2"))
	s.Increment(s.Hash("noise-3"))

	// After the reset, "hot" must clear the doorkeeper again before its
	// next touch is counted in the sketch.
	before := s.Estimate(h)
	s.Increment(h)
	after := s.Estimate(h)
	if after > before {
		t.Errorf("expected the touch right after a reset to be absorbed by the doorkeeper (estimate unchanged), went from %d to %d", before, after)
	}
}
