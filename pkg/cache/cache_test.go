package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemoryCache_Concurrency(t *testing.T) {
	c := New()
	var wg sync.WaitGroup

	numIterations := 1000
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numIterations; i++ {
			c.Set(fmt.Sprintf("key-%d", i), i)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numIterations; i++ {
			c.Get(fmt.Sprintf("key-%d", i))
		}
	}()

	wg.Wait()
}

func TestMemoryCache_RaceCondition(t *testing.T) {
	c := New()
	var wg sync.WaitGroup

	numWriters := 100
	numIterations := 1000

	sharedKey := "collision-key"

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerId int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				c.Set(sharedKey, fmt.Sprintf("value-from-%d", writerId))
			}
		}(i)
	}

	wg.Wait()
}

func TestLRU_Functional(t *testing.T) {
	c := New()
	shardSize := totalMaxEntries / numShards

	for i := 0; i < shardSize; i++ {
		c.Set(fmt.Sprintf("initial-%d", i), i)
	}

	c.Set("evictor", "boom")

	totalItems := 0
	for _, s := range c.shards {
		totalItems += s.ll.Len()
	}

	if totalItems > totalMaxEntries {
		t.Errorf("Cache exceeded totalMaxEntries: got %d, want %d", totalMaxEntries, totalItems)
	}
}

func TestLRU_Promotion(t *testing.T) {
	c := New()

	c.Set("hot-key", "stay-alive")

	for i := 0; i < 1000; i++ {
		c.Get("hot-key")
		c.Set(fmt.Sprintf("filler-%d", i), i)
	}

	if _, ok := c.Get("hot-key"); !ok {
		t.Error("Hot ket evicted despite frequent access")
	}
}

func TestHitRatio_DeadHotKey(t *testing.T) {
	c := New()
	c.sketch.sampleSize = 100

	shardLimit := 2
	for _, s := range c.shards {
		s.maxEntries = shardLimit
	}

	var deadHot, newHot, filler string
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%d", i)
		if c.getShardIndex(k) == 0 {
			if deadHot == "" {
				deadHot = k
			} else if newHot == "" {
				newHot = k
			} else {
				filler = k
				break
			}
		}
	}

	for i := 0; i < 255; i++ {
		c.sketch.Increment(deadHot)
		c.sketch.Increment(newHot)
	}

	c.Set(deadHot, "dead-hot")
	c.Set(filler, "filler-val")

	c.Set(newHot, "new-hot")

	for i := 0; i < 300; i++ {
		c.sketch.Increment("noise")
	}

	for i := 0; i < 40; i++ {
		c.sketch.Increment(newHot)
	}

	c.Set(newHot, "admitted")

	if _, ok := c.Get(deadHot); ok {
		t.Error("FAIL: Old hotkey was not evicted")
	}

	if _, ok := c.Get(newHot); !ok {
		t.Error("FAIL: New hotkey was not admitted")
	} else {
		fmt.Println("PASS: New hotkey was admitted and old hotkey was evicted")
	}
}

func TestMaintenanceBuffer_AsynUpdate(t *testing.T) {
	c := New()
	key := "test-key"

	if freq := c.sketch.Estimate(key); freq != 0 {
		t.Errorf("Expected initial frequency of 0, got %d", freq)
	}

	for i := 0; i < 10; i++ {
		c.Get(key)
	}

	time.Sleep(50 * time.Millisecond)

	if freq := c.sketch.Estimate(key); freq < 10 {
		t.Errorf("Expected frequency to be at least 10, got %d", freq)
	}
}

func TestMaintenanceBuffer_LossyFlood(t *testing.T) {
	c := New()

	floodSize := 10000
	for i := 0; i < floodSize; i++ {
		c.Get("overload")
	}

	fmt.Println("Handled flood without blocking")
}

func BenchmarkTest(b *testing.B) {
	c := New()
	for i := 0; i < b.N; i++ {
		c.Set("key", i)
	}
}

func BenchmarkSetParallel(b *testing.B) {
	c := New()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Set(fmt.Sprintf("key-%d", i), i)
			i++
		}
	})
}

func TestHitRatio_Battle(t *testing.T) {
	testSize := 1000
	shardLimit := testSize / numShards
	c := New()

	for _, s := range c.shards {
		s.maxEntries = shardLimit
	}

	var wg sync.WaitGroup
	hotKeyCount := 100

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20000; i++ {
			key := fmt.Sprintf("hot-%d", i%hotKeyCount)
			c.Set(key, "val")
			c.Get(key)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20000; i++ {
			c.Set(fmt.Sprintf("garbage-%d", i), "trash")
		}
	}()

	wg.Wait()

	hits := 0
	for i := 0; i < hotKeyCount; i++ {
		if _, ok := c.Get(fmt.Sprintf("hot-%d", i)); ok {
			hits++
		}
	}

	fmt.Printf("--- LRU Results ---\n")
	fmt.Printf("Hot Key Survival: %d/%d (%d%%)\n", hits, hotKeyCount, hits*100/hotKeyCount)
	fmt.Printf("-------------------\n")
}

func BenchmarkGetParallel(b *testing.B) {
	c := New()
	for i := 0; i < 10000; i++ {
		c.Set(fmt.Sprintf("key-%d", i), i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Get(fmt.Sprintf("key-%d", i%10000))
			i++
		}
	})
}

func BenchmarkMixedWorkLoad(b *testing.B) {
	c := New()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				c.Set(fmt.Sprintf("key-%d", i), i)
			} else {
				c.Get(fmt.Sprintf("key-%d", i))
			}
			i++
		}
	})
}

func BenchmarkSketchContention(b *testing.B) {
	c := New()

	hotKey := "popular"

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get(hotKey)
		}
	})
}
