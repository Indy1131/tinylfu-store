package cache

import (
	"fmt"
	"sync"
	"testing"
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
