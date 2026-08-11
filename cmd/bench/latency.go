package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Indy1131/tinylfu-store/pkg/cache"
)

const (
	warmKeys        = 20_000
	opsPerGoroutine = 50_000
	setEveryNOps    = 10 // 1 Set per 10 ops -> a 90/10 read/write mix
)

type latencyRow struct {
	goroutines    int
	p50, p95, p99 time.Duration
	opsPerSec     float64
}

func runLatencyThroughput(resultsDir string) {
	concurrencyLevels := []int{1, runtime.NumCPU(), runtime.NumCPU() * 4}

	fmt.Println("=== Latency & Throughput (TinyLFU cache, 90% Get / 10% Set) ===")
	fmt.Printf("GOMAXPROCS/NumCPU=%d\n", runtime.NumCPU())
	fmt.Printf("%-12s %-10s %-10s %-10s %-14s\n", "Goroutines", "p50", "p95", "p99", "ops/sec")

	rows := make([]latencyRow, 0, len(concurrencyLevels))
	for _, g := range concurrencyLevels {
		row := runLatencyTrial(g)
		rows = append(rows, row)
		fmt.Printf("%-12d %-10s %-10s %-10s %-14.0f\n", g, row.p50, row.p95, row.p99, row.opsPerSec)
	}

	if err := writeLatencyCSV(resultsDir+"/latency.csv", rows); err != nil {
		fmt.Fprintln(os.Stderr, "failed to write latency.csv:", err)
	}
}

func runLatencyTrial(goroutines int) latencyRow {
	c := cache.New()
	for i := 0; i < warmKeys; i++ {
		c.Set(strconv.Itoa(i), i)
	}

	totalOps := goroutines * opsPerGoroutine
	latencies := make([]time.Duration, totalOps)
	var next int64

	var wg sync.WaitGroup
	start := time.Now()
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < opsPerGoroutine; i++ {
				key := strconv.Itoa(r.Intn(warmKeys))

				t0 := time.Now()
				if i%setEveryNOps == 0 {
					c.Set(key, i)
				} else {
					c.Get(key)
				}
				d := time.Since(t0)

				idx := atomic.AddInt64(&next, 1) - 1
				latencies[idx] = d
			}
		}(int64(g))
	}
	wg.Wait()
	elapsed := time.Since(start)

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	return latencyRow{
		goroutines: goroutines,
		p50:        percentile(latencies, 0.50),
		p95:        percentile(latencies, 0.95),
		p99:        percentile(latencies, 0.99),
		opsPerSec:  float64(totalOps) / elapsed.Seconds(),
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func writeLatencyCSV(path string, rows []latencyRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{"goroutines", "p50_ns", "p95_ns", "p99_ns", "ops_per_sec"}); err != nil {
		return err
	}
	for _, r := range rows {
		record := []string{
			strconv.Itoa(r.goroutines),
			strconv.FormatInt(r.p50.Nanoseconds(), 10),
			strconv.FormatInt(r.p95.Nanoseconds(), 10),
			strconv.FormatInt(r.p99.Nanoseconds(), 10),
			strconv.FormatFloat(r.opsPerSec, 'f', 0, 64),
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	return w.Error()
}
