package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/Indy1131/tinylfu-store/pkg/cache"
)

// This benchmark is the direct evidence for the "preventing cache
// pollution" claim: a small set of hot keys is accessed repeatedly while,
// concurrently, a flood of one-off "cold" keys is inserted. A plain LRU
// evicts purely by recency, so a big enough flood pushes hot keys out
// even though they're the most valuable entries in the cache. TinyLFU's
// admission check rejects most of the flood outright once it recognizes
// the incoming keys have lower estimated frequency than the current LRU
// victim, so hot keys survive.
const (
	hotKeyCount  = 500
	hotAccessOps = 200_000
)

var floodMultiples = []float64{1, 5, 10, 20}

type pollutionRow struct {
	multiple        float64
	floodOps        uint64
	lruSurvival     float64
	tinyLFUSurvival float64
	improvement     float64
}

func runPollutionResistance(resultsDir string) {
	capacity := uint64(cache.New().Capacity())

	fmt.Println("=== Cache Pollution Resistance: Hot-Key Survival Under a Flood ===")
	fmt.Printf("%d hot keys, %d hot accesses, cache capacity=%d\n", hotKeyCount, hotAccessOps, capacity)
	fmt.Printf("%-14s %-12s %-12s %-12s\n", "Flood/Cap.", "LRU-only", "TinyLFU", "Improvement")

	rows := make([]pollutionRow, 0, len(floodMultiples))
	for _, m := range floodMultiples {
		floodOps := uint64(float64(capacity) * m)

		lruSurvival := simulatePollution(cache.New(cache.WithAdmission(false)), floodOps)
		tinyLFUSurvival := simulatePollution(cache.New(cache.WithAdmission(true)), floodOps)

		// Percentage-point difference (see hitratio.go for why not a
		// relative percentage) - this is what stays meaningful even when
		// the LRU-only baseline collapses to 0% survival.
		improvement := (tinyLFUSurvival - lruSurvival) * 100

		rows = append(rows, pollutionRow{
			multiple:        m,
			floodOps:        floodOps,
			lruSurvival:     lruSurvival,
			tinyLFUSurvival: tinyLFUSurvival,
			improvement:     improvement,
		})

		fmt.Printf("%-14s %-12s %-12s %+.1fpp\n",
			fmt.Sprintf("%.0fx", m),
			formatPct(lruSurvival),
			formatPct(tinyLFUSurvival),
			improvement,
		)
	}

	if err := writePollutionCSV(resultsDir+"/pollution_resistance.csv", rows); err != nil {
		fmt.Fprintln(os.Stderr, "failed to write pollution_resistance.csv:", err)
	}
}

// simulatePollution returns the fraction of hot keys still present in c
// after a concurrent hot-access workload races against a flood of unique,
// never-repeated keys.
func simulatePollution(c *cache.MemoryCache, floodOps uint64) float64 {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < hotAccessOps; i++ {
			key := "hot-" + strconv.Itoa(i%hotKeyCount)
			c.Set(key, "val")
			c.Get(key)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(0); i < floodOps; i++ {
			c.Set("flood-"+strconv.FormatUint(i, 10), "trash")
		}
	}()

	wg.Wait()

	survived := 0
	for i := 0; i < hotKeyCount; i++ {
		if _, ok := c.Get("hot-" + strconv.Itoa(i)); ok {
			survived++
		}
	}
	return float64(survived) / float64(hotKeyCount)
}

func writePollutionCSV(path string, rows []pollutionRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{"flood_ratio", "flood_ops", "lru_only_survival", "tinylfu_survival", "improvement_pp"}); err != nil {
		return err
	}
	for _, r := range rows {
		record := []string{
			strconv.FormatFloat(r.multiple, 'f', 0, 64),
			strconv.FormatUint(r.floodOps, 10),
			strconv.FormatFloat(r.lruSurvival, 'f', 4, 64),
			strconv.FormatFloat(r.tinyLFUSurvival, 'f', 4, 64),
			strconv.FormatFloat(r.improvement, 'f', 2, 64),
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	return w.Error()
}
