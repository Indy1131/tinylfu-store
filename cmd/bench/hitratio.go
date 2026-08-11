package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"

	"github.com/Indy1131/tinylfu-store/pkg/cache"
)

// This benchmark models everyday, well-mixed traffic: a small set of keys
// dominates access frequency while a long tail is accessed rarely, with no
// single "flood" event. It is a useful baseline showing that, absent a
// pollution event, both policies perform similarly - see pollution.go for
// the workload that actually differentiates them.
//
// zipfSkew mirrors real-world key popularity distributions.
const zipfSkew = 1.07

const opsPerTrial = 500_000

// keyspaceRatios express oversubscription: how many times bigger the key
// space is than the cache's capacity. A ratio of 1 would make every key
// fit, trivially yielding a 100% hit ratio for both policies, so all
// ratios here are > 1 to put real pressure on the eviction policy.
var keyspaceRatios = []float64{2, 5, 10, 20}

type hitRatioRow struct {
	ratio       float64
	keyspace    uint64
	lruOnly     float64
	tinyLFU     float64
	improvement float64
}

func runHitRatioComparison(resultsDir string) {
	capacity := uint64(cache.New().Capacity())

	fmt.Println("=== Hit Ratio: TinyLFU Admission vs Plain LRU ===")
	fmt.Printf("Workload: %d ops, Zipfian skew=%.2f, cache capacity=%d\n", opsPerTrial, zipfSkew, capacity)
	fmt.Printf("%-16s %-12s %-12s %-12s\n", "Keyspace/Cap.", "LRU-only", "TinyLFU", "Improvement")

	rows := make([]hitRatioRow, 0, len(keyspaceRatios))
	for _, ratio := range keyspaceRatios {
		keyspace := uint64(float64(capacity) * ratio)

		lruOnly := simulateWorkload(cache.New(cache.WithAdmission(false)), keyspace)
		tinyLFU := simulateWorkload(cache.New(cache.WithAdmission(true)), keyspace)

		// Percentage-point difference, not a relative percentage: it stays
		// well-defined even when the LRU-only baseline hits 0% (see the
		// pollution-resistance benchmark), and is easier to read directly
		// off the hit-ratio columns.
		improvement := (tinyLFU - lruOnly) * 100

		rows = append(rows, hitRatioRow{
			ratio:       ratio,
			keyspace:    keyspace,
			lruOnly:     lruOnly,
			tinyLFU:     tinyLFU,
			improvement: improvement,
		})

		fmt.Printf("%-16s %-12s %-12s %+.1fpp\n",
			fmt.Sprintf("%.0fx", ratio),
			formatPct(lruOnly),
			formatPct(tinyLFU),
			improvement,
		)
	}

	if err := writeHitRatioCSV(resultsDir+"/hit_ratio.csv", rows); err != nil {
		fmt.Fprintln(os.Stderr, "failed to write hit_ratio.csv:", err)
	}
}

// simulateWorkload runs a read-through workload against c: on a miss, the
// key is populated (as a real cache-aside application would do) so the
// admission policy gets to decide whether it deserves to stay.
func simulateWorkload(c *cache.MemoryCache, keyspace uint64) float64 {
	src := rand.New(rand.NewSource(1))
	zipf := rand.NewZipf(src, zipfSkew, 1, keyspace-1)

	for i := 0; i < opsPerTrial; i++ {
		key := strconv.FormatUint(zipf.Uint64(), 10)
		if _, ok := c.Get(key); !ok {
			c.Set(key, i)
		}
	}

	return c.Stats().HitRatio()
}

func formatPct(f float64) string {
	return fmt.Sprintf("%.1f%%", f*100)
}

func writeHitRatioCSV(path string, rows []hitRatioRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{"keyspace_ratio", "keyspace_size", "lru_only_hit_ratio", "tinylfu_hit_ratio", "improvement_pp"}); err != nil {
		return err
	}
	for _, r := range rows {
		record := []string{
			strconv.FormatFloat(r.ratio, 'f', 0, 64),
			strconv.FormatUint(r.keyspace, 10),
			strconv.FormatFloat(r.lruOnly, 'f', 4, 64),
			strconv.FormatFloat(r.tinyLFU, 'f', 4, 64),
			strconv.FormatFloat(r.improvement, 'f', 2, 64),
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	return w.Error()
}
