// Command bench is the evidence behind the numbers this project claims:
// it measures cache hit ratio (TinyLFU admission vs. plain LRU) under a
// realistic skewed workload, and measures request latency/throughput
// under concurrent load. Results are printed to stdout and written as CSV
// under results/ so they can be committed and referenced from the README.
package main

import (
	"fmt"
	"os"
)

func main() {
	resultsDir := "results"
	if err := os.MkdirAll(resultsDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "failed to create results directory:", err)
		os.Exit(1)
	}

	runHitRatioComparison(resultsDir)
	fmt.Println()
	runPollutionResistance(resultsDir)
	fmt.Println()
	runLatencyThroughput(resultsDir)
}
