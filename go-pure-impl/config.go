package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
)

// ResourceConfig holds the tuneable runtime parameters for one pod.
// All fields are derived from workload constants + env-var overrides.
type ResourceConfig struct {
	NumWorkersVal int   // Phase 2 goroutine pool size
	GoMaxProcs    int   // OS threads (runtime.GOMAXPROCS)
	GCPercent     int   // GC trigger threshold (debug.SetGCPercent); -1 disables GC
	MemLimitMB    int64 // Hard RSS ceiling in MB (debug.SetMemoryLimit); 0 = no limit
}

// defaultResourceConfig derives sensible defaults from current workload constants.
//
//   - Workers: min(PairsPerPod, cores) — one goroutine per OS thread so each
//     goroutine runs uninterrupted on its own thread and its pre-allocated
//     workerScratch (~14 MB) stays warm in L2/L3. More goroutines than threads
//     causes Go's async preemption to swap scratch buffers mid-pair, trashing
//     the cache and regressing Phase 2 by 3-4×.
//   - GOMAXPROCS: all available logical CPUs.
//   - GCPercent: 100 (Go default).
//   - MemLimitMB: 0 (no soft ceiling by default; set PS_MEM_LIMIT_MB in prod).
func defaultResourceConfig() ResourceConfig {
	cores := runtime.NumCPU()
	workers := min(PairsPerPod, cores)
	if workers < 1 {
		workers = 1
	}
	return ResourceConfig{
		NumWorkersVal: workers,
		GoMaxProcs:    cores,
		GCPercent:     100,
		MemLimitMB:    0,
	}
}

// initFromEnv reads PS_* environment variables and returns the active config.
// It does NOT apply any side effects — call applyRuntimeConfig(cfg) immediately
// after to make the settings take effect. Env vars override defaults; invalid
// values are silently ignored (the default is kept).
//
// Supported variables:
//
//	PS_NUM_WORKERS    — Phase 2 goroutine pool size passed to NewComputeEngine
//	PS_GOMAXPROCS     — OS thread count applied by applyRuntimeConfig
//	PS_GCPERCENT      — GC trigger percentage; -1 disables GC
//	PS_MEM_LIMIT_MB   — Hard RSS limit in MB (positive int)
func initFromEnv() ResourceConfig {
	cfg := defaultResourceConfig()

	if v := os.Getenv("PS_NUM_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.NumWorkersVal = n
		}
	}
	if v := os.Getenv("PS_GOMAXPROCS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.GoMaxProcs = n
		}
	}
	if v := os.Getenv("PS_GCPERCENT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.GCPercent = n
		}
	}
	if v := os.Getenv("PS_MEM_LIMIT_MB"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			cfg.MemLimitMB = n
		}
	}

	return cfg
}

// applyRuntimeConfig applies process-wide Go runtime settings from cfg.
// Must be called once at process startup, before any goroutines are launched.
// cfg.NumWorkersVal is NOT applied here — pass it explicitly to NewComputeEngine.
func applyRuntimeConfig(cfg ResourceConfig) {
	runtime.GOMAXPROCS(cfg.GoMaxProcs)
	debug.SetGCPercent(cfg.GCPercent)
	if cfg.MemLimitMB > 0 {
		debug.SetMemoryLimit(cfg.MemLimitMB * 1024 * 1024)
	}
}

// printResourceConfig prints the active resource configuration and memory budget.
func printResourceConfig(cfg ResourceConfig) {
	ppsMatrixMB := int64(numPPS) * int64(NumOrders) * 2 / 1_000_000
	btagIdxMB := int64(numBinTags) * int64(NumOrders) * 4 / 1_000_000 // worst-case 4B per entry
	fmt.Println("Resource config:")
	fmt.Printf("  Workers        : %d  (logical CPUs: %d)\n", cfg.NumWorkersVal, runtime.NumCPU())
	fmt.Printf("  GOMAXPROCS     : %d\n", cfg.GoMaxProcs)
	fmt.Printf("  GC percent     : %d\n", cfg.GCPercent)
	if cfg.MemLimitMB > 0 {
		fmt.Printf("  Mem limit      : %d MB\n", cfg.MemLimitMB)
	} else {
		fmt.Printf("  Mem limit      : none  (set PS_MEM_LIMIT_MB to enable)\n")
	}
	fmt.Printf("  PPSMatrix est. : %d MB  (%d PPS × %d orders × 2 B)\n",
		ppsMatrixMB, numPPS, NumOrders)
	fmt.Printf("  BinTag idx est.: %d MB  (%d tags × %d orders × 4 B, worst-case)\n",
		btagIdxMB, numBinTags, NumOrders)
	fmt.Println()
}

// printMemStats prints current heap stats — call after each cycle to track pressure.
func printMemStats() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Printf("  [mem] heap in-use: %d MB  sys: %d MB  GC cycles: %d\n",
		ms.HeapInuse/1_000_000, ms.Sys/1_000_000, ms.NumGC)
}
