package main

import (
	"fmt"
	"time"
)

func main() {
	cfg := initFromEnv()
	applyRuntimeConfig(cfg) // GOMAXPROCS, GC percent, memory limit

	fmt.Println("=============================================================")
	fmt.Println("  Priority Service — Pure Go | PPSMatrix / int16 / OrderCache")
	fmt.Println("=============================================================")
	printResourceConfig(cfg)
	fmt.Printf("  Config: %d PPS × %d BinTags = %d pairs | %d orders | %d workers\n\n",
		numPPS, numBinTags, PairsPerPod, NumOrders, cfg.NumWorkersVal)
	fmt.Printf("  PPSMatrix: %d PPS × %d orders × 2B = %d MB\n\n",
		numPPS, NumOrders, numPPS*NumOrders*2/1_000_000)

	// ── Setup ─────────────────────────────────────────────────────────────────
	pairs := buildPairs(PairsPerPod)
	db := NewDoubleBuffer(PairsPerPod)
	topo := NewTopologyState()
	for _, p := range pairs {
		topo.AddWarmingUp(p.PPSID, p.BinTagID)
		topo.Activate(p.PPSID, p.BinTagID)
	}

	eng := NewComputeEngine(pairs, db, topo, cfg.NumWorkersVal)
	srv := NewPriorityServer(topo, db, pairs)

	// ── Seed orders ───────────────────────────────────────────────────────────
	fmt.Printf("Seeding %d orders into registry + PPSMatrix...\n", NumOrders)
	seedStart := time.Now()
	stream := NewMockKafkaOrderStream(PairsPerPod)
	nowSecs := time.Now().Unix()
	for i := 0; i < NumOrders; i++ {
		evt := stream.Next(uint32(i))
		IngestOrder(evt, eng.Registry, eng.Indexes, eng.Matrix, nowSecs)
	}
	fmt.Printf("  Seed complete in %s\n\n", time.Since(seedStart).Round(time.Millisecond))

	// ── Worst-case verification ───────────────────────────────────────────────
	{
		activeCount := 0
		for i := 0; i < NumOrders; i++ {
			if eng.Registry.Meta[i].Active {
				activeCount++
			}
		}
		btMin, btMax := NumOrders, 0
		for bt := uint32(1); bt <= numBinTags; bt++ {
			n := len(eng.Indexes.BinTagToOrders[bt])
			if n < btMin { btMin = n }
			if n > btMax { btMax = n }
		}
		fmt.Println("=== Worst-case verification ===")
		fmt.Printf("  Active orders in registry : %d\n", activeCount)
		fmt.Printf("  BinTag IDs in index       : %d\n", len(eng.Indexes.BinTagToOrders))
		fmt.Printf("  Eligible orders per BinTag: min=%d max=%d (expected %d)\n", btMin, btMax, NumOrders)
		fmt.Printf("  TPIDs in tpid_to_orders   : %d\n", len(eng.Indexes.TPIDToOrders))
		fmt.Printf("  Pairs to rank in Phase 2  : %d\n", len(pairs))
		fmt.Printf("  PPSMatrix size            : %d MB\n\n", numPPS*NumOrders*2/1_000_000)
	}

	// ── 3 compute cycles ──────────────────────────────────────────────────────
	for cycle := 1; cycle <= 3; cycle++ {
		fmt.Printf("--- Cycle %d ---\n", cycle)
		update := eng.ItemDict.FetchUpdate()
		fmt.Printf("  ItemDict update: %d TPID deltas\n", len(update.TPIDDeltas))

		stats := eng.RunCycleWithDict(update)

		slaStr := "PASS"
		if !stats.SLAOk {
			slaStr = "FAIL"
		}
		fmt.Printf("  Phase 1 (PPSMatrix update): %4d ms\n", stats.Phase1Ms)
		fmt.Printf("  Phase 2 (rank):             %4d ms\n", stats.Phase2Ms)
		fmt.Printf("  Total:                      %4d ms  SLA: %s\n", stats.TotalMs, slaStr)
		printMemStats()
		fmt.Println()
	}

	// ── Circuit breaker demo ──────────────────────────────────────────────────
	fmt.Println("--- Circuit Breaker Demo ---")

	newPPS := uint32(999)
	newBinTag := uint32(888)
	eng.HandleAddPair(newPPS, newBinTag)
	srv.RefreshPairIndex(append(pairs, PairInfo{
		PPSID:    newPPS,
		BinTagID: newBinTag,
		Key:      MakePairKey(newPPS, newBinTag),
	}))

	req := &GetTopKRequest{PPSID: newPPS, BinTagID: newBinTag, K: 10}
	_, err := srv.GetTopK(req)
	if err != nil {
		fmt.Printf("  GetTopK(warming-up): %v  [expected GRPCUnavailable]\n", err)
	}

	removedPair := pairs[0]
	eng.HandleRemovePair(removedPair.PPSID, removedPair.BinTagID)
	req2 := &GetTopKRequest{PPSID: removedPair.PPSID, BinTagID: removedPair.BinTagID, K: 10}
	_, err = srv.GetTopK(req2)
	if err != nil {
		fmt.Printf("  GetTopK(removed):    %v  [expected GRPCNotFound]\n", err)
	}

	activePair := pairs[1]
	req3 := &GetTopKRequest{PPSID: activePair.PPSID, BinTagID: activePair.BinTagID, K: 5}
	resp, err := srv.GetTopK(req3)
	if err != nil {
		fmt.Printf("  GetTopK(active):     error: %v\n", err)
	} else {
		nonNeg := 0
		for _, id := range resp.Heuristic {
			if id >= 0 {
				nonNeg++
			}
		}
		fmt.Printf("  GetTopK(active):     OK — %d non-empty heuristic slots\n", nonNeg)
	}

	// ── Order eviction demo ───────────────────────────────────────────────────
	fmt.Println("\n--- Order Eviction Demo ---")
	sampleOrderID := uint32(42)
	fmt.Printf("  Evicting order %d (Kafka order-removed event)...\n", sampleOrderID)
	eng.HandleOrderRemoved(sampleOrderID)
	fmt.Printf("  Order %d active after eviction: %v\n",
		sampleOrderID, eng.Registry.Meta[sampleOrderID].Active)

	fmt.Println()
	fmt.Println("=============================================================")
	fmt.Println("  Pure Go baseline complete.")
	fmt.Println("=============================================================")
}

func buildPairs(numPairs int) []PairInfo {
	pairs := make([]PairInfo, 0, numPairs)
	for pps := uint32(1); pps <= numPPS && len(pairs) < numPairs; pps++ {
		for bt := uint32(1); bt <= numBinTags && len(pairs) < numPairs; bt++ {
			pairs = append(pairs, PairInfo{
				PPSID:    pps,
				BinTagID: bt,
				Key:      MakePairKey(pps, bt),
			})
		}
	}
	return pairs
}
