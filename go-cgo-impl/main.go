package main

import (
	"fmt"
	"time"
)

func main() {
	cfg := initFromEnv()
	applyRuntimeConfig(cfg) // GOMAXPROCS, GC percent, memory limit

	fmt.Println("=== Priority Service (Go+CGo) | PPSMatrix / int16 / OrderCache ===")
	printResourceConfig(cfg)
	fmt.Printf("Pod config: %d PPS × %d BinTags = %d pairs\n", numPPS, numBinTagsPerPPS, numPPS*numBinTagsPerPPS)
	fmt.Printf("PPSMatrix: %d PPS × %d orders × 2B = %d MB\n\n",
		numPPS, NumOrders, numPPS*NumOrders*2/1_000_000)

	// ── Step 1: Build topology ───────────────────────────────────────────────
	topo := NewTopologyState()
	pairs := make([]PairInfo, 0, numPPS*numBinTagsPerPPS)
	for pps := uint32(0); pps < numPPS; pps++ {
		for bt := uint32(0); bt < numBinTagsPerPPS; bt++ {
			key := MakePairKey(pps, bt)
			topo.Activate(pps, bt)
			pairs = append(pairs, PairInfo{PPSID: pps, BinTagID: bt, Key: key})
		}
	}
	fmt.Printf("Topology: %d pairs activated\n", len(pairs))

	// ── Step 2: Create engine and ingest orders ──────────────────────────────
	db := NewDoubleBuffer(len(pairs))
	engine := NewComputeEngine(pairs, db, topo, cfg.NumWorkersVal)
	defer engine.Matrix.Free()

	stream := NewMockKafkaOrderStream(42, uint32(NumOrders), 100_000, numBinTagsPerPPS)
	fmt.Printf("Ingesting %d orders (all bintags eligible)... ", NumOrders)
	t0 := time.Now()
	nowSecs := t0.Unix()
	for i := uint32(0); i < uint32(NumOrders); i++ {
		evt := stream.Next(i)
		IngestOrder(evt, engine.Registry, engine.Indexes, engine.Matrix, nowSecs)
	}
	fmt.Printf("done in %v\n", time.Since(t0).Round(time.Millisecond))

	// ── Worst-case verification ───────────────────────────────────────────────
	{
		activeCount := 0
		for i := 0; i < NumOrders; i++ {
			if engine.Registry.Meta[i].Active {
				activeCount++
			}
		}
		btMin, btMax := NumOrders, 0
		for bt := uint32(0); bt < numBinTagsPerPPS; bt++ {
			n := len(engine.Indexes.BinTagToOrders[bt])
			if n < btMin {
				btMin = n
			}
			if n > btMax {
				btMax = n
			}
		}
		fmt.Println("\n=== Worst-case verification ===")
		fmt.Printf("  Active orders in registry : %d\n", activeCount)
		fmt.Printf("  BinTag IDs in index       : %d\n", len(engine.Indexes.BinTagToOrders))
		fmt.Printf("  Eligible orders per BinTag: min=%d max=%d (expected %d)\n", btMin, btMax, NumOrders)
		fmt.Printf("  TPIDs in tpid_to_orders   : %d\n", len(engine.Indexes.TPIDToOrders))
		fmt.Printf("  Pairs to rank in Phase 2  : %d\n", len(pairs))
		fmt.Printf("  PPSMatrix size            : %d MB\n\n", numPPS*NumOrders*2/1_000_000)
	}

	// ── Step 3: Run 3 compute cycles ─────────────────────────────────────────
	dictClient := NewMockItemDictClient(7, 100_000)
	fmt.Println("\n--- Compute cycles ---")
	for i := 1; i <= 3; i++ {
		fmt.Printf("Cycle %d: fetching ItemDict update... ", i)
		update := dictClient.Fetch()
		fmt.Printf("%d updates received\n", len(update.Updates))

		stats := engine.RunCycleWithDict(update)
		fmt.Printf("  Phase1: %d ms | Phase2: %d ms | Total: %d ms | SLA OK: %v\n",
			stats.Phase1Ms, stats.Phase2Ms, stats.TotalMs, stats.SLAOk)
		printMemStats()
	}

	// ── Step 4: GetTopK demo ─────────────────────────────────────────────────
	fmt.Println("\n--- GetTopK demo ---")
	srv := NewPriorityServer(topo, db, pairs)

	req := &GetTopKRequest{PPSID: 0, BinTagID: 0, K: 5}
	resp, err := srv.GetTopK(req)
	if err != nil {
		fmt.Printf("GetTopK(PPS=0, BinTag=0): ERROR %v\n", err)
	} else {
		fmt.Printf("GetTopK(PPS=0, BinTag=0, K=5):\n")
		fmt.Printf("  Heuristic top-5: %v\n", resp.Heuristic)
		fmt.Printf("  Large top-5:     %v\n", resp.Large)
		fmt.Printf("  PBT top-5:       %v\n", resp.PBT)
		fmt.Printf("  Critical top-5:  %v\n", resp.Critical)
	}

	// ── Step 5: Circuit breaker demo ─────────────────────────────────────────
	fmt.Println("\n--- Circuit breaker demo ---")

	newPPS := uint32(99)
	newBT := uint32(88)
	engine.HandleAddPair(newPPS, newBT)
	srv.RefreshPairIndex(engine.Pairs)

	status, _ := topo.Get(MakePairKey(newPPS, newBT))
	fmt.Printf("New pair (PPS=%d, BinTag=%d) status: %v (WarmingUp=%v)\n",
		newPPS, newBT, status, status == StatusWarmingUp)

	warmReq := &GetTopKRequest{PPSID: newPPS, BinTagID: newBT, K: 5}
	_, warmErr := srv.GetTopK(warmReq)
	if warmErr != nil {
		apiErr, ok := warmErr.(*ApiError)
		if ok {
			fmt.Printf("GetTopK(PPS=%d, BinTag=%d): ERROR code=%d msg=%q (expected GRPCUnavailable)\n",
				newPPS, newBT, apiErr.Code, apiErr.Msg)
		}
	}

	fmt.Println("\nRunning cycle to promote WarmingUp pair...")
	promoteCycle := engine.RunCycleWithDict(nil)
	fmt.Printf("  Total: %d ms | SLA OK: %v\n", promoteCycle.TotalMs, promoteCycle.SLAOk)

	srv.RefreshPairIndex(engine.Pairs)
	status2, _ := topo.Get(MakePairKey(newPPS, newBT))
	fmt.Printf("Pair (PPS=%d, BinTag=%d) status after cycle: %v (Active=%v)\n",
		newPPS, newBT, status2, status2 == StatusActive)

	resp2, err2 := srv.GetTopK(warmReq)
	if err2 != nil {
		fmt.Printf("GetTopK after promotion: ERROR %v\n", err2)
	} else {
		fmt.Printf("GetTopK after promotion: Heuristic top-5: %v\n", resp2.Heuristic)
	}

	// ── Step 6: Order eviction demo ──────────────────────────────────────────
	fmt.Println("\n--- Order eviction demo ---")
	sampleOrderID := uint32(42)
	fmt.Printf("Evicting order %d (Kafka order-removed event)...\n", sampleOrderID)
	engine.HandleOrderRemoved(sampleOrderID)
	fmt.Printf("Order %d active after eviction: %v\n",
		sampleOrderID, engine.Registry.Meta[sampleOrderID].Active)

	fmt.Println("\n=== Done ===")
}
