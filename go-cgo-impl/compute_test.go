package main

import (
	"testing"
	"time"
)

// buildPairs constructs a slice of PairInfo for n PPS IDs × m BinTag IDs.
func buildPairs(numPPSArg, numBinTags int) []PairInfo {
	pairs := make([]PairInfo, 0, numPPSArg*numBinTags)
	for p := uint32(0); p < uint32(numPPSArg); p++ {
		for b := uint32(0); b < uint32(numBinTags); b++ {
			pairs = append(pairs, PairInfo{
				PPSID:    p,
				BinTagID: b,
				Key:      MakePairKey(p, b),
			})
		}
	}
	return pairs
}

// TestComputeCycleBasic creates an engine with 5 pairs, ingests 10 000 orders via
// MockKafkaOrderStream, runs 2 cycles with MockItemDictClient, and verifies that
// the top-K heuristic list is non-empty and sorted in descending score order.
func TestComputeCycleBasic(t *testing.T) {
	nPPS := 1
	numBinTags := 5
	pairs := buildPairs(nPPS, numBinTags)

	topo := NewTopologyState()
	for _, p := range pairs {
		topo.Activate(p.PPSID, p.BinTagID)
	}

	db := NewDoubleBuffer(len(pairs))
	engine := NewComputeEngine(pairs, db, topo, NumWorkers)
	defer engine.Matrix.Free()

	// Ingest 10 000 orders.
	stream := NewMockKafkaOrderStream(42, uint32(NumOrders), 5000, uint32(numBinTags))
	events := stream.GenerateBatch(10_000)
	nowSecs := time.Now().Unix()
	for _, evt := range events {
		IngestOrder(evt, engine.Registry, engine.Indexes, engine.Matrix, nowSecs)
	}

	// Run 2 cycles with a mock ItemDict client.
	dictClient := NewMockItemDictClient(7, 5000)
	for cycle := 0; cycle < 2; cycle++ {
		update := dictClient.Fetch()
		stats := engine.RunCycleWithDict(update)
		if stats.TotalMs > 30_000 {
			t.Errorf("cycle %d took %d ms — unexpectedly slow", cycle, stats.TotalMs)
		}
	}

	// Verify that pair 0 has a non-empty heuristic list in descending order.
	buf := db.Load()
	hList := buf.GetList(0, TblHeuristic, TopK)
	if len(hList) == 0 {
		t.Fatal("expected non-empty heuristic list for pair 0")
	}

	// Verify descending score order by cross-checking the int16 scores from the matrix.
	// Pair 0 has PPSID=0, so we read row 0.
	row := engine.Matrix.Row(0)
	for i := 1; i < len(hList); i++ {
		prev := hList[i-1]
		curr := hList[i]
		if prev < 0 || curr < 0 {
			break
		}
		if row[prev] < row[curr] {
			t.Errorf("heuristic list not descending at index %d: score[%d]=%d < score[%d]=%d",
				i, prev, row[prev], curr, row[curr])
		}
	}
}

// TestTopologyWarmupToActive verifies the full warm-up promotion lifecycle:
// a new pair starts as WarmingUp, and after RunCycleWithDict it becomes Active.
func TestTopologyWarmupToActive(t *testing.T) {
	// Start with one active pair.
	pairs := buildPairs(1, 1)
	topo := NewTopologyState()
	topo.Activate(0, 0)

	db := NewDoubleBuffer(len(pairs))
	engine := NewComputeEngine(pairs, db, topo, NumWorkers)
	defer engine.Matrix.Free()

	// Add a new pair — it starts as WarmingUp.
	newPPS := uint32(99)
	newBT := uint32(88)
	engine.HandleAddPair(newPPS, newBT)

	newKey := MakePairKey(newPPS, newBT)
	status, found := topo.Get(newKey)
	if !found {
		t.Fatal("pair not found in topology after HandleAddPair")
	}
	if status != StatusWarmingUp {
		t.Fatalf("expected StatusWarmingUp, got %d", status)
	}

	// Run a cycle (empty update) — this should promote the warming pair.
	engine.RunCycleWithDict(nil)

	status, found = topo.Get(newKey)
	if !found {
		t.Fatal("pair not found in topology after RunCycleWithDict")
	}
	if status != StatusActive {
		t.Fatalf("expected StatusActive after cycle, got %d", status)
	}
}

// TestCircuitBreakerWarmingUp adds a pair in WarmingUp state to a PriorityServer
// and verifies that GetTopK returns GRPCUnavailable (14).
func TestCircuitBreakerWarmingUp(t *testing.T) {
	topo := NewTopologyState()
	ppsID := uint32(5)
	btID := uint32(3)
	topo.AddWarmingUp(ppsID, btID)

	pairs := []PairInfo{{PPSID: ppsID, BinTagID: btID, Key: MakePairKey(ppsID, btID)}}
	db := NewDoubleBuffer(1)
	srv := NewPriorityServer(topo, db, pairs)

	req := &GetTopKRequest{PPSID: ppsID, BinTagID: btID, K: 10}
	_, err := srv.GetTopK(req)
	if err == nil {
		t.Fatal("expected error for WarmingUp pair, got nil")
	}
	apiErr, ok := err.(*ApiError)
	if !ok {
		t.Fatalf("expected *ApiError, got %T: %v", err, err)
	}
	if apiErr.Code != GRPCUnavailable {
		t.Fatalf("expected GRPCUnavailable (%d), got %d", GRPCUnavailable, apiErr.Code)
	}
}

// TestCircuitBreakerRemoved adds a pair in Removed state to a PriorityServer
// and verifies that GetTopK returns GRPCNotFound (5).
func TestCircuitBreakerRemoved(t *testing.T) {
	topo := NewTopologyState()
	ppsID := uint32(7)
	btID := uint32(2)
	// Add then remove so the key exists in the map with StatusRemoved.
	topo.Activate(ppsID, btID)
	topo.Remove(ppsID, btID)

	pairs := []PairInfo{{PPSID: ppsID, BinTagID: btID, Key: MakePairKey(ppsID, btID)}}
	db := NewDoubleBuffer(1)
	srv := NewPriorityServer(topo, db, pairs)

	req := &GetTopKRequest{PPSID: ppsID, BinTagID: btID, K: 10}
	_, err := srv.GetTopK(req)
	if err == nil {
		t.Fatal("expected error for Removed pair, got nil")
	}
	apiErr, ok := err.(*ApiError)
	if !ok {
		t.Fatalf("expected *ApiError, got %T: %v", err, err)
	}
	if apiErr.Code != GRPCNotFound {
		t.Fatalf("expected GRPCNotFound (%d), got %d", GRPCNotFound, apiErr.Code)
	}
}

// TestKafkaOrderIngestion uses MockKafkaOrderStream.GenerateBatch to ingest 500 orders
// and verifies that both inverted indexes are populated.
func TestKafkaOrderIngestion(t *testing.T) {
	registry := NewOrderRegistry()
	indexes := NewInvertedIndexes()

	numBinTags := uint32(20)
	stream := NewMockKafkaOrderStream(123, uint32(NumOrders), 10_000, numBinTags)
	events := stream.GenerateBatch(500)

	// Use a small PPSMatrix (1 PPS) to keep memory reasonable in tests.
	matrix := NewPPSMatrix(1)
	defer matrix.Free()

	nowSecs := time.Now().Unix()
	for _, evt := range events {
		IngestOrder(evt, registry, indexes, matrix, nowSecs)
	}

	if len(indexes.BinTagToOrders) == 0 {
		t.Fatal("expected BinTagToOrders to be populated after ingestion")
	}
	if len(indexes.TPIDToOrders) == 0 {
		t.Fatal("expected TPIDToOrders to be populated after ingestion")
	}

	// Spot-check: every active order should appear in at least one BinTag bucket.
	totalInBuckets := 0
	for _, ids := range indexes.BinTagToOrders {
		totalInBuckets += len(ids)
	}
	if totalInBuckets < 500 {
		// Each order has 1–4 BinTag entries, so total >= 500.
		t.Errorf("expected at least 500 total BinTag index entries, got %d", totalInBuckets)
	}
}

// TestItemDictLatency calls MockItemDictClient.Fetch() three times and verifies
// that each call takes between 90 ms and 260 ms (allowing a small scheduling buffer
// around the specified 100–200 ms range).
func TestItemDictLatency(t *testing.T) {
	client := NewMockItemDictClient(999, 5_000)

	for i := 0; i < 3; i++ {
		start := time.Now()
		update := client.Fetch()
		elapsed := time.Since(start)

		if update == nil {
			t.Fatalf("call %d: Fetch returned nil", i)
		}
		if elapsed < 90*time.Millisecond {
			t.Errorf("call %d: Fetch took %v — too fast (expected >= 90 ms)", i, elapsed)
		}
		if elapsed > 260*time.Millisecond {
			t.Errorf("call %d: Fetch took %v — too slow (expected <= 260 ms)", i, elapsed)
		}
	}
}

// TestOrderEviction verifies that evicting an order deactivates it and removes
// it from both inverted indexes.
func TestOrderEviction(t *testing.T) {
	registry := NewOrderRegistry()
	indexes := NewInvertedIndexes()
	matrix := NewPPSMatrix(1)
	defer matrix.Free()

	nowSecs := time.Now().Unix()
	evt := KafkaOrderEvent{
		OrderID:         100,
		RequiredQty:     5.0,
		PBTDeadline:     -1,
		TPIDs:           []uint64{1, 2},
		EligibleBinTags: []uint32{10, 11},
		BaseScore:       0.5,
	}
	IngestOrder(evt, registry, indexes, matrix, nowSecs)

	if !registry.Meta[100].Active {
		t.Fatal("order should be active after ingestion")
	}
	if len(indexes.BinTagToOrders[10]) == 0 {
		t.Fatal("order should be in BinTag 10 index after ingestion")
	}

	// Evict the order.
	registry.EvictOrder(100, indexes, matrix)

	if registry.Meta[100].Active {
		t.Fatal("order should be inactive after eviction")
	}
	if len(indexes.BinTagToOrders[10]) != 0 {
		t.Fatalf("BinTag 10 index should be empty after eviction, got %v", indexes.BinTagToOrders[10])
	}
	if len(indexes.TPIDToOrders[1]) != 0 {
		t.Fatalf("TPID 1 index should be empty after eviction, got %v", indexes.TPIDToOrders[1])
	}
}
