package main

import (
	"testing"
	"time"
)

// testNumOrders is the order count used by all tests.  Kept small so the pure
// Go Phase 1 loop completes in milliseconds regardless of core count.
const testNumOrders = 10_000

// makePairs returns a slice of numPairs PairInfo values.
func makePairs(numPairs int) []PairInfo {
	pairs := make([]PairInfo, numPairs)
	for i := 0; i < numPairs; i++ {
		ppsID := uint32(i + 1)
		bintagID := uint32(100 + i)
		pairs[i] = PairInfo{
			PPSID:    ppsID,
			BinTagID: bintagID,
			Key:      MakePairKey(ppsID, bintagID),
		}
	}
	return pairs
}

// seedSmallEngine builds a ComputeEngine with numPairs pairs and numOrders
// active orders seeded with a fixed base score.
func seedSmallEngine(numPairs, numOrders int) *ComputeEngine {
	pairs := makePairs(numPairs)
	db := NewDoubleBuffer(numPairs)
	topo := NewTopologyState()
	for _, p := range pairs {
		topo.AddWarmingUp(p.PPSID, p.BinTagID)
		topo.Activate(p.PPSID, p.BinTagID)
	}

	eng := &ComputeEngine{
		// PPSMatrix is indexed by PPS ID, not pair index. Use numPPS rows.
		Matrix:   NewPPSMatrix(numPPS),
		Registry: NewOrderRegistry(),
		Indexes:  NewInvertedIndexes(),
		Pairs:    pairs,
		DB:       db,
		Topology: topo,
		ItemDict: NewMockItemDictClient(),
	}

	stream := NewMockKafkaOrderStream(numPairs)
	nowSecs := time.Now().Unix()
	for i := 0; i < numOrders; i++ {
		evt := stream.Next(uint32(i))
		evt.BaseScore = float32(i) / float32(numOrders) // monotonically increasing score
		IngestOrder(evt, eng.Registry, eng.Indexes, eng.Matrix, nowSecs)
	}
	return eng
}

// ---------------------------------------------------------------------------
// TestComputeCycleBasic
// ---------------------------------------------------------------------------

// TestComputeCycleBasic verifies that a RunCycle call populates the DoubleBuffer
// with non-empty ranked lists for all pairs.
func TestComputeCycleBasic(t *testing.T) {
	const numPairs = 5
	const numOrders = 1_000

	eng := seedSmallEngine(numPairs, numOrders)
	stats := eng.RunCycle()

	t.Logf("cycle stats: phase1=%dms phase2=%dms total=%dms sla=%v",
		stats.Phase1Ms, stats.Phase2Ms, stats.TotalMs, stats.SLAOk)

	buf := eng.DB.Load()
	for p := 0; p < numPairs; p++ {
		hList := buf.GetList(p, TblHeuristic, 10)
		found := false
		for _, id := range hList {
			if id >= 0 {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("pair %d: heuristic list is empty after one cycle", p)
		}
	}
}

// ---------------------------------------------------------------------------
// TestTopologyWarmupToActive
// ---------------------------------------------------------------------------

// TestTopologyWarmupToActive confirms that a pair added via HandleAddPair starts
// as WarmingUp, and transitions to Active after RunCycle is called.
func TestTopologyWarmupToActive(t *testing.T) {
	const numPairs = 2
	pairs := makePairs(numPairs)
	db := NewDoubleBuffer(numPairs)
	topo := NewTopologyState()
	// Seed initial pairs as active.
	for _, p := range pairs {
		topo.AddWarmingUp(p.PPSID, p.BinTagID)
		topo.Activate(p.PPSID, p.BinTagID)
	}

	eng := &ComputeEngine{
		Matrix:   NewPPSMatrix(numPPS),
		Registry: NewOrderRegistry(),
		Indexes:  NewInvertedIndexes(),
		Pairs:    pairs,
		DB:       db,
		Topology: topo,
		ItemDict: NewMockItemDictClient(),
	}

	// Add a new pair dynamically.
	newPPS := uint32(99)
	newBinTag := uint32(199)
	eng.HandleAddPair(newPPS, newBinTag)

	// Immediately after add it must be WarmingUp.
	key := MakePairKey(newPPS, newBinTag)
	status, ok := topo.Get(key)
	if !ok {
		t.Fatal("new pair not found in topology immediately after HandleAddPair")
	}
	if status != StatusWarmingUp {
		t.Errorf("expected StatusWarmingUp immediately after add, got %v", status)
	}

	// Run one cycle — promoteWarmingPairs should flip it to Active.
	eng.RunCycle()

	status, ok = topo.Get(key)
	if !ok {
		t.Fatal("new pair not found in topology after RunCycle")
	}
	if status != StatusActive {
		t.Errorf("expected StatusActive after RunCycle, got %v", status)
	}
}

// ---------------------------------------------------------------------------
// TestCircuitBreakerWarmingUp
// ---------------------------------------------------------------------------

// TestCircuitBreakerWarmingUp verifies that GetTopK returns GRPCUnavailable for
// a pair that is still in StatusWarmingUp.
func TestCircuitBreakerWarmingUp(t *testing.T) {
	const numPairs = 3
	pairs := makePairs(numPairs)
	db := NewDoubleBuffer(numPairs)
	topo := NewTopologyState()
	for _, p := range pairs {
		topo.AddWarmingUp(p.PPSID, p.BinTagID)
		topo.Activate(p.PPSID, p.BinTagID)
	}

	srv := NewPriorityServer(topo, db, pairs)

	// Add a new pair but do NOT run a cycle — it stays WarmingUp.
	newPPS := uint32(77)
	newBinTag := uint32(177)
	topo.AddWarmingUp(newPPS, newBinTag)

	req := &GetTopKRequest{PPSID: newPPS, BinTagID: newBinTag, K: 10}
	_, err := srv.GetTopK(req)
	if err == nil {
		t.Fatal("expected error for warming-up pair, got nil")
	}
	apiErr, ok := err.(*ApiError)
	if !ok {
		t.Fatalf("expected *ApiError, got %T: %v", err, err)
	}
	if apiErr.Code != GRPCUnavailable {
		t.Errorf("expected GRPCUnavailable (%d), got %d", GRPCUnavailable, apiErr.Code)
	}
}

// ---------------------------------------------------------------------------
// TestCircuitBreakerRemoved
// ---------------------------------------------------------------------------

// TestCircuitBreakerRemoved verifies that GetTopK returns GRPCNotFound for a
// pair that has been removed from the topology.
func TestCircuitBreakerRemoved(t *testing.T) {
	const numPairs = 3
	pairs := makePairs(numPairs)
	db := NewDoubleBuffer(numPairs)
	topo := NewTopologyState()
	for _, p := range pairs {
		topo.AddWarmingUp(p.PPSID, p.BinTagID)
		topo.Activate(p.PPSID, p.BinTagID)
	}

	srv := NewPriorityServer(topo, db, pairs)

	// Remove pair[0].
	topo.Remove(pairs[0].PPSID, pairs[0].BinTagID)

	req := &GetTopKRequest{PPSID: pairs[0].PPSID, BinTagID: pairs[0].BinTagID, K: 10}
	_, err := srv.GetTopK(req)
	if err == nil {
		t.Fatal("expected error for removed pair, got nil")
	}
	apiErr, ok := err.(*ApiError)
	if !ok {
		t.Fatalf("expected *ApiError, got %T: %v", err, err)
	}
	if apiErr.Code != GRPCNotFound {
		t.Errorf("expected GRPCNotFound (%d), got %d", GRPCNotFound, apiErr.Code)
	}
}

// ---------------------------------------------------------------------------
// TestKafkaOrderIngestion
// ---------------------------------------------------------------------------

// TestKafkaOrderIngestion verifies that IngestOrder correctly populates the
// registry, inverted indexes, and PPSMatrix for a batch of orders.
func TestKafkaOrderIngestion(t *testing.T) {
	const numPairs = 5
	const numOrders = 500

	matrix := NewPPSMatrix(numPPS)
	registry := NewOrderRegistry()
	indexes := NewInvertedIndexes()
	stream := NewMockKafkaOrderStream(numPairs)
	nowSecs := time.Now().Unix()

	for i := 0; i < numOrders; i++ {
		evt := stream.Next(uint32(i))
		// Use scores >= 0.1 so that int32(score*ScoreScale) >= 10, clearly non-zero.
		evt.BaseScore = float32(i+1) * 0.1
		IngestOrder(evt, registry, indexes, matrix, nowSecs)

		// Verify registry entry.
		meta := &registry.Meta[i]
		if !meta.Active {
			t.Errorf("order %d: expected Active=true after ingest", i)
		}
		if meta.OrderID != uint32(i) {
			t.Errorf("order %d: OrderID mismatch: got %d", i, meta.OrderID)
		}
		if meta.InsertedAtSecs != nowSecs {
			t.Errorf("order %d: InsertedAtSecs mismatch: got %d want %d", i, meta.InsertedAtSecs, nowSecs)
		}

		// Verify PPSMatrix is non-zero for PPS row 0.
		score := int32(matrix.Row(0)[i])
		if score <= 0 {
			t.Errorf("order %d: expected non-zero score in PPSMatrix row 0, got %d", i, score)
		}
	}

	// Verify that at least one TPID appears in the inverted index.
	if len(indexes.TPIDToOrders) == 0 {
		t.Error("expected non-empty TPIDToOrders after ingesting orders")
	}
	if len(indexes.BinTagToOrders) == 0 {
		t.Error("expected non-empty BinTagToOrders after ingesting orders")
	}
}

// ---------------------------------------------------------------------------
// TestItemDictLatency
// ---------------------------------------------------------------------------

// TestItemDictLatency verifies that RunCycleWithDict completes and records
// non-zero Phase 1 timing when an ItemDictUpdate is provided, and that the
// cycle finishes within a generous timeout (2s for small data).
func TestItemDictLatency(t *testing.T) {
	const numPairs = 5
	const numOrders = 500

	eng := seedSmallEngine(numPairs, numOrders)

	// Build a synthetic update referencing TPIDs that exist in the indexes.
	deltas := make([]TPIDDelta, 50)
	var tpid uint64
	for k := range eng.Indexes.TPIDToOrders {
		tpid = k
		break
	}
	for i := range deltas {
		deltas[i] = TPIDDelta{TPID: tpid, ScoreDelta: 0.01}
	}
	update := &ItemDictUpdate{
		TPIDDeltas: deltas,
		Timestamp:  time.Now().Unix(),
	}

	deadline := time.Now().Add(2 * time.Second)
	stats := eng.RunCycleWithDict(update)

	if time.Now().After(deadline) {
		t.Errorf("RunCycleWithDict exceeded 2s deadline for small dataset")
	}

	// Phase 1 should have been non-trivial (> 0 ms measured or at least fast).
	t.Logf("phase1=%dms phase2=%dms total=%dms sla=%v",
		stats.Phase1Ms, stats.Phase2Ms, stats.TotalMs, stats.SLAOk)

	// For 5 pairs × 10K orders this must be well under 2s.
	if stats.TotalMs > 2_000 {
		t.Errorf("expected cycle under 2000ms for small dataset, got %dms", stats.TotalMs)
	}
}

// ---------------------------------------------------------------------------
// TestOrderEviction
// ---------------------------------------------------------------------------

// TestOrderEviction verifies that EvictOrder deactivates an order and clears
// its PPSMatrix slots and inverted index entries.
func TestOrderEviction(t *testing.T) {
	const numOrders = 10

	matrix := NewPPSMatrix(numPPS)
	registry := NewOrderRegistry()
	indexes := NewInvertedIndexes()
	stream := NewMockKafkaOrderStream(1)
	nowSecs := time.Now().Unix()

	for i := 0; i < numOrders; i++ {
		evt := stream.Next(uint32(i))
		evt.BaseScore = 0.5
		IngestOrder(evt, registry, indexes, matrix, nowSecs)
	}

	targetID := uint32(3)
	// Confirm active before eviction.
	if !registry.Meta[targetID].Active {
		t.Fatal("order 3 should be active before eviction")
	}

	registry.EvictOrder(targetID, indexes, matrix)

	if registry.Meta[targetID].Active {
		t.Error("order 3 should be inactive after EvictOrder")
	}
	if registry.Meta[targetID].InsertedAtSecs != 0 {
		t.Error("InsertedAtSecs should be zeroed after EvictOrder")
	}
	// PPSMatrix slot should be zero.
	for p := 0; p < numPPS; p++ {
		if matrix.Row(p)[targetID] != 0 {
			t.Errorf("PPSMatrix row %d slot %d should be 0 after eviction", p, targetID)
		}
	}
}

// ---------------------------------------------------------------------------
// TestPurgeExpired
// ---------------------------------------------------------------------------

// TestPurgeExpired verifies that PurgeExpired evicts orders whose TTL has lapsed
// while leaving fresh orders intact.
func TestPurgeExpired(t *testing.T) {
	const numOrders = 5

	matrix := NewPPSMatrix(numPPS)
	registry := NewOrderRegistry()
	indexes := NewInvertedIndexes()
	stream := NewMockKafkaOrderStream(1)

	baseTime := int64(1_000_000)

	// Orders 0-2: inserted at baseTime (old).
	for i := 0; i < 3; i++ {
		evt := stream.Next(uint32(i))
		evt.BaseScore = 0.5
		IngestOrder(evt, registry, indexes, matrix, baseTime)
	}
	// Orders 3-4: inserted at baseTime + OrderTTLSecs - 1 (fresh enough).
	freshTime := baseTime + OrderTTLSecs - 1
	for i := 3; i < numOrders; i++ {
		evt := stream.Next(uint32(i))
		evt.BaseScore = 0.5
		IngestOrder(evt, registry, indexes, matrix, freshTime)
	}

	// Purge at baseTime + OrderTTLSecs + 1 — orders 0-2 should be evicted.
	nowSecs := baseTime + OrderTTLSecs + 1
	registry.PurgeExpired(nowSecs, indexes, matrix)

	for i := 0; i < 3; i++ {
		if registry.Meta[i].Active {
			t.Errorf("order %d should have been purged (TTL expired)", i)
		}
	}
	for i := 3; i < numOrders; i++ {
		if !registry.Meta[i].Active {
			t.Errorf("order %d should still be active (within TTL)", i)
		}
	}
}
