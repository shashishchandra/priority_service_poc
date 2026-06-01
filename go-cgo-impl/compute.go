package main

import (
	"sort"
	"sync"
	"time"
)

// ─── Quickselect helpers ────────────────────────────────────────────────────

// medianOfThree returns the index of the median of e[lo], e[mid], e[hi] by score.
func medianOfThree(e []scoreEntry, lo, mid, hi int) int {
	a, b, c := e[lo].score, e[mid].score, e[hi].score
	if (a >= b && b >= c) || (c >= b && b >= a) {
		return mid
	}
	if (b >= a && a >= c) || (c >= a && a >= b) {
		return lo
	}
	return hi
}

// partitionDesc partitions e[lo..hi] (inclusive) around a median-of-three pivot
// so that all entries left of the returned index have score >= pivot.score.
func partitionDesc(e []scoreEntry, lo, hi int) int {
	mid := lo + (hi-lo)/2
	pivotIdx := medianOfThree(e, lo, mid, hi)
	e[pivotIdx], e[hi] = e[hi], e[pivotIdx]
	pivot := e[hi].score

	i := lo - 1
	for j := lo; j < hi; j++ {
		if e[j].score >= pivot {
			i++
			e[i], e[j] = e[j], e[i]
		}
	}
	e[i+1], e[hi] = e[hi], e[i+1]
	return i + 1
}

// quickselectTopK rearranges e in-place so that e[0..k-1] are the k highest-scoring
// entries. Average-case O(N), worst-case O(N²) but median-of-three avoids pathological cases.
func quickselectTopK(e []scoreEntry, k int) {
	if k <= 0 || len(e) == 0 {
		return
	}
	if k >= len(e) {
		return
	}
	lo, hi := 0, len(e)-1
	for lo < hi {
		p := partitionDesc(e, lo, hi)
		if p == k {
			return
		} else if p < k {
			lo = p + 1
		} else {
			hi = p - 1
		}
	}
}

// ─── ComputeEngine ─────────────────────────────────────────────────────────

// ComputeEngine orchestrates the two-phase compute cycle for one pod.
//
// Phase 1: C-kernel SIMD score update over the full PPSMatrix (int16, per-PPS).
// Phase 2: Goroutine-pool Quickselect ranking per pair using the PPS row score
// and BinTag eligibility from the inverted index.
type ComputeEngine struct {
	Matrix        *PPSMatrix
	Registry      *OrderRegistry
	Indexes       *InvertedIndexes
	Pairs         []PairInfo
	DB            *DoubleBuffer
	Topology      *TopologyState
	ItemDict      *MockItemDictClient
	numWorkers    int // goroutine pool size for Phase 2; set from ResourceConfig.NumWorkersVal
	scratch       []workerScratch
	pendingWarmup []PairInfo
}

// NewComputeEngine creates a ComputeEngine and allocates the PPSMatrix on the C heap.
// numWorkers sets the Phase 2 goroutine pool size; pass cfg.NumWorkersVal from
// initFromEnv to wire the resource config through explicitly. A value ≤ 0 falls
// back to the NumWorkers package default.
func NewComputeEngine(pairs []PairInfo, db *DoubleBuffer, topo *TopologyState, numWorkers int) *ComputeEngine {
	nw := numWorkers
	if nw <= 0 {
		nw = NumWorkers
	}
	scratch := make([]workerScratch, nw)
	for w := range scratch {
		scratch[w] = workerScratch{
			heuristic: make([]scoreEntry, 0, NumOrders),
			large:     make([]scoreEntry, 0, NumOrders),
			pbt:       make([]scoreEntry, 0, NumOrders),
			critical:  make([]scoreEntry, 0, NumOrders),
		}
	}
	return &ComputeEngine{
		Matrix:     NewPPSMatrix(numPPS),
		Registry:   NewOrderRegistry(),
		Indexes:    NewInvertedIndexes(),
		Pairs:      pairs,
		DB:         db,
		Topology:   topo,
		numWorkers: numWorkers,
		scratch:    scratch,
	}
}

// RunCycle executes one compute cycle with no ItemDict update (scores unchanged).
func (e *ComputeEngine) RunCycle() CycleStats {
	return e.RunCycleWithDict(nil)
}

// RunCycleWithDict executes one compute cycle.
//
//  1. Evict TTL-expired orders.
//  2. Phase 1: if update != nil, build a dense int32 delta slice and call CUpdateAllScores.
//  3. Phase 2: goroutine-pool Quickselect ranking per pair.
//  4. Publish the new ReadBuffer atomically.
//  5. Promote any pending warming-up pairs to Active.
func (e *ComputeEngine) RunCycleWithDict(update *ItemDictUpdate) CycleStats {
	start := time.Now()
	nowSecs := start.Unix()

	// Evict TTL-expired orders before computing new rankings.
	e.Registry.PurgeExpired(nowSecs, e.Indexes, e.Matrix)

	p1Ms := e.phase1Update(update)
	buf, p2Ms := e.phase2Rank(nowSecs)

	e.DB.Publish(buf)
	e.promoteWarmingPairs()

	totalMs := time.Since(start).Milliseconds()
	return CycleStats{
		Phase1Ms: p1Ms,
		Phase2Ms: p2Ms,
		TotalMs:  totalMs,
		SLAOk:    totalMs < 5_000,
	}
}

// HandleAddPair registers a new (PPS, BinTag) pair as WarmingUp and records it
// in pendingWarmup so it will be promoted after the next cycle.
func (e *ComputeEngine) HandleAddPair(ppsID, bintagID uint32) {
	e.Topology.AddWarmingUp(ppsID, bintagID)
	info := PairInfo{PPSID: ppsID, BinTagID: bintagID, Key: MakePairKey(ppsID, bintagID)}
	e.pendingWarmup = append(e.pendingWarmup, info)
	for _, p := range e.Pairs {
		if p.Key == info.Key {
			return
		}
	}
	e.Pairs = append(e.Pairs, info)
}

// HandleRemovePair marks a (PPS, BinTag) pair as Removed and removes it from
// the active pairs slice so it is excluded from future ranking cycles.
func (e *ComputeEngine) HandleRemovePair(ppsID, bintagID uint32) {
	key := MakePairKey(ppsID, bintagID)
	e.Topology.Remove(ppsID, bintagID)
	filtered := e.Pairs[:0]
	for _, p := range e.Pairs {
		if p.Key != key {
			filtered = append(filtered, p)
		}
	}
	e.Pairs = filtered
}

// HandleOrderRemoved evicts an order on a Kafka order-removed event.
func (e *ComputeEngine) HandleOrderRemoved(orderID uint32) {
	e.Registry.EvictOrder(orderID, e.Indexes, e.Matrix)
}

// phase1Update builds a dense int32 delta slice from the ItemDictUpdate then fans
// the PPSMatrix update across e.numWorkers goroutines, each calling the C SIMD
// kernel for a disjoint row slice. Returns the time spent in milliseconds.
func (e *ComputeEngine) phase1Update(update *ItemDictUpdate) int64 {
	t0 := time.Now()
	if update == nil || len(update.Updates) == 0 {
		return time.Since(t0).Milliseconds()
	}

	// Build int32 delta vector to avoid overflow from multiple i16-scaled contributions.
	delta := make([]int32, NumOrders)
	for _, u := range update.Updates {
		diff := u.NewContrib - u.OldContrib
		if diff == 0 {
			continue
		}
		diffI32 := int32(diff * ScoreScale)
		orderIDs, ok := e.Indexes.TPIDToOrders[u.TPID]
		if !ok {
			continue
		}
		for _, oid := range orderIDs {
			if oid < NumOrders {
				delta[oid] += diffI32
			}
		}
	}

	// Parallel Phase 1: distribute PPS rows across e.numWorkers goroutines.
	// Each goroutine calls the C SIMD kernel (CUpdateRowRange) for its disjoint
	// row slice — no shared writes between workers.
	numW := e.numWorkers
	if numW <= 0 {
		numW = NumWorkers
	}
	if numW > e.Matrix.NumPPS {
		numW = e.Matrix.NumPPS
	}
	rowsPerWorker := (e.Matrix.NumPPS + numW - 1) / numW

	var wg sync.WaitGroup
	for start := 0; start < e.Matrix.NumPPS; start += rowsPerWorker {
		end := min(start+rowsPerWorker, e.Matrix.NumPPS)
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			CUpdateRowRange(e.Matrix, lo, hi-lo, delta)
		}(start, end)
	}
	wg.Wait()

	return time.Since(t0).Milliseconds()
}

// ensureScratch grows the scratch bank to at least numW entries.
func (e *ComputeEngine) ensureScratch(numW int) {
	for len(e.scratch) < numW {
		e.scratch = append(e.scratch, workerScratch{
			heuristic: make([]scoreEntry, 0, NumOrders),
			large:     make([]scoreEntry, 0, NumOrders),
			pbt:       make([]scoreEntry, 0, NumOrders),
			critical:  make([]scoreEntry, 0, NumOrders),
		})
	}
}

// quickselectAndCopy partitions scratch so the top-TopK highest-scoring entries
// are at the front, sorts them descending, then writes their orderIDs to dst.
func quickselectAndCopy(dst []int32, scratch []scoreEntry) {
	k := min(len(scratch), TopK)
	if k == 0 {
		return
	}
	quickselectTopK(scratch, k)
	sort.Slice(scratch[:k], func(i, j int) bool {
		return scratch[i].score > scratch[j].score
	})
	for i := 0; i < k; i++ {
		dst[i] = scratch[i].orderID
	}
}

// phase2Rank runs the Phase 2 parallel ranking and returns a freshly populated
// ReadBuffer plus the time spent in milliseconds.
//
// Algorithm: static work partitioning (no channel) — each goroutine owns a
// contiguous slice of pairs and a pre-allocated workerScratch bank entry.
// A single pass over eligible orders per pair fills all four scratch slices
// simultaneously, reducing OrderMeta cache misses 4× vs the old 4-pass approach.
func (e *ComputeEngine) phase2Rank(nowSecs int64) (*ReadBuffer, int64) {
	t0 := time.Now()
	numPairs := len(e.Pairs)
	buf := NewReadBuffer(numPairs)

	if numPairs == 0 {
		return buf, time.Since(t0).Milliseconds()
	}

	critDeadline := nowSecs + CriticalCutoff

	numW := e.numWorkers
	if numW <= 0 {
		numW = NumWorkers
	}
	if numW > numPairs {
		numW = numPairs
	}
	e.ensureScratch(numW)

	pairsPerWorker := (numPairs + numW - 1) / numW

	var wg sync.WaitGroup
	for w := 0; w < numW; w++ {
		lo := w * pairsPerWorker
		hi := min(lo+pairsPerWorker, numPairs)
		if lo >= numPairs {
			break
		}
		wg.Add(1)
		go func(workerID, start, end int) {
			defer wg.Done()
			sc := &e.scratch[workerID]

			for p := start; p < end; p++ {
				pair := e.Pairs[p]
				// PPS IDs are 0-based in go-cgo.
				ppsRow := e.Matrix.Row(int(pair.PPSID))
				eligible := e.Indexes.BinTagToOrders[pair.BinTagID]

				// ── Single pass: fill all four scratch slices simultaneously ──
				sc.heuristic = sc.heuristic[:0]
				sc.large = sc.large[:0]
				sc.pbt = sc.pbt[:0]
				sc.critical = sc.critical[:0]

				for _, oid := range eligible {
					if oid >= NumOrders {
						continue
					}
					meta := &e.Registry.Meta[oid]
					if !meta.Active {
						continue
					}
					score := int32(ppsRow[oid])
					sc.heuristic = append(sc.heuristic, scoreEntry{score, int32(oid)})
					if meta.RequiredQty >= LargeQtyThreshold {
						sc.large = append(sc.large, scoreEntry{score, int32(oid)})
					}
					if meta.PBTDeadline > 0 {
						dl := scoreEntry{int32(nowSecs - meta.PBTDeadline), int32(oid)}
						sc.pbt = append(sc.pbt, dl)
						if meta.PBTDeadline <= critDeadline {
							sc.critical = append(sc.critical, dl)
						}
					}
				}

				quickselectAndCopy(buf.ListMut(p, TblHeuristic), sc.heuristic)
				quickselectAndCopy(buf.ListMut(p, TblLarge), sc.large)
				quickselectAndCopy(buf.ListMut(p, TblPBT), sc.pbt)
				quickselectAndCopy(buf.ListMut(p, TblCritical), sc.critical)
			}
		}(w, lo, hi)
	}
	wg.Wait()

	return buf, time.Since(t0).Milliseconds()
}

// promoteWarmingPairs promotes all pairs that were warming up when the last cycle
// started to StatusActive, and clears the pendingWarmup list.
func (e *ComputeEngine) promoteWarmingPairs() {
	for _, p := range e.pendingWarmup {
		e.Topology.Activate(p.PPSID, p.BinTagID)
	}
	e.pendingWarmup = e.pendingWarmup[:0]
}
