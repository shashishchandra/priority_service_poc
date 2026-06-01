package main

import (
	"math/rand"
	"sort"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Phase 1 helpers
// ---------------------------------------------------------------------------

// clampInt16 clamps v to the int16 range [-32767, 32767].
func clampInt16(v int32) int16 {
	if v > 32767 {
		return 32767
	}
	if v < -32767 {
		return -32767
	}
	return int16(v)
}

// applyDeltaToRow adds delta[o] to every element of a single PPSMatrix row,
// clamping the result to int16 range. Called by each Phase 1 goroutine.
func applyDeltaToRow(row []int16, delta []int32) {
	for o, d := range delta {
		row[o] = clampInt16(int32(row[o]) + d)
	}
}

// ---------------------------------------------------------------------------
// Phase 2 helpers — Quickselect
// ---------------------------------------------------------------------------

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

func quickselectTopK(e []scoreEntry, k int) {
	if k <= 0 || len(e) <= k {
		return
	}
	lo, hi := 0, len(e)-1
	for lo < hi {
		p := partitionDesc(e, lo, hi)
		if p == k-1 {
			break
		} else if p < k-1 {
			lo = p + 1
		} else {
			hi = p - 1
		}
	}
	sort.Slice(e[:k], func(i, j int) bool {
		return e[i].score > e[j].score
	})
}

// ---------------------------------------------------------------------------
// ComputeEngine
// ---------------------------------------------------------------------------

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

// NewComputeEngine creates a ComputeEngine. numWorkers sets the Phase 2 goroutine
// pool size; pass cfg.NumWorkersVal from initFromEnv to wire the resource config
// through explicitly. A value ≤ 0 falls back to the NumWorkers package default.
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
		ItemDict:   NewMockItemDictClient(),
		numWorkers: numWorkers,
		scratch:    scratch,
	}
}

func (e *ComputeEngine) RunCycle() CycleStats {
	return e.RunCycleWithDict(nil)
}

func (e *ComputeEngine) RunCycleWithDict(update *ItemDictUpdate) CycleStats {
	start := time.Now()
	nowSecs := start.Unix()

	// Evict expired orders before computing new rankings.
	e.Registry.PurgeExpired(nowSecs, e.Indexes, e.Matrix)

	p1ms := e.phase1Update(update)
	buf, p2ms := e.phase2Rank(nowSecs)

	e.DB.Publish(buf)
	e.promoteWarmingPairs()

	total := time.Since(start).Milliseconds()
	return CycleStats{
		Phase1Ms: p1ms,
		Phase2Ms: p2ms,
		TotalMs:  total,
		SLAOk:    total < 5_000,
	}
}

func (e *ComputeEngine) HandleAddPair(ppsID, bintagID uint32) {
	e.Topology.AddWarmingUp(ppsID, bintagID)
	info := PairInfo{PPSID: ppsID, BinTagID: bintagID, Key: MakePairKey(ppsID, bintagID)}
	e.pendingWarmup = append(e.pendingWarmup, info)
}

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

func (e *ComputeEngine) phase1Update(update *ItemDictUpdate) int64 {
	if update == nil {
		return 0
	}
	t0 := time.Now()

	// Build int32 delta vector (accumulates multiple int16-scaled contributions).
	delta := make([]int32, NumOrders)
	for _, entry := range update.TPIDDeltas {
		orders := e.Indexes.TPIDToOrders[entry.TPID]
		for _, oid := range orders {
			delta[oid] += int32(entry.ScoreDelta * ScoreScale)
		}
	}

	// Parallel Phase 1: distribute PPS rows across e.numWorkers goroutines.
	// Each goroutine applies delta to its slice of rows independently — no
	// shared writes between workers since each owns a disjoint row range.
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
			for p := lo; p < hi; p++ {
				applyDeltaToRow(e.Matrix.Row(p), delta)
			}
		}(start, end)
	}
	wg.Wait()

	return time.Since(t0).Milliseconds()
}

// ensureScratch grows the scratch bank to at least numW entries.
// Called at the start of phase2Rank so test-constructed engines (which bypass
// NewComputeEngine) get scratch allocated on their first cycle.
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
// dst must be a TopK-length slice pre-filled with -1 (guaranteed by NewReadBuffer).
func quickselectAndCopy(dst []int32, scratch []scoreEntry) {
	k := min(len(scratch), TopK)
	if k == 0 {
		return
	}
	quickselectTopK(scratch, k) // partitions top-k to scratch[0:k] and sorts them
	// quickselectTopK only sorts when len(scratch) > k; sort the ≤k case too.
	if len(scratch) <= k {
		sort.Slice(scratch[:k], func(i, j int) bool {
			return scratch[i].score > scratch[j].score
		})
	}
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

	// Static partitioning: worker w owns pairs [lo, hi).
	// Mirrors Rust's par_chunks_exact_mut — no channel mutex, no work-stealing overhead.
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
				status, ok := e.Topology.Get(pair.Key)
				if ok && status == StatusRemoved {
					continue
				}

				// PPS IDs are 1-based in go-pure; convert to 0-based matrix index.
				ppsRowIdx := int(pair.PPSID) - 1
				if ppsRowIdx < 0 || ppsRowIdx >= e.Matrix.NumPPS {
					ppsRowIdx = int(pair.PPSID) % e.Matrix.NumPPS
				}
				ppsRow := e.Matrix.Row(ppsRowIdx)

				eligible := e.Indexes.BinTagToOrders[pair.BinTagID]
				if len(eligible) == 0 {
					// Warm-up fallback: build all-active snapshot.
					allActive := make([]uint32, 0, 1024)
					for oid := 0; oid < NumOrders; oid++ {
						if e.Registry.Meta[oid].Active {
							allActive = append(allActive, uint32(oid))
						}
					}
					eligible = allActive
				}

				// ── Single pass: fill all four scratch slices simultaneously ──
				sc.heuristic = sc.heuristic[:0]
				sc.large = sc.large[:0]
				sc.pbt = sc.pbt[:0]
				sc.critical = sc.critical[:0]

				for _, oid := range eligible {
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

				// ── Quickselect top-K and write to output buffer ───────────
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

func (e *ComputeEngine) promoteWarmingPairs() {
	remaining := e.pendingWarmup[:0]
	for _, info := range e.pendingWarmup {
		status, ok := e.Topology.Get(info.Key)
		if !ok || status == StatusWarmingUp {
			e.Topology.Activate(info.PPSID, info.BinTagID)
		} else {
			remaining = append(remaining, info)
		}
	}
	e.pendingWarmup = remaining
}

// ---------------------------------------------------------------------------
// Synthetic load helper (used by benchmarks)
// ---------------------------------------------------------------------------

func buildSyntheticDelta(n int) []int32 {
	delta := make([]int32, n)
	rng := rand.New(rand.NewSource(42))
	for i := range delta {
		v := (rng.Float32()*2 - 1) * ScoreScale
		delta[i] = int32(v)
	}
	return delta
}
