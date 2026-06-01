package main

import (
	"sync/atomic"
)

const (
	NumOrders    = 1_000_000
	numPPS       = 200
	numBinTags   = 20
	PairsPerPod  = numPPS * numBinTags
	TopK         = 1_000
	NumTables    = 4
	TblHeuristic = 0
	TblLarge     = 1
	TblPBT       = 2
	TblCritical  = 3

	LargeQtyThreshold = float32(10.0)
	CriticalCutoff    = int64(1_800)

	// ScoreScale converts float32 scores to int16 storage values.
	// stored_i16 = int16(float32_score * ScoreScale), clamped to [-32767, 32767].
	ScoreScale = float32(100.0)

	// OrderTTLSecs: orders not updated within this duration are evicted.
	OrderTTLSecs = int64(3_600)
)

// NumWorkers is the goroutine pool size for Phase 2 ranking.
// Declared as var so initFromEnv can override it at startup via PS_NUM_WORKERS.
var NumWorkers = 30

// PairKey encodes (ppsID, bintagID) as a single uint64.
type PairKey uint64

func MakePairKey(ppsID, bintagID uint32) PairKey {
	return PairKey(uint64(ppsID)<<32 | uint64(bintagID))
}

// OrderMeta holds per-order metadata. InsertedAtSecs is used for TTL eviction.
type OrderMeta struct {
	OrderID        uint32
	RequiredQty    float32
	PBTDeadline    int64
	Active         bool
	InsertedAtSecs int64
}

// PPSMatrix is a flat [numPPS × NumOrders] int16 score matrix.
// Row ppsID starts at Data[ppsID*NumOrders].
// Score is per-PPS only; BinTag determines eligibility, not score.
type PPSMatrix struct {
	Data   []int16
	NumPPS int
}

// NewPPSMatrix allocates a zeroed PPSMatrix for numPPS PPS rows.
func NewPPSMatrix(numPPS int) *PPSMatrix {
	return &PPSMatrix{
		Data:   make([]int16, numPPS*NumOrders),
		NumPPS: numPPS,
	}
}

// Row returns the score slice for ppsID (length NumOrders).
func (m *PPSMatrix) Row(ppsID int) []int16 {
	start := ppsID * NumOrders
	return m.Data[start : start+NumOrders]
}

// EvictOrder zeroes the score slot for orderID across all PPS rows.
func (m *PPSMatrix) EvictOrder(orderID uint32) {
	if int(orderID) >= NumOrders {
		return
	}
	for p := 0; p < m.NumPPS; p++ {
		m.Data[p*NumOrders+int(orderID)] = 0
	}
}

// ReadBuffer holds [numPairs × NumTables × TopK] int32 order IDs. -1 = empty.
type ReadBuffer struct {
	Data     []int32
	NumPairs int
}

func NewReadBuffer(numPairs int) *ReadBuffer {
	data := make([]int32, numPairs*NumTables*TopK)
	for i := range data {
		data[i] = -1
	}
	return &ReadBuffer{Data: data, NumPairs: numPairs}
}

func (b *ReadBuffer) listOffset(pairIdx, table int) int {
	return (pairIdx*NumTables + table) * TopK
}

func (b *ReadBuffer) GetList(pairIdx, table, k int) []int32 {
	off := b.listOffset(pairIdx, table)
	if k > TopK {
		k = TopK
	}
	return b.Data[off : off+k]
}

func (b *ReadBuffer) ListMut(pairIdx, table int) []int32 {
	off := b.listOffset(pairIdx, table)
	return b.Data[off : off+TopK]
}

// DoubleBuffer provides lock-free reads via atomic.Pointer.
type DoubleBuffer struct {
	ptr atomic.Pointer[ReadBuffer]
}

func NewDoubleBuffer(numPairs int) *DoubleBuffer {
	db := &DoubleBuffer{}
	db.ptr.Store(NewReadBuffer(numPairs))
	return db
}

func (db *DoubleBuffer) Load() *ReadBuffer       { return db.ptr.Load() }
func (db *DoubleBuffer) Publish(buf *ReadBuffer) { db.ptr.Store(buf) }

// PairInfo identifies one (PPS, BinTag) pair.
type PairInfo struct {
	PPSID    uint32
	BinTagID uint32
	Key      PairKey
}

// CycleStats records timing for one compute cycle.
type CycleStats struct {
	Phase1Ms int64
	Phase2Ms int64
	TotalMs  int64
	SLAOk    bool
}

// scoreEntry is scratch storage for Quickselect ranking. score is int32
// to hold both i16 upcast (heuristic/large) and deadline comparison (pbt/critical).
type scoreEntry struct {
	score   int32
	orderID int32
}

// workerScratch holds four pre-allocated scratch slices for one Phase 2 goroutine.
// Each goroutine owns its own workerScratch — no pool, no mutex, no GC pressure.
type workerScratch struct {
	heuristic []scoreEntry
	large     []scoreEntry
	pbt       []scoreEntry
	critical  []scoreEntry
}
