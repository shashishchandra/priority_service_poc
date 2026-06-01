package main

import (
	"sync/atomic"
)

const numPPS = 200
const numBinTagsPerPPS = 20

// NumOrders is the total number of orders the system tracks per pod.
const NumOrders = 1_000_000

// PairsPerPod is the number of (PPS, BinTag) pairs per pod.
const PairsPerPod = numPPS * numBinTagsPerPPS

// TopK is the number of top-scoring orders returned per pair per table.
const TopK = 1_000

// NumTables is the number of ranking tables maintained per pair.
const NumTables = 4

// Table index constants.
const (
	TblHeuristic = 0
	TblLarge     = 1
	TblPBT       = 2
	TblCritical  = 3
)

// LargeQtyThreshold is the minimum RequiredQty to appear in the Large table.
const LargeQtyThreshold = float32(10.0)

// CriticalCutoff is the deadline window in seconds below which an order is critical.
const CriticalCutoff = int64(1_800)

// NumWorkers is the goroutine pool size for Phase 2 ranking.
// Declared as var so initFromEnv can override it at startup via PS_NUM_WORKERS.
var NumWorkers = 30

// ScoreScale: stored_i16 = int16(float32_score * ScoreScale), clamped to [-32767, 32767].
const ScoreScale = float32(100.0)

// OrderTTLSecs: orders not updated within this duration are evicted from the cache.
const OrderTTLSecs = int64(3_600)

// PairKey is a compact 64-bit key encoding a (PPSID, BinTagID) pair.
// Upper 32 bits = PPSID, lower 32 bits = BinTagID.
type PairKey uint64

// MakePairKey encodes a (ppsID, bintagID) tuple into a single uint64 key.
func MakePairKey(ppsID, bintagID uint32) PairKey {
	return PairKey(uint64(ppsID)<<32 | uint64(bintagID))
}

// OrderMeta holds the flat per-order metadata stored in the registry.
// InsertedAtSecs enables TTL eviction.
type OrderMeta struct {
	OrderID        uint32
	RequiredQty    float32
	PBTDeadline    int64 // Unix seconds; -1 means no deadline
	Active         bool
	InsertedAtSecs int64
}

// ReadBuffer is a flat [numPairs × NumTables × TopK] int32 array.
// -1 represents an empty slot. Indexed as Data[(pairIdx*NumTables + table)*TopK + k].
type ReadBuffer struct {
	Data     []int32
	NumPairs int
}

// NewReadBuffer allocates a ReadBuffer filled with -1 (empty slots).
func NewReadBuffer(numPairs int) *ReadBuffer {
	size := numPairs * NumTables * TopK
	data := make([]int32, size)
	for i := range data {
		data[i] = -1
	}
	return &ReadBuffer{Data: data, NumPairs: numPairs}
}

func (b *ReadBuffer) listOffset(pairIdx, table int) int {
	return (pairIdx*NumTables + table) * TopK
}

// GetList returns up to k non-(-1) entries for the given pair index and table.
func (b *ReadBuffer) GetList(pairIdx, table, k int) []int32 {
	off := b.listOffset(pairIdx, table)
	if k > TopK {
		k = TopK
	}
	result := make([]int32, 0, k)
	for i := 0; i < k; i++ {
		v := b.Data[off+i]
		if v == -1 {
			break
		}
		result = append(result, v)
	}
	return result
}

// ListMut returns the mutable slice for the given pair index and table (length TopK).
func (b *ReadBuffer) ListMut(pairIdx, table int) []int32 {
	off := b.listOffset(pairIdx, table)
	return b.Data[off : off+TopK]
}

// DoubleBuffer provides lock-free reads and atomic pointer swap for the ReadBuffer.
type DoubleBuffer struct {
	ptr atomic.Pointer[ReadBuffer]
}

// NewDoubleBuffer creates a DoubleBuffer with an initial empty ReadBuffer.
func NewDoubleBuffer(numPairs int) *DoubleBuffer {
	db := &DoubleBuffer{}
	db.ptr.Store(NewReadBuffer(numPairs))
	return db
}

// Load returns the current ReadBuffer atomically.
func (db *DoubleBuffer) Load() *ReadBuffer { return db.ptr.Load() }

// Publish atomically replaces the current ReadBuffer with buf.
func (db *DoubleBuffer) Publish(buf *ReadBuffer) { db.ptr.Store(buf) }

// PairInfo describes one active (PPS, BinTag) pair in this pod.
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

// scoreEntry is scratch storage during Quickselect. int32 holds i16 upcasts and
// deadline comparisons without overflow.
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
