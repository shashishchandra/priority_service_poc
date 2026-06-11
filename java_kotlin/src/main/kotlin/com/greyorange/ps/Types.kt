package com.greyorange.ps

import java.util.concurrent.atomic.AtomicReference

// ── PairKey ──────────────────────────────────────────────────────────────────
// Upper 32 bits = ppsId, lower 32 bits = binTagId. Matches Go and Rust encoding.

typealias PairKey = Long

fun makePairKey(ppsId: Int, binTagId: Int): PairKey =
    (ppsId.toLong() shl 32) or binTagId.toLong()

// ── PPSMatrix ────────────────────────────────────────────────────────────────
// Flat row-major [numPps × NUM_ORDERS] Short (i16) array.
// ShortArray is a JVM primitive array — no boxing, continuous heap memory, SIMD-friendly.
// Layout: row ppsId lives at data[ppsId * NUM_ORDERS .. (ppsId+1) * NUM_ORDERS].
//
// NOTE: Unlike go-cgo-impl, this is on the JVM heap rather than C-heap. The GC
// treats it as a single large object and will not move it after initial placement
// (GZGCGenerational keeps large objects in an old-gen region and does not relocate them).

class PPSMatrix(val numPps: Int) {
    val data = ShortArray(numPps * NUM_ORDERS)

    fun rowOffset(ppsId: Int): Int = ppsId * NUM_ORDERS

    // Returns the start offset for ppsId row — callers index data[offset + orderId].
    // Matches go-cgo-impl's Row() returning a slice view.
    fun rowScore(ppsId: Int, orderId: Int): Short =
        data[ppsId * NUM_ORDERS + orderId]

    fun setRowScore(ppsId: Int, orderId: Int, value: Short) {
        data[ppsId * NUM_ORDERS + orderId] = value
    }

    // Zero the score slot for orderId across all PPS rows (eviction path).
    fun evictOrder(orderId: Int) {
        if (orderId >= NUM_ORDERS) return
        for (ppsId in 0 until numPps) {
            data[ppsId * NUM_ORDERS + orderId] = 0
        }
    }
}

// ── ReadBuffer ───────────────────────────────────────────────────────────────
// Flat [numPairs × NUM_TABLES × TOP_K] IntArray. -1 = empty slot.
// Indexed as: data[(pairIdx * NUM_TABLES + table) * TOP_K + k]

class ReadBuffer(val numPairs: Int) {
    val data = IntArray(numPairs * NUM_TABLES * TOP_K) { -1 }

    fun listOffset(pairIdx: Int, table: Int): Int =
        (pairIdx * NUM_TABLES + table) * TOP_K

    // Returns up to k non-(-1) entries for the given pair/table.
    fun getList(pairIdx: Int, table: Int, k: Int): IntArray {
        val off = listOffset(pairIdx, table)
        val cap = minOf(k, TOP_K)
        var count = 0
        while (count < cap && data[off + count] != -1) count++
        return data.copyOfRange(off, off + count)
    }

    // Returns the mutable slice for writing during Phase 2.
    // Caller writes directly into data[off..off+TOP_K].
    fun listSliceOffset(pairIdx: Int, table: Int): Int = listOffset(pairIdx, table)
}

// ── DoubleBuffer ─────────────────────────────────────────────────────────────
// Lock-free read path via AtomicReference — equivalent to Go's atomic.Pointer[ReadBuffer].
// Writer (compute engine) calls publish(); readers call load(). Zero contention on reads.

class DoubleBuffer(numPairs: Int) {
    private val ref = AtomicReference(ReadBuffer(numPairs))

    fun load(): ReadBuffer = ref.get()

    fun publish(buf: ReadBuffer) = ref.set(buf)
}

// ── PairInfo ─────────────────────────────────────────────────────────────────

data class PairInfo(
    val ppsId: Int,
    val binTagId: Int,
    val key: PairKey,
)

// ── CycleStats ───────────────────────────────────────────────────────────────

data class CycleStats(
    val phase1Ms: Long,
    val phase2Ms: Long,
    val totalMs: Long,
    val slaOk: Boolean,
)

// ── ScoreEntry ───────────────────────────────────────────────────────────────
// Scratch entry for Quickselect. Two ints per entry — no object overhead because
// WorkerScratch stores parallel int[] arrays (SoA), not ScoreEntry[].

// (ScoreEntry as a class is intentionally NOT used on the hot path — see WorkerScratch
// in Compute.kt which uses parallel IntArray pairs to avoid allocation.)
