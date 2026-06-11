package com.greyorange.ps

import java.util.Arrays
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinTask
import kotlin.math.min

// ── WorkerScratch ─────────────────────────────────────────────────────────────
// One LongArray per ranking table — packs (score, orderId) into a single Long:
//   upper 32 bits = -score  (so ascending sort gives descending-by-score order)
//   lower 32 bits = orderId
//
// Replaces the previous 2-IntArray-per-table SoA design:
//   - Halves the number of arrays (4 vs 8 per worker)
//   - Removes insertion sort (O(k²)) — Arrays.sort on k=1000 longs is a JVM intrinsic
//   - Single array write per eligible order vs two separate writes
//
// Each worker thread owns exactly one WorkerScratch via ThreadLocal.

class WorkerScratch {
    val hPacked    = LongArray(NUM_ORDERS)
    var hLen       = 0
    val lPacked    = LongArray(NUM_ORDERS)
    var lLen       = 0
    val pbtPacked  = LongArray(NUM_ORDERS)
    var pbtLen     = 0
    val critPacked = LongArray(NUM_ORDERS)
    var critLen    = 0
}

// ── Packed-sort helpers ───────────────────────────────────────────────────────
// Quickselect on LongArray (ascending — smallest packed = highest original score).

private fun medianOfThreeLong(arr: LongArray, lo: Int, mid: Int, hi: Int): Int {
    val a = arr[lo]; val b = arr[mid]; val c = arr[hi]
    return when {
        (a >= b && b >= c) || (c >= b && b >= a) -> mid
        (b >= a && a >= c) || (c >= a && a >= b) -> lo
        else -> hi
    }
}

private fun partitionLongAsc(arr: LongArray, lo: Int, hi: Int): Int {
    val mid = lo + (hi - lo) / 2
    val pivotIdx = medianOfThreeLong(arr, lo, mid, hi)
    arr[pivotIdx] = arr[hi].also { arr[hi] = arr[pivotIdx] }
    val pivot = arr[hi]
    var i = lo - 1
    for (j in lo until hi) {
        if (arr[j] <= pivot) {
            i++
            arr[i] = arr[j].also { arr[j] = arr[i] }
        }
    }
    arr[i + 1] = arr[hi].also { arr[hi] = arr[i + 1] }
    return i + 1
}

private fun quickselectLongTopK(arr: LongArray, n: Int, k: Int) {
    if (k <= 0 || n == 0 || k >= n) return
    var lo = 0; var hi = n - 1
    while (lo < hi) {
        val p = partitionLongAsc(arr, lo, hi)
        when {
            p == k  -> return
            p < k   -> lo = p + 1
            else    -> hi = p - 1
        }
    }
}

// Quickselect top-k entries to arr[0..k), sort that prefix ascending (= descending
// by original score due to negation), write order IDs to dst[dstOff..dstOff+k).
private fun packTopKAndCopy(dst: IntArray, dstOff: Int, packed: LongArray, len: Int) {
    val k = min(len, TOP_K)
    if (k == 0) return
    if (len > k) quickselectLongTopK(packed, len, k)
    // Arrays.sort is a JVM intrinsic (dual-pivot quicksort) — O(k log k), no boxing.
    Arrays.sort(packed, 0, k)
    for (i in 0 until k) dst[dstOff + i] = (packed[i] and 0xFFFFFFFFL).toInt()
}

// ── ComputeEngine ─────────────────────────────────────────────────────────────

class ComputeEngine(
    pairs: List<PairInfo>,
    val db: DoubleBuffer,
    val topology: TopologyState,
    numWorkers: Int,
) {
    val matrix   = PPSMatrix(NUM_PPS)
    val registry = OrderRegistry()
    val indexes  = InvertedIndexes()
    var pairs    = pairs.toMutableList()

    private val numWorkers    = if (numWorkers > 0) numWorkers else Runtime.getRuntime().availableProcessors()
    private val pool          = ForkJoinPool(this.numWorkers)
    private val pendingWarmup = mutableListOf<PairInfo>()

    // Pre-allocated delta — reused every Phase 1 cycle.
    // Eliminates NUM_ORDERS × 4B GC pressure per cycle and the GC jitter it causes.
    private val delta = IntArray(NUM_ORDERS)

    // Thread-local scratch — one WorkerScratch per ForkJoinPool worker thread.
    private val scratch = ThreadLocal.withInitial { WorkerScratch() }

    // ── Public API ────────────────────────────────────────────────────────────

    fun runCycle(): CycleStats = runCycleWithDict(null)

    fun runCycleWithDict(update: ItemDictUpdate?): CycleStats {
        val start   = System.currentTimeMillis()
        val nowSecs = start / 1000L

        registry.purgeExpired(nowSecs, indexes, matrix)

        val p1Ms = phase1Update(update)
        val (buf, p2Ms) = phase2Rank(nowSecs)

        db.publish(buf)
        promoteWarmingPairs()

        val totalMs = System.currentTimeMillis() - start
        return CycleStats(p1Ms, p2Ms, totalMs, totalMs < 5_000)
    }

    fun handleAddPair(ppsId: Int, binTagId: Int) {
        topology.addWarmingUp(ppsId, binTagId)
        val info = PairInfo(ppsId, binTagId, makePairKey(ppsId, binTagId))
        pendingWarmup.add(info)
        if (pairs.none { it.key == info.key }) pairs.add(info)
    }

    fun handleRemovePair(ppsId: Int, binTagId: Int) {
        val key = makePairKey(ppsId, binTagId)
        topology.remove(ppsId, binTagId)
        pairs.removeIf { it.key == key }
    }

    fun handleOrderRemoved(orderId: Int) {
        registry.evictOrder(orderId, indexes, matrix)
    }

    // ── Phase 1: score update ─────────────────────────────────────────────────
    // Uses pre-allocated `delta` (field) — clears in-place instead of allocating.
    // Uses `tpidFrozen` IntArrays — no Integer unboxing in the accumulation loop.

    private fun phase1Update(update: ItemDictUpdate?): Long {
        val t0 = System.currentTimeMillis()
        if (update == null || update.updates.isEmpty()) return 0L

        // Clear pre-allocated delta — no allocation, no GC pressure.
        Arrays.fill(delta, 0)

        for (u in update.updates) {
            val diff = u.newContrib - u.oldContrib
            if (diff == 0.0f) continue
            val diffI32 = (diff * SCORE_SCALE).toInt()
            // tpidFrozen is IntArray — forEach iterates primitives, zero boxing.
            indexes.tpidFrozen[u.tpid]?.forEach { oid ->
                if (oid < NUM_ORDERS) delta[oid] += diffI32
            }
        }

        // Parallel: each PPS row is independent — safe to update concurrently.
        val tasks = ArrayList<ForkJoinTask<Unit>>(numWorkers)
        val rowsPerWorker = (matrix.numPps + numWorkers - 1) / numWorkers
        var rowStart = 0
        while (rowStart < matrix.numPps) {
            val lo = rowStart
            val hi = min(lo + rowsPerWorker, matrix.numPps)
            tasks.add(pool.submit<Unit> {
                for (ppsId in lo until hi) {
                    val off = matrix.rowOffset(ppsId)
                    for (o in 0 until NUM_ORDERS) {
                        val v = matrix.data[off + o].toInt() + delta[o]
                        // Manual clamp — avoids coerceIn lambda overhead
                        matrix.data[off + o] = when {
                            v > Short.MAX_VALUE -> Short.MAX_VALUE
                            v < Short.MIN_VALUE -> Short.MIN_VALUE
                            else -> v.toShort()
                        }
                    }
                }
            })
            rowStart += rowsPerWorker
        }
        tasks.forEach { it.get() }

        return System.currentTimeMillis() - t0
    }

    // ── Phase 2: per-pair ranking ─────────────────────────────────────────────
    // Each worker owns a WorkerScratch with pre-allocated LongArrays.
    // Single pass fills all four tables. packTopKAndCopy uses Arrays.sort (JVM
    // intrinsic) on the top-k prefix instead of insertion sort.

    private fun phase2Rank(nowSecs: Long): Pair<ReadBuffer, Long> {
        val t0       = System.currentTimeMillis()
        val numPairs = pairs.size
        val buf      = ReadBuffer(numPairs)

        if (numPairs == 0) return buf to 0L

        val critDeadline     = nowSecs + CRITICAL_CUTOFF_SECS
        val pairsSnapshot    = pairs

        val effectiveWorkers = min(numWorkers, numPairs)
        val pairsPerWorker   = (numPairs + effectiveWorkers - 1) / effectiveWorkers

        val tasks = ArrayList<ForkJoinTask<Unit>>(effectiveWorkers)
        var lo = 0
        while (lo < numPairs) {
            val start = lo
            val end   = min(lo + pairsPerWorker, numPairs)
            tasks.add(pool.submit<Unit> {
                val sc = scratch.get()

                for (p in start until end) {
                    val pair     = pairsSnapshot[p]
                    // Use primitive IntArray snapshot — no Integer unboxing.
                    val eligible = indexes.binTagFrozen[pair.binTagId]
                    if (eligible == null || eligible.isEmpty()) continue

                    sc.hLen = 0; sc.lLen = 0; sc.pbtLen = 0; sc.critLen = 0

                    val ppsOff = pair.ppsId * NUM_ORDERS

                    // ── Single pass: fill all four packed scratch tables ──
                    for (oid in eligible) {
                        if (oid >= NUM_ORDERS || !registry.active[oid]) continue

                        val score = matrix.data[ppsOff + oid].toInt()
                        // Negate score so ascending LongArray sort = descending-by-score.
                        val hp = ((-score).toLong() shl 32) or (oid.toLong() and 0xFFFFFFFFL)

                        sc.hPacked[sc.hLen++] = hp

                        if (registry.requiredQty[oid] >= LARGE_QTY_THRESHOLD) {
                            sc.lPacked[sc.lLen++] = hp
                        }

                        val dl = registry.pbtDeadline[oid]
                        if (dl > 0L) {
                            val dlScore  = (nowSecs - dl).toInt()
                            // Negate deadline score for same ascending-sort trick.
                            val dp = ((-dlScore).toLong() shl 32) or (oid.toLong() and 0xFFFFFFFFL)
                            sc.pbtPacked[sc.pbtLen++]     = dp
                            if (dl <= critDeadline) sc.critPacked[sc.critLen++] = dp
                        }
                    }

                    packTopKAndCopy(buf.data, buf.listSliceOffset(p, TBL_HEURISTIC), sc.hPacked,    sc.hLen)
                    packTopKAndCopy(buf.data, buf.listSliceOffset(p, TBL_LARGE),     sc.lPacked,    sc.lLen)
                    packTopKAndCopy(buf.data, buf.listSliceOffset(p, TBL_PBT),       sc.pbtPacked,  sc.pbtLen)
                    packTopKAndCopy(buf.data, buf.listSliceOffset(p, TBL_CRITICAL),  sc.critPacked, sc.critLen)
                }
            })
            lo += pairsPerWorker
        }
        tasks.forEach { it.get() }

        return buf to (System.currentTimeMillis() - t0)
    }

    // ── Warmup promotion ──────────────────────────────────────────────────────

    private fun promoteWarmingPairs() {
        for (p in pendingWarmup) topology.activate(p.ppsId, p.binTagId)
        pendingWarmup.clear()
    }
}
