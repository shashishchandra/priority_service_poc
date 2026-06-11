package com.greyorange.ps

// ── OrderRegistry ────────────────────────────────────────────────────────────
// Struct-of-Arrays layout: parallel primitive arrays indexed by orderId.
//
// WHY SoA and not Array<OrderMeta>:
//   JVM object headers are 16 bytes each. Array<OrderMeta> with 1M entries =
//   1M × (16B header + 4+4+8+1+8 = 25B fields + 7B padding) ≈ 52 MB + pointer
//   array overhead. SoA gives 4+4+8+1+8 = 25 MB flat — matching Go's layout.
//   Phase 2 access pattern (active check + qty + pbtDeadline) hits 3 separate
//   arrays; Go hits 3 fields in one struct. Cache behaviour is approximately
//   equal at 1M scale — the benchmark will show the real difference.

class OrderRegistry {
    // All arrays are indexed by orderId (0-based, orderId < NUM_ORDERS).
    val active         = BooleanArray(NUM_ORDERS)
    val requiredQty    = FloatArray(NUM_ORDERS)
    val pbtDeadline    = LongArray(NUM_ORDERS)      // Unix seconds; -1 = no deadline
    val insertedAtSecs = LongArray(NUM_ORDERS)       // for TTL eviction

    fun upsert(
        orderId: Int,
        qty: Float,
        pbtDl: Long,
        insertedAt: Long,
    ) {
        if (orderId >= NUM_ORDERS) return
        active[orderId]         = true
        requiredQty[orderId]    = qty
        pbtDeadline[orderId]    = pbtDl
        insertedAtSecs[orderId] = insertedAt
    }

    fun deactivate(orderId: Int) {
        if (orderId >= NUM_ORDERS) return
        active[orderId]         = false
        insertedAtSecs[orderId] = 0
    }

    fun evictOrder(orderId: Int, indexes: InvertedIndexes, matrix: PPSMatrix) {
        if (orderId >= NUM_ORDERS || !active[orderId]) return
        active[orderId]         = false
        insertedAtSecs[orderId] = 0
        indexes.removeOrder(orderId)
        matrix.evictOrder(orderId)
    }

    fun purgeExpired(nowSecs: Long, indexes: InvertedIndexes, matrix: PPSMatrix) {
        for (i in 0 until NUM_ORDERS) {
            val ins = insertedAtSecs[i]
            if (active[i] && ins > 0 && (nowSecs - ins) > ORDER_TTL_SECS) {
                active[i]         = false
                insertedAtSecs[i] = 0
                indexes.removeOrder(i)
                matrix.evictOrder(i)
            }
        }
    }

    fun activeCount(): Int {
        var n = 0
        for (i in 0 until NUM_ORDERS) if (active[i]) n++
        return n
    }
}

// ── InvertedIndexes ──────────────────────────────────────────────────────────
// Same dual-index structure as Go:
//   tpidToOrders    : Map<tpid: Long, List<orderId: Int>>
//   binTagToOrders  : Map<binTagId: Int, List<orderId: Int>>
// Reverse maps for O(1) cleanup on eviction.

class InvertedIndexes {
    val tpidToOrders   = HashMap<Long, MutableList<Int>>()    // tpid → [orderId...]
    val binTagToOrders = HashMap<Int,  MutableList<Int>>()    // binTagId → [orderId...]

    // reverse maps for O(1) per-order cleanup
    private val revTpid   = HashMap<Int, MutableList<Long>>() // orderId → [tpid...]
    private val revBinTag = HashMap<Int, MutableList<Int>>()  // orderId → [binTagId...]

    fun addOrder(orderId: Int, tpids: LongArray, binTagIds: IntArray) {
        removeOrder(orderId)  // idempotent: clear stale entries first

        for (t in tpids) tpidToOrders.getOrPut(t) { mutableListOf() }.add(orderId)
        for (b in binTagIds) binTagToOrders.getOrPut(b) { mutableListOf() }.add(orderId)

        if (tpids.isNotEmpty())     revTpid[orderId]   = tpids.toMutableList()
        if (binTagIds.isNotEmpty()) revBinTag[orderId]  = binTagIds.toMutableList()
    }

    // ── Materialized (frozen) snapshots for hot-path loops ───────────────────
    // MutableList<Int> stores boxed java.lang.Integer. Every forEach on these lists
    // in Phase 1 / Phase 2 causes N unbox operations per cycle.
    // Call materialize() + materializeTpid() once after bulk ingestion; the frozen
    // IntArrays iterate with zero boxing — identical to Go's []uint32 / Rust's Vec<u32>.

    var binTagFrozen: HashMap<Int,  IntArray> = HashMap(0)
        private set

    var tpidFrozen:   HashMap<Long, IntArray> = HashMap(0)
        private set

    fun materialize() {
        binTagFrozen = freezeIntLists(binTagToOrders)
    }

    fun materializeTpid() {
        tpidFrozen = freezeLongLists(tpidToOrders)
    }

    private fun freezeIntLists(src: HashMap<Int, MutableList<Int>>): HashMap<Int, IntArray> {
        val snap = HashMap<Int, IntArray>(src.size * 2)
        for ((k, list) in src) {
            val arr = IntArray(list.size); var i = 0
            for (v in list) arr[i++] = v
            snap[k] = arr
        }
        return snap
    }

    private fun freezeLongLists(src: HashMap<Long, MutableList<Int>>): HashMap<Long, IntArray> {
        val snap = HashMap<Long, IntArray>(src.size * 2)
        for ((k, list) in src) {
            val arr = IntArray(list.size); var i = 0
            for (v in list) arr[i++] = v
            snap[k] = arr
        }
        return snap
    }

    fun removeOrder(orderId: Int) {
        revTpid.remove(orderId)?.forEach { t ->
            val list = tpidToOrders[t] ?: return@forEach
            list.remove(orderId)
            if (list.isEmpty()) tpidToOrders.remove(t)
        }
        revBinTag.remove(orderId)?.forEach { b ->
            val list = binTagToOrders[b] ?: return@forEach
            list.remove(orderId)
            if (list.isEmpty()) binTagToOrders.remove(b)
        }
    }
}

// Extension to convert LongArray to MutableList<Long> cleanly.
private fun LongArray.toMutableList(): MutableList<Long> =
    ArrayList<Long>(this.size).also { for (v in this) it.add(v) }
