package com.greyorange.ps

import java.util.concurrent.atomic.AtomicReference

// ── PairStatus ───────────────────────────────────────────────────────────────

enum class PairStatus { ACTIVE, WARMING_UP, REMOVED }

// ── TopologyState ─────────────────────────────────────────────────────────────
// Copy-on-write map via AtomicReference — equivalent to Go's atomic.Value wrapping
// map[PairKey]PairStatus. Reads are lock-free (one atomic load). Writes acquire
// the mutex, copy the map, mutate the copy, publish atomically.

class TopologyState {
    // Stores an immutable snapshot of the pair-status map.
    private val ref = AtomicReference(emptyMap<PairKey, PairStatus>())
    private val mu  = Any()  // coarse write lock; write path is rare

    fun get(key: PairKey): PairStatus? = ref.get()[key]

    fun addWarmingUp(ppsId: Int, binTagId: Int) = mutate(makePairKey(ppsId, binTagId), PairStatus.WARMING_UP)

    fun activate(ppsId: Int, binTagId: Int) = mutate(makePairKey(ppsId, binTagId), PairStatus.ACTIVE)

    fun remove(ppsId: Int, binTagId: Int) = mutate(makePairKey(ppsId, binTagId), PairStatus.REMOVED)

    fun warmingUpPairs(): List<PairKey> =
        ref.get().entries.filter { it.value == PairStatus.WARMING_UP }.map { it.key }

    private fun mutate(key: PairKey, status: PairStatus) {
        synchronized(mu) {
            val next = HashMap(ref.get())
            next[key] = status
            ref.set(next)
        }
    }
}
