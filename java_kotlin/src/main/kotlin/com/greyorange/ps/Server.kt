package com.greyorange.ps

// ── gRPC status codes used by the circuit breaker ────────────────────────────
const val GRPC_NOT_FOUND    = 5
const val GRPC_UNAVAILABLE  = 14

// ── Request / Response ───────────────────────────────────────────────────────

data class GetTopKRequest(val ppsId: Int, val binTagId: Int, val k: Int)

data class GetTopKResponse(
    val heuristic: IntArray,
    val large:     IntArray,
    val pbt:       IntArray,
    val critical:  IntArray,
)

// ── ApiError ──────────────────────────────────────────────────────────────────

class ApiError(val code: Int, val msg: String) : Exception("ApiError(code=$code): $msg")

// ── PriorityServer ────────────────────────────────────────────────────────────
// Lock-free read path:
//   1. topology check via AtomicReference snapshot
//   2. ReadBuffer load via AtomicReference
// No mutex on GetTopK — matches Go and Rust hot-path design.

class PriorityServer(
    private val topology:  TopologyState,
    private val db:        DoubleBuffer,
    pairs: List<PairInfo>,
) {
    // PairKey → index into ReadBuffer rows. Rebuilt on topology changes.
    private var pairIndex: Map<PairKey, Int> = buildIndex(pairs)

    fun getTopK(req: GetTopKRequest): GetTopKResponse {
        val key    = makePairKey(req.ppsId, req.binTagId)
        val status = topology.get(key)

        if (status == null || status == PairStatus.REMOVED)
            throw ApiError(GRPC_NOT_FOUND, "pair not found")
        if (status == PairStatus.WARMING_UP)
            throw ApiError(GRPC_UNAVAILABLE, "pair warming up")

        val idx = pairIndex[key]
            ?: throw ApiError(GRPC_UNAVAILABLE, "pair index not refreshed yet")

        val buf = db.load()
        val k   = if (req.k in 1..TOP_K) req.k else TOP_K

        return GetTopKResponse(
            heuristic = buf.getList(idx, TBL_HEURISTIC, k),
            large     = buf.getList(idx, TBL_LARGE,     k),
            pbt       = buf.getList(idx, TBL_PBT,       k),
            critical  = buf.getList(idx, TBL_CRITICAL,  k),
        )
    }

    fun refreshPairIndex(pairs: List<PairInfo>) {
        pairIndex = buildIndex(pairs)
    }

    private fun buildIndex(pairs: List<PairInfo>): Map<PairKey, Int> =
        HashMap<PairKey, Int>(pairs.size * 2).also { m ->
            pairs.forEachIndexed { i, p -> m[p.key] = i }
        }
}
