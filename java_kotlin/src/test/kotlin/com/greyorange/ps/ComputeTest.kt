package com.greyorange.ps

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import kotlin.math.min

// ── Helpers ───────────────────────────────────────────────────────────────────

private fun buildPairs(numPps: Int, numBinTags: Int): List<PairInfo> =
    (0 until numPps).flatMap { pps ->
        (0 until numBinTags).map { bt ->
            PairInfo(pps, bt, makePairKey(pps, bt))
        }
    }

// ── Tests — mirrors go-cgo-impl/compute_test.go ──────────────────────────────

class ComputeTest {

    // Equivalent to TestComputeCycleBasic: ingest 10 000 orders, run 2 cycles,
    // verify heuristic list is non-empty and sorted descending.
    @Test
    fun `compute cycle basic`() {
        val numBinTags = 5
        val pairs      = buildPairs(1, numBinTags)
        val topo       = TopologyState()
        pairs.forEach { topo.activate(it.ppsId, it.binTagId) }

        val db     = DoubleBuffer(pairs.size)
        val engine = ComputeEngine(pairs, db, topo, 4)

        val stream = MockKafkaOrderStream(42L, NUM_ORDERS, 5_000L, numBinTags)
        val events = stream.generateBatch(10_000)
        val nowSecs = System.currentTimeMillis() / 1000L
        events.forEach { ingestOrder(it, engine.registry, engine.indexes, engine.matrix, nowSecs) }

        val dictClient = MockItemDictClient(7L, 5_000L)
        for (cycle in 0..1) {
            val update = dictClient.fetch()
            val stats  = engine.runCycleWithDict(update)
            assertTrue(stats.totalMs < 30_000, "cycle $cycle took ${stats.totalMs} ms — unexpectedly slow")
        }

        val buf   = db.load()
        val hList = buf.getList(0, TBL_HEURISTIC, TOP_K)
        assertTrue(hList.isNotEmpty(), "expected non-empty heuristic list for pair 0")

        // Verify descending score order against the PPSMatrix row 0.
        for (i in 1 until hList.size) {
            val prev = hList[i - 1]; val curr = hList[i]
            if (prev < 0 || curr < 0) break
            val prevScore = engine.matrix.data[prev]   // ppsId=0, so offset=0
            val currScore = engine.matrix.data[curr]
            assertTrue(prevScore >= currScore,
                "heuristic list not descending at index $i: score[$prev]=$prevScore < score[$curr]=$currScore")
        }
    }

    // Equivalent to TestTopologyWarmupToActive.
    @Test
    fun `topology warmup to active`() {
        val pairs = buildPairs(1, 1)
        val topo  = TopologyState()
        topo.activate(0, 0)

        val db     = DoubleBuffer(pairs.size)
        val engine = ComputeEngine(pairs, db, topo, 2)

        val newPps = 99; val newBt = 88
        engine.handleAddPair(newPps, newBt)

        val key    = makePairKey(newPps, newBt)
        val status = topo.get(key)
        assertNotNull(status, "pair not found in topology after handleAddPair")
        assertEquals(PairStatus.WARMING_UP, status, "expected WARMING_UP")

        engine.runCycleWithDict(null)

        val status2 = topo.get(key)
        assertNotNull(status2, "pair not found in topology after runCycle")
        assertEquals(PairStatus.ACTIVE, status2, "expected ACTIVE after cycle")
    }

    // Equivalent to TestCircuitBreakerWarmingUp.
    @Test
    fun `circuit breaker warming up returns GRPC_UNAVAILABLE`() {
        val topo  = TopologyState()
        val ppsId = 5; val btId = 3
        topo.addWarmingUp(ppsId, btId)

        val pairs = listOf(PairInfo(ppsId, btId, makePairKey(ppsId, btId)))
        val db    = DoubleBuffer(1)
        val srv   = PriorityServer(topo, db, pairs)

        val ex = assertThrows(ApiError::class.java) {
            srv.getTopK(GetTopKRequest(ppsId, btId, 10))
        }
        assertEquals(GRPC_UNAVAILABLE, ex.code)
    }

    // Equivalent to TestCircuitBreakerRemoved.
    @Test
    fun `circuit breaker removed pair returns GRPC_NOT_FOUND`() {
        val topo  = TopologyState()
        val ppsId = 7; val btId = 2
        topo.activate(ppsId, btId)
        topo.remove(ppsId, btId)

        val pairs = listOf(PairInfo(ppsId, btId, makePairKey(ppsId, btId)))
        val db    = DoubleBuffer(1)
        val srv   = PriorityServer(topo, db, pairs)

        val ex = assertThrows(ApiError::class.java) {
            srv.getTopK(GetTopKRequest(ppsId, btId, 10))
        }
        assertEquals(GRPC_NOT_FOUND, ex.code)
    }

    // Equivalent to TestKafkaOrderIngestion.
    @Test
    fun `kafka order ingestion populates indexes`() {
        val registry = OrderRegistry()
        val indexes  = InvertedIndexes()
        val matrix   = PPSMatrix(1)
        val numBinTags = 20

        val stream = MockKafkaOrderStream(123L, NUM_ORDERS, 10_000L, numBinTags)
        val events = stream.generateBatch(500)

        val nowSecs = System.currentTimeMillis() / 1000L
        events.forEach { ingestOrder(it, registry, indexes, matrix, nowSecs) }

        assertTrue(indexes.binTagToOrders.isNotEmpty(),
            "expected binTagToOrders to be populated")
        assertTrue(indexes.tpidToOrders.isNotEmpty(),
            "expected tpidToOrders to be populated")

        val totalInBuckets = indexes.binTagToOrders.values.sumOf { it.size }
        assertTrue(totalInBuckets >= 500,
            "expected at least 500 total BinTag index entries, got $totalInBuckets")
    }

    // Equivalent to TestItemDictLatency.
    @Test
    fun `item dict fetch latency is 90-260 ms`() {
        val client = MockItemDictClient(999L, 5_000L)
        repeat(3) { i ->
            val t0      = System.currentTimeMillis()
            val update  = client.fetch()
            val elapsed = System.currentTimeMillis() - t0

            assertNotNull(update)
            assertTrue(elapsed >= 90,  "call $i: fetch took ${elapsed} ms — too fast")
            assertTrue(elapsed <= 260, "call $i: fetch took ${elapsed} ms — too slow")
        }
    }

    // Equivalent to TestOrderEviction.
    @Test
    fun `order eviction deactivates and clears indexes`() {
        val registry = OrderRegistry()
        val indexes  = InvertedIndexes()
        val matrix   = PPSMatrix(1)
        val nowSecs  = System.currentTimeMillis() / 1000L

        val evt = KafkaOrderEvent(
            orderId         = 100,
            requiredQty     = 5.0f,
            pbtDeadline     = -1L,
            tpids           = longArrayOf(1L, 2L),
            eligibleBinTags = intArrayOf(10, 11),
            baseScore       = 0.5f,
        )
        ingestOrder(evt, registry, indexes, matrix, nowSecs)

        assertTrue(registry.active[100], "order should be active after ingestion")
        assertTrue(indexes.binTagToOrders[10]!!.isNotEmpty(),
            "order should be in BinTag 10 index after ingestion")

        registry.evictOrder(100, indexes, matrix)

        assertFalse(registry.active[100], "order should be inactive after eviction")
        assertTrue(indexes.binTagToOrders[10]?.isEmpty() ?: true,
            "BinTag 10 index should be empty after eviction")
        assertTrue(indexes.tpidToOrders[1L]?.isEmpty() ?: true,
            "TPID 1 index should be empty after eviction")
    }
}
