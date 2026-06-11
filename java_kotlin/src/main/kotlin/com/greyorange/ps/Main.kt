package com.greyorange.ps

fun main() {
    val cfg = initFromEnv()

    println("=== Priority Service (Kotlin/JVM) | PPSMatrix / i16 / OrderCache ===")
    printResourceConfig(cfg)
    println("Pod config: $NUM_PPS PPS × $NUM_BIN_TAGS_PER_PPS BinTags = $PAIRS_PER_POD pairs")
    println("PPSMatrix: $NUM_PPS PPS × $NUM_ORDERS orders × 2B = ${NUM_PPS * NUM_ORDERS * 2 / 1_000_000} MB")
    println()

    // ── Step 1: Build topology ────────────────────────────────────────────────
    val topology = TopologyState()
    val pairs    = ArrayList<PairInfo>(PAIRS_PER_POD)
    for (pps in 0 until NUM_PPS) {
        for (bt in 0 until NUM_BIN_TAGS_PER_PPS) {
            topology.activate(pps, bt)
            pairs.add(PairInfo(pps, bt, makePairKey(pps, bt)))
        }
    }
    println("Topology: ${pairs.size} pairs activated")

    // ── Step 2: Create engine and ingest 1M orders ───────────────────────────
    val db     = DoubleBuffer(pairs.size)
    val engine = ComputeEngine(pairs, db, topology, cfg.numWorkers)

    val stream  = MockKafkaOrderStream(42L, NUM_ORDERS, 100_000L, NUM_BIN_TAGS_PER_PPS)
    print("Ingesting $NUM_ORDERS orders (all bintags eligible)... ")
    val t0      = System.currentTimeMillis()
    val nowSecs = t0 / 1000L
    for (i in 0 until NUM_ORDERS) {
        val evt = stream.next(i)
        ingestOrder(evt, engine.registry, engine.indexes, engine.matrix, nowSecs)
    }
    println("done in ${System.currentTimeMillis() - t0} ms")

    // Freeze index lists to primitive IntArray for the Phase 2 hot path.
    // Without this, iterating MutableList<Int> (boxed Integer) over 1M-entry
    // BinTag buckets is ~30× slower than Go's []uint32 or Rust's Vec<u32>.
    print("Materializing BinTag index... ")
    var tMat = System.currentTimeMillis()
    engine.indexes.materialize()
    println("done in ${System.currentTimeMillis() - tMat} ms")

    print("Materializing TPID index...   ")
    tMat = System.currentTimeMillis()
    engine.indexes.materializeTpid()
    println("done in ${System.currentTimeMillis() - tMat} ms")

    // ── Worst-case verification ───────────────────────────────────────────────
    run {
        val activeCount = engine.registry.activeCount()
        val btSizes     = (0 until NUM_BIN_TAGS_PER_PPS).map {
            engine.indexes.binTagToOrders[it]?.size ?: 0
        }
        val btMin = btSizes.minOrNull() ?: 0
        val btMax = btSizes.maxOrNull() ?: 0
        println("\n=== Worst-case verification ===")
        println("  Active orders in registry : $activeCount")
        println("  BinTag IDs in index       : ${engine.indexes.binTagToOrders.size}")
        println("  Eligible orders per BinTag: min=$btMin max=$btMax (expected $NUM_ORDERS)")
        println("  TPIDs in tpid_to_orders   : ${engine.indexes.tpidToOrders.size}")
        println("  Pairs to rank in Phase 2  : ${engine.pairs.size}")
        println("  PPSMatrix size            : ${NUM_PPS * NUM_ORDERS * 2 / 1_000_000} MB")
        println()
    }

    // ── Step 3: Run 3 compute cycles ─────────────────────────────────────────
    val dictClient = MockItemDictClient(7L, 100_000L)
    println("--- Compute cycles ---")
    for (i in 1..100) {
        Thread.sleep(1000)
        print("Cycle $i: fetching ItemDict update... ")
        val update = dictClient.fetch()
        println("${update.updates.size} updates received")
        val stats = engine.runCycleWithDict(update)
        println("  Phase1: ${stats.phase1Ms} ms | Phase2: ${stats.phase2Ms} ms | Total: ${stats.totalMs} ms | SLA OK: ${stats.slaOk}")
        printMemStats()
    }

    // ── Step 4: GetTopK demo ──────────────────────────────────────────────────
    println("\n--- GetTopK demo ---")
    val srv = PriorityServer(topology, db, engine.pairs)
    val req = GetTopKRequest(ppsId = 0, binTagId = 0, k = 5)
    try {
        val resp = srv.getTopK(req)
        println("GetTopK(PPS=0, BinTag=0, K=5):")
        println("  Heuristic top-5: ${resp.heuristic.toList()}")
        println("  Large top-5:     ${resp.large.toList()}")
        println("  PBT top-5:       ${resp.pbt.toList()}")
        println("  Critical top-5:  ${resp.critical.toList()}")
    } catch (e: ApiError) {
        println("GetTopK(PPS=0, BinTag=0): ERROR ${e.message}")
    }

    // ── Step 5: Circuit breaker demo ─────────────────────────────────────────
    println("\n--- Circuit breaker demo ---")
    val newPps = 99; val newBt = 88
    engine.handleAddPair(newPps, newBt)
    srv.refreshPairIndex(engine.pairs)

    val status = topology.get(makePairKey(newPps, newBt))
    println("New pair (PPS=$newPps, BinTag=$newBt) status: $status (WarmingUp=${status == PairStatus.WARMING_UP})")

    try {
        srv.getTopK(GetTopKRequest(newPps, newBt, 5))
        println("ERROR: expected ApiError for WarmingUp pair, got success")
    } catch (e: ApiError) {
        println("GetTopK(PPS=$newPps, BinTag=$newBt): ERROR code=${e.code} msg=\"${e.msg}\" (expected GRPC_UNAVAILABLE)")
    }

    println("\nRunning cycle to promote WarmingUp pair...")
    val promoteCycle = engine.runCycleWithDict(null)
    println("  Total: ${promoteCycle.totalMs} ms | SLA OK: ${promoteCycle.slaOk}")

    srv.refreshPairIndex(engine.pairs)
    val status2 = topology.get(makePairKey(newPps, newBt))
    println("Pair (PPS=$newPps, BinTag=$newBt) status after cycle: $status2 (Active=${status2 == PairStatus.ACTIVE})")

    try {
        val resp2 = srv.getTopK(GetTopKRequest(newPps, newBt, 5))
        println("GetTopK after promotion: Heuristic top-5: ${resp2.heuristic.toList()}")
    } catch (e: ApiError) {
        println("GetTopK after promotion: ERROR ${e.message}")
    }

    // ── Step 6: Order eviction demo ───────────────────────────────────────────
    println("\n--- Order eviction demo ---")
    val sampleOrderId = 42
    println("Evicting order $sampleOrderId (Kafka order-removed event)...")
    engine.handleOrderRemoved(sampleOrderId)
    println("Order $sampleOrderId active after eviction: ${engine.registry.active[sampleOrderId]}")

    println("\n=== Done ===")
}
