package com.greyorange.ps

import java.util.Random

// ── ItemDict mock ─────────────────────────────────────────────────────────────

data class TPIDUpdate(val tpid: Long, val oldContrib: Float, val newContrib: Float)

data class ItemDictUpdate(val updates: List<TPIDUpdate>)

// Simulates the upstream item-dictionary service.
// Fetch() sleeps 100–200 ms (uniform random) to model network round-trip latency.
class MockItemDictClient(seed: Long, private val numTpids: Long) {
    private val rng        = Random(seed)
    private val tpidScores = FloatArray(numTpids.toInt()) { rng.nextFloat() }
    var updateFrac: Double = 1.0   // fraction of TPIDs updated per call

    @Synchronized
    fun fetch(): ItemDictUpdate {
        val delayMs = 100 + rng.nextInt(101)   // [100, 200] ms — same as Go
        Thread.sleep(delayMs.toLong())

        val numUpdate = maxOf(1, (numTpids * updateFrac).toInt())
        val updates   = ArrayList<TPIDUpdate>(numUpdate)
        repeat(numUpdate) {
            val tpid    = (rng.nextLong() % numTpids + numTpids) % numTpids
            val oldVal  = tpidScores[tpid.toInt()]
            val newVal  = rng.nextFloat()
            tpidScores[tpid.toInt()] = newVal
            updates.add(TPIDUpdate(tpid, oldVal, newVal))
        }
        return ItemDictUpdate(updates)
    }
}

// ── Kafka order stream mock ───────────────────────────────────────────────────

data class KafkaOrderEvent(
    val orderId:         Int,
    val requiredQty:     Float,
    val pbtDeadline:     Long,      // Unix seconds; -1 = no deadline
    val tpids:           LongArray,
    val eligibleBinTags: IntArray,
    val baseScore:       Float,
)

class MockKafkaOrderStream(
    seed:                   Long,
    private val numOrders:  Int,
    private val numTpids:   Long,
    private val numBinTags: Int,
) {
    private val rng = Random(seed)

    @Synchronized
    fun generateBatch(count: Int): List<KafkaOrderEvent> {
        val now    = System.currentTimeMillis() / 1000L
        return List(count) {
            val orderId = (rng.nextLong() % numOrders + numOrders).toInt() % numOrders
            val qty     = 1.0f + rng.nextFloat() * 29.0f

            val deadline = if (rng.nextDouble() < 0.5) now + 600 + (rng.nextInt(6601)) else -1L

            val numT  = 1 + rng.nextInt(5)
            val tpids = LongArray(numT) { (rng.nextLong() % numTpids + numTpids) % numTpids }

            val numBT   = 1 + rng.nextInt(4)
            val binTags = IntArray(numBT) { (rng.nextInt(numBinTags)) }

            KafkaOrderEvent(orderId, qty, deadline, tpids, binTags, rng.nextFloat())
        }
    }

    // Next generates an event for orderId with ALL bintag IDs eligible.
    // Use for full worst-case 1M-order benchmark — same as Go's Next().
    @Synchronized
    fun next(orderId: Int): KafkaOrderEvent {
        val now = System.currentTimeMillis() / 1000L
        val qty = 1.0f + rng.nextFloat() * 29.0f

        val deadline = if (rng.nextDouble() < 0.5) now + 600 + rng.nextInt(6601) else -1L

        val numT  = 1 + rng.nextInt(5)
        val tpids = LongArray(numT) { (rng.nextLong() % numTpids + numTpids) % numTpids }

        // All bintag IDs eligible — worst-case scenario.
        val binTags = IntArray(numBinTags) { it }

        return KafkaOrderEvent(orderId, qty, deadline, tpids, binTags, rng.nextFloat())
    }
}

// ── Order ingestion ───────────────────────────────────────────────────────────
// Applies a KafkaOrderEvent to registry, indexes, and PPSMatrix.
// The base score (scaled to i16) is written into every PPS row for this order.

fun ingestOrder(
    evt:       KafkaOrderEvent,
    registry:  OrderRegistry,
    indexes:   InvertedIndexes,
    matrix:    PPSMatrix,
    nowSecs:   Long,
) {
    val oid = evt.orderId
    if (oid >= NUM_ORDERS) return

    // Remove stale index entries before upserting.
    if (registry.active[oid]) indexes.removeOrder(oid)

    registry.upsert(oid, evt.requiredQty, evt.pbtDeadline, nowSecs)
    indexes.addOrder(oid, evt.tpids, evt.eligibleBinTags)

    // Write base score into every PPS row.
    val baseI16 = (evt.baseScore * SCORE_SCALE)
        .coerceIn(Short.MIN_VALUE.toFloat(), Short.MAX_VALUE.toFloat())
        .toInt().toShort()
    for (ppsId in 0 until matrix.numPps) {
        matrix.data[ppsId * NUM_ORDERS + oid] = baseI16
    }
}
