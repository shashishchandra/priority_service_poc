package com.greyorange.ps

// ── Workload constants — identical to go-cgo-impl/types.go ──────────────────

const val NUM_PPS              = 70
const val NUM_BIN_TAGS_PER_PPS = 10
const val NUM_ORDERS           = 300_000
const val PAIRS_PER_POD        = NUM_PPS * NUM_BIN_TAGS_PER_PPS

// ReadBuffer table count and size per (PPS, BinTag) pair.
const val TOP_K      = 1_000
const val NUM_TABLES = 4

// Table index constants.
const val TBL_HEURISTIC = 0
const val TBL_LARGE     = 1
const val TBL_PBT       = 2
const val TBL_CRITICAL  = 3

// Score thresholds.
const val LARGE_QTY_THRESHOLD  = 10.0f
const val CRITICAL_CUTOFF_SECS = 1_800L

// Score encoding: stored_i16 = (f32_score * SCORE_SCALE).clamp(Short.MIN, Short.MAX)
const val SCORE_SCALE = 100.0f

// Orders not upserted within this window are TTL-evicted.
const val ORDER_TTL_SECS = 3_600L
