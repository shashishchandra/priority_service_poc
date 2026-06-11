package com.greyorange.ps

import kotlin.math.min

// ── ResourceConfig ────────────────────────────────────────────────────────────
// Matches go-cgo-impl's ResourceConfig / initFromEnv() pattern.
// JVM GC mode and heap size are controlled via Makefile JAVA_OPTS; they cannot
// be set at runtime after JVM startup, so the config records them as strings for
// logging only.

data class ResourceConfig(
    val numWorkers: Int,    // Phase 2 ForkJoinPool thread count (PS_NUM_WORKERS)
    val p1Parallelism: Int, // Phase 1 parallel stream width  (PS_P1_PARALLELISM)
    val gcMode: String,     // informational — set via -XX:+Use*GC in JAVA_OPTS
)

fun initFromEnv(): ResourceConfig {
    val cores = Runtime.getRuntime().availableProcessors()
    val defaultWorkers = min(PAIRS_PER_POD, cores).coerceAtLeast(1)

    val workers = System.getenv("PS_NUM_WORKERS")?.toIntOrNull()?.takeIf { it > 0 }
        ?: defaultWorkers

    val p1 = System.getenv("PS_P1_PARALLELISM")?.toIntOrNull()?.takeIf { it > 0 }
        ?: cores

    // Detect GC mode from system properties set by -XX flags (best-effort).
    val gcMode = when {
        System.getProperty("java.vm.info", "").contains("zgc", ignoreCase = true) -> "ZGC"
        else -> System.getenv("PS_GC_MODE") ?: "ZGC (default)"
    }

    return ResourceConfig(workers, p1, gcMode)
}

fun printResourceConfig(cfg: ResourceConfig) {
    val cores          = Runtime.getRuntime().availableProcessors()
    val ppsMb          = NUM_PPS.toLong() * NUM_ORDERS.toLong() * 2L / 1_000_000L
    val binTagIdxMb    = NUM_BIN_TAGS_PER_PPS.toLong() * NUM_ORDERS.toLong() * 4L / 1_000_000L
    val rt             = Runtime.getRuntime()
    val maxHeapMb      = rt.maxMemory() / 1_000_000L

    println("Resource config:")
    println("  Workers (Phase2)  : ${cfg.numWorkers}  (logical CPUs: $cores)")
    println("  P1 parallelism    : ${cfg.p1Parallelism}")
    println("  GC mode           : ${cfg.gcMode}")
    println("  Max heap          : ${maxHeapMb} MB  (-Xmx)")
    println("  PPSMatrix est.    : $ppsMb MB  ($NUM_PPS PPS × $NUM_ORDERS orders × 2 B)")
    println("  BinTag idx est.   : $binTagIdxMb MB  ($NUM_BIN_TAGS_PER_PPS tags × $NUM_ORDERS orders × 4 B, worst-case)")
    println()
}

fun printMemStats() {
    val rt        = Runtime.getRuntime()
    val usedMb    = (rt.totalMemory() - rt.freeMemory()) / 1_000_000L
    val totalMb   = rt.totalMemory() / 1_000_000L
    val maxMb     = rt.maxMemory() / 1_000_000L
    println("  [mem] used: ${usedMb} MB  committed: ${totalMb} MB  max: ${maxMb} MB")
}
