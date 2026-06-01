# Priority Service — Implementation Spike

Three side-by-side implementations of the in-memory Priority Service compute
engine, used to evaluate language and runtime choices before committing to a
production design.

| Directory | Language | Phase 1 kernel | Notes |
|-----------|----------|----------------|-------|
| `go-pure-impl/` | Go 1.22 | Pure Go SIMD-free | Simplest; easiest to extend |
| `go-cgo-impl/` | Go 1.22 + C | C SIMD (`scoring.c`) | PPSMatrix on C heap; avoids GC scan |
| `rust-impl/` | Rust 2021 (stable) | Rayon SIMD | Best raw throughput; higher onboarding cost |

---

## Table of Contents

1. [Repository Structure](#1-repository-structure)
2. [Architecture in One Paragraph](#2-architecture-in-one-paragraph)
3. [Prerequisites](#3-prerequisites)
4. [Building and Running the Demo](#4-building-and-running-the-demo)
5. [Running Tests](#5-running-tests)
6. [Environment Variables and Tuning](#6-environment-variables-and-tuning)
7. [PPS Warmup Lifecycle](#7-pps-warmup-lifecycle)
8. [Benchmark Results](#8-benchmark-results)
9. [Performance Analysis and Tuning Rationale](#9-performance-analysis-and-tuning-rationale)
10. [Memory Layout and Sizing](#10-memory-layout-and-sizing)
11. [Key Design Decisions](#11-key-design-decisions)
12. [Open Questions](#12-open-questions)

---

## 1. Repository Structure

```
PS_PROD/
├── README.md               ← this file
├── ARCHITECTURE.md         ← architecture overview for the org design group
│
├── go-pure-impl/           ← Pure Go implementation
│   ├── main.go             ← entry point, demo harness
│   ├── types.go            ← constants, PPSMatrix, ReadBuffer, DoubleBuffer
│   ├── compute.go          ← ComputeEngine, Phase 1 + Phase 2, Quickselect
│   ├── config.go           ← ResourceConfig, initFromEnv, applyRuntimeConfig
│   ├── registry.go         ← OrderRegistry, InvertedIndexes, TTL eviction
│   ├── server.go           ← PriorityServer, GetTopK (gRPC-style)
│   ├── topology.go         ← TopologyState (Active / WarmingUp / Removed)
│   ├── mock.go             ← MockItemDictClient, MockKafkaOrderStream
│   └── compute_test.go     ← 8 unit + integration tests
│
├── go-cgo-impl/            ← Go + C SIMD implementation
│   ├── scoring.c / .h      ← C SIMD kernel (update_all_scores_i16)
│   ├── scoring_bridge.go   ← CGo bridge; PPSMatrix allocated on C heap
│   └── ...                 ← same Go modules as go-pure-impl
│
└── rust-impl/
    ├── Cargo.toml
    └── src/
        ├── main.rs         ← entry point, demo harness
        ├── types.rs        ← PPSMatrix, ReadBuffer, DoubleBuffer, PairInfo
        ├── compute.rs      ← ComputeEngine, rayon Phase 1 + Phase 2
        ├── config.rs       ← ResourceConfig, init_from_env
        ├── registry.rs     ← OrderRegistry, InvertedIndexes, TTL eviction
        ├── server.rs       ← PriorityServer, get_top_k
        ├── topology.rs     ← TopologyState (DashMap-backed)
        ├── mock.rs         ← MockItemDictClient, MockKafkaOrderStream
        └── tests.rs        ← 7 unit + integration tests
```

---

## 2. Architecture in One Paragraph

Each pod hosts a **ComputeEngine** holding a flat `[numPPS × numOrders]` int16
**PPSMatrix** (score per PPS per order), an **OrderRegistry** (`[]OrderMeta`,
one slot per order ID), two **inverted indexes** (`TPIDToOrders`,
`BinTagToOrders`), and a **DoubleBuffer** (atomic-pointer swap of the ranked
output). A Kafka consumer writes into these structures synchronously
(**Phase 1** — score update). Every 5 s the engine fetches an ItemDict delta,
applies it to the matrix in parallel, then runs **Phase 2** — a parallel
Quickselect over eligible orders per `(PPS, BinTag)` pair, producing four
sorted `Top-1000` tables (Heuristic, Large-Qty, PBT-deadline, Critical). The
resulting `ReadBuffer` is published atomically. Butler Service reads via a
lock-free `GetTopK` gRPC call.

---

## 3. Prerequisites

### Go implementations (go-pure-impl, go-cgo-impl)

```bash
# Go 1.22+
go version          # should print go1.22.x or later

# go-cgo-impl only: a C compiler must be on PATH
cc --version        # clang or gcc
```

### Rust implementation

```bash
# Rust stable toolchain (1.75+)
rustup show         # confirm stable is active
cargo --version
```

No external services are required. All data (orders, item dict, topology
events) is generated synthetically by the mock layer.

---

## 4. Building and Running the Demo

Each demo seeds 1 M orders, runs 3 full compute cycles, exercises the circuit
breaker (WarmingUp / Removed pair handling), and prints timing + memory stats.

### go-pure-impl

```bash
cd go-pure-impl
go run .
```

Expected output (10-core machine, worst case):
```
Phase 1 (PPSMatrix update):  ~30 ms
Phase 2 (rank):            ~13–15 s
```

### go-cgo-impl

```bash
cd go-cgo-impl
go run .
```

Actual output (10-core machine, 200 PPS, 1 M orders worst case):
```
=== Priority Service (Go+CGo) | PPSMatrix / int16 / OrderCache ===
Resource config:
  Workers        : 10  (logical CPUs: 10)
  GOMAXPROCS     : 10
  GC percent     : 100
  Mem limit      : none  (set PS_MEM_LIMIT_MB to enable)
  PPSMatrix est. : 400 MB  (200 PPS × 1000000 orders × 2 B)
  BinTag idx est.: 80 MB  (20 tags × 1000000 orders × 4 B, worst-case)

Pod config: 200 PPS × 20 BinTags = 4000 pairs
PPSMatrix: 200 PPS × 1000000 orders × 2B = 400 MB

Topology: 4000 pairs activated
Ingesting 1000000 orders (all bintags eligible)... done in 722ms

=== Worst-case verification ===
  Active orders in registry : 1000000
  BinTag IDs in index       : 20
  Eligible orders per BinTag: min=1000000 max=1000000 (expected 1000000)
  TPIDs in tpid_to_orders   : 100000
  Pairs to rank in Phase 2  : 4000
  PPSMatrix size            : 400 MB

--- Compute cycles ---
Cycle 1: fetching ItemDict update... 100000 updates received
  Phase1: 43 ms | Phase2: 10096 ms | Total: 10140 ms | SLA OK: false
  [mem] heap in-use: 1182 MB  sys: 1205 MB  GC cycles: 72
Cycle 2: fetching ItemDict update... 100000 updates received
  Phase1: 23 ms | Phase2: 10012 ms | Total: 10036 ms | SLA OK: false
  [mem] heap in-use: 929 MB  sys: 1276 MB  GC cycles: 73
Cycle 3: fetching ItemDict update... 100000 updates received
  Phase1: 21 ms | Phase2: 11755 ms | Total: 11778 ms | SLA OK: false
  [mem] heap in-use: 1000 MB  sys: 1344 MB  GC cycles: 73

--- GetTopK demo ---
GetTopK(PPS=0, BinTag=0, K=5):
  Heuristic top-5: [943734 643258 774344 766562 123787]
  Large top-5:     [943734 643258 774344 123787 285807]
  PBT top-5:       [179601 212009 218925 222257 228376]
  Critical top-5:  [179601 212009 218925 222257 228376]

--- Circuit breaker demo ---
New pair (PPS=99, BinTag=88) status: 1 (WarmingUp=true)
GetTopK(PPS=99, BinTag=88): ERROR code=14 msg="pair warming up" (expected GRPCUnavailable)

Running cycle to promote WarmingUp pair...
  Total: 11925 ms | SLA OK: false
Pair (PPS=99, BinTag=88) status after cycle: 0 (Active=true)
GetTopK after promotion: Heuristic top-5: []

--- Order eviction demo ---
Evicting order 42 (Kafka order-removed event)...
Order 42 active after eviction: false

=== Done ===
```

> CGo adds a one-time ~2 ms call overhead per `CUpdateRowRange` invocation;
> gains back ~15 % on Phase 2 because the PPSMatrix lives on the C heap and
> is not scanned by the Go GC.

### rust-impl

```bash
cd rust-impl
cargo run --release    # must use --release; debug is 100× slower
```

Actual output (10-core machine, 200 PPS, 1 M orders worst case):
```
=== Priority Service — PPSMatrix / int16 / OrderCache Demo ===

Resource config:
  Workers        : 15  (logical CPUs: 10)
  Stack/worker   : 2048 KiB
  PPSMatrix est. : 400 MB  (200 PPS × 1000000 orders × 2 B)
  BinTag idx est.: 80 MB  (20 tags × 1000000 orders × 4 B, worst-case)
  Pairs/pod      : 4000

Config: 200 PPS × 20 BinTags = 4000 pairs | PPSMatrix: 200 PPS × 1M orders × 2B = 400 MB

Topology: 4000 pairs activated.

Ingesting 1000000 orders (all bintags eligible) ...
Ingestion complete in 0.5s.

=== Worst-case verification ===
  Active orders in registry : 1000000
  BinTag IDs in index       : 20
  Eligible orders per BinTag: min=1000000 max=1000000 (expected 1000000)
  TPIDs in tpid_to_orders   : 100000
  Pairs to rank in Phase 2  : 4000
  PPSMatrix size            : 400 MB

--- Cycle 1 ---
  Phase 1: 35 ms | Phase 2: 4424 ms | Total: 4461 ms | SLA OK: true
--- Cycle 2 ---
  Phase 1: 23 ms | Phase 2: 4531 ms | Total: 4555 ms | SLA OK: true
--- Cycle 3 ---
  Phase 1: 26 ms | Phase 2: 4614 ms | Total: 4642 ms | SLA OK: true

GetTopK(pps=3, bintag=7, k=10):
  Heuristic top-10 : [3556, 992918, 16333, 23777, 24521, 27537, 29609, 31158, 33254, 35760]
  Large     top-10 : [6449, 23777, 27537, 29609, 31158, 33254, 35896, 41711, 48511, 49979]
  PBT       top-10 : [11428, 119512, 122907, 139118, 162216, 162761, 228529, 256668, 319234, 409259]
  Critical  top-10 : [11428, 119512, 122907, 139118, 162216, 162761, 228529, 256668, 319234, 409259]

New pair (99, 42) topology state: Some(WarmingUp)
Circuit breaker for WarmingUp pair: Unavailable(14) (gRPC code 14)
After cycle, pair (99, 42) topology state: Some(Active)

--- Order eviction demo ---
Evicting order 42 (simulates Kafka order-removed event)...
Order 42 active after eviction: false

=== Demo complete ===
```

> Always build with `--release`. Debug builds disable LLVM optimisations and
> run 80–100× slower.

---

## 5. Running Tests

### Go (both implementations)

```bash
# go-pure-impl
cd go-pure-impl && go test -v -count=1 ./...

# go-cgo-impl
cd go-cgo-impl && go test -v -count=1 ./...
```

Test suite (8 / 7 tests respectively):

| Test | What it covers |
|------|----------------|
| `TestReadBufferGetList` | ReadBuffer slot layout, -1 sentinel |
| `TestDoubleBufferPublish` | Atomic pointer swap; concurrent reads |
| `TestOrderRegistryUpsertAndEvict` | Insert, active flag, TTL eviction |
| `TestInvertedIndexAddAndEvict` | BinTagToOrders, TPIDToOrders, EvictOrder |
| `TestComputeEnginePhase2Basic` | End-to-end ranking with 10 seeded orders |
| `TestGetTopKCircuitBreaker` | WarmingUp → GRPCUnavailable, Removed → GRPCNotFound |
| `TestHandleAddRemovePair` | HandleAddPair / HandleRemovePair pair lifecycle |
| `TestPhase2SinglePassConsistency` | All four tables populated correctly (pure only) |

### Rust

```bash
cd rust-impl
cargo test              # unit tests (fast)
cargo test -- --nocapture   # with stdout
```

Test suite (7 tests):

| Test | What it covers |
|------|----------------|
| `test_order_registry` | Upsert, active flag, TTL eviction |
| `test_inverted_indexes` | BinTagToOrders, TPIDToOrders |
| `test_double_buffer` | Atomic publish, concurrent load |
| `test_compute_engine_basic` | Phase 2 ranking with seeded orders |
| `test_circuit_breaker` | WarmingUp / Removed gRPC codes |
| `test_pair_lifecycle` | handle_add_pair / handle_remove_pair |
| `test_phase2_four_tables` | Heuristic, Large, PBT, Critical all populated |

---

## 6. Environment Variables and Tuning

All three implementations read `PS_*` env vars at startup. Invalid or missing
values are silently ignored; the workload-derived default is kept.

### Go (both implementations)

| Variable | Default | Effect |
|----------|---------|--------|
| `PS_NUM_WORKERS` | `min(PairsPerPod, numCPU)` | Phase 2 goroutine pool size |
| `PS_GOMAXPROCS` | `numCPU` | OS thread count (`runtime.GOMAXPROCS`) |
| `PS_GCPERCENT` | `100` | GC trigger threshold (`debug.SetGCPercent`); `-1` disables GC |
| `PS_MEM_LIMIT_MB` | `0` (no limit) | Hard RSS ceiling (`debug.SetMemoryLimit`) |

**Critical tuning rule — workers must equal GOMAXPROCS:**  
Setting `PS_NUM_WORKERS` above `PS_GOMAXPROCS` (= number of OS threads) means
multiple goroutines compete on the same thread. Go's async preemption (SIGURG)
then context-switches goroutines mid-pair, evicting each goroutine's 14 MB
pre-allocated `workerScratch` from L3 cache. In our benchmarks this caused a
**3–4× Phase 2 regression** (35–50 s vs 13–15 s at worst case). Always keep
`PS_NUM_WORKERS ≤ PS_GOMAXPROCS`.

**Recommended production tuning (Go):**

```bash
# Disable GC during compute-heavy pods; rely on PS_MEM_LIMIT_MB as ceiling
export PS_GCPERCENT=-1
export PS_MEM_LIMIT_MB=3000      # leave headroom above expected ~1.7 GB
export PS_NUM_WORKERS=10         # = numCPU; 1 goroutine per OS thread
export PS_GOMAXPROCS=10

# or: aggressive GC to keep heap tight (lower throughput, lower memory)
export PS_GCPERCENT=50
```

### Rust

| Variable | Default | Effect |
|----------|---------|--------|
| `PS_NUM_WORKERS` | `ceil(numCPU × 1.5)` | Rayon thread-pool size |
| `PS_STACK_KB` | `2048` | Stack size per rayon worker in KiB |

Rust workers can safely exceed the core count (`ceil(1.5 ×`) because rayon
threads are OS-level — when one thread stalls on a DRAM cache miss, macOS/Linux
schedules another thread on the same core, hiding latency. Go goroutines cannot
exploit this because user-space preemption carries its own overhead.

---

## 7. PPS Warmup Lifecycle

A `(PPS, BinTag)` pair moves through three topology states:

```
           HandleAddPair()
  (absent) ──────────────────► WarmingUp
                                    │
                              cycle completes
                                    │
                                    ▼
                                  Active ◄──── ongoing cycles
                                    │
                           HandleRemovePair()
                                    │
                                    ▼
                                 Removed
```

**WarmingUp semantics:**
- The pair is added to `engine.Pairs` immediately so it participates in the
  *next* compute cycle.
- `GetTopK` returns **gRPC Unavailable (14)** for WarmingUp pairs — the
  caller should retry after the cycle interval (~5 s).
- After one full cycle completes, the pair is promoted to **Active** and
  `GetTopK` succeeds.

**Removed semantics:**
- `HandleRemovePair` removes the pair from `engine.Pairs` immediately.
- The pair is also marked Removed in the `TopologyState` so any in-flight
  `GetTopK` calls receive **gRPC NotFound (5)**.

**Go implementation note:** `ComputeEngine.promoteWarmingPairs()` is called at
the end of every `RunCycleWithDict`, draining `pendingWarmup` and calling
`Topology.Activate()` for each entry.

**Rust implementation note:** `promote_warming_pairs()` drains `pending_warmup`
via `drain(..)` — no allocation.

---

## 8. Benchmark Results

### Machine

Apple Silicon M-series (10 logical CPUs, unified memory architecture).  
Runs are **sequential and isolated** — only one process at a time to avoid
shared memory-bus and L3-cache contention.

### Workload (worst case)

| Parameter | Value |
|-----------|-------|
| PPS count | 200 |
| BinTags per PPS | 20 |
| Pairs | 4 000 |
| Orders in registry | 1 000 000 |
| Eligible orders per BinTag | 1 000 000 (every order eligible for every BinTag) |
| Top-K per table | 1 000 |
| Tables per pair | 4 (Heuristic, Large, PBT, Critical) |
| ItemDict TPID deltas per cycle | 100 000 |

This is the **absolute worst case** — real workloads have far fewer eligible
orders per BinTag (typically 10 000–100 000), which scales Phase 2 linearly
downward.

### Results — 200 PPS, 1 M orders per BinTag (absolute worst case)

| Implementation | Workers | Phase 1 | Phase 2 (stable) | Total/cycle | 5 s SLA | Go heap |
|----------------|---------|---------|-----------------|-------------|---------|---------|
| **go-pure** | 10 | 30–37 ms | **13–15 s** | ~14 s | ✗ FAIL | ~1.7 GB |
| **go-cgo** | 10 | 21–43 ms | **10–12 s** | ~11 s | ✗ FAIL | ~1.0 GB |
| **Rust** | 15 | 23–35 ms | **4.4–4.6 s** | ~4.6 s | ✓ PASS | ~0 GC |

### Results — 50 PPS, 1 M orders per BinTag (worst case, reduced PPS)

| Implementation | Workers | Phase 1 | Phase 2 (stable) | Total/cycle | 5 s SLA | Go heap |
|----------------|---------|---------|-----------------|-------------|---------|---------|
| **go-cgo** | 10 | 16–17 ms | **2.5–2.8 s** | ~2.7 s | ✓ PASS | ~1.1 GB |
| **Rust** | 15 | 16–29 ms | **1.1–1.2 s** | ~1.1 s | ✓ PASS | ~0 GC |

> **Phase 2 scales linearly with PPS count** (pairs = PPS × BinTags).  
> 4× fewer PPS → 4× faster Phase 2 (confirmed: 200 PPS / 50 PPS = 4 ×, ratios match).

### Phase 2 regression history (go-pure)

| Configuration | Phase 2 |
|---------------|---------|
| Before Phase 2 optimisations (30 workers, no pre-alloc) | ~10 s |
| After pre-alloc (40 workers, 576 MB scratch, 4 goroutines/thread) | **35–50 s** ← regression |
| Fixed (10 workers = 1 goroutine/thread, 144 MB scratch) | **13–15 s** |

The regression was caused by over-subscribing goroutines relative to OS threads.
Pre-allocating 40 × 14.4 MB = 576 MB of `workerScratch` when only 10 goroutines
could run simultaneously meant async preemption (SIGURG) evicted every
goroutine's scratch from L3 cache on each context switch. Fixing workers to
`numCPU` eliminates the inter-goroutine eviction.

---

## 9. Performance Analysis and Tuning Rationale

### Why Phase 2 is memory-bound

Phase 2 inner loop per `(PPS, BinTag)` pair:

```
for each eligible order ID oid:
    read  meta[oid]         ← random access into 32 MB OrderMeta array
    read  ppsRow[oid]       ← random access into 2 MB int16 PPSMatrix row
    write to scratch slice  ← sequential, cache-friendly
```

Random reads into 32 MB (OrderMeta) and 2 MB (PPSMatrix row) dominate.
Worst-case total reads: 4 000 pairs × 1 M orders × ~34 B = ~136 GB.
At ~50 GB/s memory bandwidth (Apple Silicon): ~2.7 s theoretical minimum.
Actual overhead (cache misses, instruction fetch, bounds checking): 3–5× above
theoretical.

### Why 1× cores is the correct default for Go

| Workers | Goroutines/thread | workerScratch in L3 | Phase 2 (200 PPS) |
|---------|-------------------|----------------------|---------|
| 40 | 4 | Constantly evicted | 35–50 s |
| 20 | 2 | Partial eviction | ~20 s |
| 10 (= numCPU) | 1 | Stays warm | **10–12 s** |

### Why Rust uses 1.5× cores

Rayon workers are OS threads, not goroutines. When a rayon thread stalls on a
DRAM access, the OS scheduler places another thread on the same core. This
hides cache-miss latency without user-space preemption overhead. In benchmarks,
15 rayon workers outperformed 10 on a 10-core machine:

| Rust workers | Phase 2 (200 PPS) |
|---|---|
| 10 (= numCPU) | ~13–14 s |
| 15 (= 1.5 × numCPU) | **~4.4–4.6 s** |
| 20 | ~5 s (diminishing returns) |

### Why Rust is ~2.3× faster than Go+CGo at same PPS count

1. **LLVM code generation** — more aggressive inlining, branch-prediction
   hints, and auto-vectorisation in the inner loop vs. Go's compiler.
2. **Bounds-check elision** — Rust release builds elide provable bounds checks;
   Go always inserts them.
3. **Zero GC overhead** — Rust has no GC write barriers; Go's runtime checks
   every pointer store even for pointer-free structs.
4. **`jemalloc`** — Rust uses `tikv-jemallocator` for reduced allocator
   fragmentation; Go uses its own GC allocator.

The ~2.3× gap at 200 PPS (4.5 s vs 10.6 s) is **not a tuning gap** — it is the
baseline cost of Go's safety abstractions. Both implementations pass the 5 s SLA
at 50 PPS worst case, and at realistic eligible-order counts (≤ 100 K) both
pass at 200 PPS with substantial headroom.

---

## 10. Memory Layout and Sizing

### Per-pod memory breakdown (worst case, 1 M orders)

| Structure | Size | Notes |
|-----------|------|-------|
| PPSMatrix `[200 × 1M × 2B]` | **400 MB** | int16 score per (PPS, order) |
| BinTagToOrders index `[20 × 1M × 4B]` | **80 MB** | uint32 order IDs per BinTag |
| TPIDToOrders index | ~4 MB | 100 K TPIDs × ~10 orders each |
| OrderMeta registry `[1M × 32B]` | **32 MB** | metadata per order slot |
| ReadBuffer (output) `[4K × 4 × 1K × 4B]` | **64 MB** | ranked output, swapped atomically |
| workerScratch (10 workers × 14.4 MB) | **144 MB** | pre-allocated; no GC |
| **Baseline total** | **~724 MB** | |
| Go GC overhead (live × GCPercent/100) | +100–600 MB | depends on `PS_GCPERCENT` |
| **go-pure observed RSS** | **~1.7 GB** | |
| **go-cgo observed RSS** | **~0.9 GB** | PPSMatrix on C heap; not GC-scanned |
| **Rust observed RSS** | **~680 MB** | no GC overhead |

### Scaling by PPS count (measured + extrapolated, 1 M eligible orders/BinTag)

| Active PPS | PPSMatrix | Pairs | go-cgo Phase 2 | Rust Phase 2 | 5 s SLA |
|-----------|-----------|-------|----------------|--------------|---------|
| 50 | 100 MB | 1 000 | **~2.6 s** ✓ measured | **~1.1 s** ✓ measured | Both pass |
| 100 | 200 MB | 2 000 | ~5.3 s | ~2.3 s | Rust passes; Go borderline |
| 200 (max) | 400 MB | 4 000 | **~10.6 s** ✓ measured | **~4.5 s** ✓ measured | Rust passes; Go fails |

> Phase 2 scales linearly with PPS count (validated at 50 and 200 PPS).
> At realistic load (≤ 100 K eligible orders/BinTag), divide all Phase 2 numbers
> by 10 — every configuration passes the 5 s SLA comfortably.

### Consumer thread sizing

The Kafka consumer threads are synchronous writers to the in-memory maps. Each
consumer thread requires approximately:

```
per_thread_memory ≈ 0 MB at steady state (writes into pre-allocated arrays)
peak during burst ≈ batch_size × avg_order_event_size  (~100 B/event)
```

Recommended starting point: `consumer_threads = min(Kafka_partition_count, numCPU / 2)`,
leaving the other half for compute. Adjust dynamically based on observed consumer
lag. **[OPEN: finalize consumer thread count.]**

---

## 11. Max PPS per Pod, Pod Count, and Autoscaling

### Target: 500 ms – 1 s per compute cycle

Phase 2 time scales linearly with `PPS × eligible_orders_per_BinTag`.
From the measured data:

```
Phase2_ms = PPS × eligible_orders × cost_per_order
  Rust    : cost ≈ 22.5 µs / (PPS × 1M eligible) = 0.0225 ms per PPS at 1M eligible
  Go+CGo  : cost ≈ 53 µs / (PPS × 1M eligible) = 0.053  ms per PPS at 1M eligible
```

### Max PPS per pod (Phase 2 budget = 90% of cycle target)

| Eligible orders / BinTag | Rust max PPS/pod (500 ms) | Rust max PPS/pod (1 s) | Go+CGo max PPS/pod (1 s) |
|--------------------------|--------------------------|------------------------|--------------------------|
| 1 000 000 (worst case) | **20** | **44** | **17** |
| 500 000 | 40 | 80 | 34 |
| 200 000 | 100 | 200 (all) | 85 |
| 100 000 | 200 (all) | 200 (all) | 170 |
| 50 000 (estimated prod) | 200 (all) | 200 (all) | 200 (all) |

> At the expected production eligible-order density (50 K – 100 K per BinTag),
> **a single pod handles all 200 PPS within the 1 s target for both languages.**
> Multiple pods are only needed at near-worst-case eligibility loads.

### Pod count for 200 total PPS

| Eligible orders / BinTag | Rust pods (500 ms target) | Rust pods (1 s target) | Go+CGo pods (1 s target) |
|--------------------------|--------------------------|------------------------|--------------------------|
| 1 000 000 | 10 | 5 | 12 |
| 200 000 | 2 | 1 | 3 |
| 100 000 | 1 | 1 | 2 |
| 50 000 (est. prod) | **1** | **1** | **1** |

### Autoscale and PPS lifecycle management

```
           PPS count on pod     eligible orders/BinTag
                  │                       │
                  ▼                       ▼
         Compute expected Phase 2 time
                  │
         ┌────────┴─────────┐
         │                   │
   < 80% target         ≥ 80% target
         │                   │
    No action          SCALE-OUT:
                       1. Determine PPS slice to migrate
                       2. Provision new pod (Kubernetes HPA or custom controller)
                       3. New pod seeds data from Kafka replay / snapshot
                       4. WarmingUp state → 1 full cycle → Active
                       5. Update gRPC load-balancer routing table
                       6. Old pod sheds migrated PPS via HandleRemovePair()
```

**Scale-out trigger (recommended metric):**
```
observed_phase2_ms / cycle_target_ms > 0.80
```
Monitor via the `CycleStats.Phase2Ms` field exposed as a Prometheus gauge.
Alert at 80 %, scale at 90 %.

**Scale-in trigger:**
```
observed_phase2_ms / cycle_target_ms < 0.25  (sustained 3 consecutive cycles)
```
Drain pod: migrate its PPS slice to a sibling pod, wait one cycle, terminate.

**Warmup on new PPS login:**

```
Butler/topology event: PPS X logs in
        │
        ▼
Priority Service receives AddPair events (all 20 BinTags for PPS X)
        │
        ▼
ComputeEngine.HandleAddPair():
  - Insert PPS X row in PPSMatrix (pre-zeroed)
  - Add pairs [PPS X, BinTag 1..20] to engine.Pairs in WarmingUp state
  - Seed initial scores from Kafka replay or snapshot
        │
        ▼
Next compute cycle runs (~5 s):
  - Phase 1: applies ItemDict delta to PPS X row
  - Phase 2: ranks all 20 BinTag pairs for PPS X
  - PublishReadBuffer with PPS X results
        │
        ▼
promoteWarmingPairs(): PPS X pairs → Active
GetTopK now serves PPS X results; circuit-breaker UNAVAILABLE lifted
```

**Warmup on new BinTag addition under existing PPS:**

```
Butler event: BinTag Y added under PPS X
        │
        ▼
HandleAddPair(PPS X, BinTag Y): WarmingUp state
        │
        ▼
BinTagToOrders[Y] built from existing order-events replay
        │
        ▼
Next cycle ranks pair (PPS X, BinTag Y) → Active
```

**PPS logout / removal:**

```
Butler event: PPS X logs out
        │
        ├── HandleRemovePair() for each BinTag under PPS X
        │     → pairs removed from engine.Pairs immediately
        │     → topology state → Removed
        │     → GetTopK returns gRPC NOT_FOUND (5) immediately
        │
        └── PPSMatrix row for PPS X zeroed lazily (next ingest cycle)
```

### Memory per pod by PPS count (observed)

| Active PPS | PPSMatrix | go-cgo Go heap (observed) | Rust RSS (est.) |
|-----------|-----------|--------------------------|-----------------|
| 50 | 100 MB | ~1.1 GB | ~350 MB |
| 100 | 200 MB | ~1.2 GB | ~480 MB |
| 200 | 400 MB | ~1.0 GB* | ~680 MB |

> \* go-cgo PPSMatrix is on the **C heap** (not Go heap), so Go-heap RSS changes
> little with PPS count. Total process RSS for 200 PPS go-cgo ≈ 1.4 GB
> (Go heap ~1.0 GB + C heap ~400 MB).

**Recommended Kubernetes node profile (Go+CGo, production):**

| PPS per pod | vCPU | RAM | `PS_MEM_LIMIT_MB` |
|------------|------|-----|-------------------|
| ≤ 50 | 8 | 8 GB | 4000 |
| 50 – 100 | 10 | 12 GB | 6000 |
| 100 – 200 | 16 | 16 GB | 8000 |

---

## 12. Key Design Decisions

### Double-buffer (lock-free reads)

The ranked output is stored in a `DoubleBuffer` — an `atomic.Pointer[ReadBuffer]`
(Go) / `ArcSwap<ReadBuffer>` (Rust). The writer builds a new `ReadBuffer` in
private, then publishes it with a single pointer swap. Readers (`GetTopK`) load
the pointer atomically — **zero locks on the hot read path**.

### Single-pass Phase 2

Each eligible order is iterated **once** per pair, simultaneously filling all
four scratch slices (`Heuristic`, `Large`, `PBT`, `Critical`). The previous
four-pass approach read `OrderMeta` four times per order — 4× more L3 misses.
Single-pass reduced Phase 2 time by ~40 % at full 1 M orders.

### Static work partitioning

Phase 2 goroutines each own a contiguous slice of pairs `[lo, hi)`. No
channel, no work-stealing. This mirrors Rust's `par_chunks_exact_mut` and
ensures the goroutine's scratch stays in the same cache region throughout its
work slice.

### PPSMatrix as flat int16 array

Scores are stored as int16 (not float32) — `stored_i16 = int16(float32_score × 100)`.
This halves the matrix from 800 MB (float32) to 400 MB (int16), reducing L3
pressure on the hot Phase 1 write path. The `ScoreScale = 100.0` constant
preserves 2 decimal places of precision.

### Score is per-PPS, eligibility is per-BinTag

Score depends only on the PPS (item fit). BinTag determines whether an order
is **eligible** for the slot — a filter, not a scoring dimension. This allows
Phase 2 to reuse the same PPS row score for all 20 BinTags under one PPS,
rather than computing 4 000 independent score vectors.

---

## 13. Open Questions

| # | Question | Current status |
|---|----------|----------------|
| 1 | **Worker count finalisation** | Default = `numCPU`; must equal `GOMAXPROCS`. Override via `PS_NUM_WORKERS`. |
| 2 | **GC tuning in production** | `PS_GCPERCENT=-1` + `PS_MEM_LIMIT_MB` is the most predictable option; requires sizing headroom. |
| 3 | **Consumer thread count** | Determined dynamically; starting point = `Kafka_partitions / 2`. |
| 4 | **PPS state-change mechanism** | API endpoint trigger vs. event-driven (Kafka). Two event types: add / remove. |
| 5 | **Snapshot / recovery** | Not yet designed. Risk: orders missing from heap during recovery window. |
| 6 | **Horizontal scaling definition** | Stateful nature means naive horizontal split doesn't work. Load metric TBD. |
| 7 | **Machine configuration** | Low-memory multi-core vs. high-memory fewer-core. Awaiting DevOps input. |
| 8 | **Go vs. Rust for production** | Go is the current preference. Rust gives ~1.5× Phase 2 speedup at higher onboarding cost. |
| 9 | **Single binary vs. separate deployables** | Single binary simplifies ops; separate allows independent scaling of ingestion vs. serving. |
| 10 | **Redis externalisation** | Optional. Adds network round-trip; enables cross-pod cache sharing. |
