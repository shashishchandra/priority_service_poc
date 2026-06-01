// Use jemalloc as the global allocator in release builds.
// Benefits: lower fragmentation under sustained 1M-order churn, faster
// deallocation of per-cycle scratch buffers, and background purging of
// freed pages back to the OS.
#[cfg(not(test))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use priority_service::compute::ComputeEngine;
use priority_service::config::{init_from_env, print_resource_config};
use priority_service::mock::{MockItemDictClient, MockKafkaOrderStream};
use priority_service::server::{GetTopKRequest, PriorityServer};
use priority_service::topology::TopologyState;
use priority_service::types::{
    make_pair_key, DoubleBuffer, OrderMeta, PairInfo, SCORE_SCALE,
    NUM_BIN_TAGS_PER_PPS, NUM_ORDERS, NUM_PPS, PAIRS_PER_POD,
};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn main() {
    // ── Resource config (env-var driven) ─────────────────────────────────────
    let res_cfg = init_from_env();

    // Build rayon thread pool with configured worker count and stack size.
    // Must happen before any rayon parallel work.
    rayon::ThreadPoolBuilder::new()
        .num_threads(res_cfg.num_workers)
        .stack_size(res_cfg.stack_bytes)
        .build_global()
        .expect("rayon thread pool init failed");

    println!("=== Priority Service — PPSMatrix / int16 / OrderCache Demo ===\n");
    print_resource_config(&res_cfg);
    println!(
        "Config: {} PPS × {} BinTags = {} pairs | PPSMatrix: {} PPS × {}M orders × 2B = {} MB",
        NUM_PPS,
        NUM_BIN_TAGS_PER_PPS,
        PAIRS_PER_POD,
        NUM_PPS,
        NUM_ORDERS / 1_000_000,
        NUM_PPS * NUM_ORDERS * 2 / 1_000_000
    );
    println!();

    // ── 1. Topology ───────────────────────────────────────────────────────────
    let topology = Arc::new(TopologyState::new());
    let mut pairs: Vec<PairInfo> = Vec::with_capacity(PAIRS_PER_POD);
    for pps_id in 0..NUM_PPS as u32 {
        for bintag_id in 0..NUM_BIN_TAGS_PER_PPS as u32 {
            topology.activate(pps_id, bintag_id);
            pairs.push(PairInfo {
                pps_id,
                bintag_id,
                key: make_pair_key(pps_id, bintag_id),
            });
        }
    }
    println!("Topology: {} pairs activated.\n", pairs.len());

    // ── 2. Engine ─────────────────────────────────────────────────────────────
    let db = Arc::new(DoubleBuffer::new(pairs.len()));
    let mut engine = ComputeEngine::new(pairs, db.clone(), topology.clone());
    engine.item_dict_client = MockItemDictClient::new(99, 100_000);

    // ── 3. Ingest 1M orders (worst case: all bintags eligible) ───────────────
    let order_stream = MockKafkaOrderStream::new(
        7,
        NUM_ORDERS as u32,
        100_000,
        NUM_BIN_TAGS_PER_PPS as u32,
    );
    println!("Ingesting {} orders (all bintags eligible) ...", NUM_ORDERS);
    let t_ingest = Instant::now();
    let insert_ts = now_secs();

    for order_id in 0..NUM_ORDERS as u32 {
        let ev = order_stream.next(order_id);

        // Write base score to every PPS row in PPSMatrix.
        // Score is per-PPS (same for all BinTags under a PPS).
        let base_i16 = (ev.base_score * SCORE_SCALE)
            .clamp(i16::MIN as f32, i16::MAX as f32) as i16;
        for pps_id in 0..NUM_PPS {
            engine.matrix.row_mut(pps_id)[ev.order_id as usize] = base_i16;
        }

        engine.registry.upsert(OrderMeta {
            order_id: ev.order_id,
            required_qty: ev.required_qty,
            pbt_deadline: ev.pbt_deadline,
            active: true,
            inserted_at_secs: insert_ts,
        });
        engine
            .indexes
            .add_order(ev.order_id, &ev.tpids, &ev.eligible_bintag_ids);
    }
    println!(
        "Ingestion complete in {:.1}s.\n",
        t_ingest.elapsed().as_secs_f32()
    );

    // ── Worst-case verification ───────────────────────────────────────────────
    {
        let active_count = engine.registry.meta.iter().filter(|m| m.active).count();
        let bt_sizes: Vec<usize> = (0..NUM_BIN_TAGS_PER_PPS as u32)
            .map(|bt| engine.indexes.bintag_to_orders.get(&bt).map_or(0, |v| v.len()))
            .collect();
        let bt_min = bt_sizes.iter().min().copied().unwrap_or(0);
        let bt_max = bt_sizes.iter().max().copied().unwrap_or(0);
        let tpid_count = engine.indexes.tpid_to_orders.len();
        println!("=== Worst-case verification ===");
        println!("  Active orders in registry : {}", active_count);
        println!("  BinTag IDs in index       : {}", bt_sizes.len());
        println!("  Eligible orders per BinTag: min={} max={} (expected {})", bt_min, bt_max, NUM_ORDERS);
        println!("  TPIDs in tpid_to_orders   : {}", tpid_count);
        println!("  Pairs to rank in Phase 2  : {}", engine.pairs.len());
        println!("  PPSMatrix size            : {} MB", NUM_PPS * NUM_ORDERS * 2 / 1_000_000);
        println!();
    }

    // ── 4. Run 3 compute cycles ───────────────────────────────────────────────
    for cycle in 1..=3_u32 {
        println!("--- Cycle {} ---", cycle);
        let stats = engine.run_cycle();
        println!(
            "  Phase 1: {} ms | Phase 2: {} ms | Total: {} ms | SLA OK: {}",
            stats.phase1_ms, stats.phase2_ms, stats.total_ms, stats.sla_ok
        );
    }
    println!();

    // ── 5. GetTopK demo ───────────────────────────────────────────────────────
    let server = PriorityServer::new(topology.clone(), db.clone(), &engine.pairs);
    let sample_pps = 3_u32;
    let sample_bt = 7_u32;
    let req = GetTopKRequest {
        pps_id: sample_pps,
        bintag_id: sample_bt,
        k: 10,
    };
    match server.get_top_k(&req) {
        Ok(resp) => {
            println!("GetTopK(pps={}, bintag={}, k=10):", sample_pps, sample_bt);
            println!("  Heuristic top-10 : {:?}", resp.heuristic);
            println!("  Large     top-10 : {:?}", resp.large);
            println!("  PBT       top-10 : {:?}", resp.pbt);
            println!("  Critical  top-10 : {:?}", resp.critical);
        }
        Err(e) => println!("GetTopK error: {:?}", e),
    }
    println!();

    // ── 6. Circuit breaker: WarmingUp pair ───────────────────────────────────
    let new_pps = 99_u32;
    let new_bt = 42_u32;
    engine.handle_add_pair(new_pps, new_bt);
    let key = make_pair_key(new_pps, new_bt);
    println!(
        "New pair ({}, {}) topology state: {:?}",
        new_pps,
        new_bt,
        topology.get(key)
    );
    let server2 = PriorityServer::new(topology.clone(), db.clone(), &engine.pairs);
    let req2 = GetTopKRequest {
        pps_id: new_pps,
        bintag_id: new_bt,
        k: 5,
    };
    match server2.get_top_k(&req2) {
        Err(e) => println!(
            "Circuit breaker for WarmingUp pair: {:?} (gRPC code {})",
            e,
            e.grpc_code()
        ),
        Ok(_) => println!("Unexpected success for WarmingUp pair!"),
    }

    engine.run_cycle();
    println!(
        "After cycle, pair ({}, {}) topology state: {:?}",
        new_pps,
        new_bt,
        topology.get(key)
    );

    // ── 7. Order eviction demo ────────────────────────────────────────────────
    println!("\n--- Order eviction demo ---");
    let sample_order_id = 42_u32;
    println!("Evicting order {} (simulates Kafka order-removed event)...", sample_order_id);
    engine.handle_order_removed(sample_order_id);
    println!(
        "Order {} active after eviction: {}",
        sample_order_id,
        engine.registry.meta[sample_order_id as usize].active
    );

    println!("\n=== Demo complete ===");
}
