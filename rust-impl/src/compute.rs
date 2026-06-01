use crate::mock::{ItemDictUpdate, MockItemDictClient};
use crate::registry::{InvertedIndexes, OrderRegistry};
use crate::topology::TopologyState;
use crate::types::*;
use rayon::prelude::*;
use std::cell::RefCell;
use std::sync::Arc;
use std::time::Instant;

// ─── Thread-local scratch buffer ─────────────────────────────────────────────

struct WorkerScratch {
    heuristic: Vec<SEntry>,
    large:     Vec<SEntry>,
    pbt:       Vec<SEntry>,
    critical:  Vec<SEntry>,
}

thread_local! {
    static SCRATCH: RefCell<WorkerScratch> = RefCell::new(WorkerScratch {
        heuristic: Vec::with_capacity(NUM_ORDERS),
        large:     Vec::with_capacity(NUM_ORDERS),
        pbt:       Vec::with_capacity(NUM_ORDERS),
        critical:  Vec::with_capacity(NUM_ORDERS),
    });
}

// ─── ComputeEngine ───────────────────────────────────────────────────────────

pub struct ComputeEngine {
    /// [NUM_PPS × NUM_ORDERS] i16 score matrix; row = pps_id.
    pub matrix: PPSMatrix,
    pub registry: OrderRegistry,
    pub indexes: InvertedIndexes,
    pub pairs: Vec<PairInfo>,
    pub db: Arc<DoubleBuffer>,
    pub topology: Arc<TopologyState>,
    pub item_dict_client: MockItemDictClient,
    pending_warmup: Vec<(u32, u32)>,
}

impl ComputeEngine {
    pub fn new(
        pairs: Vec<PairInfo>,
        db: Arc<DoubleBuffer>,
        topology: Arc<TopologyState>,
    ) -> Self {
        Self {
            matrix: PPSMatrix::new(NUM_PPS),
            registry: OrderRegistry::new(),
            indexes: InvertedIndexes::new(),
            pairs,
            db,
            topology,
            item_dict_client: MockItemDictClient::new(42, 100_000),
            pending_warmup: Vec::new(),
        }
    }

    pub fn run_cycle(&mut self) -> CycleStats {
        let item_dict = self.item_dict_client.fetch();
        self.run_cycle_with_dict(&item_dict)
    }

    pub fn run_cycle_with_dict(&mut self, item_dict: &ItemDictUpdate) -> CycleStats {
        let cycle_start = Instant::now();
        let now_secs = current_unix_secs();

        // Evict orders whose TTL has expired before computing new rankings.
        self.registry
            .purge_expired(now_secs, &mut self.indexes, &mut self.matrix);

        let d1 = self.phase1_update(item_dict);
        let (new_buf, d2) = self.phase2_rank(now_secs);

        self.db.publish(new_buf);
        self.promote_warming_pairs();

        let total_ms = cycle_start.elapsed().as_millis() as u64;
        CycleStats {
            phase1_ms: d1.as_millis() as u64,
            phase2_ms: d2.as_millis() as u64,
            total_ms,
            sla_ok: total_ms < 5_000,
        }
    }

    // ── Phase 1 ──────────────────────────────────────────────────────────────

    fn phase1_update(&mut self, item_dict: &ItemDictUpdate) -> std::time::Duration {
        let t0 = Instant::now();

        // Accumulate a single i32 delta vector across all TPID updates.
        // i32 avoids overflow when summing multiple i16-scaled deltas.
        let mut delta = vec![0i32; NUM_ORDERS];

        for update in &item_dict.updates {
            let diff = update.new_contrib - update.old_contrib;
            if diff == 0.0 {
                continue;
            }
            let diff_i16 = (diff * SCORE_SCALE) as i32;
            if let Some(order_ids) = self.indexes.tpid_to_orders.get(&update.tpid) {
                for &oid in order_ids {
                    if (oid as usize) < NUM_ORDERS {
                        delta[oid as usize] += diff_i16;
                    }
                }
            }
        }

        // Apply delta to every PPS row in parallel.
        // Only NUM_PPS rows (e.g. 50) × NUM_ORDERS — much smaller than the old
        // num_pairs × NUM_ORDERS layout.
        self.matrix
            .data
            .par_chunks_exact_mut(NUM_ORDERS)
            .for_each(|row| {
                for (s, &d) in row.iter_mut().zip(delta.iter()) {
                    *s = s.saturating_add(d.clamp(i16::MIN as i32, i16::MAX as i32) as i16);
                }
            });

        t0.elapsed()
    }

    // ── Phase 2 ──────────────────────────────────────────────────────────────

    fn phase2_rank(&self, now_secs: i64) -> (ReadBuffer, std::time::Duration) {
        let t0 = Instant::now();
        let num_pairs = self.pairs.len();
        let mut write_buf = ReadBuffer::new(num_pairs);

        let critical_deadline = now_secs + CRITICAL_CUTOFF_SECS;
        let matrix_ref: &PPSMatrix = &self.matrix;
        let registry_ref: &OrderRegistry = &self.registry;
        let indexes_ref: &InvertedIndexes = &self.indexes;
        let pairs_ref: &[PairInfo] = &self.pairs;

        write_buf
            .data
            .par_chunks_exact_mut(NUM_TABLES * TOP_K)
            .enumerate()
            .for_each(|(pair_idx, chunk)| {
                if pair_idx >= pairs_ref.len() {
                    return;
                }
                let pair = &pairs_ref[pair_idx];
                let pps_row = matrix_ref.row(pair.pps_id as usize);
                let eligible: &[u32] = indexes_ref
                    .bintag_to_orders
                    .get(&pair.bintag_id)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);

                SCRATCH.with(|cell| {
                    let mut sc = cell.borrow_mut();

                    // ── Single pass: fill all four scratch slices simultaneously ──
                    sc.heuristic.clear();
                    sc.large.clear();
                    sc.pbt.clear();
                    sc.critical.clear();

                    for &oid in eligible {
                        let meta = &registry_ref.meta[oid as usize];
                        if !meta.active {
                            continue;
                        }
                        let score = pps_row[oid as usize] as i32;
                        sc.heuristic.push(SEntry { score, id: oid as i32 });
                        if meta.required_qty > LARGE_QTY_THRESHOLD {
                            sc.large.push(SEntry { score, id: oid as i32 });
                        }
                        if meta.pbt_deadline > 0 {
                            let dl_score = (now_secs - meta.pbt_deadline) as i32;
                            sc.pbt.push(SEntry { score: dl_score, id: oid as i32 });
                            if meta.pbt_deadline <= critical_deadline {
                                sc.critical.push(SEntry { score: dl_score, id: oid as i32 });
                            }
                        }
                    }

                    fill_topk_desc(chunk, TBL_HEURISTIC, &mut sc.heuristic);
                    fill_topk_desc(chunk, TBL_LARGE, &mut sc.large);
                    fill_topk_desc(chunk, TBL_PBT, &mut sc.pbt);
                    fill_topk_desc(chunk, TBL_CRITICAL, &mut sc.critical);
                });
            });

        (write_buf, t0.elapsed())
    }

    // ── Warmup ────────────────────────────────────────────────────────────────

    fn promote_warming_pairs(&mut self) {
        for (pps_id, bintag_id) in self.pending_warmup.drain(..) {
            self.topology.activate(pps_id, bintag_id);
        }
    }

    // ── Topology handlers ─────────────────────────────────────────────────────

    pub fn handle_add_pair(&mut self, pps_id: u32, bintag_id: u32) {
        self.topology.add_warming_up(pps_id, bintag_id);
        self.pending_warmup.push((pps_id, bintag_id));

        let key = make_pair_key(pps_id, bintag_id);
        let already_tracked = self.pairs.iter().any(|p| p.key == key);
        if !already_tracked {
            self.pairs.push(PairInfo { pps_id, bintag_id, key });
            // Grow PPSMatrix if this pps_id exceeds current row count.
            let required = pps_id as usize + 1;
            if required > self.matrix.num_pps {
                let extra_rows = required - self.matrix.num_pps;
                self.matrix.data.extend(vec![0i16; extra_rows * NUM_ORDERS]);
                self.matrix.num_pps = required;
            }
        }
    }

    pub fn handle_remove_pair(&mut self, pps_id: u32, bintag_id: u32) {
        self.topology.remove(pps_id, bintag_id);
        self.pending_warmup.retain(|&(p, b)| !(p == pps_id && b == bintag_id));
        // Remove from pairs list.
        let key = make_pair_key(pps_id, bintag_id);
        self.pairs.retain(|p| p.key != key);
    }

    /// Evict an order on a Kafka removal event.
    pub fn handle_order_removed(&mut self, order_id: u32) {
        self.registry
            .evict_order(order_id, &mut self.indexes, &mut self.matrix);
    }
}

// ─── Quickselect helper ──────────────────────────────────────────────────────

/// Partially sort `scratch` so the top-k highest-scoring entries are in
/// scratch[0..k], then sort that prefix descending, and write to chunk[table*TOP_K..].
fn fill_topk_desc(chunk: &mut [i32], table: usize, scratch: &mut Vec<SEntry>) {
    let k = scratch.len().min(TOP_K);
    if k == 0 {
        return;
    }
    if scratch.len() > k {
        scratch.select_nth_unstable_by(k - 1, |a, b| b.score.cmp(&a.score));
    }
    scratch[..k].sort_unstable_by(|a, b| b.score.cmp(&a.score));
    let slot = &mut chunk[table * TOP_K..(table + 1) * TOP_K];
    for (i, e) in scratch[..k].iter().enumerate() {
        slot[i] = e.id;
    }
}

// ─── Helper ──────────────────────────────────────────────────────────────────

pub fn current_unix_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
