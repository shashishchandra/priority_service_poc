use arc_swap::ArcSwap;
use std::sync::Arc;

// ─── Constants ───────────────────────────────────────────────────────────────

pub const NUM_ORDERS: usize = 1_000_000;
pub const NUM_PPS: usize = 200;
pub const NUM_BIN_TAGS_PER_PPS: usize = 20;
pub const PAIRS_PER_POD: usize = NUM_PPS * NUM_BIN_TAGS_PER_PPS;
pub const TOP_K: usize = 1_000;
pub const NUM_TABLES: usize = 4;
pub const TBL_HEURISTIC: usize = 0;
pub const TBL_LARGE: usize = 1;
pub const TBL_PBT: usize = 2;
pub const TBL_CRITICAL: usize = 3;
pub const LARGE_QTY_THRESHOLD: f32 = 10.0;
pub const CRITICAL_CUTOFF_SECS: i64 = 1_800;

/// Multiplier used to convert float scores to i16 integer scores.
/// score_i16 = (f32_score * SCORE_SCALE) clamped to i16 range.
pub const SCORE_SCALE: f32 = 100.0;

/// Orders not updated within this many seconds are evicted from the cache.
pub const ORDER_TTL_SECS: i64 = 3_600;

// ─── PairKey ─────────────────────────────────────────────────────────────────

pub type PairKey = u64;

#[inline]
pub fn make_pair_key(pps_id: u32, bintag_id: u32) -> PairKey {
    ((pps_id as u64) << 32) | (bintag_id as u64)
}

// ─── OrderMeta ───────────────────────────────────────────────────────────────

/// Per-order metadata stored in the OrderRegistry.
/// `inserted_at_secs` tracks when the order was last upserted for TTL eviction.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct OrderMeta {
    pub order_id: u32,
    pub required_qty: f32,
    /// Unix seconds; -1 = no PBT deadline.
    pub pbt_deadline: i64,
    pub active: bool,
    /// Unix seconds when this order was last upserted (for TTL eviction).
    pub inserted_at_secs: i64,
}

// ─── PPSMatrix ───────────────────────────────────────────────────────────────

/// Flat row-major [NUM_PPS × NUM_ORDERS] score matrix using i16 values.
/// Row `pps_id` is `data[pps_id * NUM_ORDERS .. (pps_id + 1) * NUM_ORDERS]`.
/// Scores are stored as (f32_score * SCORE_SCALE) clamped to i16 range.
/// Phase 1 sweeps all NUM_PPS rows; Phase 2 reads pps_row[bintag_eligible_oid].
pub struct PPSMatrix {
    pub data: Vec<i16>,
    pub num_pps: usize,
}

impl PPSMatrix {
    pub fn new(num_pps: usize) -> Self {
        Self {
            data: vec![0i16; num_pps * NUM_ORDERS],
            num_pps,
        }
    }

    #[inline]
    pub fn row(&self, pps_id: usize) -> &[i16] {
        let start = pps_id * NUM_ORDERS;
        &self.data[start..start + NUM_ORDERS]
    }

    #[inline]
    pub fn row_mut(&mut self, pps_id: usize) -> &mut [i16] {
        let start = pps_id * NUM_ORDERS;
        &mut self.data[start..start + NUM_ORDERS]
    }

    /// Zero out the score slot for `order_id` across all PPS rows.
    /// Called during order eviction.
    pub fn evict_order(&mut self, order_id: usize) {
        if order_id >= NUM_ORDERS {
            return;
        }
        for pps_id in 0..self.num_pps {
            self.data[pps_id * NUM_ORDERS + order_id] = 0;
        }
    }
}

// ─── ReadBuffer ──────────────────────────────────────────────────────────────

pub struct ReadBuffer {
    pub data: Vec<i32>,
    pub num_pairs: usize,
}

impl ReadBuffer {
    pub fn new(num_pairs: usize) -> Self {
        Self {
            data: vec![-1_i32; num_pairs * NUM_TABLES * TOP_K],
            num_pairs,
        }
    }

    #[inline]
    fn pair_stride() -> usize {
        NUM_TABLES * TOP_K
    }

    pub fn get_list(&self, pair_idx: usize, table: usize, k: usize) -> Vec<i32> {
        let start = pair_idx * Self::pair_stride() + table * TOP_K;
        let end = start + k.min(TOP_K);
        let slice = &self.data[start..end];
        let count = slice.iter().position(|&x| x == -1).unwrap_or(slice.len());
        slice[..count].to_vec()
    }

    pub fn list_mut(&mut self, pair_idx: usize, table: usize) -> &mut [i32] {
        let start = pair_idx * Self::pair_stride() + table * TOP_K;
        &mut self.data[start..start + TOP_K]
    }
}

// ─── DoubleBuffer ────────────────────────────────────────────────────────────

pub struct DoubleBuffer {
    read: ArcSwap<ReadBuffer>,
}

impl DoubleBuffer {
    pub fn new(num_pairs: usize) -> Self {
        Self {
            read: ArcSwap::from_pointee(ReadBuffer::new(num_pairs)),
        }
    }

    pub fn load(&self) -> arc_swap::Guard<Arc<ReadBuffer>> {
        self.read.load()
    }

    pub fn publish(&self, buf: ReadBuffer) {
        self.read.store(Arc::new(buf));
    }
}

// ─── PairInfo ────────────────────────────────────────────────────────────────

pub struct PairInfo {
    pub pps_id: u32,
    pub bintag_id: u32,
    pub key: PairKey,
}

// ─── CycleStats ──────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug)]
pub struct CycleStats {
    pub phase1_ms: u64,
    pub phase2_ms: u64,
    pub total_ms: u64,
    pub sla_ok: bool,
}

// ─── SEntry ──────────────────────────────────────────────────────────────────

/// Scratch entry for Quickselect. Uses i32 so it holds both:
/// - i16 PPSMatrix scores upcast to i32 (Heuristic/Large tables)
/// - negated relative-deadline in seconds cast to i32 (PBT/Critical tables)
#[derive(Clone, Copy)]
pub struct SEntry {
    pub score: i32,
    pub id: i32,
}
