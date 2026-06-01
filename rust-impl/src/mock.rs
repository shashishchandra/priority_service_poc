use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

// ─── ItemDict types ──────────────────────────────────────────────────────────

/// One TPID's contribution change (old → new).
pub struct TPIDUpdate {
    pub tpid: u64,
    pub old_contrib: f32,
    pub new_contrib: f32,
}

/// A batch of TPID score changes returned by the external inventory API.
pub struct ItemDictUpdate {
    pub updates: Vec<TPIDUpdate>,
}

// ─── MockItemDictClient ──────────────────────────────────────────────────────

/// Simulates an external inventory/item-dictionary API.
/// Each call to `fetch()` sleeps 100–200 ms (simulating network latency) and
/// then returns a delta for `update_fraction` of all TPIDs.
pub struct MockItemDictClient {
    rng: Mutex<StdRng>,
    tpid_scores: Mutex<HashMap<u64, f32>>,
    pub num_tpids: u64,
    /// Fraction of TPIDs updated on each fetch (e.g. 0.1 = 10 %).
    pub update_fraction: f64,
}

impl MockItemDictClient {
    pub fn new(seed: u64, num_tpids: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut scores = HashMap::with_capacity(num_tpids as usize);
        for tpid in 0..num_tpids {
            scores.insert(tpid, rng.gen::<f32>() * 100.0);
        }
        Self {
            rng: Mutex::new(StdRng::seed_from_u64(seed.wrapping_add(1))),
            tpid_scores: Mutex::new(scores),
            num_tpids,
            update_fraction: 1.0,
        }
    }

    /// Simulate the external fetch: sleep 100–200 ms, then produce deltas.
    pub fn fetch(&self) -> ItemDictUpdate {
        // Simulate network latency.
        let sleep_ms = {
            let mut rng = self.rng.lock().unwrap();
            rng.gen_range(100_u64..=200_u64)
        };
        std::thread::sleep(Duration::from_millis(sleep_ms));

        let num_updates = (self.num_tpids as f64 * self.update_fraction) as u64;
        let num_updates = num_updates.max(1);

        let mut scores = self.tpid_scores.lock().unwrap();
        let mut rng = self.rng.lock().unwrap();

        let mut updates = Vec::with_capacity(num_updates as usize);
        for _ in 0..num_updates {
            let tpid = rng.gen_range(0..self.num_tpids);
            let old_contrib = *scores.get(&tpid).unwrap_or(&0.0);
            let new_contrib = rng.gen::<f32>() * 100.0;
            scores.insert(tpid, new_contrib);
            updates.push(TPIDUpdate {
                tpid,
                old_contrib,
                new_contrib,
            });
        }

        ItemDictUpdate { updates }
    }
}

// ─── KafkaOrderEvent ─────────────────────────────────────────────────────────

/// Simulates an order event arriving on the Kafka order topic.
pub struct KafkaOrderEvent {
    pub order_id: u32,
    pub required_qty: f32,
    /// Unix seconds; -1 = no PBT deadline.
    pub pbt_deadline: i64,
    pub tpids: Vec<u64>,
    pub eligible_bintag_ids: Vec<u32>,
    pub base_score: f32,
}

// ─── MockKafkaOrderStream ────────────────────────────────────────────────────

/// Generates synthetic order events for load testing and unit tests.
pub struct MockKafkaOrderStream {
    rng: Mutex<StdRng>,
    pub num_orders: u32,
    pub num_tpids: u64,
    pub num_bintags: u32,
}

impl MockKafkaOrderStream {
    pub fn new(seed: u64, num_orders: u32, num_tpids: u64, num_bintags: u32) -> Self {
        Self {
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
            num_orders,
            num_tpids,
            num_bintags,
        }
    }

    /// Generate a single event for `order_id` with ALL bintag IDs eligible.
    /// Use this instead of `generate_batch` for full-1M-order worst-case benchmarks.
    pub fn next(&self, order_id: u32) -> KafkaOrderEvent {
        let mut rng = self.rng.lock().unwrap();
        let required_qty = rng.gen_range(1.0_f32..50.0_f32);
        let pbt_deadline: i64 = if rng.gen_bool(0.3) {
            1_700_000_000_i64 + rng.gen_range(0_i64..7200_i64)
        } else {
            -1
        };
        let num_tpids = rng.gen_range(1_usize..=5_usize);
        let tpids: Vec<u64> = (0..num_tpids)
            .map(|_| rng.gen_range(0..self.num_tpids))
            .collect();
        let base_score = rng.gen::<f32>() * 100.0;
        // All bintag IDs eligible — worst-case scenario.
        let eligible_bintag_ids: Vec<u32> = (0..self.num_bintags).collect();
        KafkaOrderEvent {
            order_id,
            required_qty,
            pbt_deadline,
            tpids,
            eligible_bintag_ids,
            base_score,
        }
    }

    /// Generate `count` random order events.
    pub fn generate_batch(&self, count: usize) -> Vec<KafkaOrderEvent> {
        let mut rng = self.rng.lock().unwrap();
        let mut events = Vec::with_capacity(count);

        for _ in 0..count {
            let order_id = rng.gen_range(0..self.num_orders);
            let required_qty = rng.gen_range(1.0_f32..50.0_f32);
            // ~30 % of orders get a PBT deadline within the next 2 hours.
            let pbt_deadline: i64 = if rng.gen_bool(0.3) {
                // Use a synthetic "now" base (epoch + some large offset) for tests.
                1_700_000_000_i64 + rng.gen_range(0_i64..7200_i64)
            } else {
                -1
            };

            // Each order references 1–5 TPIDs.
            let num_tpids = rng.gen_range(1_usize..=5_usize);
            let tpids: Vec<u64> = (0..num_tpids)
                .map(|_| rng.gen_range(0..self.num_tpids))
                .collect();

            // Each order is eligible for 1–4 bintags.
            let num_bintags = rng.gen_range(1_usize..=4_usize);
            let eligible_bintag_ids: Vec<u32> = (0..num_bintags)
                .map(|_| rng.gen_range(0..self.num_bintags))
                .collect();

            let base_score = rng.gen::<f32>() * 100.0;

            events.push(KafkaOrderEvent {
                order_id,
                required_qty,
                pbt_deadline,
                tpids,
                eligible_bintag_ids,
                base_score,
            });
        }

        events
    }
}

// ─── Topology events ─────────────────────────────────────────────────────────

pub enum TopologyEventKind {
    AddPPS,
    RemovePPS,
}

pub struct KafkaTopologyEvent {
    pub kind: TopologyEventKind,
    pub pps_id: u32,
    pub bintag_ids: Vec<u32>,
}

pub struct MockKafkaTopologyStream {
    rng: Mutex<StdRng>,
}

impl MockKafkaTopologyStream {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
        }
    }

    pub fn add_pps_event(&self, pps_id: u32, num_bintags: usize) -> KafkaTopologyEvent {
        let mut rng = self.rng.lock().unwrap();
        let bintag_ids: Vec<u32> = (0..num_bintags as u32)
            .map(|i| rng.gen_range(0..20_u32) + i * 20)
            .collect();
        KafkaTopologyEvent {
            kind: TopologyEventKind::AddPPS,
            pps_id,
            bintag_ids,
        }
    }

    pub fn remove_pps_event(&self, pps_id: u32) -> KafkaTopologyEvent {
        KafkaTopologyEvent {
            kind: TopologyEventKind::RemovePPS,
            pps_id,
            bintag_ids: vec![],
        }
    }
}
