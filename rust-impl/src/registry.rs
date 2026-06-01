use crate::types::{NUM_ORDERS, ORDER_TTL_SECS, OrderMeta, PPSMatrix};
use std::collections::HashMap;

// ─── OrderRegistry ───────────────────────────────────────────────────────────

pub struct OrderRegistry {
    pub meta: Vec<OrderMeta>,
}

impl OrderRegistry {
    pub fn new() -> Self {
        Self {
            meta: vec![OrderMeta::default(); NUM_ORDERS],
        }
    }

    pub fn upsert(&mut self, m: OrderMeta) {
        let idx = m.order_id as usize;
        if idx < NUM_ORDERS {
            self.meta[idx] = m;
        }
    }

    pub fn deactivate(&mut self, order_id: u32) {
        let idx = order_id as usize;
        if idx < NUM_ORDERS {
            self.meta[idx].active = false;
        }
    }

    /// Evict a single order: mark inactive, remove from inverted indexes,
    /// zero PPSMatrix slots. Used for Kafka order-removed events.
    pub fn evict_order(
        &mut self,
        order_id: u32,
        indexes: &mut InvertedIndexes,
        matrix: &mut PPSMatrix,
    ) {
        let idx = order_id as usize;
        if idx >= NUM_ORDERS || !self.meta[idx].active {
            return;
        }
        self.meta[idx].active = false;
        self.meta[idx].inserted_at_secs = 0;
        indexes.remove_order(order_id);
        matrix.evict_order(idx);
    }

    /// Scan and evict all orders whose TTL has expired.
    /// Call at the start of each compute cycle.
    pub fn purge_expired(
        &mut self,
        now_secs: i64,
        indexes: &mut InvertedIndexes,
        matrix: &mut PPSMatrix,
    ) {
        for idx in 0..NUM_ORDERS {
            // Read the fields we need before any mutation.
            let active = self.meta[idx].active;
            let inserted_at = self.meta[idx].inserted_at_secs;
            let order_id = self.meta[idx].order_id;

            if active && inserted_at > 0 && (now_secs - inserted_at) > ORDER_TTL_SECS {
                self.meta[idx].active = false;
                self.meta[idx].inserted_at_secs = 0;
                indexes.remove_order(order_id);
                matrix.evict_order(idx);
            }
        }
    }
}

impl Default for OrderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─── InvertedIndexes ─────────────────────────────────────────────────────────

pub struct InvertedIndexes {
    pub tpid_to_orders: HashMap<u64, Vec<u32>>,
    pub bintag_to_orders: HashMap<u32, Vec<u32>>,
    order_tpids: HashMap<u32, Vec<u64>>,
    order_bintags: HashMap<u32, Vec<u32>>,
}

impl InvertedIndexes {
    pub fn new() -> Self {
        Self {
            tpid_to_orders: HashMap::new(),
            bintag_to_orders: HashMap::new(),
            order_tpids: HashMap::new(),
            order_bintags: HashMap::new(),
        }
    }

    pub fn add_order(&mut self, order_id: u32, tpids: &[u64], bintag_ids: &[u32]) {
        self.remove_order(order_id);

        for &tpid in tpids {
            self.tpid_to_orders.entry(tpid).or_default().push(order_id);
        }
        for &bt in bintag_ids {
            self.bintag_to_orders.entry(bt).or_default().push(order_id);
        }

        self.order_tpids.insert(order_id, tpids.to_vec());
        self.order_bintags.insert(order_id, bintag_ids.to_vec());
    }

    pub fn remove_order(&mut self, order_id: u32) {
        if let Some(tpids) = self.order_tpids.remove(&order_id) {
            for tpid in tpids {
                if let Some(v) = self.tpid_to_orders.get_mut(&tpid) {
                    v.retain(|&id| id != order_id);
                }
            }
        }
        if let Some(bts) = self.order_bintags.remove(&order_id) {
            for bt in bts {
                if let Some(v) = self.bintag_to_orders.get_mut(&bt) {
                    v.retain(|&id| id != order_id);
                }
            }
        }
    }
}

impl Default for InvertedIndexes {
    fn default() -> Self {
        Self::new()
    }
}
