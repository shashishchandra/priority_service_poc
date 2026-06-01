use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;

use crate::types::{make_pair_key, PairKey};

// ─── PairStatus ──────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, Debug)]
pub enum PairStatus {
    Active,
    WarmingUp,
    Removed,
}

// ─── TopologyState ───────────────────────────────────────────────────────────

/// Lock-free topology map.  Reads are O(1) via ArcSwap; writes clone the map,
/// mutate the clone, then atomically swap the Arc pointer.
pub struct TopologyState {
    states: ArcSwap<HashMap<PairKey, PairStatus>>,
}

impl TopologyState {
    pub fn new() -> Self {
        Self {
            states: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    /// Read the current status of a pair.
    pub fn get(&self, key: PairKey) -> Option<PairStatus> {
        self.states.load().get(&key).cloned()
    }

    /// Clone → mutate → swap.
    fn set(&self, key: PairKey, status: PairStatus) {
        let current = self.states.load();
        let mut next: HashMap<PairKey, PairStatus> = (**current).clone();
        next.insert(key, status);
        self.states.store(Arc::new(next));
    }

    /// Mark a new pair as WarmingUp (not yet ready to serve traffic).
    pub fn add_warming_up(&self, pps_id: u32, bintag_id: u32) {
        self.set(make_pair_key(pps_id, bintag_id), PairStatus::WarmingUp);
    }

    /// Promote a pair from WarmingUp → Active.
    pub fn activate(&self, pps_id: u32, bintag_id: u32) {
        self.set(make_pair_key(pps_id, bintag_id), PairStatus::Active);
    }

    /// Mark a pair as Removed (no longer served by this pod).
    pub fn remove(&self, pps_id: u32, bintag_id: u32) {
        self.set(make_pair_key(pps_id, bintag_id), PairStatus::Removed);
    }

    /// Return all PairKeys currently in WarmingUp state.
    pub fn warming_up_pairs(&self) -> Vec<PairKey> {
        self.states
            .load()
            .iter()
            .filter_map(|(&k, v)| {
                if *v == PairStatus::WarmingUp {
                    Some(k)
                } else {
                    None
                }
            })
            .collect()
    }
}

impl Default for TopologyState {
    fn default() -> Self {
        Self::new()
    }
}
