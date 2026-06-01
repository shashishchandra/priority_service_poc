use crate::topology::{PairStatus, TopologyState};
use crate::types::{make_pair_key, DoubleBuffer, PairInfo, PairKey, TOP_K, TBL_CRITICAL, TBL_HEURISTIC, TBL_LARGE, TBL_PBT};
use std::collections::HashMap;
use std::sync::Arc;

// ─── gRPC status codes ───────────────────────────────────────────────────────

pub const GRPC_NOT_FOUND: u32 = 5;
pub const GRPC_UNAVAILABLE: u32 = 14;

// ─── Request / Response ──────────────────────────────────────────────────────

pub struct GetTopKRequest {
    pub pps_id: u32,
    pub bintag_id: u32,
    /// How many results to return (capped at TOP_K).
    pub k: usize,
}

#[derive(Debug)]
pub struct GetTopKResponse {
    pub heuristic: Vec<i32>,
    pub large: Vec<i32>,
    pub pbt: Vec<i32>,
    pub critical: Vec<i32>,
}

// ─── ApiError ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum ApiError {
    NotFound(u32),
    Unavailable(u32),
    Internal(String),
}

impl ApiError {
    pub fn grpc_code(&self) -> u32 {
        match self {
            ApiError::NotFound(_) => GRPC_NOT_FOUND,
            ApiError::Unavailable(_) => GRPC_UNAVAILABLE,
            ApiError::Internal(_) => 13,
        }
    }
}

// ─── PriorityServer ──────────────────────────────────────────────────────────

/// Read-path server.  All reads are O(1) and lock-free via ArcSwap.
pub struct PriorityServer {
    pub topology: Arc<TopologyState>,
    pub db: Arc<DoubleBuffer>,
    /// Maps PairKey → index into the ReadBuffer.
    pub pair_index: HashMap<PairKey, usize>,
}

impl PriorityServer {
    /// Construct from the shared topology + DB, and the initial pair list.
    pub fn new(
        topology: Arc<TopologyState>,
        db: Arc<DoubleBuffer>,
        pairs: &[PairInfo],
    ) -> Self {
        let mut pair_index = HashMap::with_capacity(pairs.len());
        for (idx, p) in pairs.iter().enumerate() {
            pair_index.insert(p.key, idx);
        }
        Self {
            topology,
            db,
            pair_index,
        }
    }

    /// O(1) lock-free read with circuit breaker.
    ///
    /// Circuit breaker logic:
    /// 1. Build PairKey from (pps_id, bintag_id).
    /// 2. `None | Some(Removed)` → `Err(ApiError::NotFound(GRPC_NOT_FOUND))`
    /// 3. `Some(WarmingUp)` → `Err(ApiError::Unavailable(GRPC_UNAVAILABLE))`
    /// 4. `Some(Active)` → load snapshot, read lists.
    pub fn get_top_k(&self, req: &GetTopKRequest) -> Result<GetTopKResponse, ApiError> {
        let key = make_pair_key(req.pps_id, req.bintag_id);

        match self.topology.get(key) {
            None | Some(PairStatus::Removed) => {
                return Err(ApiError::NotFound(GRPC_NOT_FOUND));
            }
            Some(PairStatus::WarmingUp) => {
                return Err(ApiError::Unavailable(GRPC_UNAVAILABLE));
            }
            Some(PairStatus::Active) => {}
        }

        let pair_idx = match self.pair_index.get(&key) {
            Some(&idx) => idx,
            None => {
                return Err(ApiError::Internal(format!(
                    "pair ({}, {}) Active but missing from pair_index",
                    req.pps_id, req.bintag_id
                )));
            }
        };

        let k = req.k.min(TOP_K);
        let snapshot = self.db.load();

        Ok(GetTopKResponse {
            heuristic: snapshot.get_list(pair_idx, TBL_HEURISTIC, k),
            large: snapshot.get_list(pair_idx, TBL_LARGE, k),
            pbt: snapshot.get_list(pair_idx, TBL_PBT, k),
            critical: snapshot.get_list(pair_idx, TBL_CRITICAL, k),
        })
    }

    /// Rebuild pair_index after a topology change (add / remove pair).
    pub fn refresh_pair_index(&mut self, pairs: &[PairInfo]) {
        self.pair_index.clear();
        for (idx, p) in pairs.iter().enumerate() {
            self.pair_index.insert(p.key, idx);
        }
    }
}
