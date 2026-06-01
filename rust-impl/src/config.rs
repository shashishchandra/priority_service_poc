//! Dynamic resource configuration for the Priority Service.
//!
//! All tuneables are read from environment variables at startup.
//! Invalid or missing values fall back to workload-derived defaults.
//!
//! # Supported env vars
//!
//! | Variable           | Effect                                          | Default              |
//! |--------------------|--------------------------------------------------|----------------------|
//! | `PS_NUM_WORKERS`   | Rayon thread-pool size for Phase 2               | `ceil(cores × 1.5)`  |
//! | `PS_STACK_KB`      | Stack size per rayon worker in KiB               | `2048` (2 MB)        |
//!
//! # Example
//!
//! ```bash
//! PS_NUM_WORKERS=48 PS_STACK_KB=4096 cargo run --release
//! ```

use crate::types::{NUM_BIN_TAGS_PER_PPS, NUM_ORDERS, NUM_PPS, PAIRS_PER_POD};
use std::env;

/// Active resource configuration for this process.
#[derive(Debug, Clone)]
pub struct ResourceConfig {
    /// Rayon worker threads for Phase 2 parallel ranking.
    pub num_workers: usize,
    /// Stack size per rayon worker in bytes.
    pub stack_bytes: usize,
}

/// Derives safe defaults based on workload constants and available hardware.
///
/// - `num_workers` = min(PAIRS_PER_POD, ceil(cores × 1.5)) — slightly above
///   the physical core count so rayon threads can keep cores busy while other
///   threads stall on DRAM cache misses. Unlike Go goroutines, rayon workers
///   are OS threads scheduled by the kernel, so there is no user-space
///   preemption overhead from having more workers than cores.
/// - `stack_bytes` = 2 MiB (vs. the 8 MiB OS default); Phase 2 workers
///   hold only a `WorkerScratch` with four `Vec<SEntry>` buffers, so 2 MiB
///   is ample.
fn default_config() -> ResourceConfig {
    let cores = available_parallelism();
    // ceil(cores × 1.5) via integer arithmetic.
    let workers = PAIRS_PER_POD.min((cores * 3 + 1) / 2).max(1);
    ResourceConfig {
        num_workers: workers,
        stack_bytes: 2 * 1024 * 1024, // 2 MiB
    }
}

/// Reads `PS_*` env vars and returns the active `ResourceConfig`.
/// Must be called before `rayon::ThreadPoolBuilder` is invoked.
pub fn init_from_env() -> ResourceConfig {
    let mut cfg = default_config();

    if let Ok(v) = env::var("PS_NUM_WORKERS") {
        if let Ok(n) = v.parse::<usize>() {
            if n > 0 {
                cfg.num_workers = n;
            }
        }
    }
    if let Ok(v) = env::var("PS_STACK_KB") {
        if let Ok(n) = v.parse::<usize>() {
            if n > 0 {
                cfg.stack_bytes = n * 1024;
            }
        }
    }

    cfg
}

/// Prints the active configuration and estimated memory budget to stdout.
pub fn print_resource_config(cfg: &ResourceConfig) {
    let logical_cpus = available_parallelism();
    let pps_matrix_mb = NUM_PPS * NUM_ORDERS * 2 / 1_000_000;
    let btag_idx_mb = NUM_BIN_TAGS_PER_PPS * NUM_ORDERS * 4 / 1_000_000;

    println!("Resource config:");
    println!(
        "  Workers        : {}  (logical CPUs: {})",
        cfg.num_workers, logical_cpus
    );
    println!(
        "  Stack/worker   : {} KiB",
        cfg.stack_bytes / 1024
    );
    println!(
        "  PPSMatrix est. : {} MB  ({} PPS × {} orders × 2 B)",
        pps_matrix_mb, NUM_PPS, NUM_ORDERS
    );
    println!(
        "  BinTag idx est.: {} MB  ({} tags × {} orders × 4 B, worst-case)",
        btag_idx_mb, NUM_BIN_TAGS_PER_PPS, NUM_ORDERS
    );
    println!(
        "  Pairs/pod      : {}",
        PAIRS_PER_POD
    );
    println!();
}

/// Returns the number of logical CPUs available to this process.
/// Falls back to 1 if the OS query fails (containers with no CPU quota set).
fn available_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
