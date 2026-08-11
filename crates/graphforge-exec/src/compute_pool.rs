//! Instance-owned bounded CPU pool for deterministic algorithm kernels (#337 / #342 / #343 / #344 / #504 / #515).
//!
//! GraphForge never installs work onto Rayon's process-global pool. Parallel
//! cosine KNN (#342), PageRank (#343), Node2Vec walk generation (#344),
//! clustering coefficient (#504), triangles (#515), and sibling CPU kernels
//! consume this private pool sized from [`crate`]-facing `compute_threads` on
//! the embedded resource policy.

use std::sync::Arc;

use graphforge_core::GfError;
use rayon::{ThreadPool, ThreadPoolBuilder};

/// Private, instance-owned Rayon pool bounded by the resource-policy compute budget.
#[derive(Debug)]
pub struct ComputePool {
    threads: usize,
    /// `None` when the budget is one thread — callers stay on the serial path.
    pool: Option<ThreadPool>,
}

impl ComputePool {
    /// Build a private pool with exactly `threads` workers (`1` keeps no pool).
    ///
    /// # Errors
    /// Returns [`GfError::Execution`] when the private Rayon pool cannot be created.
    pub fn new(threads: usize) -> Result<Self, GfError> {
        let threads = threads.max(1);
        if threads == 1 {
            return Ok(Self {
                threads: 1,
                pool: None,
            });
        }
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("graphforge-compute-{index}"))
            .build()
            .map_err(|error| GfError::Execution(format!("compute pool unavailable: {error}")))?;
        Ok(Self {
            threads,
            pool: Some(pool),
        })
    }

    /// Declared worker count from the resource policy.
    #[must_use]
    pub fn num_threads(&self) -> usize {
        self.threads
    }

    /// Whether a private multi-thread pool is installed.
    #[must_use]
    pub fn is_parallel(&self) -> bool {
        self.pool.is_some()
    }

    /// Run `op` on this pool (or inline when the budget is one thread).
    pub fn install<OP, R>(&self, op: OP) -> R
    where
        OP: FnOnce() -> R + Send,
        R: Send,
    {
        match &self.pool {
            Some(pool) => pool.install(op),
            None => op(),
        }
    }
}

/// Shared handle stored on the facade and passed into algorithm controls.
pub type SharedComputePool = Arc<ComputePool>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_thread_skips_pool_construction() {
        let pool = ComputePool::new(1).unwrap();
        assert_eq!(pool.num_threads(), 1);
        assert!(!pool.is_parallel());
        assert_eq!(pool.install(|| 7), 7);
    }

    #[test]
    fn multi_thread_pool_runs_on_private_workers() {
        let pool = ComputePool::new(2).unwrap();
        assert!(pool.is_parallel());
        let sum = pool.install(|| {
            use rayon::prelude::*;
            (0..32_usize)
                .into_par_iter()
                .map(|value| {
                    let thread = std::thread::current();
                    let name = thread.name().unwrap_or("unnamed");
                    assert!(
                        name.starts_with("graphforge-compute-"),
                        "expected private pool worker, got {name:?}"
                    );
                    value * value
                })
                .sum::<usize>()
        });
        assert_eq!(sum, (0..32).map(|value| value * value).sum::<usize>());
    }
}
