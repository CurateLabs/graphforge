//! The byte budget of the read caches a query session keeps resident (#1987).
//!
//! The memory pool bounds what an operator holds while it runs. Four consumers
//! outlive an operator and sit outside the pool: the decoded CSR shards, the
//! per-ordinal label index built from the node files, the per-route edge-id
//! maps built from the edge-property index, and the authenticated
//! ordinal-identity blocks. Each used to carry a budget of its own, so the
//! process held several gibibytes the pool never saw. This module derives all
//! of them from the one number the pool is built from.
//!
//! # Formula
//!
//! For a query memory budget `B`:
//!
//! ```text
//! retained read caches R = B / 2
//! topology share       P = R / 2 = B / 4   decoded shards + label index + edge-id maps
//! identity share       I = R / 2 = B / 4   authenticated ordinal blocks
//! ```
//!
//! so a provider and its identity handle together retain at most `B / 2`
//! (plus one decoded shard, below 24 MiB, when a single shard exceeds the
//! topology share), and a query's peak is the pool `B` plus `B / 2` retained.
//!
//! # Symmetric accounting inside the topology share
//!
//! The three topology consumers draw on `P` together, and each one's budget is
//! `P` less what the others hold, so none can crowd out another by building
//! first:
//!
//! * the label index builds under `P - maps`, and declines when it does not fit;
//! * each edge-id map builds under `P - labels - the other routes' maps`, and
//!   declines when it does not fit;
//! * the decoded-shard cache holds `P - (labels + maps)` through
//!   [`ShardCache::set_external_bytes`](crate::adjacency::ShardCache::set_external_bytes),
//!   evicting least-recently-used shards when a view is built or dropped.
//!
//! The identity blocks have their own share, `I`, enforced by the handle's
//! `max_ordinal_cache_bytes`
//! ([`V4OrdinalIdentityLimits::for_query_memory_budget`](crate::V4OrdinalIdentityLimits::for_query_memory_budget)).
//!
//! The bound is per provider and per identity handle. A session builds one pair
//! per hydrated graph generation, so `g` live generations retain at most
//! `g * B / 2`.
//!
//! At the 512 MiB default `B` each share is 128 MiB, at the 2 GiB a 4 GiB
//! cgroup derives each is 512 MiB.

/// The default session memory budget: 512 MiB, the pool size a session gets
/// when no resource policy states one.
pub const DEFAULT_QUERY_MEMORY_BUDGET_BYTES: u64 = 512 * 1024 * 1024;

/// How the retained read caches divide a query memory budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadCacheBudget {
    /// Decoded CSR shards, the label index and the edge-id maps.
    pub topology_bytes: u64,
    /// Authenticated ordinal-identity blocks.
    pub identity_bytes: u64,
}

impl ReadCacheBudget {
    /// The shares of a query memory budget of `query_budget_bytes` (module docs).
    #[must_use]
    pub const fn from_query_budget(query_budget_bytes: u64) -> Self {
        let share = query_budget_bytes / 4;
        Self {
            topology_bytes: share,
            identity_bytes: share,
        }
    }

    /// The shares of [`DEFAULT_QUERY_MEMORY_BUDGET_BYTES`].
    pub const DEFAULT: Self = Self::from_query_budget(DEFAULT_QUERY_MEMORY_BUDGET_BYTES);

    /// Everything both caches may retain together: `B / 2`.
    #[must_use]
    pub const fn total_bytes(&self) -> u64 {
        self.topology_bytes + self.identity_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_shares_sum_to_half_the_query_budget() {
        for budget in [
            16 << 20,
            DEFAULT_QUERY_MEMORY_BUDGET_BYTES,
            2 << 30,
            32 << 30,
        ] {
            let shares = ReadCacheBudget::from_query_budget(budget);
            assert_eq!(shares.topology_bytes, budget / 4);
            assert_eq!(shares.identity_bytes, budget / 4);
            assert_eq!(shares.total_bytes(), budget / 2);
        }
        assert_eq!(
            ReadCacheBudget::DEFAULT,
            ReadCacheBudget::from_query_budget(DEFAULT_QUERY_MEMORY_BUDGET_BYTES)
        );
    }
}
