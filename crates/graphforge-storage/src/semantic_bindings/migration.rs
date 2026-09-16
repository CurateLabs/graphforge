//! Retained-data semantic migration planning and materialization.

mod materialization;
mod planning;
pub use materialization::materialize_semantic_migration;
#[cfg(test)]
pub(super) use materialization::migrated_semantic_relative;

#[cfg(test)]
mod tests;
