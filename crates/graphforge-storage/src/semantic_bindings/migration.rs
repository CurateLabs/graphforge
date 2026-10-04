//! Retained-data semantic migration planning and materialization.

mod materialization;
mod planning;
#[cfg(test)]
pub(super) use materialization::migrated_semantic_relative;
pub use materialization::{
    materialize_semantic_migration, materialize_semantic_migration_from_files,
};

#[cfg(test)]
mod tests;
