//! Research classification, claim relationships and scoped acceptance history.
//! Evidence, assertions, confidence and hypothesis groups retain their owners.
mod canonical;
mod claims;
mod decisions;
mod encoding;
mod enums;
mod lineage;
mod model;
mod registry;
mod suppressions;
pub use claims::ResearchClaimLedger;
pub use decisions::ResearchDecisionLedger;
pub use encoding::CLAIM_SUPPRESSION_SCHEMA;
pub use encoding::{
    CLAIM_RELATION_SCHEMA, RESEARCH_CLAIM_SCHEMA, RESEARCH_DECISION_SCHEMA, RESEARCH_RECORD_VERSION,
};
pub use model::*;
pub(crate) use registry::schema_registry_entries;
pub use suppressions::ResearchSuppressionLedger;

/// Bounded rows per research family, including complete retained decision history.
pub const MAX_RESEARCH_ROWS: usize = 65_536;

#[cfg(test)]
mod tests;
