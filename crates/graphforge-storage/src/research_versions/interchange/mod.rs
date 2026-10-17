//! Authenticated research interchange content; imported history is not live authority.
mod model;
pub use model::{ResearchForkRecord, ResearchInterchangeManifest, ResearchInterchangeSelection};

mod registry;
pub(super) use registry::{preserve, validate_registry};

mod transfer;
pub(in crate::research_versions) use transfer::copy_version;
pub use transfer::materialize_research_interchange;

pub(crate) mod portable;

mod canonical_projection;
pub use canonical_projection::canonicalize_prepared_research_projection;
