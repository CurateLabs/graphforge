//! Authenticated research interchange content; imported history is not live authority.
mod model;
pub use model::{ResearchForkRecord, ResearchInterchangeManifest, ResearchInterchangeSelection};

mod registry;
pub(super) use registry::{preserve, validate_registry};

mod transfer;
pub use transfer::materialize_research_interchange;

pub(crate) mod portable;

mod canonical_projection;
pub use canonical_projection::canonicalize_prepared_research_projection;
