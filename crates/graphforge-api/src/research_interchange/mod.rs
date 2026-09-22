//! Consumer-neutral research references and explicit native interchange.
mod reference;
pub use reference::{ResearchReference, ResearchReferenceTarget};

mod export;
pub use export::ExportResearchRequest;

#[cfg(test)]
mod tests;

mod projection;
pub use projection::ResearchExportProjection;

pub(crate) mod validation;

mod accepted;

mod manifest;

mod fork;
pub use fork::{ForkResearchRequest, ForkResearchResult};

#[cfg(test)]
mod closure_tests;
