//! Immutable selected submissions and native atomic parent review.
mod accepted_subset;
mod apply_graph;
mod canonical;
mod dependencies;
mod destination;
mod history;
mod model;
mod ontology;
mod output;
mod preview;
mod release;
mod review;
mod selection;
mod submit;
pub use model::*;
#[cfg(test)]
mod tests;

use crate::GfError;
use sha2::{Digest, Sha256};
use uuid::Uuid;

fn identity(operation: Uuid, role: &str) -> Uuid {
    let mut hash = Sha256::new();
    hash.update(b"graphforge-proposal-identity/1");
    hash.update(operation.as_bytes());
    hash.update(role.as_bytes());
    graphforge_core::canonical::uuid_v8(hash.finalize().into())
}

fn invalid(message: &str) -> GfError {
    GfError::Validation(message.into())
}

fn key(unit: &crate::ResearchFieldIdentity) -> crate::branches::fields::Key {
    (
        unit.object_kind.clone(),
        unit.object_uuid,
        unit.field.clone(),
    )
}
