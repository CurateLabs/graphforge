//! Passive identity-free storage receipt contracts and arithmetic validation.

use crate::GfError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Exhaustive storage categories used by scale qualification evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactCategory {
    /// Canonical node topology shards.
    TopologyNodes,
    /// Canonical edge topology shards and authoritative edge deltas.
    TopologyEdges,
    /// Node and edge property shards.
    Properties,
    /// UUID membership and surrogate reverse indexes.
    UuidAndSurrogates,
    /// Derived adjacency manifests and CSR shards.
    Adjacency,
    /// Runtime catalogs, generation participants, and compact-manifest nodes.
    CatalogAndManifests,
    /// Receipt-authenticated construction staging and spill artifacts.
    ConstructionStaging,
    /// One immutable portable export package.
    PortablePackage,
    /// The authoritative retained project produced by a clean import.
    CleanImportedProject,
    /// Unclassified retained graph artifact. Qualification must reject this.
    Other,
}

impl ArtifactCategory {
    /// Canonical category inventory, including zero-valued categories.
    pub const ALL: [Self; 10] = [
        Self::TopologyNodes,
        Self::TopologyEdges,
        Self::Properties,
        Self::UuidAndSurrogates,
        Self::Adjacency,
        Self::CatalogAndManifests,
        Self::ConstructionStaging,
        Self::PortablePackage,
        Self::CleanImportedProject,
        Self::Other,
    ];
}

/// Reconciled totals for one artifact category.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactStorageTotals {
    /// Logical references in the authenticated inventory.
    pub logical_references: u64,
    /// Sum of referenced logical bytes; shared objects count per reference.
    pub logical_bytes: u64,
    /// Distinct retained physical files, deduplicated by native identity.
    pub physical_objects: u64,
    /// Logical EOF bytes of distinct physical files.
    pub physical_logical_bytes: u64,
    /// Filesystem-allocated bytes of distinct physical files.
    pub allocated_bytes: u64,
}

/// Identity-free, closed storage evidence suitable for ordinary CLI output.
///
/// This receipt deliberately omits generation identities, native file identities,
/// paths, and graph content. Every category is present, including truthful zeros.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageAttributionReceipt {
    /// Versioned semantic contract for consumers of this receipt.
    pub contract: String,
    /// Every authenticated artifact category exactly once.
    pub categories: BTreeMap<ArtifactCategory, ArtifactStorageTotals>,
    /// Reconciled logical references across categories.
    pub logical_references: u64,
    /// Reconciled referenced logical bytes across categories.
    pub logical_bytes: u64,
    /// Logical EOF bytes of distinct retained physical files.
    pub retained_logical_eof_bytes: u64,
    /// Filesystem-allocated bytes of distinct retained physical files.
    pub allocated_physical_bytes: u64,
    /// Distinct retained physical files, deduplicated by native identity.
    pub physical_objects: u64,
}

impl StorageAttributionReceipt {
    /// Recheck the public arithmetic without relying on stripped identities.
    pub fn validate_reconciliation(&self) -> Result<(), GfError> {
        if self.contract != "graphforge-storage-attribution/1"
            || ArtifactCategory::ALL
                .iter()
                .any(|category| !self.categories.contains_key(category))
            || self.categories.len() != ArtifactCategory::ALL.len()
        {
            return Err(validation(
                "storage attribution receipt contract is incomplete",
            ));
        }
        let mut total = ArtifactStorageTotals::default();
        for category in ArtifactCategory::ALL {
            add_totals(&mut total, &self.categories[&category])?;
        }
        if total.logical_references != self.logical_references
            || total.logical_bytes != self.logical_bytes
            || total.physical_objects != self.physical_objects
            || total.physical_logical_bytes != self.retained_logical_eof_bytes
            || total.allocated_bytes != self.allocated_physical_bytes
        {
            return Err(validation(
                "storage attribution receipt totals do not reconcile",
            ));
        }
        Ok(())
    }
}

fn add_totals(
    target: &mut ArtifactStorageTotals,
    value: &ArtifactStorageTotals,
) -> Result<(), GfError> {
    target.logical_references = checked_add(target.logical_references, value.logical_references)?;
    target.logical_bytes = checked_add(target.logical_bytes, value.logical_bytes)?;
    target.physical_objects = checked_add(target.physical_objects, value.physical_objects)?;
    target.physical_logical_bytes =
        checked_add(target.physical_logical_bytes, value.physical_logical_bytes)?;
    target.allocated_bytes = checked_add(target.allocated_bytes, value.allocated_bytes)?;
    Ok(())
}

fn checked_add(left: u64, right: u64) -> Result<u64, GfError> {
    left.checked_add(right)
        .ok_or_else(|| validation("storage attribution counter overflow"))
}
fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}
