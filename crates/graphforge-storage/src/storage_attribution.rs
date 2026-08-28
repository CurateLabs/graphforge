//! Authenticated, non-enumerating storage attribution for committed projects.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;

use graphforge_core::GfError;
use serde::{Deserialize, Serialize};

use crate::{
    GraphConstructionEvidence, GraphFileEntry, GraphFilesParticipant, ResolvedProjectGeneration,
};

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

/// Closed lifecycle-phase inventory for application-observed storage I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageIoPhase {
    /// Chunk append and bounded external merge work.
    AppendMerge,
    /// Seal-time authentication of staged inputs.
    SealAuthentication,
    /// Canonical shape consumption and reauthentication.
    ShapeConsumeReauthentication,
    /// Canonical encoding plus post-write authentication.
    EncodeWritePostwriteAuthentication,
    /// Publication control preauthentication.
    PublicationPreauthentication,
    /// Content-addressed installation reads and writes.
    CasInstallReadWrite,
    /// Workspace hydration and verification.
    HydrationVerification,
    /// Explicit file and directory synchronization barriers.
    FsyncSynchronization,
    /// Crash-recovery reauthentication.
    RecoveryReauthentication,
}

impl StorageIoPhase {
    /// Complete phase inventory, including phases with zero observations.
    pub const ALL: [Self; 9] = [
        Self::AppendMerge,
        Self::SealAuthentication,
        Self::ShapeConsumeReauthentication,
        Self::EncodeWritePostwriteAuthentication,
        Self::PublicationPreauthentication,
        Self::CasInstallReadWrite,
        Self::HydrationVerification,
        Self::FsyncSynchronization,
        Self::RecoveryReauthentication,
    ];
}

/// Exact application-I/O totals owned by one lifecycle phase.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseIoTotals {
    /// Payload and control bytes returned to the application.
    pub read_bytes: u64,
    /// Payload and control bytes submitted by the application.
    pub write_bytes: u64,
    /// Application-observed read calls.
    pub read_calls: u64,
    /// Application-observed write calls.
    pub write_calls: u64,
    /// Immutable objects handled by this phase.
    pub object_count: u64,
    /// Fixed-size authenticated or buffered blocks handled by this phase.
    pub block_count: u64,
    /// File and directory durability barriers completed by this phase.
    pub fsync_calls: u64,
}

/// Closed, reconciled phase attribution for one construction lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstructionPhaseAttribution {
    /// Every lifecycle phase exactly once, including zero observations.
    pub phases: BTreeMap<StorageIoPhase, PhaseIoTotals>,
    /// Exact sum of all phase rows.
    pub totals: PhaseIoTotals,
}

impl ConstructionPhaseAttribution {
    /// Derive phase ownership from storage-owned construction counters.
    #[must_use]
    pub fn from_construction(evidence: &GraphConstructionEvidence) -> Self {
        let mut phases: BTreeMap<_, _> = StorageIoPhase::ALL
            .into_iter()
            .map(|phase| (phase, PhaseIoTotals::default()))
            .collect();
        phases.insert(
            StorageIoPhase::AppendMerge,
            PhaseIoTotals {
                read_bytes: evidence.replay_validation_read_bytes,
                write_bytes: evidence.write_bytes,
                read_calls: evidence.replay_validation_read_operations,
                write_calls: evidence.write_operations,
                object_count: evidence.parquet_shards,
                ..Default::default()
            },
        );
        phases.insert(
            StorageIoPhase::SealAuthentication,
            PhaseIoTotals {
                read_bytes: evidence.seal_application_read_bytes,
                read_calls: evidence.authentication_read_operations,
                ..Default::default()
            },
        );
        phases.insert(
            StorageIoPhase::ShapeConsumeReauthentication,
            PhaseIoTotals {
                read_bytes: evidence.shape_application_read_bytes,
                write_bytes: evidence.merge_written_bytes,
                read_calls: evidence
                    .shape_input_validation_read_operations
                    .saturating_add(evidence.parquet_read_operations),
                write_calls: evidence.parquet_write_operations,
                block_count: evidence
                    .merge_read_blocks
                    .saturating_add(evidence.merge_write_blocks),
                ..Default::default()
            },
        );
        phases.insert(
            StorageIoPhase::EncodeWritePostwriteAuthentication,
            PhaseIoTotals {
                read_bytes: evidence.encode_application_read_bytes,
                write_bytes: evidence.canonical_output_bytes,
                read_calls: evidence.shaped_output_authentication_operations,
                ..Default::default()
            },
        );
        phases.insert(
            StorageIoPhase::PublicationPreauthentication,
            PhaseIoTotals {
                read_bytes: evidence.publication_application_read_bytes,
                read_calls: evidence.publication_application_read_operations,
                ..Default::default()
            },
        );
        phases.insert(
            StorageIoPhase::CasInstallReadWrite,
            PhaseIoTotals {
                read_bytes: evidence.cas_application_read_bytes,
                write_bytes: evidence.cas_application_write_bytes,
                read_calls: evidence.cas_application_read_operations,
                write_calls: evidence.cas_application_write_operations,
                fsync_calls: evidence.cas_fsync_operations,
                ..Default::default()
            },
        );
        phases.insert(
            StorageIoPhase::HydrationVerification,
            PhaseIoTotals {
                read_bytes: evidence.hydration_application_read_bytes,
                write_bytes: evidence.hydration_application_write_bytes,
                read_calls: evidence.hydration_application_read_operations,
                write_calls: evidence.hydration_application_write_operations,
                fsync_calls: evidence.hydration_fsync_operations,
                ..Default::default()
            },
        );
        phases.insert(
            StorageIoPhase::FsyncSynchronization,
            PhaseIoTotals {
                fsync_calls: evidence
                    .fsync_operations
                    .saturating_add(evidence.merge_fsync_operations),
                ..Default::default()
            },
        );
        phases.insert(
            StorageIoPhase::RecoveryReauthentication,
            PhaseIoTotals {
                read_bytes: evidence.recovery_application_read_bytes,
                read_calls: evidence.recovery_application_read_operations,
                ..Default::default()
            },
        );
        let totals = phases
            .values()
            .fold(PhaseIoTotals::default(), |mut total, value| {
                add_phase_totals_saturating(&mut total, value);
                total
            });
        Self { phases, totals }
    }

    /// Reject missing phases or totals that do not equal the phase sum.
    pub fn validate_reconciliation(&self) -> Result<(), GfError> {
        if StorageIoPhase::ALL
            .iter()
            .any(|phase| !self.phases.contains_key(phase))
        {
            return Err(validation("storage phase attribution is missing a phase"));
        }
        let mut total = PhaseIoTotals::default();
        for phase in StorageIoPhase::ALL {
            add_phase_totals(&mut total, &self.phases[&phase])?;
        }
        if total != self.totals {
            return Err(validation(
                "storage phase attribution totals do not reconcile",
            ));
        }
        Ok(())
    }
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

/// Authenticated storage attribution for one lifetime-pinned generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageAttributionSnapshot {
    /// Selected immutable generation UUID.
    pub generation_uuid: uuid::Uuid,
    /// SHA-256 of exact authenticated generation manifest bytes.
    pub generation_manifest_sha256: [u8; 32],
    /// Every category exactly once, including zero totals.
    pub categories: BTreeMap<ArtifactCategory, ArtifactStorageTotals>,
    /// Reconciled logical references across categories.
    pub logical_references: u64,
    /// Reconciled referenced logical bytes across categories.
    pub logical_bytes: u64,
    /// Reconciled distinct physical objects across categories.
    pub physical_objects: u64,
    /// Reconciled distinct-file EOF bytes across categories.
    pub physical_logical_bytes: u64,
    /// Reconciled distinct-file allocated bytes across categories.
    pub allocated_bytes: u64,
}

impl StorageAttributionSnapshot {
    /// Whether every retained graph artifact was assigned a qualifying category.
    #[must_use]
    pub fn is_fully_classified(&self) -> bool {
        self.categories
            .get(&ArtifactCategory::Other)
            .is_none_or(|totals| totals.logical_references == 0 && totals.physical_objects == 0)
    }

    /// Recompute and validate snapshot totals.
    pub fn validate_reconciliation(&self) -> Result<(), GfError> {
        if ArtifactCategory::ALL
            .iter()
            .any(|category| !self.categories.contains_key(category))
        {
            return Err(validation("storage attribution is missing a category"));
        }
        let mut total = ArtifactStorageTotals::default();
        for category in ArtifactCategory::ALL {
            let value = &self.categories[&category];
            add_totals(&mut total, value)?;
        }
        if total.logical_references != self.logical_references
            || total.logical_bytes != self.logical_bytes
            || total.physical_objects != self.physical_objects
            || total.physical_logical_bytes != self.physical_logical_bytes
            || total.allocated_bytes != self.allocated_bytes
        {
            return Err(validation("storage attribution totals do not reconcile"));
        }
        Ok(())
    }

    /// Validate the stricter scale-qualification contract.
    ///
    /// Qualification is fail-closed when any retained graph artifact remains
    /// in [`ArtifactCategory::Other`].
    pub fn validate_for_qualification(&self) -> Result<(), GfError> {
        self.validate_reconciliation()?;
        if !self.is_fully_classified() {
            return Err(validation(
                "storage attribution contains unclassified retained artifacts",
            ));
        }
        Ok(())
    }
}

/// Classify one authenticated graph inventory path.
#[must_use]
pub fn classify_graph_artifact(relative_path: &str) -> ArtifactCategory {
    let path = Path::new(relative_path);
    let mut components = path.components().filter_map(|component| match component {
        std::path::Component::Normal(value) => value.to_str(),
        _ => None,
    });
    match (components.next(), components.next()) {
        (Some("topology"), Some("nodes" | "nodes.parquet")) => ArtifactCategory::TopologyNodes,
        (Some("topology"), Some("edges" | "uuid-membership")) => {
            if relative_path.starts_with("topology/uuid-membership/") {
                ArtifactCategory::UuidAndSurrogates
            } else {
                ArtifactCategory::TopologyEdges
            }
        }
        (Some("topology"), Some("surrogate_tails.parquet")) => ArtifactCategory::UuidAndSurrogates,
        (Some("topology"), Some("runtime_catalog.parquet" | "generation.json")) => {
            ArtifactCategory::CatalogAndManifests
        }
        (Some("deltas"), _) => ArtifactCategory::TopologyEdges,
        (Some("properties" | "edge_properties"), _) => ArtifactCategory::Properties,
        (Some("indexes" | "index"), Some("adjacency")) => ArtifactCategory::Adjacency,
        (Some("indexes" | "index"), Some(name))
            if name.contains("uuid") || name.contains("surrogate") =>
        {
            ArtifactCategory::UuidAndSurrogates
        }
        (Some(name), _) if name.starts_with("runtime_catalog") => {
            ArtifactCategory::CatalogAndManifests
        }
        _ => ArtifactCategory::Other,
    }
}

/// Capture attribution from a pinned generation and its authenticated compact
/// inventory. No project directory is recursively enumerated.
pub fn capture_storage_attribution(
    generation: &ResolvedProjectGeneration,
) -> Result<StorageAttributionSnapshot, GfError> {
    let mut accumulator = Accumulator::new(generation);
    let generation_root =
        graphforge_filesystem::StableDirectory::open(generation.generation_root())
            .map_err(storage)?;
    let generation_manifest = generation_root
        .open_child_file(std::ffi::OsStr::new("manifest.json"))
        .map_err(storage)?;
    let generation_manifest_usage =
        graphforge_filesystem::file_space_usage(&generation_manifest).map_err(storage)?;
    accumulator.add_logical(
        ArtifactCategory::CatalogAndManifests,
        generation_manifest_usage.logical_bytes,
    )?;
    accumulator.add_physical(
        ArtifactCategory::CatalogAndManifests,
        &generation_manifest,
        generation_manifest_usage.logical_bytes,
    )?;
    for descriptor in generation.participant_descriptors()? {
        let Some(snapshot) = generation
            .participant_snapshot(&descriptor.capability_id, &descriptor.record_family_id)?
        else {
            return Err(validation("declared participant disappeared"));
        };
        let path =
            generation.participant_path(&descriptor.capability_id, &descriptor.record_family_id)?;
        let file = File::open(&path).map_err(storage)?;
        accumulator.add_physical(
            ArtifactCategory::CatalogAndManifests,
            &file,
            u64::try_from(snapshot.bytes.len()).map_err(|_| validation("participant too large"))?,
        )?;
        accumulator.add_logical(
            ArtifactCategory::CatalogAndManifests,
            u64::try_from(snapshot.bytes.len()).map_err(|_| validation("participant too large"))?,
        )?;
    }

    match generation.declared_graph_files_participant()? {
        Some(GraphFilesParticipant::V2(root)) => {
            let lease =
                crate::graph_object_store::begin_graph_object_read(generation.container_root())?;
            let mut manifest_objects = BTreeSet::new();
            let (entries, _) = crate::resolve_graph_manifest(
                &root,
                crate::GraphManifestLimits::default(),
                |digest| {
                    let bytes = crate::read_graph_object_by_digest(
                        generation.container_root(),
                        digest,
                        64 * 1024 * 1024,
                    )?;
                    manifest_objects.insert((
                        digest.to_owned(),
                        u64::try_from(bytes.len())
                            .map_err(|_| validation("graph manifest object too large"))?,
                    ));
                    Ok(bytes)
                },
            )?;
            for (digest, length) in manifest_objects {
                let object = lease.open(&digest, length)?;
                accumulator.add_physical(
                    ArtifactCategory::CatalogAndManifests,
                    object.as_ref(),
                    length,
                )?;
            }
            for entry in entries {
                add_compact_entry(&mut accumulator, &lease, &entry)?;
            }
        }
        Some(GraphFilesParticipant::V1(inventory)) => {
            crate::verify_graph_tree(&generation.graph_tree_root(), &inventory)?;
            let graph_root = generation.graph_tree_root();
            for entry in inventory.files {
                let category = classify_graph_artifact(&entry.relative_path);
                accumulator.add_logical(category, entry.byte_length)?;
                let file = open_inventory_file(&graph_root, &entry.relative_path)?;
                accumulator.add_physical(category, &file, entry.byte_length)?;
            }
        }
        None => {}
    }
    accumulator.finish()
}

fn open_inventory_file(root: &Path, relative: &str) -> Result<File, GfError> {
    let components = Path::new(relative)
        .components()
        .map(|component| match component {
            std::path::Component::Normal(value) => Ok(value.to_owned()),
            _ => Err(validation("graph inventory path is not normalized")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (file_name, directories) = components
        .split_last()
        .ok_or_else(|| validation("graph inventory path is empty"))?;
    let mut directory = graphforge_filesystem::StableDirectory::open(root).map_err(storage)?;
    for name in directories {
        directory = directory.open_child_directory(name).map_err(storage)?;
    }
    directory.open_child_file(file_name).map_err(storage)
}

fn add_compact_entry(
    accumulator: &mut Accumulator,
    lease: &crate::graph_object_store::GraphObjectReadLease,
    entry: &GraphFileEntry,
) -> Result<(), GfError> {
    let category = classify_graph_artifact(&entry.relative_path);
    accumulator.add_logical(category, entry.byte_length)?;
    let object = lease.open(&entry.content_sha256, entry.byte_length)?;
    accumulator.add_physical(category, object.as_ref(), entry.byte_length)
}

struct Accumulator {
    generation_uuid: uuid::Uuid,
    generation_manifest_sha256: [u8; 32],
    categories: BTreeMap<ArtifactCategory, ArtifactStorageTotals>,
    physical_seen: BTreeSet<(u64, [u8; 16])>,
}

impl Accumulator {
    fn new(generation: &ResolvedProjectGeneration) -> Self {
        Self {
            generation_uuid: generation.generation_uuid(),
            generation_manifest_sha256: generation.manifest_sha256(),
            categories: ArtifactCategory::ALL
                .into_iter()
                .map(|category| (category, ArtifactStorageTotals::default()))
                .collect(),
            physical_seen: BTreeSet::new(),
        }
    }

    fn add_logical(&mut self, category: ArtifactCategory, bytes: u64) -> Result<(), GfError> {
        let totals = self
            .categories
            .get_mut(&category)
            .expect("complete categories");
        totals.logical_references = checked_add(totals.logical_references, 1)?;
        totals.logical_bytes = checked_add(totals.logical_bytes, bytes)?;
        Ok(())
    }

    fn add_physical(
        &mut self,
        category: ArtifactCategory,
        file: &File,
        expected_logical_bytes: u64,
    ) -> Result<(), GfError> {
        let identity = graphforge_filesystem::file_identity(file).map_err(storage)?;
        let usage = graphforge_filesystem::file_space_usage(file).map_err(storage)?;
        if usage.logical_bytes != expected_logical_bytes {
            return Err(validation(
                "authenticated artifact length changed during attribution",
            ));
        }
        if self
            .physical_seen
            .insert((identity.volume_serial, identity.file_id))
        {
            let totals = self
                .categories
                .get_mut(&category)
                .expect("complete categories");
            totals.physical_objects = checked_add(totals.physical_objects, 1)?;
            totals.physical_logical_bytes =
                checked_add(totals.physical_logical_bytes, usage.logical_bytes)?;
            totals.allocated_bytes = checked_add(totals.allocated_bytes, usage.allocated_bytes)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<StorageAttributionSnapshot, GfError> {
        let mut total = ArtifactStorageTotals::default();
        for value in self.categories.values() {
            add_totals(&mut total, value)?;
        }
        let snapshot = StorageAttributionSnapshot {
            generation_uuid: self.generation_uuid,
            generation_manifest_sha256: self.generation_manifest_sha256,
            categories: self.categories,
            logical_references: total.logical_references,
            logical_bytes: total.logical_bytes,
            physical_objects: total.physical_objects,
            physical_logical_bytes: total.physical_logical_bytes,
            allocated_bytes: total.allocated_bytes,
        };
        snapshot.validate_reconciliation()?;
        Ok(snapshot)
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

fn add_phase_totals(target: &mut PhaseIoTotals, value: &PhaseIoTotals) -> Result<(), GfError> {
    target.read_bytes = checked_add(target.read_bytes, value.read_bytes)?;
    target.write_bytes = checked_add(target.write_bytes, value.write_bytes)?;
    target.read_calls = checked_add(target.read_calls, value.read_calls)?;
    target.write_calls = checked_add(target.write_calls, value.write_calls)?;
    target.object_count = checked_add(target.object_count, value.object_count)?;
    target.block_count = checked_add(target.block_count, value.block_count)?;
    target.fsync_calls = checked_add(target.fsync_calls, value.fsync_calls)?;
    Ok(())
}

fn add_phase_totals_saturating(target: &mut PhaseIoTotals, value: &PhaseIoTotals) {
    target.read_bytes = target.read_bytes.saturating_add(value.read_bytes);
    target.write_bytes = target.write_bytes.saturating_add(value.write_bytes);
    target.read_calls = target.read_calls.saturating_add(value.read_calls);
    target.write_calls = target.write_calls.saturating_add(value.write_calls);
    target.object_count = target.object_count.saturating_add(value.object_count);
    target.block_count = target.block_count.saturating_add(value.block_count);
    target.fsync_calls = target.fsync_calls.saturating_add(value.fsync_calls);
}

fn checked_add(left: u64, right: u64) -> Result<u64, GfError> {
    left.checked_add(right)
        .ok_or_else(|| validation("storage attribution counter overflow"))
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn classifier_is_exhaustive_and_specific() {
        assert_eq!(
            classify_graph_artifact("topology/nodes/1.parquet"),
            ArtifactCategory::TopologyNodes
        );
        assert_eq!(
            classify_graph_artifact("topology/edges/KNOWS/1.parquet"),
            ArtifactCategory::TopologyEdges
        );
        assert_eq!(
            classify_graph_artifact("properties/Person/1.parquet"),
            ArtifactCategory::Properties
        );
        assert_eq!(
            classify_graph_artifact("topology/uuid-membership/manifest.json"),
            ArtifactCategory::UuidAndSurrogates
        );
        assert_eq!(
            classify_graph_artifact("indexes/adjacency/_all.out.csr"),
            ArtifactCategory::Adjacency
        );
        assert_eq!(
            classify_graph_artifact("runtime_catalog.parquet"),
            ArtifactCategory::CatalogAndManifests
        );
        assert_eq!(
            classify_graph_artifact("topology/runtime_catalog.parquet"),
            ArtifactCategory::CatalogAndManifests
        );
        assert_eq!(
            classify_graph_artifact("topology/generation.json"),
            ArtifactCategory::CatalogAndManifests
        );
        assert_eq!(
            classify_graph_artifact("topology/surrogate_tails.parquet"),
            ArtifactCategory::UuidAndSurrogates
        );
        assert_eq!(
            classify_graph_artifact("unknown.bin"),
            ArtifactCategory::Other
        );
    }

    #[test]
    fn reconciliation_rejects_mismatch_and_missing_category() {
        let mut categories: BTreeMap<_, _> = ArtifactCategory::ALL
            .into_iter()
            .map(|category| (category, ArtifactStorageTotals::default()))
            .collect();
        categories
            .get_mut(&ArtifactCategory::TopologyNodes)
            .unwrap()
            .logical_bytes = 7;
        let mut snapshot = StorageAttributionSnapshot {
            generation_uuid: uuid::Uuid::nil(),
            generation_manifest_sha256: [0; 32],
            categories,
            logical_references: 0,
            logical_bytes: 7,
            physical_objects: 0,
            physical_logical_bytes: 0,
            allocated_bytes: 0,
        };
        snapshot.validate_reconciliation().unwrap();
        snapshot.logical_bytes = 8;
        assert!(snapshot.validate_reconciliation().is_err());
        snapshot.categories.remove(&ArtifactCategory::Other);
        assert!(snapshot.validate_reconciliation().is_err());
    }

    #[test]
    fn qualification_rejects_other_artifacts() {
        let mut categories: BTreeMap<_, _> = ArtifactCategory::ALL
            .into_iter()
            .map(|category| (category, ArtifactStorageTotals::default()))
            .collect();
        categories
            .get_mut(&ArtifactCategory::Other)
            .unwrap()
            .logical_references = 1;
        let snapshot = StorageAttributionSnapshot {
            generation_uuid: uuid::Uuid::nil(),
            generation_manifest_sha256: [0; 32],
            categories,
            logical_references: 1,
            logical_bytes: 0,
            physical_objects: 0,
            physical_logical_bytes: 0,
            allocated_bytes: 0,
        };
        assert!(snapshot.validate_reconciliation().is_ok());
        assert!(snapshot.validate_for_qualification().is_err());
    }

    #[test]
    fn construction_phase_inventory_reconciles_and_rejects_omission() {
        let evidence = GraphConstructionEvidence {
            seal_application_read_bytes: 11,
            shape_application_read_bytes: 13,
            encode_application_read_bytes: 17,
            publication_application_read_bytes: 19,
            publication_application_read_operations: 2,
            cas_application_read_bytes: 23,
            cas_application_read_operations: 3,
            cas_application_write_bytes: 43,
            cas_application_write_operations: 4,
            cas_fsync_operations: 5,
            hydration_application_read_bytes: 29,
            hydration_application_read_operations: 6,
            hydration_application_write_bytes: 47,
            hydration_application_write_operations: 7,
            hydration_fsync_operations: 8,
            recovery_application_read_bytes: 41,
            recovery_application_read_operations: 2,
            canonical_output_bytes: 31,
            write_bytes: 37,
            write_operations: 3,
            authentication_read_operations: 5,
            merge_fsync_operations: 7,
            ..Default::default()
        };
        let mut attribution = ConstructionPhaseAttribution::from_construction(&evidence);
        attribution.validate_reconciliation().unwrap();
        assert_eq!(attribution.phases.len(), StorageIoPhase::ALL.len());
        assert_eq!(attribution.totals.read_bytes, 153);
        assert_eq!(
            attribution.phases[&StorageIoPhase::RecoveryReauthentication].read_calls,
            2
        );
        assert_eq!(attribution.totals.write_bytes, 158);
        assert_eq!(attribution.totals.read_calls, 18);
        assert_eq!(attribution.totals.write_calls, 14);
        assert_eq!(attribution.totals.fsync_calls, 20);
        attribution
            .phases
            .remove(&StorageIoPhase::RecoveryReauthentication);
        assert!(attribution.validate_reconciliation().is_err());
    }

    #[test]
    fn construction_phase_inventory_rejects_double_counted_total() {
        let mut attribution =
            ConstructionPhaseAttribution::from_construction(&GraphConstructionEvidence::default());
        attribution.totals.read_bytes = 1;
        assert!(attribution.validate_reconciliation().is_err());
    }

    #[test]
    fn one_physical_identity_is_counted_once_for_shared_references() {
        let project = tempfile::tempdir().unwrap();
        let generation = crate::open_or_initialize_ephemeral_project(project.path()).unwrap();
        let artifact = tempfile::NamedTempFile::new().unwrap();
        artifact.as_file().write_all(b"shared").unwrap();
        artifact.as_file().sync_all().unwrap();
        let mut accumulator = Accumulator::new(&generation);
        accumulator
            .add_logical(ArtifactCategory::TopologyNodes, 6)
            .unwrap();
        accumulator
            .add_logical(ArtifactCategory::Properties, 6)
            .unwrap();
        accumulator
            .add_physical(ArtifactCategory::TopologyNodes, artifact.as_file(), 6)
            .unwrap();
        accumulator
            .add_physical(ArtifactCategory::Properties, artifact.as_file(), 6)
            .unwrap();
        let snapshot = accumulator.finish().unwrap();
        assert_eq!(snapshot.logical_references, 2);
        assert_eq!(snapshot.logical_bytes, 12);
        assert_eq!(snapshot.physical_objects, 1);
        assert_eq!(snapshot.physical_logical_bytes, 6);
        assert!(snapshot.allocated_bytes >= 6);
    }
}
