//! Authenticated, non-enumerating storage attribution for committed projects.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;

use graphforge_core::GfError;
use serde::{Deserialize, Serialize};

use crate::{GraphFileEntry, GraphFilesParticipant, ResolvedProjectGeneration};

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
    /// Unclassified retained graph artifact. Qualification must reject this.
    Other,
}

impl ArtifactCategory {
    /// Canonical category inventory, including zero-valued categories.
    pub const ALL: [Self; 7] = [
        Self::TopologyNodes,
        Self::TopologyEdges,
        Self::Properties,
        Self::UuidAndSurrogates,
        Self::Adjacency,
        Self::CatalogAndManifests,
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
