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
                write_bytes: evidence.encode_application_write_bytes,
                read_calls: evidence.encode_application_read_operations,
                write_calls: evidence.encode_application_write_operations,
                fsync_calls: evidence.encode_fsync_operations,
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

    /// Add writer-reported recovery reauthentication completed outside the
    /// construction session, such as interrupted portable finalization.
    pub fn add_recovery_reauthentication(&mut self, read_bytes: u64, read_calls: u64) {
        let recovery = self
            .phases
            .entry(StorageIoPhase::RecoveryReauthentication)
            .or_default();
        recovery.read_bytes = recovery.read_bytes.saturating_add(read_bytes);
        recovery.read_calls = recovery.read_calls.saturating_add(read_calls);
        self.totals.read_bytes = self.totals.read_bytes.saturating_add(read_bytes);
        self.totals.read_calls = self.totals.read_calls.saturating_add(read_calls);
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

    /// Validate the qualification semantics in addition to arithmetic
    /// reconciliation. Ordinary lifecycle phases must carry source-owned work;
    /// recovery alone may be absent for an uninterrupted run. Byte and call
    /// counters are paired so a synthetic byte-only or call-only row cannot be
    /// presented as observed application I/O.
    pub fn validate_for_qualification(&self) -> Result<(), GfError> {
        self.validate_reconciliation()?;
        for phase in StorageIoPhase::ALL {
            let totals = &self.phases[&phase];
            let observed = totals.read_bytes != 0
                || totals.write_bytes != 0
                || totals.read_calls != 0
                || totals.write_calls != 0
                || totals.object_count != 0
                || totals.block_count != 0
                || totals.fsync_calls != 0;
            if phase != StorageIoPhase::RecoveryReauthentication && !observed {
                return Err(validation(
                    "required lifecycle phase has no source-owned observation",
                ));
            }
            if (totals.read_bytes == 0) != (totals.read_calls == 0) {
                return Err(validation("phase read bytes and calls disagree"));
            }
            if (totals.write_bytes == 0) != (totals.write_calls == 0) {
                return Err(validation("phase write bytes and calls disagree"));
            }
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
    /// Distinct native identities and their allocation. This authenticated
    /// union is the cross-owner input to lifecycle peak tracking.
    #[serde(skip)]
    pub physical_identity_allocated_bytes: BTreeMap<String, u64>,
}

/// Exact native-identity union for the retained project container.
///
/// This includes `FORMAT`, `CURRENT`, and every authenticated generation still
/// installed in the retained generation namespace. Shared CAS objects are
/// deduplicated by native identity. Cleanup is the only operation allowed to
/// remove a generation from this inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectStorageIdentityUnion {
    /// Selected generation at the time of capture.
    pub selected_generation_uuid: uuid::Uuid,
    /// Authenticated generation identities represented in the union.
    pub retained_generation_uuids: BTreeSet<uuid::Uuid>,
    /// Exact native identities and allocated bytes.
    pub physical_identity_allocated_bytes: BTreeMap<String, u64>,
    /// Reconciled allocation of the identity union.
    pub allocated_bytes: u64,
}

/// One writer-owned change to an authenticated allocation owner. Transitions
/// are replayed in durable operation order so files removed before an API call
/// returns still contribute to the exact full-lifecycle high-water mark.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageAllocationTransition {
    /// Newly retained native identities and their allocated bytes.
    pub installed: BTreeMap<String, u64>,
    /// Native identities no longer retained by this owner.
    pub removed: BTreeSet<String>,
}

/// Capture the retained project/container allocation without recursively
/// scanning the project namespace.
///
/// The bounded generation namespace and each generation's authenticated
/// inventories are the authority. Only the immediate `generations/` directory
/// is enumerated; graph/project payload trees are never recursively scanned.
/// This deliberately includes checkpoint branches and unreachable generations
/// until an explicit cleanup/GC operation removes them.
pub fn capture_project_storage_identity_union(
    selected: &ResolvedProjectGeneration,
) -> Result<ProjectStorageIdentityUnion, GfError> {
    const MAX_RETAINED_GENERATIONS: usize = 4_096;
    let mut identities = BTreeMap::new();
    for control in ["FORMAT", "CURRENT"] {
        let file = File::open(selected.container_root().join(control)).map_err(storage)?;
        add_identity_allocation(&mut identities, &file)?;
    }

    let generations_root = selected.container_root().join("generations");
    let retained = std::fs::read_dir(&generations_root)
        .map_err(storage)?
        .map(|entry| {
            let entry = entry.map_err(storage)?;
            let file_type = entry.file_type().map_err(storage)?;
            if !file_type.is_dir() || file_type.is_symlink() {
                return Err(validation(
                    "retained generation namespace contains a non-directory entry",
                ));
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| validation("retained generation name is not UTF-8"))?;
            uuid::Uuid::parse_str(&name)
                .map_err(|_| validation("retained generation name is not a UUID"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if retained.len() > MAX_RETAINED_GENERATIONS {
        return Err(validation(
            "retained generation namespace exceeds attribution bound",
        ));
    }
    if !retained.contains(&selected.generation_uuid()) {
        return Err(validation(
            "selected generation is absent from retained namespace",
        ));
    }
    for uuid in &retained {
        let generation = crate::resolve_generation_by_uuid(selected.container_root(), *uuid)?;
        let snapshot = capture_storage_attribution(&generation)?;
        merge_identity_allocations(&mut identities, &snapshot.physical_identity_allocated_bytes)?;
    }
    let allocated_bytes = identities
        .values()
        .try_fold(0_u64, |total, value| checked_add(total, *value))?;
    Ok(ProjectStorageIdentityUnion {
        selected_generation_uuid: selected.generation_uuid(),
        retained_generation_uuids: retained,
        physical_identity_allocated_bytes: identities,
        allocated_bytes,
    })
}

fn add_identity_allocation(
    identities: &mut BTreeMap<String, u64>,
    file: &File,
) -> Result<(), GfError> {
    let identity = graphforge_filesystem::file_identity(file).map_err(storage)?;
    let usage = graphforge_filesystem::file_space_usage(file).map_err(storage)?;
    let key = native_identity_key(identity.volume_serial, &identity.file_id);
    merge_identity_allocations(identities, &BTreeMap::from([(key, usage.allocated_bytes)]))
}

fn merge_identity_allocations(
    target: &mut BTreeMap<String, u64>,
    source: &BTreeMap<String, u64>,
) -> Result<(), GfError> {
    for (identity, allocated) in source {
        if let Some(existing) = target.get(identity) {
            if existing != allocated {
                return Err(validation("retained identity allocation changed"));
            }
        } else {
            target.insert(identity.clone(), *allocated);
        }
    }
    Ok(())
}

/// Exact high-water tracker for simultaneously active authenticated files.
/// Owners are replaced atomically; aliases share one native identity and are
/// counted once until the final owner removes it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageAllocationLifecycle {
    owners: BTreeMap<String, BTreeSet<String>>,
    active: BTreeMap<String, (u64, u64)>,
    current_allocated_bytes: u64,
    peak_allocated_bytes: u64,
}

impl StorageAllocationLifecycle {
    /// Replace an owner's exact authenticated identity inventory.
    pub fn replace_owner(
        &mut self,
        owner: impl Into<String>,
        identities: &BTreeMap<String, u64>,
    ) -> Result<(), GfError> {
        let mut candidate = self.clone();
        candidate.replace_owner_inner(owner.into(), identities)?;
        *self = candidate;
        Ok(())
    }

    fn replace_owner_inner(
        &mut self,
        owner: String,
        identities: &BTreeMap<String, u64>,
    ) -> Result<(), GfError> {
        self.remove_owner(&owner)?;
        let mut installed = BTreeSet::new();
        for (identity, allocated) in identities {
            match self.active.get_mut(identity) {
                Some((existing, references)) => {
                    if existing != allocated {
                        return Err(validation("active identity allocation changed"));
                    }
                    *references = checked_add(*references, 1)?;
                }
                None => {
                    self.current_allocated_bytes =
                        checked_add(self.current_allocated_bytes, *allocated)?;
                    self.active.insert(identity.clone(), (*allocated, 1));
                }
            }
            installed.insert(identity.clone());
            self.peak_allocated_bytes = self.peak_allocated_bytes.max(self.current_allocated_bytes);
        }
        self.owners.insert(owner, installed);
        Ok(())
    }

    /// Replace an owner from a generation-bound storage snapshot.
    pub fn replace_snapshot_owner(
        &mut self,
        owner: impl Into<String>,
        snapshot: &StorageAttributionSnapshot,
    ) -> Result<(), GfError> {
        snapshot.validate_reconciliation()?;
        self.replace_owner(owner, &snapshot.physical_identity_allocated_bytes)
    }

    /// Apply one writer-owned install/remove transition without reconstructing
    /// an operation's historical state from its final filesystem layout.
    pub fn apply_owner_transition(
        &mut self,
        owner: impl Into<String>,
        transition: &StorageAllocationTransition,
    ) -> Result<(), GfError> {
        let mut candidate = self.clone();
        candidate.apply_owner_transition_inner(owner.into(), transition)?;
        *self = candidate;
        Ok(())
    }

    fn apply_owner_transition_inner(
        &mut self,
        owner: String,
        transition: &StorageAllocationTransition,
    ) -> Result<(), GfError> {
        if transition
            .installed
            .keys()
            .any(|identity| transition.removed.contains(identity))
        {
            return Err(validation(
                "allocation transition installs and removes one identity",
            ));
        }
        let owned = self.owners.entry(owner).or_default();
        for identity in &transition.removed {
            if !owned.remove(identity) {
                return Err(validation(
                    "allocation transition removes an unowned identity",
                ));
            }
            let (allocated, references) = self
                .active
                .get(identity)
                .copied()
                .ok_or_else(|| validation("active transition identity is absent"))?;
            if references == 1 {
                self.active.remove(identity);
                self.current_allocated_bytes = self
                    .current_allocated_bytes
                    .checked_sub(allocated)
                    .ok_or_else(|| validation("active allocation underflow"))?;
            } else {
                self.active
                    .insert(identity.clone(), (allocated, references - 1));
            }
        }
        for (identity, allocated) in &transition.installed {
            if !owned.insert(identity.clone()) {
                return Err(validation(
                    "allocation transition installs an owned identity",
                ));
            }
            match self.active.get_mut(identity) {
                Some((existing, references)) => {
                    if existing != allocated {
                        return Err(validation("active identity allocation changed"));
                    }
                    *references = checked_add(*references, 1)?;
                }
                None => {
                    self.current_allocated_bytes =
                        checked_add(self.current_allocated_bytes, *allocated)?;
                    self.active.insert(identity.clone(), (*allocated, 1));
                }
            }
            self.peak_allocated_bytes = self.peak_allocated_bytes.max(self.current_allocated_bytes);
        }
        Ok(())
    }

    /// Remove an owner and decrement every exact identity reference.
    pub fn remove_owner(&mut self, owner: &str) -> Result<(), GfError> {
        let Some(identities) = self.owners.remove(owner) else {
            return Ok(());
        };
        for identity in identities {
            let (allocated, references) = self
                .active
                .get(&identity)
                .copied()
                .ok_or_else(|| validation("active identity owner is absent"))?;
            if references == 1 {
                self.active.remove(&identity);
                self.current_allocated_bytes = self
                    .current_allocated_bytes
                    .checked_sub(allocated)
                    .ok_or_else(|| validation("active allocation underflow"))?;
            } else {
                self.active.insert(identity, (allocated, references - 1));
            }
        }
        Ok(())
    }

    /// Current exact identity-union allocation.
    #[must_use]
    pub const fn current_allocated_bytes(&self) -> u64 {
        self.current_allocated_bytes
    }

    /// Exact high-water allocation observed after every owner transition.
    #[must_use]
    pub const fn peak_allocated_bytes(&self) -> u64 {
        self.peak_allocated_bytes
    }
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
        let identity_allocated = self
            .physical_identity_allocated_bytes
            .values()
            .try_fold(0_u64, |total, value| checked_add(total, *value))?;
        if identity_allocated != self.allocated_bytes
            || self.physical_identity_allocated_bytes.len() as u64 != self.physical_objects
        {
            return Err(validation(
                "storage attribution identity union does not reconcile",
            ));
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
    physical_identity_allocated_bytes: BTreeMap<String, u64>,
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
            physical_identity_allocated_bytes: BTreeMap::new(),
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
            self.physical_identity_allocated_bytes.insert(
                native_identity_key(identity.volume_serial, &identity.file_id),
                usage.allocated_bytes,
            );
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
            physical_identity_allocated_bytes: self.physical_identity_allocated_bytes,
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

fn native_identity_key(volume_serial: u64, file_id: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut value = format!("{volume_serial:016x}:");
    for byte in file_id {
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
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
            physical_identity_allocated_bytes: BTreeMap::new(),
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
            physical_identity_allocated_bytes: BTreeMap::new(),
        };
        assert!(snapshot.validate_reconciliation().is_ok());
        assert!(snapshot.validate_for_qualification().is_err());
    }

    #[test]
    fn lifecycle_union_deduplicates_identities_and_decrements_owners() {
        let mut lifecycle = StorageAllocationLifecycle::default();
        let first = BTreeMap::from([("dev:a".to_owned(), 4096), ("dev:b".to_owned(), 8192)]);
        let alias = BTreeMap::from([("dev:b".to_owned(), 8192), ("dev:c".to_owned(), 4096)]);
        lifecycle.replace_owner("source", &first).unwrap();
        assert_eq!(lifecycle.current_allocated_bytes(), 12_288);
        lifecycle.replace_owner("import", &alias).unwrap();
        assert_eq!(lifecycle.current_allocated_bytes(), 16_384);
        assert_eq!(lifecycle.peak_allocated_bytes(), 16_384);
        lifecycle.remove_owner("source").unwrap();
        assert_eq!(lifecycle.current_allocated_bytes(), 12_288);
        lifecycle.remove_owner("import").unwrap();
        assert_eq!(lifecycle.current_allocated_bytes(), 0);
        assert_eq!(lifecycle.peak_allocated_bytes(), 16_384);
    }

    #[test]
    fn lifecycle_union_rejects_identity_allocation_disagreement() {
        let mut lifecycle = StorageAllocationLifecycle::default();
        lifecycle
            .replace_owner("first", &BTreeMap::from([("dev:a".to_owned(), 4096)]))
            .unwrap();
        assert!(
            lifecycle
                .replace_owner("alias", &BTreeMap::from([("dev:a".to_owned(), 8192)]))
                .is_err()
        );
    }

    #[test]
    fn lifecycle_transition_preserves_removed_intra_operation_peak() {
        let mut lifecycle = StorageAllocationLifecycle::default();
        lifecycle
            .apply_owner_transition(
                "construction",
                &StorageAllocationTransition {
                    installed: BTreeMap::from([
                        ("dev:staging".to_owned(), 4096),
                        ("dev:merge".to_owned(), 8192),
                    ]),
                    removed: BTreeSet::new(),
                },
            )
            .unwrap();
        lifecycle
            .apply_owner_transition(
                "construction",
                &StorageAllocationTransition {
                    installed: BTreeMap::from([("dev:encoded".to_owned(), 16_384)]),
                    removed: BTreeSet::new(),
                },
            )
            .unwrap();
        lifecycle
            .apply_owner_transition(
                "construction",
                &StorageAllocationTransition {
                    installed: BTreeMap::new(),
                    removed: BTreeSet::from(["dev:staging".to_owned(), "dev:merge".to_owned()]),
                },
            )
            .unwrap();
        assert_eq!(lifecycle.current_allocated_bytes(), 16_384);
        assert_eq!(lifecycle.peak_allocated_bytes(), 28_672);
    }

    #[test]
    fn construction_phase_inventory_reconciles_and_rejects_omission() {
        let evidence = GraphConstructionEvidence {
            seal_application_read_bytes: 11,
            shape_application_read_bytes: 13,
            shape_input_validation_read_operations: 1,
            merge_written_bytes: 5,
            parquet_write_operations: 1,
            encode_application_read_bytes: 17,
            encode_application_read_operations: 2,
            encode_application_write_bytes: 31,
            encode_application_write_operations: 4,
            encode_fsync_operations: 9,
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
        attribution.validate_for_qualification().unwrap();
        assert_eq!(attribution.phases.len(), StorageIoPhase::ALL.len());
        assert_eq!(attribution.totals.read_bytes, 153);
        assert_eq!(
            attribution.phases[&StorageIoPhase::RecoveryReauthentication].read_calls,
            2
        );
        attribution.add_recovery_reauthentication(9, 1);
        attribution.validate_for_qualification().unwrap();
        assert_eq!(
            attribution.phases[&StorageIoPhase::RecoveryReauthentication].read_bytes,
            50
        );
        assert_eq!(attribution.totals.read_bytes, 162);
        assert_eq!(attribution.totals.write_bytes, 163);
        assert_eq!(attribution.totals.read_calls, 22);
        assert_eq!(attribution.totals.write_calls, 19);
        assert_eq!(attribution.totals.fsync_calls, 29);
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
