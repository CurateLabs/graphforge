//! Authenticated, non-enumerating storage attribution for committed projects.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;

use graphforge_core::GfError;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

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
    pub fn from_construction(evidence: &GraphConstructionEvidence) -> Result<Self, GfError> {
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
            shape_phase_totals(evidence)?,
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
                fsync_calls: checked_add(
                    evidence.fsync_operations,
                    evidence.merge_fsync_operations,
                )?,
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
        let mut totals = PhaseIoTotals::default();
        for value in phases.values() {
            add_phase_totals(&mut totals, value)?;
        }
        Ok(Self { phases, totals })
    }

    /// Add writer-reported recovery reauthentication completed outside the
    /// construction session, such as interrupted portable finalization.
    pub fn add_recovery_reauthentication(
        &mut self,
        read_bytes: u64,
        read_calls: u64,
    ) -> Result<(), GfError> {
        let recovery = self
            .phases
            .entry(StorageIoPhase::RecoveryReauthentication)
            .or_default();
        recovery.read_bytes = checked_add(recovery.read_bytes, read_bytes)?;
        recovery.read_calls = checked_add(recovery.read_calls, read_calls)?;
        self.totals.read_bytes = checked_add(self.totals.read_bytes, read_bytes)?;
        self.totals.read_calls = checked_add(self.totals.read_calls, read_calls)?;
        Ok(())
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
    /// reconciliation. Every lifecycle phase must be present, while a phase
    /// that truthfully performed no I/O remains an explicit zero row. Byte and
    /// call counters are paired so a synthetic byte-only or call-only row
    /// cannot be presented as observed application I/O.
    pub fn validate_for_qualification(&self) -> Result<(), GfError> {
        self.validate_reconciliation()?;
        for phase in StorageIoPhase::ALL {
            let totals = &self.phases[&phase];
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

fn shape_phase_totals(evidence: &GraphConstructionEvidence) -> Result<PhaseIoTotals, GfError> {
    Ok(PhaseIoTotals {
        read_bytes: evidence.shape_application_read_bytes,
        write_bytes: checked_add(evidence.merge_written_bytes, evidence.parquet_write_bytes)?,
        read_calls: [
            evidence.shape_input_validation_read_operations,
            evidence.merge_read_operations,
            evidence.parquet_read_operations,
            evidence.shaped_output_authentication_operations,
            evidence.parent_catalog_read_operations,
            evidence.retained_probe_block_loads,
        ]
        .into_iter()
        .try_fold(0_u64, checked_add)?,
        write_calls: checked_add(
            evidence.merge_write_operations,
            evidence.parquet_write_operations,
        )?,
        block_count: checked_add(evidence.merge_read_blocks, evidence.merge_write_blocks)?,
        ..Default::default()
    })
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

/// Safe context binding category evidence to native receipt and identity roots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactCategoryAuthorityContext {
    /// Evidence contract domain.
    pub contract: String,
    /// Authority format version.
    pub version: u32,
    /// Deterministic lifecycle rung.
    pub rung: u64,
    /// Authenticated generation-manifest digest.
    pub generation_sha256: String,
    /// Storage owner within the lifecycle.
    pub owner: String,
    /// Digest of the storage-owned receipt/category ledger.
    pub receipt_authority_sha256: String,
    /// Digest of native identity membership and allocation.
    pub native_identity_authority_sha256: String,
    /// Per-category native identity-membership roots.
    pub native_category_identity_authority_sha256: BTreeMap<ArtifactCategory, String>,
    /// Authoritative reopened node denominator.
    pub live_nodes: u64,
    /// Authoritative reopened edge denominator.
    pub live_edges: u64,
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
    /// Independently accumulated receipt and native-identity category totals.
    #[serde(skip)]
    category_authorities: BTreeMap<ArtifactCategory, ArtifactStorageTotals>,
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
    /// Native identity allocation partitioned by its authenticated category.
    #[serde(skip)]
    category_physical_identity_allocated_bytes: BTreeMap<ArtifactCategory, BTreeMap<String, u64>>,
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
    /// Strip private identities from a validated authenticated snapshot.
    pub fn from_snapshot(snapshot: &StorageAttributionSnapshot) -> Result<Self, GfError> {
        snapshot.validate_for_qualification()?;
        Ok(Self {
            contract: "graphforge-storage-attribution/1".to_owned(),
            categories: snapshot.categories.clone(),
            logical_references: snapshot.logical_references,
            logical_bytes: snapshot.logical_bytes,
            retained_logical_eof_bytes: snapshot.physical_logical_bytes,
            allocated_physical_bytes: snapshot.allocated_bytes,
            physical_objects: snapshot.physical_objects,
        })
    }

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
    let retained_cas = crate::graph_object_store::capture_retained_graph_object_identities(
        selected.container_root(),
    )?;
    merge_identity_allocations(&mut identities, &retained_cas)?;
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
            if let Some((existing, references)) = self.active.get_mut(identity) {
                if existing != allocated {
                    return Err(validation("active identity allocation changed"));
                }
                *references = checked_add(*references, 1)?;
            } else {
                self.current_allocated_bytes =
                    checked_add(self.current_allocated_bytes, *allocated)?;
                self.active.insert(identity.clone(), (*allocated, 1));
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
        let owner_identities = self.owners.entry(owner).or_default();
        for identity in &transition.removed {
            if !owner_identities.remove(identity) {
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
            if !owner_identities.insert(identity.clone()) {
                return Err(validation(
                    "allocation transition installs an owned identity",
                ));
            }
            if let Some((existing, references)) = self.active.get_mut(identity) {
                if existing != allocated {
                    return Err(validation("active identity allocation changed"));
                }
                *references = checked_add(*references, 1)?;
            } else {
                self.current_allocated_bytes =
                    checked_add(self.current_allocated_bytes, *allocated)?;
                self.active.insert(identity.clone(), (*allocated, 1));
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

    /// Exact deduplicated allocation for a closed set of active owners.
    ///
    /// Shared native identities are counted once. Missing owners are rejected
    /// so an emitted aggregate cannot silently omit a lifecycle owner.
    pub fn owner_union_allocated_bytes<'a>(
        &self,
        owners: impl IntoIterator<Item = &'a str>,
    ) -> Result<u64, GfError> {
        let mut identities = BTreeSet::new();
        for owner in owners {
            let owned = self
                .owners
                .get(owner)
                .ok_or_else(|| validation("allocation union owner is absent"))?;
            identities.extend(owned.iter());
        }
        identities.into_iter().try_fold(0_u64, |total, identity| {
            let (allocated, _) = self
                .active
                .get(identity)
                .ok_or_else(|| validation("allocation union identity is absent"))?;
            checked_add(total, *allocated)
        })
    }
}

impl StorageAttributionSnapshot {
    /// Identity-free category authorities derived from authenticated inventory
    /// receipts and the per-category native-identity union.
    pub fn category_authorities(
        &self,
    ) -> Result<BTreeMap<ArtifactCategory, ArtifactStorageTotals>, GfError> {
        self.validate_reconciliation()?;
        if self.category_authorities.len() != ArtifactCategory::ALL.len()
            || self.category_authorities != self.categories
        {
            return Err(validation(
                "reported categories differ from independent receipt/identity authorities",
            ));
        }
        Ok(self.category_authorities.clone())
    }

    /// Sanitized commitments to independently accumulated category authority.
    pub fn category_authority_commitments(
        &self,
        context: &ArtifactCategoryAuthorityContext,
    ) -> Result<BTreeMap<ArtifactCategory, String>, GfError> {
        self.category_authorities()?;
        Ok(self
            .category_authorities
            .iter()
            .map(|(category, totals)| {
                (
                    *category,
                    artifact_category_authority_commitment(context, *category, totals),
                )
            })
            .collect())
    }

    /// Build safe authority context from hidden receipt and identity membership.
    pub fn category_authority_context(
        &self,
        contract: &str,
        rung: u64,
        owner: &str,
        live_nodes: u64,
        live_edges: u64,
    ) -> Result<ArtifactCategoryAuthorityContext, GfError> {
        self.category_authorities()?;
        if contract.is_empty()
            || owner.is_empty()
            || rung == 0
            || live_nodes == 0
            || live_edges == 0
        {
            return Err(validation("category authority context is invalid"));
        }
        Ok(ArtifactCategoryAuthorityContext {
            contract: contract.to_owned(),
            version: 1,
            rung,
            generation_sha256: format_sha256(&self.generation_manifest_sha256),
            owner: owner.to_owned(),
            receipt_authority_sha256: category_map_authority_sha256(
                b"graphforge-storage-receipt-authority-v1\0",
                &self.category_authorities,
            ),
            native_identity_authority_sha256: identity_map_authority_sha256(
                &self.physical_identity_allocated_bytes,
            ),
            native_category_identity_authority_sha256: self
                .category_physical_identity_allocated_bytes
                .iter()
                .map(|(category, identities)| {
                    (*category, identity_map_authority_sha256(identities))
                })
                .collect(),
            live_nodes,
            live_edges,
        })
    }

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
            || ArtifactCategory::ALL
                .iter()
                .any(|category| !self.category_authorities.contains_key(category))
            || self.categories.len() != ArtifactCategory::ALL.len()
            || self.category_authorities.len() != ArtifactCategory::ALL.len()
        {
            return Err(validation("storage attribution is missing a category"));
        }
        if self.categories != self.category_authorities {
            return Err(validation(
                "storage categories differ from independent receipt/identity authorities",
            ));
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
    /// in [`ArtifactCategory::Other`], or when any category reports more
    /// physical objects than logical references.
    pub fn validate_for_qualification(&self) -> Result<(), GfError> {
        self.validate_reconciliation()?;
        if !self.is_fully_classified() {
            return Err(validation(
                "storage attribution contains unclassified retained artifacts",
            ));
        }
        for category in ArtifactCategory::ALL {
            let totals = &self.categories[&category];
            if totals.physical_objects > totals.logical_references {
                return Err(validation(
                    "storage attribution category physical identities contradict",
                ));
            }
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
            // Graph-manifest CAS chunks are retained catalog artifacts. Count each
            // as a logical reference before attributing its physical identity so
            // category reconciliation cannot report physical_objects without refs.
            for (digest, length) in manifest_objects {
                let object = lease.open(&digest, length)?;
                accumulator.add_logical(ArtifactCategory::CatalogAndManifests, length)?;
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
    category_authorities: BTreeMap<ArtifactCategory, ArtifactStorageTotals>,
    physical_seen: BTreeSet<(u64, [u8; 16])>,
    physical_identity_allocated_bytes: BTreeMap<String, u64>,
    category_physical_identity_allocated_bytes: BTreeMap<ArtifactCategory, BTreeMap<String, u64>>,
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
            category_authorities: ArtifactCategory::ALL
                .into_iter()
                .map(|category| (category, ArtifactStorageTotals::default()))
                .collect(),
            physical_seen: BTreeSet::new(),
            physical_identity_allocated_bytes: BTreeMap::new(),
            category_physical_identity_allocated_bytes: ArtifactCategory::ALL
                .into_iter()
                .map(|category| (category, BTreeMap::new()))
                .collect(),
        }
    }

    fn add_logical(&mut self, category: ArtifactCategory, bytes: u64) -> Result<(), GfError> {
        let totals = self
            .categories
            .get_mut(&category)
            .expect("complete categories");
        totals.logical_references = checked_add(totals.logical_references, 1)?;
        totals.logical_bytes = checked_add(totals.logical_bytes, bytes)?;
        let authority = self
            .category_authorities
            .get_mut(&category)
            .expect("complete category authorities");
        authority.logical_references = checked_add(authority.logical_references, 1)?;
        authority.logical_bytes = checked_add(authority.logical_bytes, bytes)?;
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
            let identity_key = native_identity_key(identity.volume_serial, &identity.file_id);
            self.physical_identity_allocated_bytes
                .insert(identity_key.clone(), usage.allocated_bytes);
            self.category_physical_identity_allocated_bytes
                .get_mut(&category)
                .expect("complete category identity authorities")
                .insert(identity_key, usage.allocated_bytes);
            let totals = self
                .categories
                .get_mut(&category)
                .expect("complete categories");
            totals.physical_objects = checked_add(totals.physical_objects, 1)?;
            totals.physical_logical_bytes =
                checked_add(totals.physical_logical_bytes, usage.logical_bytes)?;
            totals.allocated_bytes = checked_add(totals.allocated_bytes, usage.allocated_bytes)?;
            let authority = self
                .category_authorities
                .get_mut(&category)
                .expect("complete category authorities");
            authority.physical_objects = checked_add(authority.physical_objects, 1)?;
            authority.physical_logical_bytes =
                checked_add(authority.physical_logical_bytes, usage.logical_bytes)?;
            authority.allocated_bytes =
                checked_add(authority.allocated_bytes, usage.allocated_bytes)?;
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
            category_authorities: self.category_authorities,
            logical_references: total.logical_references,
            logical_bytes: total.logical_bytes,
            physical_objects: total.physical_objects,
            physical_logical_bytes: total.physical_logical_bytes,
            allocated_bytes: total.allocated_bytes,
            physical_identity_allocated_bytes: self.physical_identity_allocated_bytes,
            category_physical_identity_allocated_bytes: self
                .category_physical_identity_allocated_bytes,
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

/// Commit to one independently derived category without exposing identities.
#[must_use]
pub fn artifact_category_authority_commitment(
    context: &ArtifactCategoryAuthorityContext,
    category: ArtifactCategory,
    totals: &ArtifactStorageTotals,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-category-authority-v2\0");
    update_authority_context(&mut digest, context, category);
    digest.update(artifact_category_name(category));
    for value in artifact_totals_values(totals) {
        digest.update(value.to_be_bytes());
    }
    format_sha256(&digest.finalize())
}

/// Commit to one independently derived category allocation high-water mark.
#[must_use]
pub fn artifact_category_peak_authority_commitment(
    context: &ArtifactCategoryAuthorityContext,
    category: ArtifactCategory,
    allocated_bytes: u64,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-category-peak-authority-v2\0");
    update_authority_context(&mut digest, context, category);
    digest.update(artifact_category_name(category));
    digest.update(allocated_bytes.to_be_bytes());
    format_sha256(&digest.finalize())
}

fn update_authority_context(
    digest: &mut Sha256,
    context: &ArtifactCategoryAuthorityContext,
    category: ArtifactCategory,
) {
    for value in [
        context.contract.as_bytes(),
        context.generation_sha256.as_bytes(),
        context.owner.as_bytes(),
        context.receipt_authority_sha256.as_bytes(),
        context.native_identity_authority_sha256.as_bytes(),
    ] {
        digest.update((value.len() as u128).to_be_bytes());
        digest.update(value);
    }
    digest.update(context.version.to_be_bytes());
    digest.update(context.rung.to_be_bytes());
    digest.update(context.live_nodes.to_be_bytes());
    digest.update(context.live_edges.to_be_bytes());
    let category_identity = context
        .native_category_identity_authority_sha256
        .get(&category)
        .expect("complete native category identity authority");
    digest.update((category_identity.len() as u128).to_be_bytes());
    digest.update(category_identity.as_bytes());
}

pub(crate) fn category_map_authority_sha256(
    domain: &[u8],
    categories: &BTreeMap<ArtifactCategory, ArtifactStorageTotals>,
) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    for category in ArtifactCategory::ALL {
        digest.update(artifact_category_name(category));
        for value in artifact_totals_values(&categories[&category]) {
            digest.update(value.to_be_bytes());
        }
    }
    format_sha256(&digest.finalize())
}

pub(crate) fn identity_map_authority_sha256(identities: &BTreeMap<String, u64>) -> String {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-native-identity-authority-v1\0");
    for (identity, allocated_bytes) in identities {
        digest.update((identity.len() as u128).to_be_bytes());
        digest.update(identity.as_bytes());
        digest.update(allocated_bytes.to_be_bytes());
    }
    format_sha256(&digest.finalize())
}

fn artifact_category_name(category: ArtifactCategory) -> &'static [u8] {
    match category {
        ArtifactCategory::TopologyNodes => b"topology_nodes",
        ArtifactCategory::TopologyEdges => b"topology_edges",
        ArtifactCategory::Properties => b"properties",
        ArtifactCategory::UuidAndSurrogates => b"uuid_and_surrogates",
        ArtifactCategory::Adjacency => b"adjacency",
        ArtifactCategory::CatalogAndManifests => b"catalog_and_manifests",
        ArtifactCategory::ConstructionStaging => b"construction_staging",
        ArtifactCategory::PortablePackage => b"portable_package",
        ArtifactCategory::CleanImportedProject => b"clean_imported_project",
        ArtifactCategory::Other => b"other",
    }
}

fn format_sha256(digest: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut value = String::with_capacity(7 + digest.len() * 2);
    value.push_str("sha256:");
    for byte in digest {
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

fn artifact_totals_values(totals: &ArtifactStorageTotals) -> [u64; 5] {
    [
        totals.logical_references,
        totals.logical_bytes,
        totals.physical_objects,
        totals.physical_logical_bytes,
        totals.allocated_bytes,
    ]
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

    fn publish_compact_fixture(
        project: &Path,
        workspace: &Path,
    ) -> crate::ResolvedProjectGeneration {
        let (_, graph) = crate::capture_graph_files(workspace).unwrap();
        let mut participants = crate::empty_workspace_participants().unwrap();
        participants.insert(0, graph);
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: uuid::Uuid::now_v7(),
            generation_uuid: uuid::Uuid::now_v7(),
            capabilities: vec![
                crate::ProjectCapability {
                    capability_id: crate::GRAPH_CAPABILITY_ID.into(),
                    capability_version: crate::GRAPH_CAPABILITY_VERSION,
                },
                crate::ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants,
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation_with_graph_tree(project, &request, Some(workspace))
                .unwrap()
        else {
            panic!("fresh compact fixture unexpectedly replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        crate::resolve_project_generation(project).unwrap()
    }

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
            category_authorities: categories.clone(),
            categories,
            logical_references: 0,
            logical_bytes: 7,
            physical_objects: 0,
            physical_logical_bytes: 0,
            allocated_bytes: 0,
            physical_identity_allocated_bytes: BTreeMap::new(),
            category_physical_identity_allocated_bytes: ArtifactCategory::ALL
                .into_iter()
                .map(|category| (category, BTreeMap::new()))
                .collect(),
        };
        snapshot.validate_reconciliation().unwrap();
        snapshot
            .categories
            .get_mut(&ArtifactCategory::TopologyNodes)
            .unwrap()
            .logical_bytes = 6;
        snapshot
            .categories
            .get_mut(&ArtifactCategory::TopologyEdges)
            .unwrap()
            .logical_bytes = 1;
        assert!(snapshot.validate_reconciliation().is_err());
        snapshot.categories = snapshot.category_authorities.clone();
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
            category_authorities: categories.clone(),
            categories,
            logical_references: 1,
            logical_bytes: 0,
            physical_objects: 0,
            physical_logical_bytes: 0,
            allocated_bytes: 0,
            physical_identity_allocated_bytes: BTreeMap::new(),
            category_physical_identity_allocated_bytes: ArtifactCategory::ALL
                .into_iter()
                .map(|category| (category, BTreeMap::new()))
                .collect(),
        };
        assert!(snapshot.validate_reconciliation().is_ok());
        assert!(snapshot.validate_for_qualification().is_err());
    }

    #[test]
    fn qualification_rejects_category_physical_without_logical_refs() {
        let mut categories: BTreeMap<_, _> = ArtifactCategory::ALL
            .into_iter()
            .map(|category| (category, ArtifactStorageTotals::default()))
            .collect();
        categories
            .get_mut(&ArtifactCategory::CatalogAndManifests)
            .unwrap()
            .physical_objects = 2;
        categories
            .get_mut(&ArtifactCategory::CatalogAndManifests)
            .unwrap()
            .logical_references = 1;
        let snapshot = StorageAttributionSnapshot {
            generation_uuid: uuid::Uuid::nil(),
            generation_manifest_sha256: [0; 32],
            category_authorities: categories.clone(),
            categories,
            logical_references: 1,
            logical_bytes: 0,
            physical_objects: 2,
            physical_logical_bytes: 0,
            allocated_bytes: 0,
            physical_identity_allocated_bytes: BTreeMap::from([
                ("dev:a".to_owned(), 0),
                ("dev:b".to_owned(), 0),
            ]),
            category_physical_identity_allocated_bytes: ArtifactCategory::ALL
                .into_iter()
                .map(|category| {
                    let identities = if category == ArtifactCategory::CatalogAndManifests {
                        BTreeMap::from([("dev:a".to_owned(), 0), ("dev:b".to_owned(), 0)])
                    } else {
                        BTreeMap::new()
                    };
                    (category, identities)
                })
                .collect(),
        };
        assert!(snapshot.validate_reconciliation().is_ok());
        assert!(snapshot.validate_for_qualification().is_err());
    }

    #[test]
    fn compact_v2_catalog_manifest_objects_have_matching_logical_refs() {
        // Durable project admission rejects tmpfs; keep the fixture on the build
        // target volume (ext4 on OVHC-AGENCY / CI runners).
        let scratch = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .filter(|path| path.is_dir())
            .unwrap_or_else(std::env::temp_dir);
        let project = tempfile::TempDir::new_in(&scratch).unwrap();
        let workspace = tempfile::TempDir::new_in(&scratch).unwrap();
        let topology = workspace.path().join("topology").join("nodes");
        std::fs::create_dir_all(&topology).unwrap();
        // Enough inventory entries that the compact graph-files root spans multiple
        // CAS objects (the OVHC-AGENCY S18 failure mode).
        for index in 0..64 {
            std::fs::write(
                topology.join(format!("{index}.parquet")),
                format!("compact-catalog-fixture-{index}"),
            )
            .unwrap();
        }
        let _ = crate::open_or_initialize_project(project.path()).unwrap();
        let generation = publish_compact_fixture(project.path(), workspace.path());
        let snapshot = capture_storage_attribution(&generation).unwrap();
        snapshot.validate_reconciliation().unwrap();
        snapshot.validate_for_qualification().unwrap();
        let catalog = &snapshot.categories[&ArtifactCategory::CatalogAndManifests];
        assert!(
            catalog.physical_objects > 1,
            "fixture must exercise multi-object catalog manifests; got {}",
            catalog.physical_objects
        );
        assert!(
            catalog.physical_objects <= catalog.logical_references,
            "catalog physical_objects={} exceeded logical_references={}",
            catalog.physical_objects,
            catalog.logical_references
        );
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
        assert_eq!(
            lifecycle
                .owner_union_allocated_bytes(["source", "import"])
                .unwrap(),
            16_384
        );
        assert_eq!(
            lifecycle.owner_union_allocated_bytes(["source"]).unwrap(),
            12_288
        );
        assert!(lifecycle.owner_union_allocated_bytes(["missing"]).is_err());
        let contradictory = BTreeMap::from([("dev:b".to_owned(), 16_384)]);
        assert!(lifecycle.replace_owner("invalid", &contradictory).is_err());
        assert_eq!(lifecycle.current_allocated_bytes(), 16_384);
        assert_eq!(lifecycle.peak_allocated_bytes(), 16_384);
        lifecycle.remove_owner("source").unwrap();
        assert_eq!(lifecycle.current_allocated_bytes(), 12_288);
        lifecycle.remove_owner("import").unwrap();
        assert_eq!(lifecycle.current_allocated_bytes(), 0);
        assert_eq!(lifecycle.peak_allocated_bytes(), 16_384);
    }

    #[test]
    fn project_union_keeps_noncurrent_generations_and_deduplicates_shared_cas_identity() {
        let project = tempfile::tempdir().unwrap();
        let initial = crate::open_or_initialize_project(project.path()).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let topology = workspace.path().join("topology");
        std::fs::create_dir_all(&topology).unwrap();
        std::fs::write(topology.join("nodes.parquet"), b"shared compact payload").unwrap();

        let first = publish_compact_fixture(project.path(), workspace.path());
        let first_snapshot = capture_storage_attribution(&first).unwrap();
        let current = publish_compact_fixture(project.path(), workspace.path());
        let current_snapshot = capture_storage_attribution(&current).unwrap();
        let union = capture_project_storage_identity_union(&current).unwrap();

        assert_ne!(first.generation_uuid(), current.generation_uuid());
        assert!(
            union
                .retained_generation_uuids
                .contains(&initial.generation_uuid())
        );
        assert!(
            union
                .retained_generation_uuids
                .contains(&first.generation_uuid())
        );
        assert!(
            union
                .retained_generation_uuids
                .contains(&current.generation_uuid())
        );
        for identity in first_snapshot
            .physical_identity_allocated_bytes
            .keys()
            .chain(current_snapshot.physical_identity_allocated_bytes.keys())
        {
            assert!(
                union
                    .physical_identity_allocated_bytes
                    .contains_key(identity)
            );
        }
        let mut repeated_reference = first_snapshot.physical_identity_allocated_bytes.clone();
        merge_identity_allocations(
            &mut repeated_reference,
            &first_snapshot.physical_identity_allocated_bytes,
        )
        .unwrap();
        assert_eq!(
            repeated_reference, first_snapshot.physical_identity_allocated_bytes,
            "a shared physical identity must remain one union member"
        );
        let mut deduplicated = first_snapshot.physical_identity_allocated_bytes.clone();
        merge_identity_allocations(
            &mut deduplicated,
            &current_snapshot.physical_identity_allocated_bytes,
        )
        .unwrap();
        assert_eq!(
            union.allocated_bytes,
            union
                .physical_identity_allocated_bytes
                .values()
                .copied()
                .sum::<u64>()
        );
    }

    #[test]
    fn project_union_retains_unreferenced_cas_identity_until_explicit_gc_receipt() {
        let project = tempfile::tempdir().unwrap();
        let generation = crate::open_or_initialize_project(project.path()).unwrap();
        let (digest, installed) = crate::graph_object_store::install_graph_object_bytes(
            project.path(),
            b"unreferenced retained CAS payload",
        )
        .unwrap();
        assert!(installed.bytes_installed > 0);
        let object = File::open(
            crate::graph_object_store::graph_object_path(project.path(), &digest).unwrap(),
        )
        .unwrap();
        let identity = graphforge_filesystem::file_identity(&object).unwrap();
        let key = native_identity_key(identity.volume_serial, &identity.file_id);

        let before = capture_project_storage_identity_union(&generation).unwrap();
        assert!(
            before.physical_identity_allocated_bytes.contains_key(&key),
            "sealed CAS remains retained even when no generation references it"
        );
        let gc = crate::graph_object_store::gc_graph_objects(
            project.path(),
            &[],
            crate::GraphManifestLimits::default(),
        )
        .unwrap();
        assert_eq!(gc.objects_removed, 1);
        assert!(gc.bytes_removed > 0);
        assert_eq!(
            gc.removed_identity_allocated_bytes.get(&key),
            before.physical_identity_allocated_bytes.get(&key)
        );
        let reopened = crate::resolve_project_generation(project.path()).unwrap();
        let after = capture_project_storage_identity_union(&reopened).unwrap();
        assert!(!after.physical_identity_allocated_bytes.contains_key(&key));
        assert!(after.allocated_bytes < before.allocated_bytes);
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
            merge_read_operations: 2,
            parquet_read_operations: 3,
            shaped_output_authentication_operations: 4,
            parent_catalog_read_operations: 5,
            retained_probe_block_loads: 6,
            merge_written_bytes: 5,
            merge_write_operations: 2,
            parquet_write_bytes: 7,
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
        let mut attribution = ConstructionPhaseAttribution::from_construction(&evidence).unwrap();
        attribution.validate_reconciliation().unwrap();
        attribution.validate_for_qualification().unwrap();
        assert_eq!(attribution.phases.len(), StorageIoPhase::ALL.len());
        assert_eq!(
            attribution.phases[&StorageIoPhase::ShapeConsumeReauthentication],
            PhaseIoTotals {
                read_bytes: 13,
                write_bytes: 12,
                read_calls: 21,
                write_calls: 3,
                ..Default::default()
            }
        );
        assert_eq!(attribution.totals.read_bytes, 153);
        assert_eq!(
            attribution.phases[&StorageIoPhase::RecoveryReauthentication].read_calls,
            2
        );
        attribution.add_recovery_reauthentication(9, 1).unwrap();
        attribution.validate_for_qualification().unwrap();
        assert_eq!(
            attribution.phases[&StorageIoPhase::RecoveryReauthentication].read_bytes,
            50
        );
        assert_eq!(attribution.totals.read_bytes, 162);
        assert_eq!(attribution.totals.write_bytes, 170);
        assert_eq!(attribution.totals.read_calls, 42);
        assert_eq!(attribution.totals.write_calls, 21);
        assert_eq!(attribution.totals.fsync_calls, 29);
        attribution
            .phases
            .remove(&StorageIoPhase::RecoveryReauthentication);
        assert!(attribution.validate_reconciliation().is_err());
    }

    #[test]
    fn construction_phase_inventory_rejects_double_counted_total() {
        let mut attribution =
            ConstructionPhaseAttribution::from_construction(&GraphConstructionEvidence::default())
                .unwrap();
        attribution.totals.read_bytes = 1;
        assert!(attribution.validate_reconciliation().is_err());
    }

    #[test]
    fn construction_phase_derivation_rejects_counter_overflow() {
        let evidence = GraphConstructionEvidence {
            shape_input_validation_read_operations: u64::MAX,
            merge_read_operations: 1,
            ..GraphConstructionEvidence::default()
        };
        assert!(ConstructionPhaseAttribution::from_construction(&evidence).is_err());
        let evidence = GraphConstructionEvidence {
            shape_application_read_bytes: u64::MAX,
            encode_application_read_bytes: 1,
            ..GraphConstructionEvidence::default()
        };
        assert!(evidence.total_application_read_bytes().is_err());
    }

    #[test]
    fn construction_category_authority_is_independent_from_reported_view() {
        let mut categories: BTreeMap<_, _> = ArtifactCategory::ALL
            .into_iter()
            .map(|category| (category, ArtifactStorageTotals::default()))
            .collect();
        categories
            .get_mut(&ArtifactCategory::ConstructionStaging)
            .unwrap()
            .allocated_bytes = 4096;
        let mut peaks: BTreeMap<_, _> = ArtifactCategory::ALL
            .into_iter()
            .map(|category| (category, 0))
            .collect();
        peaks.insert(ArtifactCategory::ConstructionStaging, 4096);
        let mut evidence = GraphConstructionEvidence {
            storage_current: categories.clone(),
            storage_receipt_category_authorities: categories,
            storage_transient_peak_allocated_bytes: peaks.clone(),
            storage_receipt_transient_peak_authorities: peaks,
            storage_active_identity_allocated_bytes: BTreeMap::from([(
                "native-identity".to_owned(),
                4096,
            )]),
            ..GraphConstructionEvidence::default()
        };
        evidence.storage_category_authorities().unwrap();
        let context = evidence
            .storage_category_authority_context(
                "test-contract",
                1,
                "sha256:generation",
                "construction",
                1,
                1,
            )
            .unwrap();
        assert_ne!(
            context.native_category_identity_authority_sha256
                [&ArtifactCategory::ConstructionStaging],
            context.native_category_identity_authority_sha256[&ArtifactCategory::TopologyNodes]
        );
        evidence
            .storage_current
            .get_mut(&ArtifactCategory::ConstructionStaging)
            .unwrap()
            .logical_bytes = 1;
        assert!(evidence.storage_category_authorities().is_err());
    }

    #[test]
    fn qualification_preserves_truthful_zero_io_phase_rows() {
        let attribution =
            ConstructionPhaseAttribution::from_construction(&GraphConstructionEvidence::default())
                .unwrap();
        attribution.validate_for_qualification().unwrap();
        assert_eq!(attribution.phases.len(), StorageIoPhase::ALL.len());
        assert!(
            attribution
                .phases
                .values()
                .all(|totals| totals == &PhaseIoTotals::default())
        );
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

        let receipt = StorageAttributionReceipt::from_snapshot(&snapshot).unwrap();
        receipt.validate_reconciliation().unwrap();
        assert_eq!(receipt.retained_logical_eof_bytes, 6);
        assert_eq!(receipt.allocated_physical_bytes, snapshot.allocated_bytes);
        let json = serde_json::to_string(&receipt).unwrap();
        assert!(!json.contains(&snapshot.generation_uuid.to_string()));
        assert!(!json.contains("generation_uuid"));
        assert!(!json.contains("sha256"));
        assert!(!json.contains(project.path().to_string_lossy().as_ref()));
    }
}
