//! Verification-first, bounded portable-v2 project import.

use std::fs::{self, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use graphforge_core::GfError;
use graphforge_ontology::{
    ActivationMode, ActivationRecord, ActivationScope, AuthoredModule, BridgeSetId,
    CompositionLimits, InventoryCompileRequest, OntologyModuleId, compile_inventory,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::project_generation::open_or_initialize_project_admitted;
use crate::project_portable::{prepare_import_target, semantically_pristine_generation};
use crate::project_portable_v2::{
    PortableV2Error, PortableV2ErrorCode, PortableV2Limits, PortableV2PackageClass,
    PortableV2Report, RUNTIME_MAP_PATH, decode_runtime_map, materialize_verified_portable_v2,
};
use crate::project_publication::{
    ProjectCapability, ProjectFileParticipant, ProjectGenerationRequest, ProjectParticipant,
    ProjectParticipantEncoding, ProjectPublicationReceipt, ProjectStageOutcome,
    stage_project_generation_from_files_admitted,
};

#[derive(Debug)]
/// Durable receipt for one verified complete-package import.
pub struct PortableV2ImportReceipt {
    /// Canonical semantic package identity.
    pub package_digest: String,
    /// Representation-specific transport identity.
    pub transport_digest: Option<String>,
    /// Atomic project-generation publication receipt.
    pub publication: ProjectPublicationReceipt,
    /// Durable non-authoritative composition candidate, when imported.
    pub staged_composition: Option<PortableV2StagedCompositionReceipt>,
    /// Exact native identities simultaneously retained by private materialization.
    pub materialized_identity_allocated_bytes: std::collections::BTreeMap<String, u64>,
    /// Exact authenticated identity union of the published generation.
    pub published_identity_allocated_bytes: std::collections::BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Payload-free identity of the durable non-authoritative candidate.
pub struct PortableV2StagedCompositionReceipt {
    /// Verified portable package identity.
    pub package_digest: String,
    /// Verified portable composition identity.
    pub portable_composition_digest: String,
    /// Recompiled workspace composition identity.
    pub workspace_composition_fingerprint: String,
}

/// Authenticated selective-package state passed to an explicit typed consumer.
///
/// Payloads remain in callback-scoped private staging and are removed before
/// [`consume_selective_portable_v2`] returns. The ontology candidate is never
/// installed as project authority implicitly.
#[derive(Debug)]
pub struct PortableV2SelectiveCandidate {
    /// Full authenticated package report.
    pub report: PortableV2Report,
    /// Recompiled, non-authoritative ontology candidate when present.
    pub ontology: Option<crate::WorkspacePortableOntologyStaging>,
    /// Payload-free staged-composition receipt when present.
    pub staged_composition: Option<PortableV2StagedCompositionReceipt>,
}

/// Verify and privately materialize a selective package for one typed consumer.
///
/// Complete packages must use [`import_complete_portable_v2`]. The callback is
/// the only scope in which authenticated data paths are available; staging is
/// deterministically removed on success or error and no project is mutated.
pub fn consume_selective_portable_v2<T>(
    source: impl AsRef<Path>,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
    consume: impl FnOnce(&Path, &PortableV2SelectiveCandidate) -> Result<T, PortableV2Error>,
) -> Result<T, PortableV2Error> {
    let owner = tempfile::tempdir().map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "cannot create selective staging")
    })?;
    let stage = owner.path().join("materialized");
    let report = materialize_verified_portable_v2(source.as_ref(), &stage, limits, cancelled)?;
    if report.package_class == PortableV2PackageClass::Complete {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "complete package requires complete-project import",
        ));
    }
    let (ontology, staged_composition) = if report.ontology_composition.is_some() {
        let (candidate, receipt) = build_staged_composition(&stage, &report, limits)?;
        let bytes = read_bounded_payload(
            &candidate.source,
            limits.max_manifest_bytes,
            "selective staged composition",
        )?;
        (
            Some(
                crate::WorkspacePortableOntologyStaging::from_canonical_json(&bytes)
                    .map_err(storage)?,
            ),
            Some(receipt),
        )
    } else {
        (None, None)
    };
    consume(
        &stage,
        &PortableV2SelectiveCandidate {
            report,
            ontology,
            staged_composition,
        },
    )
}

/// Reopen and validate the durable non-authoritative ontology candidate.
///
/// This function never changes active ontology authority. It is the storage
/// seam consumed by an explicit lifecycle request.
pub fn load_portable_ontology_staging(
    generation: &crate::ResolvedProjectGeneration,
    limits: PortableV2Limits,
) -> Result<Option<crate::WorkspacePortableOntologyStaging>, PortableV2Error> {
    let present = generation
        .participant_descriptors()
        .map_err(storage)?
        .iter()
        .any(|descriptor| {
            descriptor.capability_id == crate::WORKSPACE_CAPABILITY_ID
                && descriptor.record_family_id == crate::WORKSPACE_PORTABLE_ONTOLOGY_STAGING_FAMILY
        });
    if !present {
        return Ok(None);
    }
    let path = generation
        .participant_path(
            crate::WORKSPACE_CAPABILITY_ID,
            crate::WORKSPACE_PORTABLE_ONTOLOGY_STAGING_FAMILY,
        )
        .map_err(storage)?;
    let bytes = read_bounded_payload(&path, limits.max_manifest_bytes, "staged composition")?;
    crate::WorkspacePortableOntologyStaging::from_canonical_json(&bytes)
        .map(Some)
        .map_err(storage)
}

/// Sanitized import lifecycle phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortableV2ImportPhase {
    /// Shared verification and private materialization are running.
    Verifying,
    /// Authenticated component entries are ready for publication.
    Materialized,
    /// The imported generation was published and reopened.
    Published,
}

/// Aggregate, payload-free portable import progress.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortableV2ImportProgress {
    /// Current lifecycle phase.
    pub phase: PortableV2ImportPhase,
    /// Authenticated package entry count, when known.
    pub entries: u64,
    /// Authenticated aggregate payload bytes, when known.
    pub bytes: u64,
    /// Canonical package digest, available only after verification.
    pub package_digest: Option<String>,
}

/// Verify, stream, publish, and reopen one complete portable-v2 project.
///
/// Selective classes are refused before project admission. The shared verifier
/// authenticates the complete source before component bytes are materialized;
/// publication then streams those files into one private generation.
pub fn import_complete_portable_v2(
    source: impl AsRef<Path>,
    target: impl AsRef<Path>,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    supported_capabilities: &[ProjectCapability],
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableV2ImportReceipt, PortableV2Error> {
    import_complete_portable_v2_with_progress(
        source,
        target,
        transaction_uuid,
        generation_uuid,
        supported_capabilities,
        limits,
        cancelled,
        |_| {},
    )
}

/// Import a complete package while reporting sanitized aggregate progress.
#[expect(clippy::too_many_arguments, reason = "import authority is explicit")]
pub fn import_complete_portable_v2_with_progress(
    source: impl AsRef<Path>,
    target: impl AsRef<Path>,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    supported_capabilities: &[ProjectCapability],
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
    mut progress: impl FnMut(PortableV2ImportProgress),
) -> Result<PortableV2ImportReceipt, PortableV2Error> {
    let source = source.as_ref();
    let target = target.as_ref();
    progress(PortableV2ImportProgress {
        phase: PortableV2ImportPhase::Verifying,
        entries: 0,
        bytes: 0,
        package_digest: None,
    });
    let target_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            PortableV2Error::new(PortableV2ErrorCode::InvalidPath, "invalid import target")
        })?;
    let stage = target
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(
            ".{target_name}.portable-v2-{}",
            transaction_uuid.hyphenated()
        ));
    let (owner, owned_retry) = claim_stage(&stage, target_name, transaction_uuid, generation_uuid)?;
    let mut materialized_identity_allocated_bytes = std::collections::BTreeMap::new();
    let owner_file = fs::File::open(&owner).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "cannot open import ownership")
    })?;
    record_import_file_identity(&owner_file, &mut materialized_identity_allocated_bytes)?;
    let report = match crate::project_portable_v2::materialize_verified_portable_v2_observed(
        source,
        &stage,
        limits,
        cancelled,
        |file| record_import_file_identity(file, &mut materialized_identity_allocated_bytes),
    ) {
        Ok(report) => report,
        Err(error) => {
            let _ = fs::remove_file(&owner);
            let _ = sync_parent(&owner);
            return Err(error.with_allocation_identities(materialized_identity_allocated_bytes));
        }
    };
    progress(PortableV2ImportProgress {
        phase: PortableV2ImportPhase::Materialized,
        entries: report.entry_count,
        bytes: report.payload_bytes,
        package_digest: Some(report.package_digest.clone()),
    });
    let allocation_on_error = materialized_identity_allocated_bytes.clone();
    let result = import_materialized(
        &stage,
        target,
        transaction_uuid,
        generation_uuid,
        supported_capabilities,
        limits,
        cancelled,
        &report,
        owned_retry,
    )
    .map(|mut receipt| {
        receipt.materialized_identity_allocated_bytes = materialized_identity_allocated_bytes;
        receipt
    })
    .map_err(|error| {
        error
            .with_allocation_identities(allocation_on_error)
            // The shared verifier has authenticated every materialized payload
            // before finalization begins. Preserve that completed read work on
            // a finalization error instead of rediscovering the staging tree.
            .with_recovery_reauthentication(report.payload_bytes, report.entry_count)
    });
    let _ = fs::remove_dir_all(&stage);
    let _ = fs::remove_file(&owner);
    let _ = sync_parent(&owner);
    if result.is_ok() {
        progress(PortableV2ImportProgress {
            phase: PortableV2ImportPhase::Published,
            entries: report.entry_count,
            bytes: report.payload_bytes,
            package_digest: Some(report.package_digest.clone()),
        });
    }
    result
}

fn claim_stage(
    stage: &Path,
    target_name: &str,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
) -> Result<(PathBuf, bool), PortableV2Error> {
    let owner = stage.with_file_name(format!(
        ".{target_name}.portable-v2-{}.owner",
        transaction_uuid.hyphenated()
    ));
    let expected = transaction_uuid.hyphenated().to_string();
    let mut owned_retry = false;
    if owner.exists() && fs::read_to_string(&owner).ok().as_deref() != Some(expected.as_str()) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "import staging identity already exists",
        ));
    }
    if stage.exists() {
        if fs::read_to_string(&owner).ok().as_deref() != Some(expected.as_str()) {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Io,
                "import staging identity already exists",
            ));
        }
        fs::remove_dir_all(stage).map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::Io,
                "owned import staging is unavailable",
            )
        })?;
        owned_retry = true;
    }
    if owner.exists() {
        owned_retry = true;
    } else {
        let mut marker = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&owner)
            .map_err(|_| {
                PortableV2Error::new(
                    PortableV2ErrorCode::Io,
                    "cannot claim import staging ownership",
                )
            })?;
        marker.write_all(expected.as_bytes()).map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::Io,
                "cannot mark import staging ownership",
            )
        })?;
        marker.sync_all().map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::Io,
                "cannot sync import staging ownership",
            )
        })?;
        sync_parent(&owner)?;
    }
    crate::project_failpoint::hit(
        "portable_import.after_owner",
        Some(transaction_uuid),
        Some(generation_uuid),
        "IMPORT_OWNER",
        false,
    )
    .map_err(storage)?;
    Ok((owner, owned_retry))
}

fn sync_parent(path: &Path) -> Result<(), PortableV2Error> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    sync_directory_handle(parent).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "cannot sync import staging parent")
    })
}

#[cfg(not(windows))]
fn sync_directory_handle(path: &Path) -> std::io::Result<()> {
    fs::File::open(path)?.sync_all()
}

#[cfg(windows)]
fn sync_directory_handle(path: &Path) -> std::io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?
        .sync_all()
}

#[expect(clippy::too_many_arguments, reason = "import authority is explicit")]
#[expect(
    clippy::too_many_lines,
    reason = "verification, compatibility, admission, publication, and reopen gates remain explicit"
)]
fn import_materialized(
    stage: &Path,
    target: &Path,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    supported_capabilities: &[ProjectCapability],
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
    report: &PortableV2Report,
    owned_retry: bool,
) -> Result<PortableV2ImportReceipt, PortableV2Error> {
    if report.package_class != PortableV2PackageClass::Complete {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "selective package requires an explicit class-specific consumer",
        ));
    }
    if cancelled.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed)) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Cancelled,
            "verification cancelled",
        ));
    }
    let runtime_path = stage.join(RUNTIME_MAP_PATH);
    let bytes = fs::read(&runtime_path).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            RUNTIME_MAP_PATH,
            "runtime map unavailable",
        )
    })?;
    if bytes.len() as u64 > limits.max_manifest_bytes {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "runtime map exceeds limit",
        ));
    }
    let (_, runtime) = decode_runtime_map(&bytes)?;
    for capability in &runtime.capabilities {
        if !supported_capabilities.iter().any(|supported| {
            supported.capability_id == capability.capability_id
                && supported.capability_version == capability.capability_version
        }) {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "runtime capability is unsupported",
            ));
        }
    }
    let capabilities = runtime
        .capabilities
        .into_iter()
        .map(|capability| ProjectCapability {
            capability_id: capability.capability_id,
            capability_version: capability.capability_version,
        })
        .collect();
    let mut participants = Vec::with_capacity(runtime.participants.len() + 2);
    let mut semantic_composition_fingerprint = None;
    for participant in runtime.participants {
        let source = find_participant_file(stage, &participant.participant_id, limits)?;
        let metadata = fs::metadata(&source).map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::ConcurrentMutation,
                "participant vanished",
            )
        })?;
        let content_sha256: [u8; 32] = hash_file(&source, limits.copy_buffer_bytes, cancelled)?;
        if participant.capability_id == crate::GRAPH_CAPABILITY_ID
            && participant.record_family_id == crate::GRAPH_SEMANTIC_BINDINGS_FAMILY
        {
            let bytes = read_bounded_payload(
                &source,
                crate::semantic_bindings::MAX_SEMANTIC_BINDING_BYTES as u64,
                "semantic bindings",
            )?;
            let bindings =
                crate::SemanticStorageBindings::from_canonical_json(&bytes).map_err(storage)?;
            semantic_composition_fingerprint = Some(bindings.composition_fingerprint);
        }
        let encoding = match participant.encoding.as_str() {
            "json" => ProjectParticipantEncoding::Json,
            "parquet" => ProjectParticipantEncoding::Parquet,
            "arrow" => ProjectParticipantEncoding::Arrow,
            _ => {
                return Err(PortableV2Error::new(
                    PortableV2ErrorCode::Incompatible,
                    "participant encoding is unsupported",
                ));
            }
        };
        let schema_fingerprint = parse_digest(&participant.schema_fingerprint)?;
        participants.push(ProjectFileParticipant {
            participant: ProjectParticipant {
                capability_id: participant.capability_id,
                capability_version: participant.capability_version,
                record_family_id: participant.record_family_id,
                record_version: participant.record_version,
                encoding,
                schema_fingerprint,
                row_count: participant.row_count,
                bytes: Vec::new(),
            },
            source,
            byte_length: metadata.len(),
            content_sha256,
        });
    }
    let staged_composition = if report.ontology_composition.is_some() {
        let (candidate, receipt) = build_staged_composition(stage, report, limits)?;
        if let Some(expected) = semantic_composition_fingerprint.as_deref() {
            let staged_bytes = read_bounded_payload(
                &candidate.source,
                limits.max_manifest_bytes,
                "staged composition",
            )?;
            let staged =
                crate::WorkspacePortableOntologyStaging::from_canonical_json(&staged_bytes)
                    .map_err(storage)?;
            if staged.composition.composition_fingerprint != expected {
                return Err(PortableV2Error::new(
                    PortableV2ErrorCode::Incompatible,
                    "semantic bindings and portable composition fingerprints disagree",
                ));
            }
            participants.push(persist_composition_authority(stage, &staged.composition)?);
        }
        participants.push(candidate);
        Some(receipt)
    } else {
        if semantic_composition_fingerprint.is_some() {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "semantic bindings require portable composition authority",
            ));
        }
        None
    };
    participants.sort_by(|left, right| {
        (
            &left.participant.capability_id,
            &left.participant.record_family_id,
        )
            .cmp(&(
                &right.participant.capability_id,
                &right.participant.record_family_id,
            ))
    });
    let request = ProjectGenerationRequest {
        transaction_uuid,
        generation_uuid,
        capabilities,
        participants: participants
            .iter()
            .map(|participant| participant.participant.clone())
            .collect(),
    };
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        target,
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
        crate::filesystem_admission::ProjectRootRequirement::CreateIfMissing,
    )
    .map_err(storage)?;
    admission.revalidate_identity().map_err(storage)?;
    let replay = crate::published_project_transaction(admission.root(), transaction_uuid)
        .map_err(storage)?
        .is_some();
    let existing = if replay {
        Some(crate::resolve_project_generation(admission.root()).map_err(storage)?)
    } else if owned_retry {
        let generation = semantically_pristine_generation(admission.root()).map_err(storage)?;
        if generation.is_none() {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Io,
                "owned retry target is not pristine",
            ));
        }
        Some(crate::resolve_project_generation(admission.root()).map_err(storage)?)
    } else {
        prepare_import_target(admission.root()).map_err(storage)?
    };
    let parent = match existing {
        Some(parent) => parent,
        None => open_or_initialize_project_admitted(admission.root()).map_err(storage)?,
    };
    let graph_tree = runtime
        .graph_tree
        .as_ref()
        .map(|_| stage.join("data/components/graph-data/graph-tree"));
    let publication = match stage_project_generation_from_files_admitted(
        admission,
        parent,
        &request,
        &participants,
        graph_tree.as_deref(),
        cancelled,
        limits.copy_buffer_bytes,
    )
    .map_err(|error| storage_or_cancel(error, cancelled))?
    {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => {
            let validated = staged
                .validate(|_| Ok(()), |_, _| Ok(()))
                .map_err(storage)?;
            validated.publish().map_err(storage)?
        }
    };
    let reopened = crate::resolve_project_generation(target).map_err(storage)?;
    if reopened.generation_uuid() != generation_uuid {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "published generation did not reopen",
        ));
    }
    Ok(PortableV2ImportReceipt {
        package_digest: report.package_digest.clone(),
        transport_digest: report.transport_digest.clone(),
        publication,
        staged_composition,
        materialized_identity_allocated_bytes: std::collections::BTreeMap::new(),
        published_identity_allocated_bytes: crate::capture_storage_attribution(&reopened)
            .map_err(storage)?
            .physical_identity_allocated_bytes,
    })
}

fn record_import_file_identity(
    file: &fs::File,
    identities: &mut std::collections::BTreeMap<String, u64>,
) -> Result<(), PortableV2Error> {
    let identity = graphforge_filesystem::file_identity(file).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "cannot identify owned import artifact",
        )
    })?;
    let usage = graphforge_filesystem::file_space_usage(file).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "cannot measure owned import artifact",
        )
    })?;
    let mut file_id = String::with_capacity(32);
    for byte in identity.file_id {
        use std::fmt::Write as _;
        write!(&mut file_id, "{byte:02x}").expect("writing to String cannot fail");
    }
    let key = format!("{:016x}:{file_id}", identity.volume_serial);
    if identities.insert(key, usage.allocated_bytes).is_some() {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "owned import identity is duplicated",
        ));
    }
    Ok(())
}

fn parse_mode(value: &str) -> Result<ActivationMode, PortableV2Error> {
    match value {
        "exploratory" => Ok(ActivationMode::Exploratory),
        "advisory" => Ok(ActivationMode::Advisory),
        "strict" => Ok(ActivationMode::Strict),
        _ => Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "portable activation mode",
        )),
    }
}

fn build_staged_composition(
    stage: &Path,
    report: &PortableV2Report,
    limits: PortableV2Limits,
) -> Result<(ProjectFileParticipant, PortableV2StagedCompositionReceipt), PortableV2Error> {
    let control = report.ontology_composition.as_ref().ok_or_else(|| {
        PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "composition control absent",
        )
    })?;
    let module_ids = control
        .modules
        .iter()
        .map(|module| {
            (
                format!(
                    "ontology-module-{}",
                    module.content_digest.trim_start_matches("sha256:")
                ),
                OntologyModuleId {
                    ontology_id: module.ontology_id.clone(),
                    authored_version: module.version.clone(),
                    canonical_digest: module
                        .content_digest
                        .trim_start_matches("sha256:")
                        .to_owned(),
                },
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let bridge_ids = control
        .bridge_sets
        .iter()
        .map(|bridge| {
            (
                format!(
                    "ontology-bridge-{}",
                    bridge.content_digest.trim_start_matches("sha256:")
                ),
                BridgeSetId {
                    bridge_id: bridge.bridge_id.clone(),
                    authored_version: bridge.version.clone(),
                    canonical_digest: bridge
                        .content_digest
                        .trim_start_matches("sha256:")
                        .to_owned(),
                },
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let (authored, bridges) = load_staged_composition_entries(stage, report, limits, &module_ids)?;
    let activations = build_staged_activations(control)?;
    let exact_bridges = resolve_staged_bridge_ids(&bridges, &bridge_ids)?;
    let compiled = compile_inventory(InventoryCompileRequest {
        modules: &authored,
        bridges: &exact_bridges,
        activation: &activations,
        profile_default: parse_mode(&control.activation_profile.profile_default)?,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "staged composition closure",
        )
    })?;
    let composition = crate::WorkspaceOntologyComposition::from_compiled(&compiled, bridges);
    let staged = crate::WorkspacePortableOntologyStaging {
        contract_version: 1,
        package_digest: report.package_digest.clone(),
        portable_composition_digest: control.composition_digest.clone(),
        composition,
    };
    let (participant, source, bytes) = persist_staged_composition(stage, &staged)?;
    let receipt = PortableV2StagedCompositionReceipt {
        package_digest: report.package_digest.clone(),
        portable_composition_digest: control.composition_digest.clone(),
        workspace_composition_fingerprint: staged.composition.composition_fingerprint.clone(),
    };
    Ok((
        ProjectFileParticipant {
            participant: ProjectParticipant {
                bytes: Vec::new(),
                ..participant
            },
            source,
            byte_length: bytes.len() as u64,
            content_sha256: Sha256::digest(&bytes).into(),
        },
        receipt,
    ))
}

fn load_staged_composition_entries(
    stage: &Path,
    report: &PortableV2Report,
    limits: PortableV2Limits,
    module_ids: &std::collections::BTreeMap<String, OntologyModuleId>,
) -> Result<
    (
        Vec<AuthoredModule>,
        Vec<graphforge_ontology::BridgeDocument>,
    ),
    PortableV2Error,
> {
    let mut authored = Vec::new();
    let mut bridges = Vec::new();
    for entry in &report.ontology_composition_entries {
        let bytes = read_bounded_payload(
            &stage.join(&entry.path),
            limits.max_manifest_bytes,
            &entry.path,
        )?;
        if entry.kind != "ontology" {
            bridges.push(serde_json::from_slice(&bytes).map_err(|_| {
                PortableV2Error::at(
                    PortableV2ErrorCode::InvalidStructure,
                    &entry.path,
                    "staged ontology bridge",
                )
            })?);
            continue;
        }
        let document = serde_json::from_slice(&bytes).map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                &entry.path,
                "staged ontology module",
            )
        })?;
        let id = module_ids
            .get(entry.path.split('/').nth(3).unwrap_or_default())
            .cloned()
            .ok_or_else(|| {
                PortableV2Error::new(PortableV2ErrorCode::Incompatible, "staged module identity")
            })?;
        let dependencies = entry
            .required_dependencies
            .iter()
            .map(|dependency| {
                module_ids.get(dependency).cloned().ok_or_else(|| {
                    PortableV2Error::new(
                        PortableV2ErrorCode::Incompatible,
                        "staged module dependency closure",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        authored.push(AuthoredModule {
            allow_projected_identity: id.ontology_id.starts_with("legacy:"),
            id,
            dependencies,
            doc: document,
        });
    }
    Ok((authored, bridges))
}

fn build_staged_activations(
    control: &crate::project_portable_v2::PortableV2OntologyComposition,
) -> Result<Vec<ActivationRecord>, PortableV2Error> {
    control
        .activation_profile
        .overrides
        .iter()
        .map(|activation| {
            let scope = match activation.scope.as_str() {
                "module" => ActivationScope::Module,
                "bridge" => ActivationScope::Bridge,
                _ => {
                    return Err(PortableV2Error::new(
                        PortableV2ErrorCode::Incompatible,
                        "activation scope",
                    ));
                }
            };
            Ok(ActivationRecord {
                scope,
                subject: format!(
                    "{}@{}#{}",
                    activation.subject.id,
                    activation.subject.version,
                    activation
                        .subject
                        .content_digest
                        .trim_start_matches("sha256:")
                ),
                mode: parse_mode(&activation.mode)?,
            })
        })
        .collect()
}

fn resolve_staged_bridge_ids(
    bridges: &[graphforge_ontology::BridgeDocument],
    bridge_ids: &std::collections::BTreeMap<String, BridgeSetId>,
) -> Result<Vec<BridgeSetId>, PortableV2Error> {
    bridges
        .iter()
        .map(|bridge| {
            bridge_ids
                .values()
                .find(|id| {
                    id.bridge_id == bridge.bridge_id
                        && id.authored_version == bridge.authored_version
                })
                .cloned()
                .ok_or_else(|| {
                    PortableV2Error::new(
                        PortableV2ErrorCode::Incompatible,
                        "staged bridge identity",
                    )
                })
        })
        .collect()
}

fn persist_staged_composition(
    stage: &Path,
    staged: &crate::WorkspacePortableOntologyStaging,
) -> Result<(ProjectParticipant, PathBuf, Vec<u8>), PortableV2Error> {
    let participant = staged.to_project_participant().map_err(storage)?;
    let bytes = participant.bytes.clone();
    let source = stage.join("portable-ontology-staging.json");
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&source)
        .map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::Io,
                "cannot stage composition candidate",
            )
        })?;
    output.write_all(&bytes).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "cannot write composition candidate",
        )
    })?;
    output.sync_all().map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "cannot sync composition candidate")
    })?;
    Ok((participant, source, bytes))
}

fn persist_composition_authority(
    stage: &Path,
    composition: &crate::WorkspaceOntologyComposition,
) -> Result<ProjectFileParticipant, PortableV2Error> {
    let participant = composition.to_project_participant().map_err(storage)?;
    let bytes = participant.bytes.clone();
    let source = stage.join("ontology-composition-authority.json");
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&source)
        .map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::Io,
                "cannot stage composition authority",
            )
        })?;
    output.write_all(&bytes).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "cannot write composition authority",
        )
    })?;
    output.sync_all().map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "cannot sync composition authority")
    })?;
    Ok(ProjectFileParticipant {
        participant: ProjectParticipant {
            bytes: Vec::new(),
            ..participant
        },
        source,
        byte_length: bytes.len() as u64,
        content_sha256: Sha256::digest(&bytes).into(),
    })
}

fn read_bounded_payload(
    path: &Path,
    limit: u64,
    context: &str,
) -> Result<Vec<u8>, PortableV2Error> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            context,
            "payload unavailable",
        )
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > limit {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::LimitExceeded,
            context,
            "payload bound",
        ));
    }
    let mut file =
        crate::project_portable_v2_export::open_source_no_follow(path).map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                context,
                "payload open",
            )
        })?;
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| PortableV2Error::at(PortableV2ErrorCode::Io, context, "payload read"))?;
    if bytes.len() as u64 > limit || bytes.len() as u64 != metadata.len() {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::LimitExceeded,
            context,
            "payload bound",
        ));
    }
    Ok(bytes)
}

fn find_participant_file(
    stage: &Path,
    participant_id: &str,
    limits: PortableV2Limits,
) -> Result<PathBuf, PortableV2Error> {
    let root = stage.join("data/components");
    let mut found = None;
    for kind in fs::read_dir(&root).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "components unavailable",
        )
    })? {
        let candidate = kind
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "component unavailable"))?
            .path()
            .join(participant_id);
        if !candidate.is_dir() {
            continue;
        }
        for entry in fs::read_dir(candidate)
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "participant unavailable"))?
        {
            let path = entry
                .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "entry unavailable"))?
                .path();
            if path.is_file() && found.replace(path).is_some() {
                return Err(PortableV2Error::new(
                    PortableV2ErrorCode::Incompatible,
                    "runtime participant has multiple payloads",
                ));
            }
        }
    }
    let path = found.ok_or_else(|| {
        PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "runtime participant is absent",
        )
    })?;
    if fs::metadata(&path)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "entry unavailable"))?
        .len()
        > limits.max_entry_bytes
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "participant exceeds limit",
        ));
    }
    Ok(path)
}

fn hash_file(
    path: &Path,
    buffer_size: usize,
    cancelled: Option<&AtomicBool>,
) -> Result<[u8; 32], PortableV2Error> {
    use std::io::Read;
    let mut file = fs::File::open(path)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "participant unavailable"))?;
    let mut buffer = vec![0; buffer_size];
    let mut hash = Sha256::new();
    loop {
        if cancelled.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed)) {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Cancelled,
                "verification cancelled",
            ));
        }
        let count = file
            .read(&mut buffer)
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "participant unreadable"))?;
        if count == 0 {
            return Ok(hash.finalize().into());
        }
        hash.update(&buffer[..count]);
    }
}

fn parse_digest(value: &str) -> Result<[u8; 32], PortableV2Error> {
    let mut digest = [0; 32];
    if value.len() != 64 {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "schema fingerprint is invalid",
        ));
    }
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair).map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "schema fingerprint is invalid",
            )
        })?;
        digest[index] = u8::from_str_radix(text, 16).map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "schema fingerprint is invalid",
            )
        })?;
    }
    Ok(digest)
}

fn storage(_: GfError) -> PortableV2Error {
    PortableV2Error::new(
        PortableV2ErrorCode::Io,
        "portable import publication failed",
    )
}

fn storage_or_cancel(error: GfError, cancelled: Option<&AtomicBool>) -> PortableV2Error {
    if cancelled.is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed)) {
        PortableV2Error::new(PortableV2ErrorCode::Cancelled, "verification cancelled")
    } else {
        storage(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HELPER: &str = "project_portable_v2_import::tests::subprocess_crash_import";
    const COOKIE: &str = "graphforge-internal-subprocess-v1";

    fn supported() -> Vec<ProjectCapability> {
        vec![
            ProjectCapability {
                capability_id: "graph".into(),
                capability_version: 1,
            },
            ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ]
    }

    fn composition_package() -> (tempfile::TempDir, PathBuf) {
        let source = tempfile::tempdir().unwrap();
        let parent = crate::open_or_initialize_project(source.path()).unwrap();
        let document = graphforge_ontology::OntologyDoc {
            ontology_id: "https://graphforge.dev/ontology/portable-import".into(),
            version: "release-2026.08".into(),
            entity_types: Vec::new(),
            relation_types: Vec::new(),
            properties: Vec::new(),
            constraints: Vec::new(),
            migrations: Vec::new(),
        };
        let legacy = crate::WorkspaceOntology {
            contract_version: 1,
            mode: crate::WorkspaceOntologyMode::Strict,
            source_format: Some(crate::WorkspaceOntologySourceFormat::Json),
            canonical_ontology_sha256: Some("a".repeat(64)),
            canonical_ontology: Some(serde_json::to_value(document).unwrap()),
        };
        let composition = crate::WorkspaceOntologyComposition::virtual_legacy(&legacy)
            .unwrap()
            .unwrap();
        let mut participants = crate::empty_workspace_participants().unwrap();
        participants.push(
            crate::SemanticStorageBindings::new(
                composition.composition_fingerprint.clone(),
                Vec::new(),
            )
            .unwrap()
            .to_project_participant()
            .unwrap(),
        );
        participants.push(composition.to_project_participant().unwrap());
        participants.sort_by(|left, right| {
            (&left.capability_id, &left.record_family_id)
                .cmp(&(&right.capability_id, &right.record_family_id))
        });
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::new_v4(),
            generation_uuid: Uuid::new_v4(),
            capabilities: supported(),
            participants,
        };
        let ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(source.path(), &request).unwrap()
        else {
            panic!("fresh composition generation replayed");
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap();
        drop(parent);
        let generation = crate::resolve_project_generation(source.path()).unwrap();
        let package_parent = tempfile::tempdir().unwrap();
        let package = package_parent.path().join("composition.gfproject");
        let limits = crate::PortableV2ExportLimits::default();
        let plan = crate::plan_complete_portable_v2(&generation, limits).unwrap();
        crate::export_complete_portable_v2(
            &plan,
            &package,
            crate::PortableV2Output::Expanded,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        (package_parent, package)
    }

    #[test]
    fn durable_composition_staging_reopens_replays_and_fails_without_partial_authority() {
        let (_package_parent, package) = composition_package();
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("imported");
        let transaction = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let first = import_complete_portable_v2(
            &package,
            &target,
            transaction,
            generation,
            &supported(),
            PortableV2Limits::default(),
            None,
        )
        .unwrap();
        assert!(first.staged_composition.is_some());
        let reopened = crate::resolve_project_generation(&target).unwrap();
        let staged = load_portable_ontology_staging(&reopened, PortableV2Limits::default())
            .unwrap()
            .expect("verified candidate must survive reopen");
        assert_eq!(staged.package_digest, first.package_digest);
        assert!(
            reopened
                .participant_snapshots()
                .unwrap()
                .iter()
                .any(|entry| {
                    entry.record_family_id == crate::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY
                })
        );
        assert!(
            crate::semantic_storage_bindings(&reopened)
                .unwrap()
                .is_some()
        );

        let replay = import_complete_portable_v2(
            &package,
            &target,
            transaction,
            generation,
            &supported(),
            PortableV2Limits::default(),
            None,
        )
        .unwrap();
        assert_eq!(replay.publication.generation_uuid, generation);
        let conflict = import_complete_portable_v2(
            &package,
            &target,
            transaction,
            Uuid::new_v4(),
            &supported(),
            PortableV2Limits::default(),
            None,
        )
        .unwrap_err();
        assert_eq!(conflict.code, PortableV2ErrorCode::Io);
        assert_eq!(
            crate::resolve_project_generation(&target)
                .unwrap()
                .generation_uuid(),
            generation
        );

        for (name, capabilities, cancelled, expected) in [
            (
                "cancelled",
                supported(),
                AtomicBool::new(true),
                PortableV2ErrorCode::Cancelled,
            ),
            (
                "unsupported",
                Vec::new(),
                AtomicBool::new(false),
                PortableV2ErrorCode::Incompatible,
            ),
        ] {
            let failed = parent.path().join(name);
            let transaction = Uuid::new_v4();
            let error = import_complete_portable_v2(
                &package,
                &failed,
                transaction,
                Uuid::new_v4(),
                &capabilities,
                PortableV2Limits::default(),
                Some(&cancelled),
            )
            .unwrap_err();
            assert_eq!(error.code, expected, "{name}");
            assert!(!failed.exists(), "{name} published a target");
            let residue = parent
                .path()
                .join(format!(".{name}.portable-v2-{}", transaction.hyphenated()));
            assert!(!residue.exists(), "{name} retained private staging");
            let owner = residue.with_file_name(format!(
                ".{name}.portable-v2-{}.owner",
                transaction.hyphenated()
            ));
            assert!(!owner.exists(), "{name} retained its ownership marker");
        }
    }

    #[test]
    fn resource_ladder_fails_before_target_admission() {
        let source_project = tempfile::tempdir().unwrap();
        let source_generation = crate::open_or_initialize_project(source_project.path()).unwrap();
        let package_parent = tempfile::tempdir().unwrap();
        let package = package_parent.path().join("complete.gfproject");
        let export_limits = crate::PortableV2ExportLimits::default();
        let plan = crate::plan_complete_portable_v2(&source_generation, export_limits).unwrap();
        crate::export_complete_portable_v2(
            &plan,
            &package,
            crate::PortableV2Output::Expanded,
            export_limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();
        for (name, limits) in [
            (
                "entries",
                PortableV2Limits {
                    max_entries: 1,
                    ..PortableV2Limits::default()
                },
            ),
            (
                "total",
                PortableV2Limits {
                    max_total_bytes: 1,
                    ..PortableV2Limits::default()
                },
            ),
            (
                "entry",
                PortableV2Limits {
                    max_entry_bytes: 1,
                    ..PortableV2Limits::default()
                },
            ),
            (
                "manifest",
                PortableV2Limits {
                    max_manifest_bytes: 1,
                    ..PortableV2Limits::default()
                },
            ),
        ] {
            let target = package_parent.path().join(format!("target-{name}"));
            let error = import_complete_portable_v2(
                &package,
                &target,
                Uuid::new_v4(),
                Uuid::new_v4(),
                &supported(),
                limits,
                None,
            )
            .unwrap_err();
            assert_eq!(error.code, PortableV2ErrorCode::LimitExceeded, "{name}");
            assert!(
                !error.allocation_identity_allocated_bytes.is_empty(),
                "{name} must report its durable ownership allocation"
            );
            assert!(!target.exists(), "{name}");
        }
    }

    #[test]
    fn crash_windows_recover_old_or_new_and_retry_cleans_owned_residue() {
        let source_project = tempfile::tempdir().unwrap();
        let source_generation = crate::open_or_initialize_project(source_project.path()).unwrap();
        let package_parent = tempfile::tempdir().unwrap();
        let package = package_parent.path().join("complete.gfproject");
        let limits = crate::PortableV2ExportLimits::default();
        let plan = crate::plan_complete_portable_v2(&source_generation, limits).unwrap();
        crate::export_complete_portable_v2(
            &plan,
            &package,
            crate::PortableV2Output::Expanded,
            limits,
            &AtomicBool::new(false),
            |_| {},
        )
        .unwrap();

        for failpoint in [
            "portable_import.after_owner",
            "project.after_writer_lock",
            "project.after_participant_write",
            "project.after_manifest_fsync",
            "project.before_current_replace",
            "project.after_current_replace",
        ] {
            let parent = tempfile::tempdir().unwrap();
            let target = parent.path().join("project");
            let old = crate::open_or_initialize_project(&target)
                .unwrap()
                .generation_uuid();
            let transaction = Uuid::new_v4();
            let generation = Uuid::new_v4();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg(HELPER)
                .arg("--nocapture")
                .env("GRAPHFORGE_PORTABLE_V2_CRASH_PACKAGE", &package)
                .env("GRAPHFORGE_PORTABLE_V2_CRASH_TARGET", &target)
                .env(
                    "GRAPHFORGE_PORTABLE_V2_CRASH_TRANSACTION",
                    transaction.to_string(),
                )
                .env(
                    "GRAPHFORGE_PORTABLE_V2_CRASH_GENERATION",
                    generation.to_string(),
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINTS", COOKIE)
                .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));

            let _ = crate::recover_project_transactions(&target).unwrap();
            let recovered = crate::resolve_project_generation(&target)
                .unwrap()
                .generation_uuid();
            assert!(recovered == old || recovered == generation);
            let receipt = import_complete_portable_v2(
                &package,
                &target,
                transaction,
                generation,
                &supported(),
                PortableV2Limits::default(),
                None,
            )
            .unwrap_or_else(|error| panic!("retry after {failpoint} failed: {error:?}"));
            assert_eq!(receipt.publication.generation_uuid, generation);
            assert_eq!(
                crate::resolve_project_generation(&target)
                    .unwrap()
                    .generation_uuid(),
                generation
            );
            let residue = target
                .parent()
                .unwrap()
                .join(format!(".project.portable-v2-{}", transaction.hyphenated()));
            assert!(!residue.exists(), "owned residue survived {failpoint}");
            let owner_residue = residue.with_file_name(format!(
                "{}.owner",
                residue.file_name().unwrap().to_string_lossy()
            ));
            assert!(!owner_residue.exists(), "owned marker survived {failpoint}");
        }
    }

    #[test]
    fn subprocess_crash_import() {
        let Ok(package) = std::env::var("GRAPHFORGE_PORTABLE_V2_CRASH_PACKAGE") else {
            return;
        };
        let target = std::env::var("GRAPHFORGE_PORTABLE_V2_CRASH_TARGET").unwrap();
        let transaction =
            Uuid::parse_str(&std::env::var("GRAPHFORGE_PORTABLE_V2_CRASH_TRANSACTION").unwrap())
                .unwrap();
        let generation =
            Uuid::parse_str(&std::env::var("GRAPHFORGE_PORTABLE_V2_CRASH_GENERATION").unwrap())
                .unwrap();
        let _ = import_complete_portable_v2(
            package,
            target,
            transaction,
            generation,
            &supported(),
            PortableV2Limits::default(),
            None,
        );
        panic!("configured portable import failpoint did not terminate the process");
    }
}
