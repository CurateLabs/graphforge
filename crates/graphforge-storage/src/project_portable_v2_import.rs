//! Verification-first, bounded portable-v2 project import.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use graphforge_core::GfError;
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
    let report = match materialize_verified_portable_v2(source, &stage, limits, cancelled) {
        Ok(report) => report,
        Err(error) => {
            let _ = fs::remove_file(&owner);
            let _ = sync_parent(&owner);
            return Err(error);
        }
    };
    progress(PortableV2ImportProgress {
        phase: PortableV2ImportPhase::Materialized,
        entries: report.entry_count,
        bytes: report.payload_bytes,
        package_digest: Some(report.package_digest.clone()),
    });
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
    );
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
    let mut participants = Vec::with_capacity(runtime.participants.len());
    for participant in runtime.participants {
        let source = find_participant_file(stage, &participant.participant_id, limits)?;
        let metadata = fs::metadata(&source).map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::ConcurrentMutation,
                "participant vanished",
            )
        })?;
        let content_sha256: [u8; 32] = hash_file(&source, limits.copy_buffer_bytes, cancelled)?;
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
    })
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
