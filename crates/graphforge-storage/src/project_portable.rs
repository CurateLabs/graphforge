//! Deterministic, bounded portable project-generation envelopes.
//!
//! The envelope is intentionally not an archive of the live project layout.
//! It contains one resolved immutable generation, its canonical capability and
//! participant inventory, and participant bytes. Locks, journals, attempts,
//! trash, caches, leases, manifests, and `CURRENT` are never enumerated.

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::path::Path;

use atomicwrites::{AtomicFile, DisallowOverwrite};
use graphforge_core::{GfError, ProjectErrorCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
    ProjectPublicationReceipt, ProjectStageOutcome, ResolvedProjectGeneration,
    open_or_initialize_project, resolve_project_generation, stage_project_generation,
};

const MAGIC: &[u8; 16] = b"graphforge-exp\0\n";
const FORMAT: &str = "graphforge-portable-export";
const FORMAT_VERSION: u32 = 1;
const HEADER_LENGTH_BYTES: usize = 8;

/// Default resource bounds for portable envelope decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortableProjectLimits {
    /// Maximum complete envelope size.
    pub max_envelope_bytes: u64,
    /// Maximum canonical JSON header size.
    pub max_header_bytes: u64,
    /// Maximum participant count.
    pub max_participants: usize,
    /// Maximum size of one participant.
    pub max_participant_bytes: u64,
}

impl Default for PortableProjectLimits {
    fn default() -> Self {
        Self {
            max_envelope_bytes: 16 * 1024 * 1024 * 1024,
            max_header_bytes: 4 * 1024 * 1024,
            max_participants: 100_000,
            max_participant_bytes: 8 * 1024 * 1024 * 1024,
        }
    }
}

/// Deterministic export result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortableExportReceipt {
    /// Exported immutable generation.
    pub generation_uuid: Uuid,
    /// SHA-256 of the complete envelope.
    pub envelope_sha256: [u8; 32],
    /// Complete envelope byte length.
    pub byte_length: u64,
    /// Number of participants in the envelope.
    pub participant_count: usize,
}

/// Import result after atomic generation publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortableImportReceipt {
    /// Integrity digest of the imported envelope.
    pub envelope_sha256: [u8; 32],
    /// Original exported generation identity recorded by the envelope.
    pub source_generation_uuid: Uuid,
    /// Receipt for the newly published local generation.
    pub publication: ProjectPublicationReceipt,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeHeader {
    format: String,
    format_version: u32,
    source_generation_uuid: String,
    capabilities: Vec<EnvelopeCapability>,
    participants: Vec<EnvelopeParticipant>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeCapability {
    capability_id: String,
    capability_version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeParticipant {
    capability_id: String,
    capability_version: u32,
    record_family_id: String,
    record_version: u32,
    encoding: String,
    schema_fingerprint: String,
    row_count: u64,
    byte_length: u64,
    content_sha256: String,
}

struct ValidatedEnvelope {
    source_generation_uuid: Uuid,
    envelope_sha256: [u8; 32],
    capabilities: Vec<ProjectCapability>,
    participants: Vec<ProjectParticipant>,
}

/// Encode one already-resolved generation into deterministic portable bytes.
///
/// The caller may resolve either `CURRENT` or a checkpoint before calling this
/// function. Resolution pins the generation; this function never follows live
/// pointers or enumerates the project container.
pub fn encode_portable_project(
    generation: &ResolvedProjectGeneration,
    limits: PortableProjectLimits,
) -> Result<(Vec<u8>, PortableExportReceipt), GfError> {
    let snapshots = generation.participant_snapshots()?;
    enforce_count(snapshots.len(), limits.max_participants)?;
    let capabilities = generation
        .capabilities()
        .into_iter()
        .map(|item| EnvelopeCapability {
            capability_id: item.capability_id,
            capability_version: item.capability_version,
        })
        .collect();
    let mut participants = Vec::with_capacity(snapshots.len());
    let mut body_length = 0_u64;
    for snapshot in &snapshots {
        let byte_length = u64::try_from(snapshot.bytes.len())
            .map_err(|_| resource("portable participant byte length exceeds u64"))?;
        enforce_size(
            byte_length,
            limits.max_participant_bytes,
            "portable participant",
        )?;
        body_length = body_length
            .checked_add(byte_length)
            .ok_or_else(|| resource("portable envelope size overflow"))?;
        participants.push(EnvelopeParticipant {
            capability_id: snapshot.capability_id.clone(),
            capability_version: snapshot.capability_version,
            record_family_id: snapshot.record_family_id.clone(),
            record_version: snapshot.record_version,
            encoding: snapshot.encoding.clone(),
            schema_fingerprint: hex(snapshot.schema_fingerprint),
            row_count: snapshot.row_count,
            byte_length,
            content_sha256: hex(Sha256::digest(&snapshot.bytes).into()),
        });
    }
    let header = EnvelopeHeader {
        format: FORMAT.into(),
        format_version: FORMAT_VERSION,
        source_generation_uuid: generation.generation_uuid().hyphenated().to_string(),
        capabilities,
        participants,
    };
    let mut header_bytes = serde_json::to_vec(&header)
        .map_err(|error| GfError::Storage(format!("failed to encode portable header: {error}")))?;
    header_bytes.push(b'\n');
    let header_length = u64::try_from(header_bytes.len())
        .map_err(|_| resource("portable header byte length exceeds u64"))?;
    enforce_size(header_length, limits.max_header_bytes, "portable header")?;
    let total = u64::try_from(MAGIC.len() + HEADER_LENGTH_BYTES)
        .expect("fixed prefix fits u64")
        .checked_add(header_length)
        .and_then(|value| value.checked_add(body_length))
        .ok_or_else(|| resource("portable envelope size overflow"))?;
    enforce_size(total, limits.max_envelope_bytes, "portable envelope")?;
    let capacity = usize::try_from(total)
        .map_err(|_| resource("portable envelope does not fit address space"))?;
    let mut envelope = Vec::with_capacity(capacity);
    envelope.extend_from_slice(MAGIC);
    envelope.extend_from_slice(&header_length.to_be_bytes());
    envelope.extend_from_slice(&header_bytes);
    for snapshot in snapshots {
        envelope.extend_from_slice(&snapshot.bytes);
    }
    let envelope_sha256 = Sha256::digest(&envelope).into();
    Ok((
        envelope,
        PortableExportReceipt {
            generation_uuid: generation.generation_uuid(),
            envelope_sha256,
            byte_length: total,
            participant_count: header.participants.len(),
        },
    ))
}

/// Atomically write a deterministic portable envelope to a regular file.
pub fn export_portable_project(
    generation: &ResolvedProjectGeneration,
    destination: impl AsRef<Path>,
    limits: PortableProjectLimits,
) -> Result<PortableExportReceipt, GfError> {
    let destination = destination.as_ref();
    reject_export_destination(destination)?;
    let (bytes, receipt) = encode_portable_project(generation, limits)?;
    AtomicFile::new(destination, DisallowOverwrite)
        .write(|file| {
            file.write_all(&bytes)?;
            file.sync_all()
        })
        .map_err(|error| GfError::Storage(format!("failed to write portable export: {error}")))?;
    Ok(receipt)
}

/// Validate a complete envelope before initializing and atomically publishing
/// it into a new, empty, or pristine initialized project directory.
///
/// `supported_capabilities` is the exact `(id, version)` inventory implemented
/// by the calling binary. Unknown or version-mismatched capabilities fail
/// before target mutation.
pub fn import_portable_project(
    envelope: &[u8],
    target: impl AsRef<Path>,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    supported_capabilities: &[ProjectCapability],
    limits: PortableProjectLimits,
) -> Result<PortableImportReceipt, GfError> {
    let validated = validate_envelope(envelope, supported_capabilities, limits)?;
    let target = target.as_ref();
    let existing_parent = prepare_import_target(target)?;
    let initialized_parent;
    let _parent = if let Some(parent) = existing_parent {
        parent
    } else {
        initialized_parent = open_or_initialize_project(target)?;
        initialized_parent
    };
    let request = ProjectGenerationRequest {
        transaction_uuid,
        generation_uuid,
        capabilities: validated.capabilities,
        participants: validated.participants,
    };
    let publication = match stage_project_generation(target, &request)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => {
            staged.validate(|_| Ok(()), |_, _| Ok(()))?.publish()?
        }
    };
    Ok(PortableImportReceipt {
        envelope_sha256: validated.envelope_sha256,
        source_generation_uuid: validated.source_generation_uuid,
        publication,
    })
}

/// Read a bounded regular envelope file and import it.
///
/// The source and every caller-controlled existing path component must not be
/// symbolic links. The size bound is checked from metadata and again while
/// reading, so replacement or growth races cannot cause an unbounded allocation.
pub fn import_portable_project_file(
    source: impl AsRef<Path>,
    target: impl AsRef<Path>,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
    supported_capabilities: &[ProjectCapability],
    limits: PortableProjectLimits,
) -> Result<PortableImportReceipt, GfError> {
    let source = source.as_ref();
    reject_symlink_components(source, "portable import source")?;
    let metadata = std::fs::symlink_metadata(source).map_err(|error| {
        GfError::Storage(format!("failed to inspect portable import source: {error}"))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(project_error(
            ProjectErrorCode::UnsupportedFilesystem,
            "portable import source is linked or not a regular file",
        ));
    }
    enforce_size(
        metadata.len(),
        limits.max_envelope_bytes,
        "portable envelope",
    )?;
    let bounded_capacity = usize::try_from(metadata.len())
        .map_err(|_| resource("portable envelope does not fit address space"))?;
    let mut bytes = Vec::with_capacity(bounded_capacity);
    let mut file = open_regular_nofollow(source).map_err(|error| {
        GfError::Storage(format!("failed to open portable import source: {error}"))
    })?;
    let opened_metadata = file.metadata().map_err(|error| {
        GfError::Storage(format!(
            "failed to inspect opened portable import source: {error}"
        ))
    })?;
    if !opened_metadata.is_file() || !same_file_identity(&metadata, &opened_metadata) {
        return Err(project_error(
            ProjectErrorCode::UnsupportedFilesystem,
            "portable import source changed while it was being opened",
        ));
    }
    reject_symlink_components(source, "portable import source")?;
    Read::by_ref(&mut file)
        .take(limits.max_envelope_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| {
            GfError::Storage(format!("failed to read portable import source: {error}"))
        })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limits.max_envelope_bytes {
        return Err(resource("portable envelope exceeds limit"));
    }
    import_portable_project(
        &bytes,
        target,
        transaction_uuid,
        generation_uuid,
        supported_capabilities,
        limits,
    )
}

fn validate_envelope(
    envelope: &[u8],
    supported_capabilities: &[ProjectCapability],
    limits: PortableProjectLimits,
) -> Result<ValidatedEnvelope, GfError> {
    let envelope_length = u64::try_from(envelope.len())
        .map_err(|_| resource("portable envelope byte length exceeds u64"))?;
    enforce_size(
        envelope_length,
        limits.max_envelope_bytes,
        "portable envelope",
    )?;
    let prefix = MAGIC.len() + HEADER_LENGTH_BYTES;
    if envelope.len() < prefix || &envelope[..MAGIC.len()] != MAGIC {
        return Err(corrupt("portable envelope magic is invalid"));
    }
    let header_length = u64::from_be_bytes(
        envelope[MAGIC.len()..prefix]
            .try_into()
            .expect("fixed header length slice"),
    );
    enforce_size(header_length, limits.max_header_bytes, "portable header")?;
    let header_length = usize::try_from(header_length)
        .map_err(|_| resource("portable header does not fit address space"))?;
    let header_end = prefix
        .checked_add(header_length)
        .ok_or_else(|| corrupt("portable header length overflows"))?;
    let header_bytes = envelope
        .get(prefix..header_end)
        .ok_or_else(|| corrupt("portable envelope header is truncated"))?;
    let header: EnvelopeHeader = serde_json::from_slice(header_bytes)
        .map_err(|_| corrupt("portable envelope header is not valid canonical JSON"))?;
    let canonical = {
        let mut bytes = serde_json::to_vec(&header).map_err(|error| {
            GfError::Storage(format!("failed to canonicalize portable header: {error}"))
        })?;
        bytes.push(b'\n');
        bytes
    };
    if canonical != header_bytes {
        return Err(corrupt("portable envelope header is not canonical"));
    }
    if header.format != FORMAT || header.format_version != FORMAT_VERSION {
        return Err(project_error(
            ProjectErrorCode::UnsupportedProjectFormat,
            "portable envelope format or version is unsupported",
        ));
    }
    let source_generation_uuid = parse_canonical_uuid(&header.source_generation_uuid)?;
    enforce_count(header.participants.len(), limits.max_participants)?;
    validate_capabilities(&header.capabilities, supported_capabilities)?;
    let participants = validate_participants(&header, envelope, header_end, limits)?;
    let capabilities = header
        .capabilities
        .into_iter()
        .map(|item| ProjectCapability {
            capability_id: item.capability_id,
            capability_version: item.capability_version,
        })
        .collect();
    Ok(ValidatedEnvelope {
        source_generation_uuid,
        envelope_sha256: Sha256::digest(envelope).into(),
        capabilities,
        participants,
    })
}

fn validate_participants(
    header: &EnvelopeHeader,
    envelope: &[u8],
    header_end: usize,
    limits: PortableProjectLimits,
) -> Result<Vec<ProjectParticipant>, GfError> {
    let mut cursor = header_end;
    let mut identities = BTreeSet::new();
    let mut prior_identity: Option<(&str, &str)> = None;
    let mut participants = Vec::with_capacity(header.participants.len());
    for item in &header.participants {
        validate_machine_id(&item.capability_id)?;
        validate_machine_id(&item.record_family_id)?;
        let identity = (item.capability_id.as_str(), item.record_family_id.as_str());
        if prior_identity.is_some_and(|prior| prior >= identity) {
            return Err(corrupt("portable participant inventory is not canonical"));
        }
        prior_identity = Some(identity);
        if !identities.insert((&item.capability_id, &item.record_family_id)) {
            return Err(corrupt(
                "portable envelope has duplicate participant identity",
            ));
        }
        enforce_size(
            item.byte_length,
            limits.max_participant_bytes,
            "portable participant",
        )?;
        let length = usize::try_from(item.byte_length)
            .map_err(|_| resource("portable participant does not fit address space"))?;
        let end = cursor
            .checked_add(length)
            .ok_or_else(|| corrupt("portable participant length overflows"))?;
        let bytes = envelope
            .get(cursor..end)
            .ok_or_else(|| corrupt("portable participant is truncated"))?;
        if Sha256::digest(bytes).as_slice() != parse_digest(&item.content_sha256)? {
            return Err(corrupt(
                "portable participant content digest does not match",
            ));
        }
        let encoding = match item.encoding.as_str() {
            "parquet" => ProjectParticipantEncoding::Parquet,
            "arrow" => ProjectParticipantEncoding::Arrow,
            "json" => ProjectParticipantEncoding::Json,
            _ => return Err(corrupt("portable participant encoding is unsupported")),
        };
        if !header.capabilities.iter().any(|capability| {
            capability.capability_id == item.capability_id
                && capability.capability_version == item.capability_version
        }) {
            return Err(corrupt("portable participant capability is not declared"));
        }
        participants.push(ProjectParticipant {
            capability_id: item.capability_id.clone(),
            capability_version: item.capability_version,
            record_family_id: item.record_family_id.clone(),
            record_version: item.record_version,
            encoding,
            schema_fingerprint: parse_digest(&item.schema_fingerprint)?,
            row_count: item.row_count,
            bytes: bytes.to_vec(),
        });
        cursor = end;
    }
    if cursor != envelope.len() {
        return Err(corrupt("portable envelope has trailing bytes"));
    }
    Ok(participants)
}

fn validate_capabilities(
    capabilities: &[EnvelopeCapability],
    supported: &[ProjectCapability],
) -> Result<(), GfError> {
    let mut prior: Option<&str> = None;
    for item in capabilities {
        validate_machine_id(&item.capability_id)?;
        if item.capability_version == 0
            || prior.is_some_and(|value| value >= item.capability_id.as_str())
        {
            return Err(corrupt("portable capability inventory is not canonical"));
        }
        prior = Some(&item.capability_id);
        if !supported.iter().any(|candidate| {
            candidate.capability_id == item.capability_id
                && candidate.capability_version == item.capability_version
        }) {
            return Err(project_error(
                ProjectErrorCode::UnsupportedCapabilityVersion,
                format!(
                    "portable capability {}@{} is unsupported",
                    item.capability_id, item.capability_version
                ),
            ));
        }
    }
    Ok(())
}

fn prepare_import_target(target: &Path) -> Result<Option<ResolvedProjectGeneration>, GfError> {
    reject_symlink_components(target, "portable import target")?;
    match std::fs::symlink_metadata(target) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            Err(project_error(
                ProjectErrorCode::UnsupportedProjectFormat,
                "portable import target is linked or not a directory",
            ))
        }
        Ok(_) => {
            let is_empty = std::fs::read_dir(target)
                .map_err(|error| {
                    GfError::Storage(format!("failed to inspect portable import target: {error}"))
                })?
                .next()
                .is_none();
            if is_empty {
                Ok(None)
            } else if is_pristine_initialized_target(target)? {
                resolve_project_generation(target).map(Some)
            } else {
                Err(project_error(
                    ProjectErrorCode::UnsupportedProjectFormat,
                    "portable import target must be empty or pristine",
                ))
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(target).map_err(|error| {
                GfError::Storage(format!("failed to create portable import target: {error}"))
            })?;
            Ok(None)
        }
        Err(error) => Err(GfError::Storage(format!(
            "failed to inspect portable import target: {error}"
        ))),
    }
}

fn is_pristine_initialized_target(target: &Path) -> Result<bool, GfError> {
    let Ok(resolved) = resolve_project_generation(target) else {
        return Ok(false);
    };
    let capabilities = resolved.capabilities();
    if capabilities.len() != 2
        || capabilities[0].capability_id != "graph"
        || capabilities[0].capability_version != 1
        || capabilities[1].capability_id != "workspace"
        || capabilities[1].capability_version != 1
    {
        return Ok(false);
    }
    let actual = resolved.participant_snapshots()?;
    let expected = crate::workspace_participants::empty_workspace_participants()?;
    if actual.len() != expected.len()
        || !actual.iter().zip(&expected).all(|(actual, expected)| {
            actual.capability_id == expected.capability_id
                && actual.capability_version == expected.capability_version
                && actual.record_family_id == expected.record_family_id
                && actual.record_version == expected.record_version
                && actual.encoding
                    == match expected.encoding {
                        ProjectParticipantEncoding::Parquet => "parquet",
                        ProjectParticipantEncoding::Arrow => "arrow",
                        ProjectParticipantEncoding::Json => "json",
                    }
                && actual.schema_fingerprint == expected.schema_fingerprint
                && actual.row_count == expected.row_count
                && actual.bytes == expected.bytes
        })
    {
        return Ok(false);
    }
    has_exact_pristine_layout(target, resolved.generation_uuid())
}

fn has_exact_pristine_layout(target: &Path, generation_uuid: Uuid) -> Result<bool, GfError> {
    let root_names = directory_names(target)?;
    if root_names != ["CURRENT", "FORMAT", "generations"] {
        return Ok(false);
    }
    let generations = target.join("generations");
    if directory_names(&generations)? != [generation_uuid.hyphenated().to_string()] {
        return Ok(false);
    }
    let generation = generations.join(generation_uuid.hyphenated().to_string());
    if directory_names(&generation)? != ["lease.lock", "manifest.json", "participants"] {
        return Ok(false);
    }
    let participants = generation.join("participants");
    if directory_names(&participants)? != ["workspace"] {
        return Ok(false);
    }
    Ok(
        directory_names(&participants.join("workspace"))?
            == ["configuration.json", "ontology.json"],
    )
}

fn directory_names(path: &Path) -> Result<Vec<String>, GfError> {
    let mut names = std::fs::read_dir(path)
        .map_err(|error| {
            GfError::Storage(format!("failed to inspect portable import target: {error}"))
        })?
        .map(|entry| {
            entry
                .map_err(|error| {
                    GfError::Storage(format!("failed to inspect portable import target: {error}"))
                })?
                .file_name()
                .into_string()
                .map_err(|_| {
                    project_error(
                        ProjectErrorCode::UnsupportedProjectFormat,
                        "portable import target contains a non-UTF-8 entry",
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    names.sort();
    Ok(names)
}

fn reject_export_destination(path: &Path) -> Result<(), GfError> {
    reject_symlink_components(path, "portable export destination")?;
    if std::fs::symlink_metadata(path).is_ok() {
        return Err(project_error(
            ProjectErrorCode::UnsupportedProjectFormat,
            "portable export destination already exists",
        ));
    }
    let parent = path.parent().ok_or_else(|| {
        project_error(
            ProjectErrorCode::UnsupportedFilesystem,
            "portable export destination has no parent",
        )
    })?;
    let metadata = std::fs::symlink_metadata(parent).map_err(|error| {
        GfError::Storage(format!("failed to inspect portable export parent: {error}"))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(project_error(
            ProjectErrorCode::UnsupportedFilesystem,
            "portable export parent is linked or not a directory",
        ));
    }
    Ok(())
}

fn reject_symlink_components(path: &Path, name: &str) -> Result<(), GfError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| {
                GfError::Storage(format!("failed to resolve current directory: {error}"))
            })?
            .join(path)
    };
    for component in absolute.ancestors() {
        match std::fs::symlink_metadata(component) {
            Ok(metadata)
                if metadata.file_type().is_symlink() && !trusted_platform_symlink(&metadata) =>
            {
                return Err(project_error(
                    ProjectErrorCode::UnsupportedFilesystem,
                    format!("{name} has a symbolic-link path component"),
                ));
            }
            Ok(metadata)
                if component != absolute
                    && !metadata.is_dir()
                    && !metadata.file_type().is_symlink() =>
            {
                return Err(project_error(
                    ProjectErrorCode::UnsupportedFilesystem,
                    format!("{name} has a non-directory ancestor"),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(GfError::Storage(format!(
                    "failed to inspect {name} path components: {error}"
                )));
            }
        }
    }
    Ok(())
}

// macOS exposes stable system prefixes such as `/var` through root-owned
// links. Those are outside a caller's control; project-local links are not.
#[cfg(unix)]
fn trusted_platform_symlink(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    metadata.uid() == 0
}

#[cfg(not(unix))]
fn trusted_platform_symlink(_metadata: &std::fs::Metadata) -> bool {
    false
}

#[cfg(unix)]
fn open_regular_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn open_regular_nofollow(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::File::open(path)
}

#[cfg(unix)]
fn same_file_identity(before: &std::fs::Metadata, after: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    before.dev() == after.dev() && before.ino() == after.ino()
}

#[cfg(not(unix))]
fn same_file_identity(before: &std::fs::Metadata, after: &std::fs::Metadata) -> bool {
    before.len() == after.len()
        && before.modified().ok() == after.modified().ok()
        && before.created().ok() == after.created().ok()
}

fn validate_machine_id(value: &str) -> Result<(), GfError> {
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
        })
    {
        return Err(corrupt(
            "portable envelope contains an invalid machine identifier",
        ));
    }
    Ok(())
}

fn parse_canonical_uuid(value: &str) -> Result<Uuid, GfError> {
    let parsed = Uuid::parse_str(value)
        .map_err(|_| corrupt("portable envelope generation UUID is invalid"))?;
    if parsed.hyphenated().to_string() != value {
        return Err(corrupt(
            "portable envelope generation UUID is not canonical",
        ));
    }
    Ok(parsed)
}

fn parse_digest(value: &str) -> Result<[u8; 32], GfError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(corrupt("portable envelope digest is invalid"));
    }
    let mut digest = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        digest[index] = (hex_nibble(chunk[0]) << 4) | hex_nibble(chunk[1]);
    }
    Ok(digest)
}

fn hex_nibble(byte: u8) -> u8 {
    if byte <= b'9' {
        byte - b'0'
    } else {
        byte - b'a' + 10
    }
}

fn hex(digest: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(DIGITS[usize::from(byte >> 4)] as char);
        output.push(DIGITS[usize::from(byte & 0xf)] as char);
    }
    output
}

fn enforce_count(actual: usize, limit: usize) -> Result<(), GfError> {
    if actual > limit {
        Err(resource("portable participant count exceeds limit"))
    } else {
        Ok(())
    }
}

fn enforce_size(actual: u64, limit: u64, name: &str) -> Result<(), GfError> {
    if actual > limit {
        Err(resource(format!("{name} exceeds limit")))
    } else {
        Ok(())
    }
}

fn corrupt(message: impl Into<String>) -> GfError {
    project_error(ProjectErrorCode::ProjectCorrupt, message)
}
fn resource(message: impl Into<String>) -> GfError {
    project_error(ProjectErrorCode::ResourceLimit, message)
}
fn project_error(code: ProjectErrorCode, message: impl Into<String>) -> GfError {
    GfError::Project {
        code,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::process::Command;

    use super::*;

    const ENABLE_COOKIE: &str = "graphforge-internal-subprocess-v1";
    const IMPORT_HELPER: &str = "project_portable::tests::subprocess_portable_import_writer";

    fn supported(generation: &ResolvedProjectGeneration) -> Vec<ProjectCapability> {
        generation
            .capabilities()
            .into_iter()
            .map(|item| ProjectCapability {
                capability_id: item.capability_id,
                capability_version: item.capability_version,
            })
            .collect()
    }

    fn mutate_header(envelope: &[u8], mutate: impl FnOnce(&mut EnvelopeHeader)) -> Vec<u8> {
        let prefix = MAGIC.len() + HEADER_LENGTH_BYTES;
        let header_length =
            u64::from_be_bytes(envelope[MAGIC.len()..prefix].try_into().unwrap()) as usize;
        let header_end = prefix + header_length;
        let mut header: EnvelopeHeader =
            serde_json::from_slice(&envelope[prefix..header_end]).unwrap();
        mutate(&mut header);
        let mut header_bytes = serde_json::to_vec(&header).unwrap();
        header_bytes.push(b'\n');
        let mut rebuilt = Vec::new();
        rebuilt.extend_from_slice(MAGIC);
        rebuilt.extend_from_slice(&u64::try_from(header_bytes.len()).unwrap().to_be_bytes());
        rebuilt.extend_from_slice(&header_bytes);
        rebuilt.extend_from_slice(&envelope[header_end..]);
        rebuilt
    }

    #[test]
    fn subprocess_portable_import_writer() {
        let Ok(target) = std::env::var("GRAPHFORGE_TEST_PROJECT_ROOT") else {
            return;
        };
        let envelope =
            std::fs::read(std::env::var("GRAPHFORGE_TEST_PORTABLE_ENVELOPE").unwrap()).unwrap();
        import_portable_project(
            &envelope,
            target,
            Uuid::parse_str(&std::env::var("GRAPHFORGE_TEST_TRANSACTION_UUID").unwrap()).unwrap(),
            Uuid::parse_str(&std::env::var("GRAPHFORGE_TEST_GENERATION_UUID").unwrap()).unwrap(),
            &[
                ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            PortableProjectLimits::default(),
        )
        .unwrap();
    }

    #[test]
    fn prepublication_failure_keeps_pristine_current_authoritative() {
        let source = tempfile::tempdir().unwrap();
        let source_generation = open_or_initialize_project(source.path()).unwrap();
        let (envelope, _) =
            encode_portable_project(&source_generation, PortableProjectLimits::default()).unwrap();
        let envelope_path = source.path().join("portable.gfportable");
        std::fs::write(&envelope_path, envelope).unwrap();

        let target = tempfile::tempdir().unwrap();
        let parent = open_or_initialize_project(target.path())
            .unwrap()
            .generation_uuid();
        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(IMPORT_HELPER)
            .arg("--nocapture")
            .env("GRAPHFORGE_TEST_PROJECT_ROOT", target.path())
            .env("GRAPHFORGE_TEST_PORTABLE_ENVELOPE", &envelope_path)
            .env(
                "GRAPHFORGE_TEST_TRANSACTION_UUID",
                Uuid::new_v4().to_string(),
            )
            .env(
                "GRAPHFORGE_TEST_GENERATION_UUID",
                Uuid::new_v4().to_string(),
            )
            .env("GRAPHFORGE_PROJECT_FAILPOINTS", ENABLE_COOKIE)
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT",
                "project.before_current_replace",
            )
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
        assert_eq!(
            resolve_project_generation(target.path())
                .unwrap()
                .generation_uuid(),
            parent
        );
    }

    #[test]
    fn deterministic_round_trip_publishes_complete_new_generation() {
        let source = tempfile::tempdir().unwrap();
        let source_generation = open_or_initialize_project(source.path()).unwrap();
        let expected = source_generation.participant_snapshots().unwrap();
        let limits = PortableProjectLimits::default();
        let (first, first_receipt) = encode_portable_project(&source_generation, limits).unwrap();
        let (second, second_receipt) = encode_portable_project(&source_generation, limits).unwrap();
        assert_eq!(first, second);
        assert_eq!(first_receipt, second_receipt);

        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("imported project");
        let imported = import_portable_project(
            &first,
            &target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported(&source_generation),
            limits,
        )
        .unwrap();
        assert_eq!(
            imported.source_generation_uuid,
            source_generation.generation_uuid()
        );
        assert_eq!(imported.envelope_sha256, first_receipt.envelope_sha256);
        let reopened = resolve_project_generation(&target).unwrap();
        assert_eq!(
            reopened.generation_uuid(),
            imported.publication.generation_uuid
        );
        assert_eq!(reopened.participant_snapshots().unwrap(), expected);
        assert!(!target.join("trash").exists());
        assert!(!target.join("cache").exists());
    }

    #[test]
    fn pristine_initialized_target_is_importable() {
        let source = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(source.path()).unwrap();
        let limits = PortableProjectLimits::default();
        let (envelope, _) = encode_portable_project(&generation, limits).unwrap();
        let target = tempfile::tempdir().unwrap();
        let prior = open_or_initialize_project(target.path()).unwrap();
        let prior_uuid = prior.generation_uuid();
        drop(prior);

        let imported = import_portable_project(
            &envelope,
            target.path(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported(&generation),
            limits,
        )
        .unwrap();
        assert_ne!(imported.publication.generation_uuid, prior_uuid);
        assert_eq!(
            resolve_project_generation(target.path())
                .unwrap()
                .generation_uuid(),
            imported.publication.generation_uuid
        );
    }

    #[test]
    fn validation_failure_preserves_pristine_target_current() {
        let source = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(source.path()).unwrap();
        let limits = PortableProjectLimits::default();
        let (envelope, _) = encode_portable_project(&generation, limits).unwrap();
        let target = tempfile::tempdir().unwrap();
        let prior = open_or_initialize_project(target.path()).unwrap();
        let prior_uuid = prior.generation_uuid();
        drop(prior);

        let error = import_portable_project(
            &envelope,
            target.path(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            &[],
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_UNSUPPORTED_CAPABILITY_VERSION");
        assert_eq!(
            resolve_project_generation(target.path())
                .unwrap()
                .generation_uuid(),
            prior_uuid
        );
    }

    #[test]
    fn export_refuses_to_overwrite_existing_destination() {
        let source = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(source.path()).unwrap();
        let destination = source.path().join("existing.gfx");
        std::fs::write(&destination, b"keep").unwrap();

        let error =
            export_portable_project(&generation, &destination, PortableProjectLimits::default())
                .unwrap_err();
        assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
        assert_eq!(std::fs::read(destination).unwrap(), b"keep");
    }

    #[cfg(unix)]
    #[test]
    fn linked_ancestor_is_rejected_for_source_export_and_target() {
        use std::os::unix::fs::symlink;

        let source_project = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(source_project.path()).unwrap();
        let limits = PortableProjectLimits::default();
        let (envelope, _) = encode_portable_project(&generation, limits).unwrap();
        let real = tempfile::tempdir().unwrap();
        let links = tempfile::tempdir().unwrap();
        let linked = links.path().join("linked");
        symlink(real.path(), &linked).unwrap();

        let export_error =
            export_portable_project(&generation, linked.join("out.gfx"), limits).unwrap_err();
        assert_eq!(export_error.code(), "GF_UNSUPPORTED_FILESYSTEM");
        assert!(!real.path().join("out.gfx").exists());

        std::fs::write(real.path().join("in.gfx"), &envelope).unwrap();
        let source_error = import_portable_project_file(
            linked.join("in.gfx"),
            real.path().join("unused-target"),
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported(&generation),
            limits,
        )
        .unwrap_err();
        assert_eq!(source_error.code(), "GF_UNSUPPORTED_FILESYSTEM");
        assert!(!real.path().join("unused-target").exists());

        let input = links.path().join("input.gfx");
        std::fs::write(&input, envelope).unwrap();
        let target_error = import_portable_project_file(
            input,
            linked.join("target"),
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported(&generation),
            limits,
        )
        .unwrap_err();
        assert_eq!(target_error.code(), "GF_UNSUPPORTED_FILESYSTEM");
        assert!(!real.path().join("target").exists());
    }

    #[test]
    fn corruption_and_trailing_bytes_fail_before_target_creation() {
        let source = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(source.path()).unwrap();
        let limits = PortableProjectLimits::default();
        let (mut envelope, _) = encode_portable_project(&generation, limits).unwrap();
        *envelope.last_mut().unwrap() ^= 1;
        let parent = tempfile::tempdir().unwrap();
        let corrupt_target = parent.path().join("corrupt");
        let error = import_portable_project(
            &envelope,
            &corrupt_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported(&generation),
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
        assert!(!corrupt_target.exists());

        let (mut envelope, _) = encode_portable_project(&generation, limits).unwrap();
        envelope.push(0);
        let trailing_target = parent.path().join("trailing");
        let error = import_portable_project(
            &envelope,
            &trailing_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported(&generation),
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
        assert!(!trailing_target.exists());
    }

    #[test]
    fn resource_and_capability_checks_precede_mutation() {
        let source = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(source.path()).unwrap();
        let limits = PortableProjectLimits::default();
        let (envelope, _) = encode_portable_project(&generation, limits).unwrap();
        let parent = tempfile::tempdir().unwrap();

        let bounded_target = parent.path().join("bounded");
        let tiny = PortableProjectLimits {
            max_envelope_bytes: u64::try_from(envelope.len() - 1).unwrap(),
            ..limits
        };
        let error = import_portable_project(
            &envelope,
            &bounded_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported(&generation),
            tiny,
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_RESOURCE_LIMIT");
        assert!(!bounded_target.exists());

        let capability_target = parent.path().join("unsupported");
        let error = import_portable_project(
            &envelope,
            &capability_target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &[],
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_UNSUPPORTED_CAPABILITY_VERSION");
        assert!(!capability_target.exists());
    }

    #[test]
    fn nonempty_target_is_never_modified() {
        let source = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(source.path()).unwrap();
        let limits = PortableProjectLimits::default();
        let (envelope, _) = encode_portable_project(&generation, limits).unwrap();
        let target = tempfile::tempdir().unwrap();
        std::fs::write(target.path().join("keep.txt"), b"keep").unwrap();
        let error = import_portable_project(
            &envelope,
            target.path(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported(&generation),
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
        assert_eq!(
            std::fs::read(target.path().join("keep.txt")).unwrap(),
            b"keep"
        );
        assert!(!target.path().join("CURRENT").exists());
    }

    #[test]
    fn traversal_identity_is_rejected_before_target_creation() {
        let source = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(source.path()).unwrap();
        let limits = PortableProjectLimits::default();
        let (envelope, _) = encode_portable_project(&generation, limits).unwrap();
        let prefix = MAGIC.len() + HEADER_LENGTH_BYTES;
        let header_length =
            u64::from_be_bytes(envelope[MAGIC.len()..prefix].try_into().unwrap()) as usize;
        let header_end = prefix + header_length;
        let mut header: EnvelopeHeader =
            serde_json::from_slice(&envelope[prefix..header_end]).unwrap();
        header.participants[0].record_family_id = "../escape".into();
        let mut header_bytes = serde_json::to_vec(&header).unwrap();
        header_bytes.push(b'\n');
        let mut malicious = Vec::new();
        malicious.extend_from_slice(MAGIC);
        malicious.extend_from_slice(&u64::try_from(header_bytes.len()).unwrap().to_be_bytes());
        malicious.extend_from_slice(&header_bytes);
        malicious.extend_from_slice(&envelope[header_end..]);

        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("traversal");
        let error = import_portable_project(
            &malicious,
            &target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &supported(&generation),
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
        assert!(!target.exists());
    }

    #[test]
    fn file_import_round_trip_reopens_complete_generation() {
        let source = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(source.path()).unwrap();
        let capabilities = supported(&generation);
        let envelope_path = source.path().join("snapshot.gfproject");
        let exported = export_portable_project(
            &generation,
            &envelope_path,
            PortableProjectLimits::default(),
        )
        .unwrap();
        assert_eq!(exported.generation_uuid, generation.generation_uuid());

        let target = tempfile::tempdir().unwrap();
        let target_path = target.path().join("imported");
        let transaction_uuid = Uuid::now_v7();
        let generation_uuid = Uuid::now_v7();
        let first = import_portable_project_file(
            &envelope_path,
            &target_path,
            transaction_uuid,
            generation_uuid,
            &capabilities,
            PortableProjectLimits::default(),
        )
        .unwrap();
        assert_eq!(first.envelope_sha256, exported.envelope_sha256);
        assert_eq!(first.source_generation_uuid, generation.generation_uuid());
        assert_eq!(first.publication.generation_uuid, generation_uuid);
        assert!(!first.publication.idempotent_replay);
        drop(generation);

        let reopened = resolve_project_generation(&target_path).unwrap();
        assert_eq!(reopened.generation_uuid(), generation_uuid);
        reopened.validate_complete_participant_inventory().unwrap();
        let snapshots = reopened.participant_snapshots().unwrap();
        assert!(!snapshots.is_empty());
    }

    #[test]
    fn file_import_rejects_nonregular_and_oversized_sources_before_target_creation() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        let error = import_portable_project_file(
            root.path(),
            &target,
            Uuid::now_v7(),
            Uuid::now_v7(),
            &[],
            PortableProjectLimits::default(),
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_UNSUPPORTED_FILESYSTEM");
        assert!(!target.exists());

        let source = root.path().join("oversized.gfproject");
        fs::write(&source, b"too large").unwrap();
        let limits = PortableProjectLimits {
            max_envelope_bytes: 1,
            ..PortableProjectLimits::default()
        };
        let error = import_portable_project_file(
            &source,
            &target,
            Uuid::now_v7(),
            Uuid::now_v7(),
            &[],
            limits,
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_RESOURCE_LIMIT");
        assert!(!target.exists());
    }

    #[test]
    fn portable_identity_digest_and_count_validation_matrix_is_total() {
        let uuid = Uuid::now_v7();
        assert_eq!(
            parse_canonical_uuid(&uuid.hyphenated().to_string()).unwrap(),
            uuid
        );
        for value in [
            "bad".to_owned(),
            uuid.simple().to_string(),
            uuid.hyphenated().to_string().to_uppercase(),
        ] {
            assert_eq!(
                parse_canonical_uuid(&value).unwrap_err().code(),
                "GF_PROJECT_CORRUPT"
            );
        }
        assert_eq!(parse_digest(&"00".repeat(32)).unwrap(), [0; 32]);
        for value in ["short".to_owned(), "AA".repeat(32), "zz".repeat(32)] {
            assert_eq!(
                parse_digest(&value).unwrap_err().code(),
                "GF_PROJECT_CORRUPT"
            );
        }
        assert!(enforce_count(4, 4).is_ok());
        assert_eq!(enforce_count(5, 4).unwrap_err().code(), "GF_RESOURCE_LIMIT");
    }

    #[test]
    fn portable_header_contract_matrix_rejects_noncanonical_or_inconsistent_inventory() {
        let source = tempfile::tempdir().unwrap();
        let generation = open_or_initialize_project(source.path()).unwrap();
        let limits = PortableProjectLimits::default();
        let (envelope, _) = encode_portable_project(&generation, limits).unwrap();
        let supported = supported(&generation);

        let malformed = [
            mutate_header(&envelope, |header| header.format = "other".into()),
            mutate_header(&envelope, |header| header.format_version += 1),
            mutate_header(&envelope, |header| {
                header.capabilities[0].capability_version = 0;
            }),
            mutate_header(&envelope, |header| {
                header.capabilities.swap(0, 1);
            }),
            mutate_header(&envelope, |header| {
                header.participants.swap(0, 1);
            }),
            mutate_header(&envelope, |header| {
                header.participants[0].encoding = "opaque".into();
            }),
            mutate_header(&envelope, |header| {
                header.participants[0].capability_version += 1;
            }),
            mutate_header(&envelope, |header| {
                header.participants[0].content_sha256 = "00".repeat(32);
            }),
            mutate_header(&envelope, |header| {
                header.participants[0].byte_length += 1;
            }),
        ];
        for candidate in malformed {
            let error = validate_envelope(&candidate, &supported, limits)
                .err()
                .expect("malformed header must fail");
            assert!(
                matches!(
                    error.code(),
                    "GF_PROJECT_CORRUPT"
                        | "GF_UNSUPPORTED_PROJECT_FORMAT"
                        | "GF_UNSUPPORTED_CAPABILITY_VERSION"
                ),
                "unexpected error contract: {error}"
            );
        }

        let prefix = MAGIC.len() + HEADER_LENGTH_BYTES;
        let header_length =
            u64::from_be_bytes(envelope[MAGIC.len()..prefix].try_into().unwrap()) as usize;
        let header_end = prefix + header_length;
        let mut noncanonical = Vec::new();
        noncanonical.extend_from_slice(MAGIC);
        noncanonical.extend_from_slice(&u64::try_from(header_length + 1).unwrap().to_be_bytes());
        noncanonical.push(b' ');
        noncanonical.extend_from_slice(&envelope[prefix..header_end]);
        noncanonical.extend_from_slice(&envelope[header_end..]);
        assert_eq!(
            validate_envelope(&noncanonical, &supported, limits)
                .err()
                .expect("noncanonical header must fail")
                .code(),
            "GF_PROJECT_CORRUPT"
        );

        for truncated in [
            Vec::new(),
            MAGIC[..MAGIC.len() - 1].to_vec(),
            envelope[..prefix].to_vec(),
            envelope[..header_end - 1].to_vec(),
        ] {
            assert_eq!(
                validate_envelope(&truncated, &supported, limits)
                    .err()
                    .expect("truncated envelope must fail")
                    .code(),
                "GF_PROJECT_CORRUPT"
            );
        }
    }

    #[test]
    fn export_destination_kind_matrix_preserves_existing_state() {
        let root = tempfile::tempdir().unwrap();
        let missing_parent = root.path().join("missing/out.gfx");
        assert_eq!(
            reject_export_destination(&missing_parent)
                .unwrap_err()
                .code(),
            "GF_IO"
        );
        assert!(!root.path().join("missing").exists());

        let directory = root.path().join("directory.gfx");
        std::fs::create_dir(&directory).unwrap();
        assert_eq!(
            reject_export_destination(&directory).unwrap_err().code(),
            "GF_UNSUPPORTED_PROJECT_FORMAT"
        );
        assert!(directory.is_dir());

        let regular = root.path().join("regular.gfx");
        std::fs::write(&regular, b"caller bytes").unwrap();
        assert_eq!(
            reject_export_destination(&regular).unwrap_err().code(),
            "GF_UNSUPPORTED_PROJECT_FORMAT"
        );
        assert_eq!(std::fs::read(&regular).unwrap(), b"caller bytes");

        let available = root.path().join("available.gfx");
        assert!(reject_export_destination(&available).is_ok());
        assert!(!available.exists());

        let import_file = root.path().join("import-target");
        std::fs::write(&import_file, b"caller bytes").unwrap();
        assert_eq!(
            prepare_import_target(&import_file).unwrap_err().code(),
            "GF_UNSUPPORTED_PROJECT_FORMAT"
        );
        assert_eq!(std::fs::read(&import_file).unwrap(), b"caller bytes");
    }

    #[test]
    fn pristine_import_target_requires_exact_layout_and_participant_bytes() {
        let root = tempfile::tempdir().unwrap();
        let resolved = open_or_initialize_project(root.path()).unwrap();
        assert!(is_pristine_initialized_target(root.path()).unwrap());
        assert!(has_exact_pristine_layout(root.path(), resolved.generation_uuid()).unwrap());

        let extra = root.path().join("caller-file");
        std::fs::write(&extra, b"preserve").unwrap();
        assert!(!is_pristine_initialized_target(root.path()).unwrap());
        assert!(!has_exact_pristine_layout(root.path(), resolved.generation_uuid()).unwrap());
        assert_eq!(
            directory_names(root.path()).unwrap().last().unwrap(),
            "generations"
        );
        std::fs::remove_file(&extra).unwrap();

        let generations = root.path().join("generations");
        let extra_generation = generations.join(Uuid::now_v7().hyphenated().to_string());
        std::fs::create_dir(&extra_generation).unwrap();
        assert!(!has_exact_pristine_layout(root.path(), resolved.generation_uuid()).unwrap());
        std::fs::remove_dir(&extra_generation).unwrap();

        let generation = root
            .path()
            .join("generations")
            .join(resolved.generation_uuid().hyphenated().to_string());
        let extra_generation_entry = generation.join("caller-file");
        std::fs::write(&extra_generation_entry, b"preserve").unwrap();
        assert!(!has_exact_pristine_layout(root.path(), resolved.generation_uuid()).unwrap());
        std::fs::remove_file(&extra_generation_entry).unwrap();

        let participants = generation.join("participants");
        let extra_participant = participants.join("caller-domain");
        std::fs::create_dir(&extra_participant).unwrap();
        assert!(!has_exact_pristine_layout(root.path(), resolved.generation_uuid()).unwrap());
        std::fs::remove_dir(&extra_participant).unwrap();

        let workspace = participants.join("workspace");
        let extra_workspace = workspace.join("caller.json");
        std::fs::write(&extra_workspace, b"preserve").unwrap();
        assert!(!has_exact_pristine_layout(root.path(), resolved.generation_uuid()).unwrap());
        std::fs::remove_file(&extra_workspace).unwrap();

        let participant = generation.join("participants/workspace/configuration.json");
        let stable = std::fs::read(&participant).unwrap();
        std::fs::write(&participant, b"different").unwrap();
        assert!(is_pristine_initialized_target(root.path()).is_err());
        std::fs::write(&participant, stable).unwrap();

        let non_project = tempfile::tempdir().unwrap();
        assert!(!is_pristine_initialized_target(non_project.path()).unwrap());
    }

    #[test]
    fn public_import_replay_fails_closed_after_reopen_without_extra_publication() {
        let source = tempfile::tempdir().unwrap();
        let source_generation = open_or_initialize_project(source.path()).unwrap();
        let limits = PortableProjectLimits::default();
        let (envelope, _) = encode_portable_project(&source_generation, limits).unwrap();
        let target_parent = tempfile::tempdir().unwrap();
        let target = target_parent.path().join("replayed-import");
        let transaction_uuid = Uuid::now_v7();
        let generation_uuid = Uuid::now_v7();
        let capabilities = supported(&source_generation);

        let first = import_portable_project(
            &envelope,
            &target,
            transaction_uuid,
            generation_uuid,
            &capabilities,
            limits,
        )
        .unwrap();
        let second = import_portable_project(
            &envelope,
            &target,
            transaction_uuid,
            generation_uuid,
            &capabilities,
            limits,
        )
        .unwrap_err();
        assert_eq!(first.publication.generation_uuid, generation_uuid);
        assert_eq!(second.code(), "GF_UNSUPPORTED_PROJECT_FORMAT");
        assert_eq!(
            resolve_project_generation(&target)
                .unwrap()
                .generation_uuid(),
            generation_uuid
        );
        assert_eq!(
            crate::published_project_transaction(&target, transaction_uuid)
                .unwrap()
                .unwrap()
                .generation_uuid,
            generation_uuid
        );
    }
}
