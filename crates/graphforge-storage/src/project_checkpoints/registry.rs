//! Authenticated registry pairs, bounded admission, and recovery.

use super::{
    BTreeSet, CHECKSUM_FILE, CheckpointReadLock, CheckpointRecord, CheckpointTombstone,
    Deserialize, GfError, INTENT_FILE, MAX_ACTIVE, MAX_REGISTRY_BYTES, MAX_TOMBSTONES, OpenOptions,
    Path, ProjectErrorCode, REGISTRY_FILE, Serialize, Sha256, Uuid, acquire_checkpoint_read_lock,
    acquire_mutation_locks, delete_request_digest_values, fs, project_error, project_failpoint,
    storage_io, sync_directory, validate_description, validate_name, validate_record_identity,
};
use sha2::Digest as _;
use std::fmt::Write as _;
use std::io::{Read as _, Write as _};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Registry {
    format: String,
    format_version: u32,
    pub(super) revision: u64,
    pub(super) active: Vec<CheckpointRecord>,
    pub(super) tombstones: Vec<CheckpointTombstone>,
}

impl Registry {
    pub(super) fn empty() -> Self {
        Self {
            format: "graphforge-checkpoints".into(),
            format_version: 1,
            revision: 0,
            active: Vec::new(),
            tombstones: Vec::new(),
        }
    }

    pub(super) fn canonical_bytes(&self) -> Result<Vec<u8>, GfError> {
        validate_registry(self)?;
        let mut bytes = serde_json::to_vec(self).map_err(registry_serde)?;
        bytes.push(b'\n');
        if bytes.len() as u64 > MAX_REGISTRY_BYTES {
            return Err(project_error(
                ProjectErrorCode::ResourceLimit,
                "checkpoint registry exceeds 8 MiB",
            ));
        }
        Ok(bytes)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryIntent {
    transaction_uuid: Uuid,
    previous_revision: Option<u64>,
    previous_sha256: Option<String>,
    next_revision: u64,
    next_sha256: String,
    registry_temp: String,
    checksum_temp: String,
}

pub(super) fn read_registry_for_read(
    root: &Path,
    checkpoint_root: &Path,
) -> Result<(CheckpointReadLock, Registry), GfError> {
    let checkpoint = acquire_checkpoint_read_lock(root)?;
    if !checkpoint_root.join(INTENT_FILE).exists() {
        return read_registry(checkpoint_root).map(|registry| (checkpoint, registry));
    }
    drop(checkpoint);
    {
        let _locks = acquire_mutation_locks(root)?;
        recover_pair(checkpoint_root)?;
    }
    let checkpoint = acquire_checkpoint_read_lock(root)?;
    let registry = read_registry(checkpoint_root)?;
    Ok((checkpoint, registry))
}

pub(super) fn read_registry(root: &Path) -> Result<Registry, GfError> {
    let registry_path = root.join(REGISTRY_FILE);
    let checksum_path = root.join(CHECKSUM_FILE);
    if !registry_path.exists() && !checksum_path.exists() {
        return Ok(Registry::empty());
    }
    let bytes = read_regular_bounded(&registry_path, MAX_REGISTRY_BYTES)?;
    let checksum = read_regular_bounded(&checksum_path, 128)?;
    let expected = format!("{}\n", hex(&Sha256::digest(&bytes).into()));
    if checksum != expected.as_bytes() {
        return Err(registry_corrupt(
            "registry checksum does not match exact bytes",
        ));
    }
    let registry: Registry = serde_json::from_slice(&bytes)
        .map_err(|_| registry_corrupt("registry JSON is malformed"))?;
    if registry.canonical_bytes()? != bytes {
        return Err(registry_corrupt("registry JSON is noncanonical"));
    }
    Ok(registry)
}

pub(super) fn commit_registry(
    root: &Path,
    registry: &Registry,
    transaction_uuid: Uuid,
) -> Result<(), GfError> {
    let next = registry.canonical_bytes()?;
    let next_digest = hex(&Sha256::digest(&next).into());
    let previous = read_valid_pair(root)?;
    let registry_temp = format!(".registry.{transaction_uuid}.json.next");
    let checksum_temp = format!(".registry.{transaction_uuid}.sha256.next");
    prepare_temp_path(&root.join(&registry_temp))?;
    prepare_temp_path(&root.join(&checksum_temp))?;
    write_new_synced(&root.join(&registry_temp), &next)?;
    write_new_synced(
        &root.join(&checksum_temp),
        format!("{next_digest}\n").as_bytes(),
    )?;
    sync_directory(root)?;
    project_failpoint::hit(
        "checkpoint.registry.after_file_fsync",
        Some(transaction_uuid),
        None,
        "REGISTRY_STAGED",
        false,
    )?;
    let intent = RegistryIntent {
        transaction_uuid,
        previous_revision: previous.as_ref().map(|(registry, _)| registry.revision),
        previous_sha256: previous.as_ref().map(|(_, digest)| digest.clone()),
        next_revision: registry.revision,
        next_sha256: next_digest,
        registry_temp: registry_temp.clone(),
        checksum_temp: checksum_temp.clone(),
    };
    write_intent(root, &intent)?;
    project_failpoint::hit(
        "checkpoint.registry.before_replace",
        Some(transaction_uuid),
        None,
        "REGISTRY_INTENT_DURABLE",
        false,
    )?;
    fs::rename(root.join(&registry_temp), root.join(REGISTRY_FILE)).map_err(storage_io)?;
    project_failpoint::hit(
        "checkpoint.registry.after_replace",
        Some(transaction_uuid),
        None,
        "REGISTRY_REPLACED",
        true,
    )?;
    fs::rename(root.join(&checksum_temp), root.join(CHECKSUM_FILE)).map_err(storage_io)?;
    sync_directory(root)?;
    project_failpoint::hit(
        "checkpoint.registry.after_dir_fsync",
        Some(transaction_uuid),
        None,
        "REGISTRY_DURABLE",
        true,
    )?;
    fs::remove_file(root.join(INTENT_FILE)).map_err(storage_io)?;
    sync_directory(root)
}

pub(super) fn recover_pair(root: &Path) -> Result<(), GfError> {
    let intent_path = root.join(INTENT_FILE);
    if !intent_path.exists() {
        read_registry(root)?;
        return Ok(());
    }
    let bytes = read_regular_bounded(&intent_path, 16 * 1024)?;
    let intent: RegistryIntent = serde_json::from_slice(&bytes)
        .map_err(|_| registry_corrupt("registry intent is malformed"))?;
    let mut canonical = serde_json::to_vec(&intent).map_err(registry_serde)?;
    canonical.push(b'\n');
    if canonical != bytes
        || !valid_private_name(&intent.registry_temp, intent.transaction_uuid, "json")
        || !valid_private_name(&intent.checksum_temp, intent.transaction_uuid, "sha256")
    {
        return Err(registry_corrupt(
            "registry intent is noncanonical or names unsafe files",
        ));
    }
    if let Ok(Some((current, digest))) = read_valid_pair(root) {
        if current.revision == intent.next_revision && digest == intent.next_sha256 {
            cleanup_intent(root, &intent)?;
            return Ok(());
        }
        if Some(current.revision) == intent.previous_revision
            && Some(digest) == intent.previous_sha256
        {
            validate_staged_pair(root, &intent)?;
            cleanup_intent(root, &intent)?;
            return Ok(());
        }
    }
    if intent.previous_revision.is_none()
        && !root.join(REGISTRY_FILE).exists()
        && !root.join(CHECKSUM_FILE).exists()
    {
        validate_staged_pair(root, &intent)?;
        cleanup_intent(root, &intent)?;
        return Ok(());
    }
    let registry_bytes = read_regular_bounded(&root.join(REGISTRY_FILE), MAX_REGISTRY_BYTES)?;
    if hex(&Sha256::digest(&registry_bytes).into()) == intent.next_sha256 {
        let checksum_bytes = read_regular_bounded(&root.join(&intent.checksum_temp), 128)?;
        if checksum_bytes == format!("{}\n", intent.next_sha256).as_bytes() {
            fs::rename(root.join(&intent.checksum_temp), root.join(CHECKSUM_FILE))
                .map_err(storage_io)?;
            sync_directory(root)?;
            cleanup_intent(root, &intent)?;
            read_registry(root)?;
            return Ok(());
        }
    }
    Err(registry_corrupt(
        "registry transaction is not a validated previous or staged next state",
    ))
}

fn validate_staged_pair(root: &Path, intent: &RegistryIntent) -> Result<(), GfError> {
    let registry_bytes =
        read_regular_bounded(&root.join(&intent.registry_temp), MAX_REGISTRY_BYTES)?;
    let checksum_bytes = read_regular_bounded(&root.join(&intent.checksum_temp), 128)?;
    let digest = hex(&Sha256::digest(&registry_bytes).into());
    if digest != intent.next_sha256
        || checksum_bytes != format!("{}\n", intent.next_sha256).as_bytes()
    {
        return Err(registry_corrupt(
            "registry staged pair does not match its durable intent",
        ));
    }
    let registry: Registry = serde_json::from_slice(&registry_bytes)
        .map_err(|_| registry_corrupt("staged checkpoint registry is malformed"))?;
    if registry.canonical_bytes()? != registry_bytes || registry.revision != intent.next_revision {
        return Err(registry_corrupt(
            "registry staged pair is noncanonical or has the wrong revision",
        ));
    }
    Ok(())
}

fn read_valid_pair(root: &Path) -> Result<Option<(Registry, String)>, GfError> {
    if !root.join(REGISTRY_FILE).exists() && !root.join(CHECKSUM_FILE).exists() {
        return Ok(None);
    }
    let registry = read_registry(root)?;
    let digest = hex(&Sha256::digest(registry.canonical_bytes()?).into());
    Ok(Some((registry, digest)))
}

fn cleanup_intent(root: &Path, intent: &RegistryIntent) -> Result<(), GfError> {
    for name in [&intent.registry_temp, &intent.checksum_temp] {
        let path = root.join(name);
        if path.exists() {
            validate_single_link_regular(&path, "registry transaction temporary file")?;
        }
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(storage_io(error)),
        }
    }
    fs::remove_file(root.join(INTENT_FILE)).map_err(storage_io)?;
    sync_directory(root)
}

fn write_intent(root: &Path, intent: &RegistryIntent) -> Result<(), GfError> {
    let temp = root.join(format!(".registry.{}.txn.next", intent.transaction_uuid));
    let mut bytes = serde_json::to_vec(intent).map_err(registry_serde)?;
    bytes.push(b'\n');
    prepare_temp_path(&temp)?;
    write_new_synced(&temp, &bytes)?;
    project_failpoint::hit(
        "checkpoint.registry.after_intent_file_fsync",
        Some(intent.transaction_uuid),
        None,
        "REGISTRY_INTENT_STAGED",
        false,
    )?;
    fs::rename(temp, root.join(INTENT_FILE)).map_err(storage_io)?;
    sync_directory(root)
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<(), GfError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(storage_io)?;
    file.write_all(bytes).map_err(storage_io)?;
    file.sync_all().map_err(storage_io)
}

fn prepare_temp_path(path: &Path) -> Result<(), GfError> {
    if !path.exists() {
        return Ok(());
    }
    let metadata = fs::symlink_metadata(path).map_err(storage_io)?;
    if !metadata.file_type().is_file() {
        return Err(registry_corrupt(
            "checkpoint registry temporary path is linked or special",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(registry_corrupt(
                "checkpoint registry temporary path is hard-linked",
            ));
        }
    }
    fs::remove_file(path).map_err(storage_io)
}

fn read_regular_bounded(path: &Path, max: u64) -> Result<Vec<u8>, GfError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| registry_corrupt("checkpoint registry file is missing"))?;
    if !metadata.file_type().is_file() || metadata.len() > max {
        return Err(registry_corrupt(
            "checkpoint registry file is linked, special, or oversized",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(registry_corrupt("checkpoint registry file is hard-linked"));
        }
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path).map_err(|_| {
        registry_corrupt("checkpoint registry file could not be opened without following links")
    })?;
    let opened = file.metadata().map_err(storage_io)?;
    if !opened.is_file() || opened.len() != metadata.len() {
        return Err(registry_corrupt(
            "checkpoint registry file identity changed while opening",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() || opened.nlink() != 1 {
            return Err(registry_corrupt(
                "checkpoint registry file identity changed while opening",
            ));
        }
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| registry_corrupt("checkpoint registry file length exceeds address space"))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(max + 1)
        .read_to_end(&mut bytes)
        .map_err(storage_io)?;
    if bytes.len() as u64 > max {
        return Err(registry_corrupt(
            "checkpoint registry file exceeds its read bound",
        ));
    }
    Ok(bytes)
}

fn validate_registry(registry: &Registry) -> Result<(), GfError> {
    if registry.format != "graphforge-checkpoints"
        || registry.format_version != 1
        || registry.active.len() > MAX_ACTIVE
        || registry.tombstones.len() > MAX_TOMBSTONES
    {
        return Err(registry_corrupt(
            "checkpoint registry header or bounds are invalid",
        ));
    }
    if !registry.active.windows(2).all(|pair| {
        (&pair[0].name, pair[0].checkpoint_uuid) < (&pair[1].name, pair[1].checkpoint_uuid)
    }) {
        return Err(registry_corrupt(
            "active checkpoints are not strictly sorted",
        ));
    }
    if !registry.tombstones.windows(2).all(|pair| {
        (pair[0].deleted_revision, pair[0].checkpoint_uuid)
            < (pair[1].deleted_revision, pair[1].checkpoint_uuid)
    }) {
        return Err(registry_corrupt(
            "checkpoint tombstones are not strictly sorted",
        ));
    }
    let mut names = BTreeSet::new();
    let mut checkpoint_uuids = BTreeSet::new();
    let mut create_operations = BTreeSet::new();
    let mut delete_operations = BTreeSet::new();
    for row in &registry.active {
        validate_checkpoint_content(&CheckpointContentRef {
            label: "active checkpoint",
            checkpoint_uuid: row.checkpoint_uuid,
            create_operation_uuid: row.create_operation_uuid,
            name: &row.name,
            description: row.description.as_deref(),
            created_by: row.created_by,
            generation_manifest_sha256: &row.generation_manifest_sha256,
            create_request_sha256: &row.create_request_sha256,
        })?;
        if row.created_revision == 0
            || row.created_revision > registry.revision
            || !names.insert(row.name.as_str())
            || !checkpoint_uuids.insert(row.checkpoint_uuid)
            || !create_operations.insert(row.create_operation_uuid)
        {
            return Err(registry_corrupt(
                "active checkpoint identities or revision are inconsistent",
            ));
        }
    }
    for row in &registry.tombstones {
        // Preserve pre-consolidation order: content fields, then delete digest,
        // then deterministic create-request identity (error precedence).
        validate_name(&row.name)
            .map_err(|_| registry_corrupt("checkpoint tombstone name is invalid"))?;
        validate_description(row.description.as_deref())
            .map_err(|_| registry_corrupt("checkpoint tombstone description is invalid"))?;
        validate_digest(&row.generation_manifest_sha256)?;
        validate_digest(&row.create_request_sha256)?;
        validate_digest(&row.delete_request_sha256)?;
        validate_record_identity(
            row.checkpoint_uuid,
            row.create_operation_uuid,
            &row.name,
            row.description.as_deref(),
            row.created_by,
            &row.create_request_sha256,
        )?;
        let expected_delete =
            delete_request_digest_values(row.delete_operation_uuid, &row.name, row.deleted_by);
        if row.created_revision == 0
            || row.created_revision >= row.deleted_revision
            || row.deleted_revision > registry.revision
            || !checkpoint_uuids.insert(row.checkpoint_uuid)
            || !create_operations.insert(row.create_operation_uuid)
            || delete_operations.contains(&row.create_operation_uuid)
            || !delete_operations.insert(row.delete_operation_uuid)
            || create_operations.contains(&row.delete_operation_uuid)
            || row.delete_request_sha256 != hex(&expected_delete)
        {
            return Err(registry_corrupt(
                "checkpoint tombstone identities or revisions are inconsistent",
            ));
        }
    }
    Ok(())
}

struct CheckpointContentRef<'a> {
    label: &'static str,
    checkpoint_uuid: Uuid,
    create_operation_uuid: Uuid,
    name: &'a str,
    description: Option<&'a str>,
    created_by: Option<Uuid>,
    generation_manifest_sha256: &'a str,
    create_request_sha256: &'a str,
}

fn validate_checkpoint_content(content: &CheckpointContentRef<'_>) -> Result<(), GfError> {
    validate_name(content.name)
        .map_err(|_| registry_corrupt(format!("{} name is invalid", content.label)))?;
    validate_description(content.description)
        .map_err(|_| registry_corrupt(format!("{} description is invalid", content.label)))?;
    validate_digest(content.generation_manifest_sha256)?;
    validate_digest(content.create_request_sha256)?;
    validate_record_identity(
        content.checkpoint_uuid,
        content.create_operation_uuid,
        content.name,
        content.description,
        content.created_by,
        content.create_request_sha256,
    )
}

fn validate_single_link_regular(path: &Path, label: &str) -> Result<(), GfError> {
    let metadata = fs::symlink_metadata(path).map_err(storage_io)?;
    if !metadata.file_type().is_file() {
        return Err(registry_corrupt(format!("{label} is linked or special")));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(registry_corrupt(format!("{label} is hard-linked")));
        }
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), GfError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(registry_corrupt("checkpoint digest is noncanonical"));
    }
    Ok(())
}

pub(super) fn decode_digest(value: &str) -> Result<[u8; 32], GfError> {
    validate_digest(value)?;
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair)
            .map_err(|_| registry_corrupt("checkpoint digest is not UTF-8"))?;
        digest[index] = u8::from_str_radix(text, 16)
            .map_err(|_| registry_corrupt("checkpoint digest is not lowercase hex"))?;
    }
    Ok(digest)
}

fn valid_private_name(name: &str, uuid: Uuid, kind: &str) -> bool {
    name == format!(".registry.{uuid}.{kind}.next")
}
pub(super) fn hex(bytes: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing hexadecimal to String cannot fail");
    }
    output
}

fn registry_serde(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("checkpoint registry encoding failed: {error}"))
}
pub(super) fn registry_corrupt(message: impl Into<String>) -> GfError {
    project_error(ProjectErrorCode::CheckpointRegistryCorrupt, message)
}

#[cfg(test)]
mod tests;
