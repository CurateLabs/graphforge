//! Durable recovery for mutable graph-file rewrite batches.
//!
//! Once the intent below is durable, recovery always rolls forward.  Data and
//! auxiliary receipts are installed first; `topology/generation.json` is the
//! final authority switch.  Journal paths are bounded, canonical relative
//! paths and every recovery input is authenticated before it is used.

use std::fmt::Write as _;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::{Component, Path};

use graphforge_core::GfError;
use graphforge_filesystem::{FileIdentity, StableDirectory};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

static PROCESS_REWRITE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

use crate::filesystem_admission::{ProjectLifecycleMode, ProjectRootRequirement};
use crate::staging::RewriteBatch;

const JOURNAL: &str = ".graphforge-rewrite-v1.json";
const LOCK: &str = ".graphforge-rewrite.lock";
const MAX_ENTRIES: usize = 16_384;
const MAX_JOURNAL_BYTES: u64 = 8 * 1024 * 1024;

/// Authenticated control receipt committed atomically with a rewrite.
///
/// This generic hook lets another storage participant bind its own typed,
/// durable receipt to the same generation-last transaction without coupling
/// this layer to that participant's format.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuxiliaryReceipt {
    /// Stable schema/type identifier understood by the auxiliary participant.
    pub kind: String,
    /// Participant receipt schema version.
    pub schema_version: u32,
    /// Canonical project-relative destination containing the receipt bytes.
    pub path: String,
    /// Lowercase SHA-256 digest of its exact durable receipt bytes.
    pub digest: String,
    /// Exact staged receipt length.
    pub bytes: u64,
}

/// Authoritative rewrite state exposed to an auxiliary participant while the
/// admitted project rewrite lock is held.
pub(crate) struct ParticipantPreparationContext<'a> {
    /// Exact recovered generation pair preceding this transaction.
    pub prior: GenerationPair,
    /// Exact generation pair that this transaction will publish.
    pub next: GenerationPair,
    /// Retained, admitted project directory capability.
    pub project: &'a StableDirectory,
    /// Canonical admitted project root.
    pub project_root: &'a Path,
}

/// One-shot participant preparation executed inside the retained rewrite
/// critical section.
pub(crate) type RewriteParticipantPreparer<'a> = Box<
    dyn FnOnce(
            ParticipantPreparationContext<'_>,
            &mut RewriteBatch,
        ) -> Result<Option<AuxiliaryReceipt>, GfError>
        + 'a,
>;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Topology and search authorities selected for one durable rewrite.
pub(crate) struct GenerationPair {
    /// Topology generation published by the transaction.
    pub topology: u64,
    /// Search generation published by the transaction.
    pub search: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Intent {
    version: u8,
    state: IntentState,
    transaction: String,
    root_volume: u64,
    root_file: String,
    prior: GenerationPair,
    next: GenerationPair,
    auxiliary: Option<AuxiliaryReceipt>,
    entries: Vec<Entry>,
    checksum: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum IntentState {
    Preparing,
    Durable,
}

#[derive(Debug, Serialize, Deserialize)]
struct Entry {
    class: EntryClass,
    destination: String,
    temporary: String,
    parent_volume: u64,
    parent_file: String,
    bytes: u64,
    sha256: String,
    temporary_volume: u64,
    temporary_file: String,
    prior_destination: Option<AuthenticatedFile>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AuthenticatedFile {
    volume: u64,
    file: String,
    bytes: u64,
    sha256: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum EntryClass {
    Data,
    GenerationAuthority,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AuxiliaryReconcileOutcome {
    Committed,
    NotCommitted,
}

pub(crate) fn reconcile_auxiliary(
    root: &Path,
    prior: GenerationPair,
    next: GenerationPair,
    receipt: &AuxiliaryReceipt,
) -> Result<AuxiliaryReconcileOutcome, GfError> {
    let guard = acquire(root)?;
    guard.revalidate()?;
    let raw = crate::generation::read_generation_state_raw(root)?;
    recover_locked(
        root,
        &guard.directory,
        GenerationPair {
            topology: raw.topology,
            search: raw.search,
        },
    )?;
    let current = crate::generation::read_generation_state_raw(root)?;
    let relative = Path::new(&receipt.path);
    canonical_journal_path(&receipt.path)?;
    let mut directory = graphforge_filesystem::StableDirectory::open(root).map_err(storage)?;
    let mut components = relative.components().peekable();
    let mut file = None;
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(storage("auxiliary receipt path is not canonical"));
        };
        if components.peek().is_some() {
            directory = directory.open_child_directory(name).map_err(storage)?;
        } else {
            file = match directory.open_child_file(name) {
                Ok(file) => Some(file),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(storage(error)),
            };
        }
    }
    let exact = if let Some(file) = file {
        let (bytes, digest) = hash_reader(file.try_clone().map_err(storage)?)?;
        directory.revalidate_named().map_err(storage)?;
        bytes == receipt.bytes && digest == receipt.digest
    } else {
        false
    };
    let current = GenerationPair {
        topology: current.topology,
        search: current.search,
    };
    if current == next && exact {
        Ok(AuxiliaryReconcileOutcome::Committed)
    } else if current == prior && !exact {
        Ok(AuxiliaryReconcileOutcome::NotCommitted)
    } else {
        Err(storage(
            "auxiliary rewrite outcome is ambiguous or substituted",
        ))
    }
}

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(error.to_string())
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len().saturating_mul(2)),
        |mut encoded, byte| {
            write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
            encoded
        },
    )
}

fn canonical_relative(root: &Path, path: &Path) -> Result<String, GfError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| storage("rewrite destination escapes project root"))?;
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(storage(
            "rewrite destination is not a canonical relative path",
        ));
    }
    relative
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| storage("rewrite destination is not UTF-8"))
}

fn hash_reader(mut file: File) -> Result<(u64, String), GfError> {
    file.rewind().map_err(storage)?;
    let mut hash = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024].into_boxed_slice();
    loop {
        let count = file.read(&mut buffer).map_err(storage)?;
        if count == 0 {
            break;
        }
        bytes = bytes
            .checked_add(count as u64)
            .ok_or_else(|| storage("rewrite byte count overflow"))?;
        hash.update(&buffer[..count]);
    }
    Ok((bytes, hex(&hash.finalize())))
}

fn authenticated_file(file: &File) -> Result<AuthenticatedFile, GfError> {
    let identity = graphforge_filesystem::file_identity(file).map_err(storage)?;
    let (bytes, sha256) = hash_reader(file.try_clone().map_err(storage)?)?;
    Ok(AuthenticatedFile {
        volume: identity.volume_serial,
        file: hex(&identity.file_id),
        bytes,
        sha256,
    })
}

struct RewriteGuard {
    _process: std::sync::MutexGuard<'static, ()>,
    admission: crate::filesystem_admission::ProjectLifecycleAdmission,
    directory: StableDirectory,
    lifecycle: File,
    lifecycle_identity: FileIdentity,
}

impl Drop for RewriteGuard {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.lifecycle);
    }
}

fn acquire(root: &Path) -> Result<RewriteGuard, GfError> {
    let process = PROCESS_REWRITE_LOCK
        .lock()
        .map_err(|_| storage("process rewrite lock is poisoned"))?;
    // The lifecycle guard binds the named project root. Ephemeral mode avoids
    // repeating the expensive filesystem probe; durable projects have already
    // passed it at facade admission.
    let admission = crate::filesystem_admission::admit_project_lifecycle(
        root,
        ProjectLifecycleMode::Ephemeral,
        ProjectRootRequirement::Existing,
    )?;
    let directory = StableDirectory::open(root)
        .map_err(|error| storage(format!("rewrite root open failed: {error}")))?;
    let lock = directory
        .open_or_create_child_file(std::ffi::OsStr::new(LOCK))
        .map_err(|error| storage(format!("rewrite lock open failed: {error}")))?;
    lock.sync_all()
        .map_err(|error| storage(format!("rewrite lock sync failed: {error}")))?;
    directory
        .sync()
        .map_err(|error| storage(format!("rewrite root sync failed: {error}")))?;
    crate::file_lock::lock_exclusive(&lock)
        .map_err(|error| storage(format!("rewrite lock acquisition failed: {error}")))?;
    admission.revalidate_identity()?;
    let lifecycle_identity = graphforge_filesystem::file_identity(&lock).map_err(storage)?;
    Ok(RewriteGuard {
        _process: process,
        admission,
        directory,
        lifecycle: lock,
        lifecycle_identity,
    })
}

impl RewriteGuard {
    fn revalidate(&self) -> Result<(), GfError> {
        self.admission.revalidate_identity()?;
        let named = self
            .directory
            .open_child_file(std::ffi::OsStr::new(LOCK))
            .map_err(storage)?;
        if graphforge_filesystem::file_identity(&named).map_err(storage)? != self.lifecycle_identity
            || graphforge_filesystem::file_link_count(&named).map_err(storage)? != 1
        {
            return Err(storage("rewrite lifecycle lock identity changed"));
        }
        Ok(())
    }
}

fn intent_bytes(intent: &Intent) -> Result<Vec<u8>, GfError> {
    serde_json::to_vec(intent).map_err(storage)
}

fn checksum(intent: &Intent) -> Result<String, GfError> {
    let unsigned = Intent {
        checksum: String::new(),
        version: intent.version,
        state: intent.state,
        transaction: intent.transaction.clone(),
        root_volume: intent.root_volume,
        root_file: intent.root_file.clone(),
        prior: intent.prior,
        next: intent.next,
        auxiliary: intent.auxiliary.clone(),
        entries: intent
            .entries
            .iter()
            .map(|e| Entry {
                destination: e.destination.clone(),
                class: e.class,
                temporary: e.temporary.clone(),
                parent_volume: e.parent_volume,
                parent_file: e.parent_file.clone(),
                bytes: e.bytes,
                sha256: e.sha256.clone(),
                temporary_volume: e.temporary_volume,
                temporary_file: e.temporary_file.clone(),
                prior_destination: e.prior_destination.as_ref().map(|prior| AuthenticatedFile {
                    volume: prior.volume,
                    file: prior.file.clone(),
                    bytes: prior.bytes,
                    sha256: prior.sha256.clone(),
                }),
            })
            .collect(),
    };
    Ok(hex(&Sha256::digest(intent_bytes(&unsigned)?)))
}

fn publish_journal(root: &StableDirectory, intent: &Intent) -> Result<(), GfError> {
    let bytes = intent_bytes(intent)?;
    if bytes.len() as u64 > MAX_JOURNAL_BYTES {
        return Err(storage("rewrite journal exceeds bound"));
    }
    let name = format!(".{JOURNAL}.{}.tmp", intent.transaction);
    let mut temp = root
        .create_replaceable_child_file(std::ffi::OsStr::new(&name))
        .map_err(storage)?;
    temp.write_all(&bytes)
        .and_then(|()| temp.sync_all())
        .map_err(storage)?;
    let expected = graphforge_filesystem::file_identity(&temp).map_err(storage)?;
    root.replace_child(
        std::ffi::OsStr::new(&name),
        expected,
        std::ffi::OsStr::new(JOURNAL),
    )
    .map_err(storage)?;
    root.sync().map_err(storage)
}

fn install(root_path: &Path, root: &StableDirectory, entry: &Entry) -> Result<(), GfError> {
    let (parent, target) = retained_parent_at(root_path, root, &entry.destination)?;
    let (temporary_parent, temporary) = retained_parent_at(root_path, root, &entry.temporary)?;
    let parent_id = parent.identity();
    if (parent_id.volume_serial, hex(&parent_id.file_id))
        != (entry.parent_volume, entry.parent_file.clone())
    {
        return Err(storage("rewrite parent identity changed"));
    }
    if temporary_parent.identity() != parent_id {
        return Err(storage("rewrite temporary and destination parents differ"));
    }

    match parent.open_child_file(&temporary) {
        Ok(temp) => {
            authenticate_staged_file(&temp, entry)?;
            authenticate_prior_destination(&parent, &target, entry.prior_destination.as_ref())?;
            let expected = graphforge_filesystem::file_identity(&temp).map_err(storage)?;
            drop(temp);
            parent
                .replace_child(&temporary, expected, &target)
                .map_err(storage)?;
            parent.sync().map_err(storage)?;
            authenticate_installed_destination(&parent, &target, entry)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A missing retained temporary is valid only after its exact file
            // identity was installed at the destination by an earlier pass.
            authenticate_installed_destination(&parent, &target, entry)?;
        }
        Err(error) => return Err(storage(error)),
    }
    Ok(())
}

fn authenticate_staged_file(file: &File, entry: &Entry) -> Result<(), GfError> {
    let identity = graphforge_filesystem::file_identity(file).map_err(storage)?;
    if (identity.volume_serial, hex(&identity.file_id))
        != (entry.temporary_volume, entry.temporary_file.clone())
    {
        return Err(storage("rewrite temporary identity changed"));
    }
    if hash_reader(file.try_clone().map_err(storage)?)? != (entry.bytes, entry.sha256.clone()) {
        return Err(storage("rewrite recovery input is missing or corrupt"));
    }
    Ok(())
}

fn authenticate_prior_destination(
    parent: &StableDirectory,
    target: &std::ffi::OsStr,
    prior: Option<&AuthenticatedFile>,
) -> Result<(), GfError> {
    match (parent.open_child_file(target), prior) {
        (Err(error), None) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        (Ok(file), Some(prior)) => {
            let identity = graphforge_filesystem::file_identity(&file).map_err(storage)?;
            if (identity.volume_serial, hex(&identity.file_id))
                != (prior.volume, prior.file.clone())
                || hash_reader(file)? != (prior.bytes, prior.sha256.clone())
            {
                return Err(storage("rewrite destination changed after intent"));
            }
            Ok(())
        }
        (Err(error), Some(_)) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(storage("rewrite destination disappeared after intent"))
        }
        (Ok(_), None) => Err(storage("rewrite destination appeared after intent")),
        (Err(error), _) => Err(storage(error)),
    }
}

fn authenticate_installed_destination(
    parent: &StableDirectory,
    target: &std::ffi::OsStr,
    entry: &Entry,
) -> Result<(), GfError> {
    let destination = parent.open_child_file(target).map_err(storage)?;
    let identity = graphforge_filesystem::file_identity(&destination).map_err(storage)?;
    if (identity.volume_serial, hex(&identity.file_id))
        != (entry.temporary_volume, entry.temporary_file.clone())
        || hash_reader(destination)? != (entry.bytes, entry.sha256.clone())
    {
        return Err(storage(
            "installed rewrite destination failed authentication",
        ));
    }
    Ok(())
}

fn retained_parent_at(
    root_path: &Path,
    root: &StableDirectory,
    relative: &str,
) -> Result<(StableDirectory, std::ffi::OsString), GfError> {
    let path = Path::new(relative);
    let mut components = path.components().peekable();
    let mut directory = StableDirectory::open(root_path).map_err(storage)?;
    if directory.identity() != root.identity() {
        return Err(storage("rewrite root identity changed"));
    }
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(storage("non-canonical journal path"));
        };
        if components.peek().is_none() {
            return Ok((directory, name.to_os_string()));
        }
        directory = directory.open_child_directory(name).map_err(storage)?;
    }
    Err(storage("empty journal path"))
}

fn remove_journal(root: &StableDirectory) -> Result<(), GfError> {
    let file = root
        .open_child_file(std::ffi::OsStr::new(JOURNAL))
        .map_err(storage)?;
    let id = graphforge_filesystem::file_identity(&file).map_err(storage)?;
    drop(file);
    root.unlink_child_if_identity(std::ffi::OsStr::new(JOURNAL), id)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

fn read_journal(root: &StableDirectory) -> Result<Option<(Vec<u8>, FileIdentity)>, GfError> {
    let file = match root.open_child_file(std::ffi::OsStr::new(JOURNAL)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
    };
    let id = graphforge_filesystem::file_identity(&file).map_err(storage)?;
    let metadata = file.metadata().map_err(storage)?;
    if metadata.len() > MAX_JOURNAL_BYTES {
        return Err(storage("rewrite journal exceeds bound"));
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| storage("rewrite journal length exceeds address space"))?;
    let mut bytes = Vec::with_capacity(capacity);
    file.take(MAX_JOURNAL_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(storage)?;
    Ok(Some((bytes, id)))
}

fn recover_locked(
    root_path: &Path,
    root: &StableDirectory,
    current: GenerationPair,
) -> Result<(), GfError> {
    let Some((bytes, _journal_id)) = read_journal(root)? else {
        return Ok(());
    };
    let intent: Intent = serde_json::from_slice(&bytes)
        .map_err(|e| storage(format!("corrupt rewrite journal: {e}")))?;
    if intent.version != 1
        || intent.entries.len() > MAX_ENTRIES
        || checksum(&intent)? != intent.checksum
    {
        return Err(storage("rewrite journal authentication failed"));
    }
    validate_intent(&intent)?;
    let root_id = root.identity();
    if (root_id.volume_serial, hex(&root_id.file_id))
        != (intent.root_volume, intent.root_file.clone())
    {
        return Err(storage("rewrite project identity changed"));
    }
    if current != intent.prior && current != intent.next {
        return Err(storage("rewrite generation authority diverged"));
    }
    if intent.state == IntentState::Preparing {
        for entry in &intent.entries {
            cleanup_preparing_input(root_path, root, entry)?;
        }
        return remove_journal(root);
    }
    let authority_count = intent
        .entries
        .iter()
        .filter(|entry| entry.class == EntryClass::GenerationAuthority)
        .count();
    if authority_count != 1 {
        return Err(storage(
            "rewrite journal must contain exactly one generation authority",
        ));
    }
    let data = intent
        .entries
        .iter()
        .filter(|entry| entry.class == EntryClass::Data)
        .collect::<Vec<_>>();
    for (index, entry) in data.iter().enumerate() {
        install(root_path, root, entry)?;
        let boundary = if index == 0 {
            "rewrite.after_first_data_install"
        } else if index + 1 == data.len() {
            "rewrite.after_last_data_install"
        } else {
            "rewrite.after_middle_data_install"
        };
        crate::project_failpoint::hit(boundary, None, None, "REWRITE_DATA", false)?;
    }
    crate::project_failpoint::hit(
        "rewrite.before_generation_authority",
        None,
        None,
        "REWRITE_GENERATION",
        false,
    )?;
    root.revalidate_named().map_err(storage)?;
    let authority = intent
        .entries
        .iter()
        .find(|entry| entry.class == EntryClass::GenerationAuthority)
        .expect("count checked");
    verify_generation_authority(root_path, root, authority, intent.next)?;
    root.revalidate_named().map_err(storage)?;
    install(root_path, root, authority)?;
    crate::project_failpoint::hit(
        "rewrite.after_generation_authority",
        None,
        None,
        "REWRITE_GENERATION",
        false,
    )?;
    remove_journal(root)
}

fn cleanup_preparing_input(
    root_path: &Path,
    root: &StableDirectory,
    entry: &Entry,
) -> Result<(), GfError> {
    let (parent, temporary) = retained_parent_at(root_path, root, &entry.temporary)?;
    let parent_id = parent.identity();
    if (parent_id.volume_serial, hex(&parent_id.file_id))
        != (entry.parent_volume, entry.parent_file.clone())
    {
        return Err(storage("preparing rewrite parent identity changed"));
    }
    let file = match parent.open_child_file(&temporary) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage(error)),
    };
    let identity = graphforge_filesystem::file_identity(&file).map_err(storage)?;
    if (identity.volume_serial, hex(&identity.file_id))
        != (entry.temporary_volume, entry.temporary_file.clone())
    {
        return Err(storage("preparing rewrite temporary identity changed"));
    }
    drop(file);
    parent
        .unlink_child_if_identity(&temporary, identity)
        .map_err(storage)?;
    parent.sync().map_err(storage)
}

fn verify_generation_authority(
    root_path: &Path,
    root: &StableDirectory,
    entry: &Entry,
    next: GenerationPair,
) -> Result<(), GfError> {
    let (parent, target) = retained_parent_at(root_path, root, &entry.destination)?;
    let (_, temporary) = retained_parent_at(root_path, root, &entry.temporary)?;
    let file = match parent.open_child_file(&temporary) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            parent.open_child_file(&target).map_err(storage)?
        }
        Err(error) => return Err(storage(error)),
    };
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes).map_err(storage)?;
    if bytes.len() > 4096 {
        return Err(storage("generation authority exceeds bound"));
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(storage)?;
    if value
        .get("topology_generation")
        .and_then(serde_json::Value::as_u64)
        != Some(next.topology)
        || value
            .get("search_generation")
            .and_then(serde_json::Value::as_u64)
            != Some(next.search)
    {
        return Err(storage(
            "generation authority bytes do not encode journal next state",
        ));
    }
    Ok(())
}

fn validate_intent(intent: &Intent) -> Result<(), GfError> {
    let mut destinations = std::collections::HashSet::new();
    let mut temporaries = std::collections::HashSet::new();
    for entry in &intent.entries {
        if !destinations.insert(&entry.destination) || !temporaries.insert(&entry.temporary) {
            return Err(storage("rewrite journal contains duplicate paths"));
        }
        canonical_journal_path(&entry.destination)?;
        canonical_journal_path(&entry.temporary)?;
        if entry.destination == JOURNAL
            || entry.destination == LOCK
            || entry.temporary == JOURNAL
            || entry.temporary == LOCK
        {
            return Err(storage("rewrite journal targets a reserved control"));
        }
    }
    let authority = intent
        .entries
        .iter()
        .filter(|entry| entry.class == EntryClass::GenerationAuthority)
        .collect::<Vec<_>>();
    if authority.len() != 1 || authority[0].destination != "topology/generation.json" {
        return Err(storage("rewrite journal has invalid generation authority"));
    }
    if intent.next.topology < intent.prior.topology
        || intent.next.search < intent.prior.search
        || intent.next.topology > intent.prior.topology.saturating_add(1)
        || intent.next.search > intent.prior.search.saturating_add(1)
    {
        return Err(storage("rewrite generation transition is not monotonic"));
    }
    if let Some(receipt) = &intent.auxiliary {
        let entry = intent
            .entries
            .iter()
            .find(|entry| entry.destination == receipt.path)
            .ok_or_else(|| storage("auxiliary receipt path is not staged"))?;
        if entry.class != EntryClass::Data
            || entry.sha256 != receipt.digest
            || entry.bytes != receipt.bytes
            || receipt.kind.is_empty()
            || receipt.schema_version == 0
        {
            return Err(storage(
                "auxiliary receipt digest is not bound to staged bytes",
            ));
        }
    }
    Ok(())
}

fn canonical_journal_path(value: &str) -> Result<(), GfError> {
    let path = Path::new(value);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(storage("rewrite journal contains non-canonical path"));
    }
    Ok(())
}

pub(crate) fn recover(root: &Path) -> Result<(), GfError> {
    let guard = acquire(root)?;
    let current = crate::generation::read_generation_state_raw(root)?;
    recover_locked(
        guard.admission.root(),
        &guard.directory,
        GenerationPair {
            topology: current.topology,
            search: current.search,
        },
    )
}

pub(crate) fn recovery_required(root: &Path) -> Result<bool, GfError> {
    match std::fs::symlink_metadata(root.join(JOURNAL)) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(storage(format!("rewrite journal probe failed: {error}"))),
    }
}

#[cfg(test)]
static FAIL_AFTER_DURABLE_INTENT: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Execute bounded maintenance under the same recovered project rewrite lock.
pub(crate) fn with_rewrite_lock<T>(
    root: &Path,
    operation: impl FnOnce(&StableDirectory) -> Result<T, GfError>,
) -> Result<T, GfError> {
    let guard = acquire(root)?;
    guard.revalidate()?;
    let current = crate::generation::read_generation_state_raw(root)?;
    recover_locked(
        root,
        &guard.directory,
        GenerationPair {
            topology: current.topology,
            search: current.search,
        },
    )?;
    guard.revalidate()?;
    let output = operation(&guard.directory)?;
    guard.revalidate()?;
    Ok(output)
}

#[allow(clippy::too_many_lines)]
pub(crate) fn commit(
    batch: RewriteBatch,
    root: &Path,
    bump_topology: bool,
    bump_search: bool,
    auxiliary: Option<AuxiliaryReceipt>,
) -> Result<GenerationPair, GfError> {
    commit_with_participant(batch, root, bump_topology, bump_search, auxiliary, None)
}

#[allow(clippy::too_many_lines)] // Linear crash-barrier state machine; splitting obscures order.
pub(crate) fn commit_with_participant(
    mut batch: RewriteBatch,
    root: &Path,
    bump_topology: bool,
    bump_search: bool,
    auxiliary: Option<AuxiliaryReceipt>,
    participant: Option<RewriteParticipantPreparer<'_>>,
) -> Result<GenerationPair, GfError> {
    let has_participant = participant.is_some();
    let guard = acquire(root)?;
    guard.revalidate()?;
    let prior_state = crate::generation::read_generation_state_raw(root)?;
    let prior = GenerationPair {
        topology: prior_state.topology,
        search: prior_state.search,
    };
    recover_locked(root, &guard.directory, prior)?;
    let recovered = crate::generation::read_generation_state_raw(root)?;
    let prior = GenerationPair {
        topology: recovered.topology,
        search: recovered.search,
    };
    if batch.is_empty() && !bump_topology && !bump_search && auxiliary.is_none() {
        return Ok(prior);
    }
    let next = GenerationPair {
        topology: if bump_topology {
            prior
                .topology
                .checked_add(1)
                .ok_or_else(|| storage("topology generation counter overflow"))?
        } else {
            prior.topology
        },
        search: if bump_search {
            prior
                .search
                .checked_add(1)
                .ok_or_else(|| storage("search generation counter overflow"))?
        } else {
            prior.search
        },
    };
    let participant_baseline = batch
        .staged_paths()
        .map(Path::to_path_buf)
        .collect::<std::collections::BTreeSet<_>>();
    let participant_auxiliary = if let Some(prepare) = participant {
        prepare(
            ParticipantPreparationContext {
                prior,
                next,
                project: &guard.directory,
                project_root: root,
            },
            &mut batch,
        )?
    } else {
        None
    };
    if auxiliary.is_some() && participant_auxiliary.is_some() {
        return Err(storage("rewrite has multiple auxiliary participants"));
    }
    let auxiliary = participant_auxiliary.or(auxiliary);
    let reserved = root.join(".graphforge-cache/uuid-membership");
    if has_participant
        && batch
            .staged_paths()
            .filter(|destination| !participant_baseline.contains(*destination))
            .any(|destination| !destination.starts_with(&reserved))
    {
        return Err(storage(
            "rewrite participant staged a destination outside its reserved namespace",
        ));
    }
    let stages_reserved = batch
        .staged_paths()
        .any(|destination| destination.starts_with(&reserved));
    if stages_reserved && !has_participant {
        return Err(storage(
            "uuid-membership namespace requires the sealed rewrite participant",
        ));
    }
    if stages_reserved
        && auxiliary.as_ref().is_none_or(|receipt| {
            receipt.kind != "uuid-membership/v3"
                || receipt.schema_version != 3
                || receipt.path != ".graphforge-cache/uuid-membership/topology-receipt.json"
        })
    {
        return Err(storage(
            "UUID rewrite participant must stage its namespace and exact typed receipt",
        ));
    }
    let generation_bytes = crate::generation::encode_generation_state(next.topology, next.search)?;
    guard.revalidate()?;
    let transaction = Uuid::now_v7().simple().to_string();
    let root_identity = guard.directory.identity();
    let mut entries = Vec::new();
    let mut staged = batch.into_staged();
    let generation_path = root.join("topology/generation.json");
    std::fs::create_dir_all(generation_path.parent().expect("generation has parent"))
        .map_err(storage)?;
    let mut generation = tempfile::Builder::new()
        .prefix("generation.json.")
        .suffix(".tmp")
        .tempfile_in(generation_path.parent().unwrap())
        .map_err(storage)?;
    generation
        .write_all(&generation_bytes)
        .and_then(|()| generation.as_file().sync_all())
        .map_err(storage)?;
    staged.push((generation, generation_path));
    if staged.len() > MAX_ENTRIES {
        return Err(storage("rewrite batch exceeds entry bound"));
    }
    let last = staged.len().saturating_sub(1);
    for (ordinal, (temp, destination)) in staged.iter().enumerate() {
        let relative = canonical_relative(root, destination)?;
        let (parent, target) = retained_parent_at(root, &guard.directory, &relative)?;
        let parent_identity = parent.identity();
        temp.as_file().sync_all().map_err(storage)?;
        let original = graphforge_filesystem::file_identity(temp.as_file()).map_err(storage)?;
        let durable = temp.path().to_path_buf();
        let temp_relative = canonical_relative(root, &durable)?;
        let (temp_parent, temp_name) = retained_parent_at(root, &guard.directory, &temp_relative)?;
        if temp_parent.identity() != parent_identity {
            return Err(storage("rewrite temporary and destination parents differ"));
        }
        let named_temp = temp_parent.open_child_file(&temp_name).map_err(storage)?;
        if graphforge_filesystem::file_identity(&named_temp).map_err(storage)? != original {
            return Err(storage("rewrite temporary identity changed before intent"));
        }
        parent.sync().map_err(storage)?;
        let (bytes, sha256) = hash_reader(temp.as_file().try_clone().map_err(storage)?)?;
        let prior_destination = match parent.open_child_file(&target) {
            Ok(file) => Some(authenticated_file(&file)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(storage(error)),
        };
        entries.push(Entry {
            class: if ordinal == last {
                EntryClass::GenerationAuthority
            } else {
                EntryClass::Data
            },
            destination: relative,
            temporary: temp_relative,
            parent_volume: parent_identity.volume_serial,
            parent_file: hex(&parent_identity.file_id),
            bytes,
            sha256,
            temporary_volume: original.volume_serial,
            temporary_file: hex(&original.file_id),
            prior_destination,
        });
    }
    let mut intent = Intent {
        version: 1,
        state: IntentState::Preparing,
        transaction,
        root_volume: root_identity.volume_serial,
        root_file: hex(&root_identity.file_id),
        prior,
        next,
        auxiliary,
        entries,
        checksum: String::new(),
    };
    validate_intent(&intent)?;
    intent.checksum = checksum(&intent)?;
    crate::project_failpoint::hit(
        "rewrite.before_intent",
        None,
        None,
        "REWRITE_BEFORE_INTENT",
        false,
    )?;
    publish_journal(&guard.directory, &intent)?;
    // Before intent, NamedTempFile owns cleanup. Preparing intent permits
    // identity-safe abort if disarming any handle fails. Only after every temp
    // is intentionally retained do we publish the durable roll-forward state.
    for (index, (temp, _)) in staged.into_iter().enumerate() {
        temp.into_temp_path()
            .keep()
            .map_err(|error| storage(error.error))?;
        if index == 0 {
            crate::project_failpoint::hit(
                "rewrite.after_preparing_disarm",
                None,
                None,
                "REWRITE_PREPARING",
                false,
            )?;
        }
    }
    intent.state = IntentState::Durable;
    intent.checksum = checksum(&intent)?;
    publish_journal(&guard.directory, &intent)?;
    #[cfg(test)]
    if FAIL_AFTER_DURABLE_INTENT.swap(false, std::sync::atomic::Ordering::SeqCst) {
        return Err(storage("injected ordinary error after durable intent"));
    }
    crate::project_failpoint::hit(
        "rewrite.after_durable_intent",
        None,
        None,
        "REWRITE_INTENT",
        false,
    )?;
    // Use the same classified replay path as crash recovery so authority order
    // cannot drift between the initial commit and roll-forward.
    guard.revalidate()?;
    recover_locked(root, &guard.directory, prior)?;
    guard.revalidate()?;
    crate::io_stats::record_rewrite_commit();
    Ok(next)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::TempDir;

    use super::*;

    static DURABLE_INTENT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn entry(destination: &str, class: EntryClass) -> Entry {
        Entry {
            class,
            destination: destination.to_owned(),
            temporary: format!("topology/{destination}.abc.tmp"),
            parent_volume: 1,
            parent_file: "01".to_owned(),
            bytes: 2,
            sha256: "aa".repeat(32),
            temporary_volume: 1,
            temporary_file: "02".to_owned(),
            prior_destination: None,
        }
    }

    fn intent() -> Intent {
        Intent {
            version: 1,
            state: IntentState::Durable,
            transaction: "tx".to_owned(),
            root_volume: 1,
            root_file: "01".to_owned(),
            prior: GenerationPair {
                topology: 4,
                search: 3,
            },
            next: GenerationPair {
                topology: 5,
                search: 4,
            },
            auxiliary: None,
            entries: vec![
                entry("topology/nodes.parquet", EntryClass::Data),
                entry("topology/generation.json", EntryClass::GenerationAuthority),
            ],
            checksum: String::new(),
        }
    }

    fn leave_durable_intent(root: &Path) -> Intent {
        let _test_guard = DURABLE_INTENT_TEST_LOCK.lock().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![7_i64]))],
        )
        .unwrap();
        let mut rewrite = RewriteBatch::new();
        rewrite
            .stage(&root.join("topology/nodes.parquet"), schema, &batch)
            .unwrap();
        FAIL_AFTER_DURABLE_INTENT.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(commit(rewrite, root, true, true, None).is_err());
        let stable = StableDirectory::open(root).unwrap();
        let (bytes, _) = read_journal(&stable).unwrap().unwrap();
        let value: Intent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value.state, IntentState::Durable);
        value
    }

    fn republish_intent(root: &Path, intent: &mut Intent) {
        intent.checksum = checksum(intent).unwrap();
        publish_journal(&StableDirectory::open(root).unwrap(), intent).unwrap();
    }

    fn assert_recovery_fails_before_authority(root: &Path) {
        assert!(recover(root).is_err());
        assert_eq!(
            crate::generation::read_generation_state_raw(root)
                .unwrap()
                .topology,
            0
        );
    }

    #[test]
    fn intent_validation_rejects_duplicate_reserved_and_nonmonotonic_authority() {
        assert!(validate_intent(&intent()).is_ok());
        let mut duplicate = intent();
        duplicate.entries[0].temporary = duplicate.entries[1].temporary.clone();
        assert!(validate_intent(&duplicate).is_err());
        let mut reserved = intent();
        reserved.entries[0].destination = JOURNAL.to_owned();
        assert!(validate_intent(&reserved).is_err());
        let mut backwards = intent();
        backwards.next.topology = 3;
        assert!(validate_intent(&backwards).is_err());
        let mut wrong_authority = intent();
        wrong_authority.entries[1].destination = "topology/other.json".to_owned();
        assert!(validate_intent(&wrong_authority).is_err());
    }

    #[test]
    fn auxiliary_receipt_must_name_and_digest_an_exact_staged_entry() {
        let mut valid = intent();
        valid.auxiliary = Some(AuxiliaryReceipt {
            kind: "uuid-membership/v3".to_owned(),
            schema_version: 3,
            path: valid.entries[0].destination.clone(),
            digest: valid.entries[0].sha256.clone(),
            bytes: valid.entries[0].bytes,
        });
        assert!(validate_intent(&valid).is_ok());
        valid.auxiliary.as_mut().unwrap().digest = "00".repeat(32);
        assert!(validate_intent(&valid).is_err());
    }

    #[test]
    fn checksum_authenticates_every_recovery_control() {
        let mut value = intent();
        value.checksum = checksum(&value).unwrap();
        assert_eq!(checksum(&value).unwrap(), value.checksum);
        value.entries[0].bytes += 1;
        assert_ne!(checksum(&value).unwrap(), value.checksum);
    }

    #[test]
    fn hostile_root_traversal_and_stale_intents_fail_closed() {
        for mutation in ["root", "traversal", "stale"] {
            let root = TempDir::new().unwrap();
            let mut value = leave_durable_intent(root.path());
            match mutation {
                "root" => value.root_file = "00".repeat(16),
                "traversal" => value.entries[0].destination = "../escape.parquet".to_owned(),
                "stale" => {
                    value.prior = GenerationPair {
                        topology: 7,
                        search: 7,
                    };
                    value.next = GenerationPair {
                        topology: 8,
                        search: 8,
                    };
                }
                _ => unreachable!(),
            }
            republish_intent(root.path(), &mut value);
            assert_recovery_fails_before_authority(root.path());
        }
    }

    #[test]
    fn substituted_or_truncated_temporary_fails_closed() {
        for mutation in ["substitute", "truncate"] {
            let root = TempDir::new().unwrap();
            let value = leave_durable_intent(root.path());
            let data = value
                .entries
                .iter()
                .find(|entry| entry.class == EntryClass::Data)
                .unwrap();
            let temporary = root.path().join(&data.temporary);
            let bytes = std::fs::read(&temporary).unwrap();
            if mutation == "substitute" {
                let replacement = temporary.with_extension("replacement");
                std::fs::write(&replacement, bytes).unwrap();
                std::fs::remove_file(&temporary).unwrap();
                std::fs::rename(replacement, &temporary).unwrap();
            } else {
                std::fs::write(&temporary, &bytes[..bytes.len() / 2]).unwrap();
            }
            assert_recovery_fails_before_authority(root.path());
        }
    }

    #[test]
    fn byte_identical_final_substitution_fails_closed() {
        let root = TempDir::new().unwrap();
        let value = leave_durable_intent(root.path());
        let data = value
            .entries
            .iter()
            .find(|entry| entry.class == EntryClass::Data)
            .unwrap();
        let stable = StableDirectory::open(root.path()).unwrap();
        install(root.path(), &stable, data).unwrap();
        let destination = root.path().join(&data.destination);
        let bytes = std::fs::read(&destination).unwrap();
        let replacement = destination.with_extension("replacement");
        std::fs::write(&replacement, bytes).unwrap();
        std::fs::remove_file(&destination).unwrap();
        std::fs::rename(replacement, &destination).unwrap();
        assert_recovery_fails_before_authority(root.path());
    }

    #[test]
    fn parent_substitution_and_cross_root_temp_move_fail_closed() {
        let root = TempDir::new().unwrap();
        let _value = leave_durable_intent(root.path());
        std::fs::rename(
            root.path().join("topology"),
            root.path().join("old-topology"),
        )
        .unwrap();
        std::fs::create_dir(root.path().join("topology")).unwrap();
        assert_recovery_fails_before_authority(root.path());

        let root = TempDir::new().unwrap();
        let value = leave_durable_intent(root.path());
        let data = value
            .entries
            .iter()
            .find(|entry| entry.class == EntryClass::Data)
            .unwrap();
        let outside = TempDir::new().unwrap();
        std::fs::rename(
            root.path().join(&data.temporary),
            outside.path().join("moved-input"),
        )
        .unwrap();
        assert_recovery_fails_before_authority(root.path());
    }

    #[test]
    fn ordinary_error_after_durable_intent_rolls_forward_on_double_reopen() {
        let root = TempDir::new().unwrap();
        let destination = root.path().join("topology/nodes.parquet");
        let _intent = leave_durable_intent(root.path());
        assert!(root.path().join(JOURNAL).is_file());
        assert_eq!(
            crate::generation::read_topology_generation(root.path()).unwrap(),
            1
        );
        assert_eq!(
            crate::generation::read_topology_generation(root.path()).unwrap(),
            1
        );
        assert!(destination.is_file());
        assert!(!root.path().join(JOURNAL).exists());
    }

    #[test]
    fn subprocess_crash_matrix_preserves_generation_last_and_reopens_idempotently() {
        const CHILD_ROOT: &str = "GRAPHFORGE_REWRITE_CHILD_ROOT";
        let destinations = [
            "topology/nodes.parquet",
            "properties/Person.parquet",
            "edge_properties/KNOWS.parquet",
        ];
        if let Ok(root) = std::env::var(CHILD_ROOT) {
            let root = Path::new(&root);
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
            let mut rewrite = RewriteBatch::new();
            for (index, relative) in destinations.iter().enumerate() {
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(vec![
                        i64::try_from(index).unwrap() + 10,
                    ]))],
                )
                .unwrap();
                rewrite
                    .stage(&root.join(relative), Arc::clone(&schema), &batch)
                    .unwrap();
            }
            let _ = commit(rewrite, root, true, true, None);
            panic!("child failpoint did not terminate the process");
        }

        let phases = [
            ("rewrite.before_intent", false),
            ("rewrite.after_preparing_disarm", false),
            ("rewrite.after_durable_intent", true),
            ("rewrite.after_first_data_install", true),
            ("rewrite.after_middle_data_install", true),
            ("rewrite.after_last_data_install", true),
            ("rewrite.before_generation_authority", true),
            ("rewrite.after_generation_authority", true),
        ];
        for (phase, committed) in phases {
            let root = TempDir::new().unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("durable_rewrite::tests::subprocess_crash_matrix_preserves_generation_last_and_reopens_idempotently")
                .arg("--nocapture")
                .env(CHILD_ROOT, root.path())
                .env("GRAPHFORGE_PROJECT_FAILPOINTS", "graphforge-internal-subprocess-v1")
                .env("GRAPHFORGE_PROJECT_FAILPOINT", phase)
                .status()
                .unwrap();
            assert_eq!(
                status.code(),
                Some(crate::project_failpoint::exit_code()),
                "{phase}"
            );

            let first = crate::generation::read_topology_generation(root.path()).unwrap();
            crate::staging::remove_stale_temps(root.path()).unwrap();
            let second = crate::generation::read_topology_generation(root.path()).unwrap();
            assert_eq!(
                (first, second),
                if committed { (1, 1) } else { (0, 0) },
                "{phase}"
            );
            for (index, relative) in destinations.iter().enumerate() {
                let path = root.path().join(relative);
                assert_eq!(path.exists(), committed, "{phase}: {relative}");
                if committed {
                    let mut reader =
                        ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
                            .unwrap()
                            .build()
                            .unwrap();
                    let batch = reader.next().unwrap().unwrap();
                    let values = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert_eq!(
                        values.value(0),
                        i64::try_from(index).unwrap() + 10,
                        "{phase}: {relative}"
                    );
                    assert!(reader.next().is_none(), "{phase}: duplicate payload");
                }
            }
            assert!(!root.path().join(JOURNAL).exists(), "{phase}");
            for relative in ["topology", "properties", "edge_properties"] {
                assert!(
                    std::fs::read_dir(root.path().join(relative))
                        .unwrap()
                        .filter_map(Result::ok)
                        .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp")),
                    "{phase}: temp cleanup"
                );
            }
        }
    }
}
