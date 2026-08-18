//! Deterministic persistent-filesystem fault oracle (#749).
//!
//! Models file writes, file flushes, directory flushes, atomic replacement,
//! process death, and restart against the ADR 0018 acknowledgement boundary.
//! Native POSIX/Windows subprocess failpoint matrices remain authoritative for
//! real API/handle behavior; this oracle certifies torn bytes, lost flushes,
//! and power-loss persistence subsets that process kill cannot express.
//!
//! Reusable by recovery (#750), delta (#752), compaction (#753), and final
//! certification (#756) under the `test-failpoints` feature.

#![cfg(any(test, feature = "test-failpoints"))]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use graphforge_core::{GfError, ProjectErrorCode};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::project_generation::{
    CURRENT_FILE, FORMAT_FILE, PROJECT_FORMAT_BYTES, ResolvedProjectGeneration,
    resolve_project_generation,
};
use crate::project_publication::GENERATIONS_DIR;

const MANIFEST_FILE: &str = "manifest.json";
const PARTICIPANTS_DIR: &str = "participants";
const LEASE_FILE: &str = "lease.lock";

/// ADR 0018 / publication vocabulary phases modeled by the oracle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationPhase {
    /// Participants written; not yet flushed.
    AfterParticipantWrite,
    /// Participant file bytes flushed.
    AfterParticipantFsync,
    /// Participant directory entries flushed.
    AfterParticipantDirFsync,
    /// Manifest bytes written; not flushed.
    AfterManifestWrite,
    /// Manifest file flushed.
    AfterManifestFsync,
    /// Generation directory tree flushed (durable generation).
    AfterGenerationDirFsync,
    /// Advisory durable journal recorded.
    AfterJournalDurable,
    /// Sibling CURRENT temp bytes written.
    AfterCurrentTempWrite,
    /// Sibling CURRENT temp file flushed.
    AfterCurrentTempFsync,
    /// Immediately before atomic CURRENT replace/create.
    BeforeCurrentReplace,
    /// CURRENT replaced (visibility linearization); root dir flush not done.
    AfterCurrentReplace,
    /// Project-root directory flush completed (acknowledgement boundary).
    AfterRootFsync,
    /// Advisory published journal recorded.
    AfterJournalPublished,
}

impl PublicationPhase {
    /// Resolve a native publication failpoint to its modeled ADR phase.
    ///
    /// Lock acquisition, journal preparation, and validation failpoints have no
    /// persistence operation of their own and intentionally return `None`.
    #[must_use]
    pub fn from_failpoint(failpoint: &str) -> Option<Self> {
        Self::all()
            .iter()
            .copied()
            .find(|phase| phase.failpoint() == failpoint)
    }

    /// Named failpoint cookie shared with native subprocess matrices.
    #[must_use]
    pub const fn failpoint(self) -> &'static str {
        match self {
            Self::AfterParticipantWrite => "project.after_participant_write",
            Self::AfterParticipantFsync => "project.after_participant_fsync",
            Self::AfterParticipantDirFsync => "project.after_participant_dir_fsync",
            Self::AfterManifestWrite => "project.after_manifest_write",
            Self::AfterManifestFsync => "project.after_manifest_fsync",
            Self::AfterGenerationDirFsync => "project.after_generation_dir_fsync",
            Self::AfterJournalDurable => "project.after_journal_durable",
            Self::AfterCurrentTempWrite => "project.after_current_temp_write",
            Self::AfterCurrentTempFsync => "project.after_current_temp_fsync",
            Self::BeforeCurrentReplace => "project.before_current_replace",
            Self::AfterCurrentReplace => "project.after_current_replace",
            Self::AfterRootFsync => "project.after_root_fsync",
            Self::AfterJournalPublished => "project.after_journal_published",
        }
    }

    /// Every modeled publication phase in protocol order.
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::AfterParticipantWrite,
            Self::AfterParticipantFsync,
            Self::AfterParticipantDirFsync,
            Self::AfterManifestWrite,
            Self::AfterManifestFsync,
            Self::AfterGenerationDirFsync,
            Self::AfterJournalDurable,
            Self::AfterCurrentTempWrite,
            Self::AfterCurrentTempFsync,
            Self::BeforeCurrentReplace,
            Self::AfterCurrentReplace,
            Self::AfterRootFsync,
            Self::AfterJournalPublished,
        ]
    }

    /// Whether acknowledgement has completed at this phase.
    #[must_use]
    pub const fn is_acknowledged(self) -> bool {
        matches!(self, Self::AfterRootFsync | Self::AfterJournalPublished)
    }

    /// Whether CURRENT replacement (visibility linearization) has completed.
    #[must_use]
    pub const fn is_linearized(self) -> bool {
        matches!(
            self,
            Self::AfterCurrentReplace | Self::AfterRootFsync | Self::AfterJournalPublished
        )
    }
}

/// Authority class selected after reopen / simulated recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityClass {
    /// Exact prior complete generation remains authoritative.
    PriorGeneration,
    /// Fully durable new generation is authoritative.
    NewGeneration,
    /// Fail-closed `GF_PROJECT_CORRUPT` without electing newest-by-scan.
    Corrupt,
    /// Resolution failed for a reason other than project corruption.
    Unexpected,
}

/// Native durability primitive whose successful completion defines acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityProfile {
    /// Local POSIX filesystems admitted for file plus directory `fsync`.
    PosixDirectoryFsync,
    /// Fixed, writable local NTFS using the ADR 0020 write-through handle rename.
    WindowsNtfsWriteThrough,
}

impl DurabilityProfile {
    /// Profile for the current native test target.
    #[must_use]
    pub const fn native() -> Self {
        if cfg!(windows) {
            Self::WindowsNtfsWriteThrough
        } else {
            Self::PosixDirectoryFsync
        }
    }

    /// Map #776's stable admitted filesystem class into oracle semantics.
    #[must_use]
    pub fn for_admitted_filesystem_class(filesystem_class: &str) -> Option<Self> {
        match filesystem_class {
            "ntfs" => Some(Self::WindowsNtfsWriteThrough),
            "apfs" | "ext" | "ext2" | "ext3" | "ext4" | "xfs" | "btrfs" => {
                Some(Self::PosixDirectoryFsync)
            }
            _ => None,
        }
    }
}

/// Explicit injected result for an operation that did not acknowledge success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InjectedOperationResult {
    /// A required file-content flush returned an error.
    FileFlushError,
    /// The platform-native namespace barrier returned an error.
    NamespaceBarrierError,
    /// Replacement reported a definite error before changing authority.
    ReplacementNotPerformed,
    /// Replacement reported an error with prior authority after reconciliation.
    ReplacementStateUnknownPrior,
    /// Replacement reported an error with new authority after reconciliation.
    ReplacementStateUnknownNew,
    /// Durable bytes were torn or truncated before acknowledgement.
    TornBytes,
}

/// One recorded persistence-relevant operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersistenceOp {
    /// Monotonic id within one history.
    pub id: u64,
    /// Protocol phase that emitted the op.
    pub phase: PublicationPhase,
    /// Operation kind.
    pub kind: PersistenceOpKind,
}

/// Persistence-relevant operation kinds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceOpKind {
    /// Open the staging handle retained through replacement reconciliation.
    OpenHandle {
        /// Stable trace-local handle name.
        handle: String,
        /// Project-relative staging path.
        path: String,
        /// Whether the native handle has write-through semantics.
        write_through: bool,
    },
    /// Close a previously opened staging handle.
    CloseHandle {
        /// Stable trace-local handle name.
        handle: String,
    },
    /// Create or overwrite file bytes in the volatile view.
    WriteFile {
        /// Project-relative file path.
        path: String,
        /// Exact bytes written into the volatile view.
        bytes: Vec<u8>,
    },
    /// Promote file volatile bytes to durable media.
    FsyncFile {
        /// Project-relative file path.
        path: String,
    },
    /// Create a directory in the volatile view.
    MkDir {
        /// Project-relative directory path.
        path: String,
    },
    /// Atomically install `bytes` at `path` via a flushed sibling then rename.
    AtomicReplace {
        /// Project-relative destination path.
        path: String,
        /// Exact bytes installed by the replacement.
        bytes: Vec<u8>,
        /// Staging handle retained through the replacement call.
        handle: String,
    },
    /// Flush directory entries for `path`.
    FsyncDir {
        /// Project-relative directory path.
        path: String,
    },
    /// Inject torn/truncated durable bytes (media fault).
    TearFile {
        /// Project-relative file path.
        path: String,
        /// Truncated or corrupt durable bytes.
        bytes: Vec<u8>,
    },
}

/// Expected versus actual reopen outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PhaseOutcome {
    /// Crash/injection phase.
    pub phase: PublicationPhase,
    /// Failpoint name shared with native matrices.
    pub failpoint: &'static str,
    /// Whether acknowledgement completed before the fault.
    pub acknowledged: bool,
    /// Modeled authority class.
    pub expected: AuthorityClass,
    /// Authority observed after materializing durable media and resolving.
    pub actual: AuthorityClass,
    /// Selected generation when resolution succeeded.
    pub selected_generation: Option<Uuid>,
}

/// Reproducible artifact for a simulated fault run (safe fields only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FaultOracleReport {
    /// Seed that produced the history / subset.
    pub seed: u64,
    /// Crash phase.
    pub phase: PublicationPhase,
    /// Failpoint name.
    pub failpoint: &'static str,
    /// Platform durability primitive used by this history.
    pub profile: DurabilityProfile,
    /// Whether every required operation completed successfully before death.
    pub acknowledged: bool,
    /// Injected non-success outcome, when applicable.
    pub injected_result: Option<InjectedOperationResult>,
    /// Persistence op ids that were made durable before crash.
    pub durable_op_ids: Vec<u64>,
    /// Expected authority class.
    pub expected: AuthorityClass,
    /// Actual authority class.
    pub actual: AuthorityClass,
    /// Minimized failing op-id set when shrinking applied.
    pub minimized_op_ids: Option<Vec<u64>>,
    /// Human-readable operation trace (paths are project-relative only).
    pub operation_trace: Vec<String>,
}

/// Fixed identities for one publication history under the oracle.
#[derive(Debug, Clone, Copy)]
pub struct PublicationIds {
    /// Parent generation already acknowledged on durable media.
    pub parent_generation: Uuid,
    /// New generation being published.
    pub new_generation: Uuid,
    /// Transaction identity (advisory journals only).
    pub transaction: Uuid,
}

impl PublicationIds {
    /// Deterministic UUIDs derived from `seed` (no clocks).
    #[must_use]
    pub fn from_seed(seed: u64) -> Self {
        Self {
            parent_generation: uuid_from_seed(seed, 1),
            new_generation: uuid_from_seed(seed, 2),
            transaction: uuid_from_seed(seed, 3),
        }
    }
}

fn uuid_from_seed(seed: u64, lane: u64) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    bytes[8..].copy_from_slice(&lane.to_le_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[derive(Serialize)]
struct CurrentRecord {
    format: String,
    format_version: u32,
    generation_uuid: String,
    generation_manifest_sha256: String,
}

#[derive(Serialize)]
struct ManifestRecord {
    format: String,
    format_version: u32,
    generation_uuid: String,
    parent_generation_uuid: Option<String>,
    transaction_uuid: String,
    capabilities: Vec<CapabilityRecord>,
    participants: Vec<ParticipantRecord>,
}

#[derive(Serialize)]
struct CapabilityRecord {
    capability_id: String,
    capability_version: u32,
}

#[derive(Serialize)]
struct ParticipantRecord {
    capability_id: String,
    capability_version: u32,
    record_family_id: String,
    record_version: u32,
    relative_path: String,
    encoding: String,
    byte_length: u64,
    row_count: u64,
    schema_fingerprint: String,
    content_sha256: String,
}

fn canonical_json_line<T: Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).expect("oracle fixture json");
    bytes.push(b'\n');
    bytes
}

/// In-memory durable + volatile project media.
#[derive(Debug, Clone, Default)]
struct Media {
    durable_files: BTreeMap<String, Vec<u8>>,
    volatile_files: BTreeMap<String, Vec<u8>>,
    durable_dirs: BTreeMap<String, BTreeSet<String>>,
    volatile_dirs: BTreeMap<String, BTreeSet<String>>,
    pending_replacements: BTreeMap<String, Vec<u8>>,
    open_handles: BTreeMap<String, (String, bool)>,
}

impl Media {
    fn ensure_dir_volatile(&mut self, path: &str) {
        self.volatile_dirs.entry(path.to_owned()).or_default();
        if path != "." {
            let parent = parent_path(path);
            self.link_volatile(&parent, file_name(path));
        }
    }

    fn link_volatile(&mut self, dir: &str, name: &str) {
        self.volatile_dirs
            .entry(dir.to_owned())
            .or_default()
            .insert(name.to_owned());
    }

    fn unlink_volatile(&mut self, dir: &str, name: &str) {
        if let Some(children) = self.volatile_dirs.get_mut(dir) {
            children.remove(name);
        }
    }

    fn write_file(&mut self, path: &str, bytes: Vec<u8>) {
        let parent = parent_path(path);
        self.ensure_dir_volatile(&parent);
        self.link_volatile(&parent, file_name(path));
        self.volatile_files.insert(path.to_owned(), bytes);
    }

    fn fsync_file(&mut self, path: &str) {
        if let Some(bytes) = self.volatile_files.get(path).cloned() {
            self.durable_files.insert(path.to_owned(), bytes);
        }
    }

    fn mkdir(&mut self, path: &str) {
        self.ensure_dir_volatile(path);
    }

    fn open_handle(&mut self, handle: &str, path: &str, write_through: bool) {
        assert!(
            self.open_handles
                .insert(handle.to_owned(), (path.to_owned(), write_through))
                .is_none(),
            "oracle handle names are unique"
        );
    }

    fn close_handle(&mut self, handle: &str) {
        assert!(
            self.open_handles.remove(handle).is_some(),
            "oracle closes only an open handle"
        );
    }

    fn atomic_replace(
        &mut self,
        path: &str,
        bytes: Vec<u8>,
        handle: &str,
        profile: DurabilityProfile,
    ) {
        let (_, write_through) = self
            .open_handles
            .get(handle)
            .expect("atomic replacement retains its staging handle");
        if profile == DurabilityProfile::WindowsNtfsWriteThrough {
            assert!(
                *write_through,
                "NTFS replacement requires a write-through staging handle"
            );
        }
        let parent = parent_path(path);
        let temp = format!("{parent}/.oracle-tmp-{}", file_name(path));
        self.write_file(&temp, bytes.clone());
        self.fsync_file(&temp);
        let durable_bytes = self
            .durable_files
            .remove(&temp)
            .expect("atomic replacement temp was flushed");
        self.volatile_files.remove(&temp);
        self.unlink_volatile(&parent, file_name(&temp));
        // The renamed file's bytes are durable, but the new name remains only
        // visible until either the rename itself persists or the platform's
        // later namespace barrier completes.
        self.pending_replacements
            .insert(path.to_owned(), durable_bytes);
        self.write_file(path, bytes);
    }

    fn persist_replacement(&mut self, path: &str) {
        if let Some(bytes) = self.pending_replacements.remove(path) {
            self.durable_files.insert(path.to_owned(), bytes);
            self.link_durable_only(&parent_path(path), file_name(path));
        }
    }

    fn fsync_dir(&mut self, path: &str) {
        let children = self.volatile_dirs.get(path).cloned().unwrap_or_default();
        self.durable_dirs.insert(path.to_owned(), children.clone());
        // A namespace barrier persists directory edges and completed renames;
        // it never substitutes for a file-content flush.
        for name in &children {
            let child = if path == "." {
                name.clone()
            } else {
                format!("{path}/{name}")
            };
            self.persist_replacement(&child);
        }
    }

    fn link_durable_only(&mut self, dir: &str, name: &str) {
        self.durable_dirs
            .entry(dir.to_owned())
            .or_default()
            .insert(name.to_owned());
    }

    fn tear_file(&mut self, path: &str, bytes: Vec<u8>) {
        self.durable_files.insert(path.to_owned(), bytes.clone());
        self.volatile_files.insert(path.to_owned(), bytes);
        let parent = parent_path(path);
        self.ensure_dir_volatile(&parent);
        self.link_volatile(&parent, file_name(path));
        self.link_durable_only(&parent, file_name(path));
    }

    fn crash(&mut self) {
        // Retain only files named by durable directory entries.
        let mut kept_files = BTreeMap::new();
        for (dir, children) in &self.durable_dirs {
            for name in children {
                let path = if dir == "." {
                    name.clone()
                } else {
                    format!("{dir}/{name}")
                };
                if let Some(bytes) = self.durable_files.get(&path) {
                    kept_files.insert(path, bytes.clone());
                }
            }
        }
        self.durable_files = kept_files;
        self.volatile_files = self.durable_files.clone();
        self.volatile_dirs = self.durable_dirs.clone();
        self.pending_replacements.clear();
        self.open_handles.clear();
    }

    fn apply_subset(
        &mut self,
        ops: &[PersistenceOp],
        durable_ids: &BTreeSet<u64>,
        profile: DurabilityProfile,
    ) {
        let mut scratch = Self {
            durable_files: self.durable_files.clone(),
            durable_dirs: self.durable_dirs.clone(),
            volatile_files: self.durable_files.clone(),
            volatile_dirs: self.durable_dirs.clone(),
            pending_replacements: BTreeMap::new(),
            open_handles: BTreeMap::new(),
        };

        for op in ops {
            match &op.kind {
                PersistenceOpKind::OpenHandle {
                    handle,
                    path,
                    write_through,
                } => scratch.open_handle(handle, path, *write_through),
                PersistenceOpKind::CloseHandle { handle } => scratch.close_handle(handle),
                PersistenceOpKind::WriteFile { path, bytes } => {
                    scratch.write_file(path, bytes.clone());
                    if durable_ids.contains(&op.id) {
                        scratch.fsync_file(path);
                    }
                }
                PersistenceOpKind::FsyncFile { path } => {
                    if durable_ids.contains(&op.id) {
                        scratch.fsync_file(path);
                    }
                }
                PersistenceOpKind::MkDir { path } => {
                    scratch.mkdir(path);
                    if durable_ids.contains(&op.id) {
                        // Persist only this directory's creation edge, not every
                        // sibling name under the parent.
                        scratch.fsync_dir(path);
                        scratch.link_durable_only(&parent_path(path), file_name(path));
                    }
                }
                PersistenceOpKind::AtomicReplace {
                    path,
                    bytes,
                    handle,
                } => {
                    scratch.atomic_replace(path, bytes.clone(), handle, profile);
                    if durable_ids.contains(&op.id) {
                        scratch.persist_replacement(path);
                    }
                }
                PersistenceOpKind::FsyncDir { path } => {
                    if durable_ids.contains(&op.id) {
                        scratch.fsync_dir(path);
                    }
                }
                PersistenceOpKind::TearFile { path, bytes } => {
                    if durable_ids.contains(&op.id) {
                        scratch.tear_file(path, bytes.clone());
                    }
                }
            }
        }
        scratch.crash();
        *self = scratch;
    }
}

fn trace_op(op: &PersistenceOp) -> String {
    let kind = match &op.kind {
        PersistenceOpKind::OpenHandle {
            handle,
            path,
            write_through,
        } => format!("open_handle handle={handle} path={path} write_through={write_through}"),
        PersistenceOpKind::CloseHandle { handle } => {
            format!("close_handle handle={handle}")
        }
        PersistenceOpKind::WriteFile { path, bytes } => {
            format!("write_file path={path} len={}", bytes.len())
        }
        PersistenceOpKind::FsyncFile { path } => format!("fsync_file path={path}"),
        PersistenceOpKind::MkDir { path } => format!("mkdir path={path}"),
        PersistenceOpKind::AtomicReplace {
            path,
            bytes,
            handle,
        } => {
            format!(
                "atomic_replace path={path} handle={handle} len={}",
                bytes.len()
            )
        }
        PersistenceOpKind::FsyncDir { path } => format!("fsync_dir path={path}"),
        PersistenceOpKind::TearFile { path, bytes } => {
            format!("tear_file path={path} len={}", bytes.len())
        }
    };
    format!("id={} phase={} {kind}", op.id, op.phase.failpoint())
}

fn parent_path(path: &str) -> String {
    match path.rsplit_once('/') {
        Some((parent, _)) if !parent.is_empty() => parent.to_owned(),
        _ => ".".into(),
    }
}

fn file_name(path: &str) -> &str {
    path.rsplit_once('/').map_or(path, |(_, name)| name)
}

fn hex_digest(bytes: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("String write");
    }
    output
}

fn parent_manifest_bytes(ids: PublicationIds) -> Vec<u8> {
    canonical_json_line(&ManifestRecord {
        format: "graphforge-generation".into(),
        format_version: 1,
        generation_uuid: ids.parent_generation.hyphenated().to_string(),
        parent_generation_uuid: None,
        transaction_uuid: uuid_from_seed(0, 9).hyphenated().to_string(),
        capabilities: vec![CapabilityRecord {
            capability_id: "graph".into(),
            capability_version: 1,
        }],
        participants: vec![],
    })
}

fn child_manifest_bytes(ids: PublicationIds, participant_bytes: &[u8]) -> Vec<u8> {
    let digest = hex_digest(Sha256::digest(participant_bytes).into());
    let schema = hex_digest(Sha256::digest(b"graph/nodes").into());
    canonical_json_line(&ManifestRecord {
        format: "graphforge-generation".into(),
        format_version: 1,
        generation_uuid: ids.new_generation.hyphenated().to_string(),
        parent_generation_uuid: Some(ids.parent_generation.hyphenated().to_string()),
        transaction_uuid: ids.transaction.hyphenated().to_string(),
        capabilities: vec![CapabilityRecord {
            capability_id: "graph".into(),
            capability_version: 1,
        }],
        participants: vec![ParticipantRecord {
            capability_id: "graph".into(),
            capability_version: 1,
            record_family_id: "nodes".into(),
            record_version: 1,
            relative_path: "graph/nodes.parquet".into(),
            encoding: "parquet".into(),
            byte_length: participant_bytes.len() as u64,
            row_count: 1,
            schema_fingerprint: schema,
            content_sha256: digest,
        }],
    })
}

fn current_bytes_for(generation: Uuid, manifest_bytes: &[u8]) -> Vec<u8> {
    canonical_json_line(&CurrentRecord {
        format: "graphforge-project".into(),
        format_version: 1,
        generation_uuid: generation.hyphenated().to_string(),
        generation_manifest_sha256: hex_digest(Sha256::digest(manifest_bytes).into()),
    })
}

/// Build the persistence operation history for publishing `ids` up through `until`.
#[must_use]
#[allow(clippy::too_many_lines)] // Phase-ordered durability script mirrors ADR vocabulary.
pub fn publication_ops(ids: PublicationIds, until: PublicationPhase) -> Vec<PersistenceOp> {
    publication_ops_for_profile(ids, until, DurabilityProfile::PosixDirectoryFsync)
}

/// Build a publication history using one admitted platform durability profile.
#[must_use]
#[allow(clippy::too_many_lines)] // Phase-ordered durability script mirrors ADR vocabulary.
pub fn publication_ops_for_profile(
    ids: PublicationIds,
    until: PublicationPhase,
    profile: DurabilityProfile,
) -> Vec<PersistenceOp> {
    let mut ops = Vec::new();
    let mut next_id = 1u64;
    let mut push = |phase: PublicationPhase, kind: PersistenceOpKind| {
        if phase <= until {
            ops.push(PersistenceOp {
                id: next_id,
                phase,
                kind,
            });
            next_id += 1;
        }
    };

    let neu = ids.new_generation.hyphenated().to_string();
    let participant_rel = format!("{GENERATIONS_DIR}/{neu}/{PARTICIPANTS_DIR}/graph/nodes.parquet");
    let participant_dir = format!("{GENERATIONS_DIR}/{neu}/{PARTICIPANTS_DIR}/graph");
    let participants_root = format!("{GENERATIONS_DIR}/{neu}/{PARTICIPANTS_DIR}");
    let generation_root = format!("{GENERATIONS_DIR}/{neu}");
    let manifest_path = format!("{GENERATIONS_DIR}/{neu}/{MANIFEST_FILE}");
    let lease_path = format!("{GENERATIONS_DIR}/{neu}/{LEASE_FILE}");
    let journal_path = format!("transactions/{}.json", ids.transaction.hyphenated());
    let participant_bytes = b"graph:nodes".to_vec();
    let write_through = profile == DurabilityProfile::WindowsNtfsWriteThrough;

    push(
        PublicationPhase::AfterParticipantWrite,
        PersistenceOpKind::MkDir {
            path: generation_root.clone(),
        },
    );
    push(
        PublicationPhase::AfterParticipantWrite,
        PersistenceOpKind::MkDir {
            path: participants_root.clone(),
        },
    );
    push(
        PublicationPhase::AfterParticipantWrite,
        PersistenceOpKind::MkDir {
            path: participant_dir.clone(),
        },
    );
    push(
        PublicationPhase::AfterParticipantWrite,
        PersistenceOpKind::WriteFile {
            path: lease_path.clone(),
            bytes: Vec::new(),
        },
    );
    push(
        PublicationPhase::AfterParticipantWrite,
        PersistenceOpKind::WriteFile {
            path: participant_rel.clone(),
            bytes: participant_bytes.clone(),
        },
    );
    push(
        PublicationPhase::AfterParticipantFsync,
        PersistenceOpKind::FsyncFile { path: lease_path },
    );
    push(
        PublicationPhase::AfterParticipantFsync,
        PersistenceOpKind::FsyncFile {
            path: participant_rel,
        },
    );
    push(
        PublicationPhase::AfterParticipantDirFsync,
        PersistenceOpKind::FsyncDir {
            path: participant_dir,
        },
    );
    push(
        PublicationPhase::AfterParticipantDirFsync,
        PersistenceOpKind::FsyncDir {
            path: participants_root,
        },
    );

    let manifest_bytes = child_manifest_bytes(ids, &participant_bytes);
    push(
        PublicationPhase::AfterManifestWrite,
        PersistenceOpKind::WriteFile {
            path: manifest_path.clone(),
            bytes: manifest_bytes.clone(),
        },
    );
    push(
        PublicationPhase::AfterManifestFsync,
        PersistenceOpKind::FsyncFile {
            path: manifest_path,
        },
    );
    push(
        PublicationPhase::AfterGenerationDirFsync,
        PersistenceOpKind::FsyncDir {
            path: generation_root,
        },
    );
    push(
        PublicationPhase::AfterGenerationDirFsync,
        PersistenceOpKind::FsyncDir {
            path: GENERATIONS_DIR.to_owned(),
        },
    );
    push(
        PublicationPhase::AfterJournalDurable,
        PersistenceOpKind::MkDir {
            path: "transactions".into(),
        },
    );
    push(
        PublicationPhase::AfterJournalDurable,
        PersistenceOpKind::OpenHandle {
            handle: "journal_durable_stage".into(),
            path: "transactions/.journal-durable.tmp".into(),
            write_through,
        },
    );
    push(
        PublicationPhase::AfterJournalDurable,
        PersistenceOpKind::AtomicReplace {
            path: journal_path.clone(),
            bytes: b"{\"phase\":\"DURABLE\"}\n".to_vec(),
            handle: "journal_durable_stage".into(),
        },
    );
    push(
        PublicationPhase::AfterJournalDurable,
        PersistenceOpKind::CloseHandle {
            handle: "journal_durable_stage".into(),
        },
    );
    push(
        PublicationPhase::AfterJournalDurable,
        PersistenceOpKind::FsyncDir {
            path: "transactions".into(),
        },
    );

    let current_bytes = current_bytes_for(ids.new_generation, &manifest_bytes);
    push(
        PublicationPhase::AfterCurrentTempWrite,
        PersistenceOpKind::OpenHandle {
            handle: "current_stage".into(),
            path: ".CURRENT.tmp".into(),
            write_through,
        },
    );
    push(
        PublicationPhase::AfterCurrentTempWrite,
        PersistenceOpKind::WriteFile {
            path: ".CURRENT.tmp".into(),
            bytes: current_bytes.clone(),
        },
    );
    push(
        PublicationPhase::AfterCurrentTempFsync,
        PersistenceOpKind::FsyncFile {
            path: ".CURRENT.tmp".into(),
        },
    );
    let _ = PublicationPhase::BeforeCurrentReplace;
    push(
        PublicationPhase::AfterCurrentReplace,
        PersistenceOpKind::AtomicReplace {
            path: CURRENT_FILE.into(),
            bytes: current_bytes,
            handle: "current_stage".into(),
        },
    );
    push(
        PublicationPhase::AfterCurrentReplace,
        PersistenceOpKind::CloseHandle {
            handle: "current_stage".into(),
        },
    );
    if profile == DurabilityProfile::PosixDirectoryFsync {
        push(
            PublicationPhase::AfterRootFsync,
            PersistenceOpKind::FsyncDir { path: ".".into() },
        );
    }
    push(
        PublicationPhase::AfterJournalPublished,
        PersistenceOpKind::OpenHandle {
            handle: "journal_published_stage".into(),
            path: "transactions/.journal-published.tmp".into(),
            write_through,
        },
    );
    push(
        PublicationPhase::AfterJournalPublished,
        PersistenceOpKind::AtomicReplace {
            path: journal_path,
            bytes: b"{\"phase\":\"PUBLISHED\"}\n".to_vec(),
            handle: "journal_published_stage".into(),
        },
    );
    push(
        PublicationPhase::AfterJournalPublished,
        PersistenceOpKind::CloseHandle {
            handle: "journal_published_stage".into(),
        },
    );
    push(
        PublicationPhase::AfterJournalPublished,
        PersistenceOpKind::FsyncDir {
            path: "transactions".into(),
        },
    );

    ops
}

fn install_parent_baseline(media: &mut Media, ids: PublicationIds) {
    let parent = ids.parent_generation.hyphenated().to_string();
    let generation_root = format!("{GENERATIONS_DIR}/{parent}");
    let participants_root = format!("{generation_root}/{PARTICIPANTS_DIR}");
    let manifest_path = format!("{generation_root}/{MANIFEST_FILE}");
    let lease_path = format!("{generation_root}/{LEASE_FILE}");
    let manifest = parent_manifest_bytes(ids);
    let current = current_bytes_for(ids.parent_generation, &manifest);

    media.mkdir(".");
    media.fsync_dir(".");
    media.write_file(FORMAT_FILE, PROJECT_FORMAT_BYTES.to_vec());
    media.fsync_file(FORMAT_FILE);
    media.mkdir(GENERATIONS_DIR);
    media.fsync_dir(".");
    media.mkdir(&generation_root);
    media.mkdir(&participants_root);
    media.fsync_dir(GENERATIONS_DIR);
    media.write_file(&lease_path, Vec::new());
    media.fsync_file(&lease_path);
    media.write_file(&manifest_path, manifest);
    media.fsync_file(&manifest_path);
    media.fsync_dir(&participants_root);
    media.fsync_dir(&generation_root);
    media.write_file(CURRENT_FILE, current);
    media.fsync_file(CURRENT_FILE);
    media.fsync_dir(".");
    media.crash();
}

/// Default durable-op set for a crash at `phase`.
#[must_use]
pub fn default_durable_ids(ops: &[PersistenceOp], phase: PublicationPhase) -> BTreeSet<u64> {
    ops.iter()
        .filter(|op| op.phase <= phase)
        .filter(|op| match (&op.kind, phase) {
            (PersistenceOpKind::FsyncDir { path }, PublicationPhase::AfterCurrentReplace)
                if path == "." =>
            {
                false
            }
            _ => matches!(
                op.kind,
                PersistenceOpKind::FsyncFile { .. }
                    | PersistenceOpKind::FsyncDir { .. }
                    | PersistenceOpKind::AtomicReplace { .. }
                    | PersistenceOpKind::MkDir { .. }
                    | PersistenceOpKind::TearFile { .. }
            ),
        })
        .map(|op| op.id)
        .collect()
}

/// Contract expectation for a crash at `phase` under the default durable set.
#[must_use]
pub fn expected_authority(phase: PublicationPhase) -> AuthorityClass {
    if phase.is_linearized() {
        AuthorityClass::NewGeneration
    } else {
        AuthorityClass::PriorGeneration
    }
}

/// Classify resolution of a materialized project root.
#[must_use]
pub fn classify_resolution(
    result: &Result<ResolvedProjectGeneration, GfError>,
    ids: PublicationIds,
) -> AuthorityClass {
    match result {
        Ok(resolved) if resolved.generation_uuid() == ids.new_generation => {
            AuthorityClass::NewGeneration
        }
        Ok(resolved) if resolved.generation_uuid() == ids.parent_generation => {
            AuthorityClass::PriorGeneration
        }
        Err(error) if error.code() == "GF_PROJECT_CORRUPT" => AuthorityClass::Corrupt,
        Ok(_) | Err(_) => AuthorityClass::Unexpected,
    }
}

fn materialize_durable(media: &Media, root: &Path) -> Result<(), GfError> {
    for path in media.durable_dirs.keys().chain(media.durable_files.keys()) {
        reject_relative(path)?;
    }

    std::fs::create_dir_all(root).map_err(|error| io_err(&error))?;
    let mut reachable_dirs = BTreeSet::from([".".to_owned()]);
    let mut changed = true;
    while changed {
        changed = false;
        for dir in media.durable_dirs.keys() {
            if dir == "." || reachable_dirs.contains(dir) {
                continue;
            }
            let parent = parent_path(dir);
            let linked = media
                .durable_dirs
                .get(&parent)
                .is_some_and(|children| children.contains(file_name(dir)));
            if reachable_dirs.contains(&parent) && linked {
                changed |= reachable_dirs.insert(dir.clone());
            }
        }
    }
    let mut dirs = reachable_dirs.iter().cloned().collect::<Vec<_>>();
    dirs.sort_by_key(|path| path.matches('/').count());
    for dir in dirs {
        if dir == "." {
            continue;
        }
        std::fs::create_dir_all(root.join(&dir)).map_err(|error| io_err(&error))?;
    }
    for (path, bytes) in &media.durable_files {
        let parent = parent_path(path);
        let linked = media
            .durable_dirs
            .get(&parent)
            .is_some_and(|children| children.contains(file_name(path)));
        if reachable_dirs.contains(&parent) && linked {
            std::fs::write(root.join(path), bytes).map_err(|error| io_err(&error))?;
        }
    }
    Ok(())
}

fn reject_relative(path: &str) -> Result<(), GfError> {
    if path.is_empty() || path.starts_with('/') || path.contains('\0') {
        return Err(project_corrupt(
            "oracle path must be relative and non-empty",
        ));
    }
    for component in Path::new(path).components() {
        match component {
            std::path::Component::Normal(_) | std::path::Component::CurDir => {}
            _ => return Err(project_corrupt("oracle path rejects links and traversal")),
        }
    }
    Ok(())
}

fn project_corrupt(message: &str) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ProjectCorrupt,
        message: message.into(),
    }
}

fn io_err(error: &std::io::Error) -> GfError {
    GfError::Storage(format!("fault oracle materialize failed: {error}"))
}

fn expected_authority_for_subset(
    ops: &[PersistenceOp],
    durable_ids: &BTreeSet<u64>,
) -> AuthorityClass {
    for op in ops {
        if !durable_ids.contains(&op.id) {
            continue;
        }
        if matches!(
            &op.kind,
            PersistenceOpKind::TearFile { path, .. }
                if path == CURRENT_FILE || path.ends_with(MANIFEST_FILE)
        ) {
            return AuthorityClass::Corrupt;
        }
    }

    let current_replace = ops.iter().find(|op| {
        op.phase == PublicationPhase::AfterCurrentReplace
            && matches!(
                &op.kind,
                PersistenceOpKind::AtomicReplace { path, .. } if path == CURRENT_FILE
            )
    });
    let root_fsync = ops.iter().find(|op| {
        op.phase == PublicationPhase::AfterRootFsync
            && matches!(&op.kind, PersistenceOpKind::FsyncDir { path } if path == ".")
    });

    let replace_durable = current_replace.is_some_and(|op| durable_ids.contains(&op.id));
    let root_durable = root_fsync.is_some_and(|op| durable_ids.contains(&op.id));

    if replace_durable || root_durable {
        let new_generation_complete = ops.iter().all(|op| {
            if op.phase > PublicationPhase::AfterGenerationDirFsync {
                return true;
            }
            match op.kind {
                PersistenceOpKind::FsyncFile { .. } | PersistenceOpKind::FsyncDir { .. } => {
                    durable_ids.contains(&op.id)
                }
                _ => true,
            }
        });
        if new_generation_complete {
            AuthorityClass::NewGeneration
        } else {
            AuthorityClass::Corrupt
        }
    } else {
        AuthorityClass::PriorGeneration
    }
}

fn acknowledged_for_subset(
    profile: DurabilityProfile,
    phase: PublicationPhase,
    ops: &[PersistenceOp],
    durable_ids: &BTreeSet<u64>,
) -> bool {
    let required_pre_ack_effects_complete = ops.iter().all(|op| match &op.kind {
        PersistenceOpKind::FsyncFile { .. }
        | PersistenceOpKind::FsyncDir { .. }
        | PersistenceOpKind::MkDir { .. } => durable_ids.contains(&op.id),
        PersistenceOpKind::OpenHandle { .. }
        | PersistenceOpKind::CloseHandle { .. }
        | PersistenceOpKind::WriteFile { .. }
        | PersistenceOpKind::AtomicReplace { .. }
        | PersistenceOpKind::TearFile { .. } => true,
    });
    if !required_pre_ack_effects_complete {
        return false;
    }
    match profile {
        DurabilityProfile::PosixDirectoryFsync => {
            phase.is_acknowledged()
                && ops.iter().any(|op| {
                    durable_ids.contains(&op.id)
                        && op.phase == PublicationPhase::AfterRootFsync
                        && matches!(&op.kind, PersistenceOpKind::FsyncDir { path } if path == ".")
                })
        }
        DurabilityProfile::WindowsNtfsWriteThrough => {
            phase >= PublicationPhase::AfterCurrentReplace
                && ops.iter().any(|op| {
                    durable_ids.contains(&op.id)
                        && op.phase == PublicationPhase::AfterCurrentReplace
                        && matches!(
                            &op.kind,
                            PersistenceOpKind::AtomicReplace { path, .. }
                                if path == CURRENT_FILE
                        )
                })
        }
    }
}

/// Run one simulated crash at `phase` with an explicit durable-op subset.
pub fn simulate_crash(
    seed: u64,
    phase: PublicationPhase,
    durable_ids: &BTreeSet<u64>,
) -> Result<FaultOracleReport, GfError> {
    simulate_crash_for_profile(
        seed,
        phase,
        durable_ids,
        DurabilityProfile::PosixDirectoryFsync,
    )
}

/// Run one simulated crash under a specific admitted platform profile.
pub fn simulate_crash_for_profile(
    seed: u64,
    phase: PublicationPhase,
    durable_ids: &BTreeSet<u64>,
    profile: DurabilityProfile,
) -> Result<FaultOracleReport, GfError> {
    let ids = PublicationIds::from_seed(seed);
    let ops = publication_ops_for_profile(ids, phase, profile);
    let mut media = Media::default();
    install_parent_baseline(&mut media, ids);
    media.apply_subset(&ops, durable_ids, profile);

    let root = tempfile::tempdir().map_err(|error| io_err(&error))?;
    materialize_durable(&media, root.path())?;
    let resolved = resolve_project_generation(root.path());
    let actual = classify_resolution(&resolved, ids);
    let expected = expected_authority_for_subset(&ops, durable_ids);

    Ok(FaultOracleReport {
        seed,
        phase,
        failpoint: phase.failpoint(),
        profile,
        acknowledged: acknowledged_for_subset(profile, phase, &ops, durable_ids),
        injected_result: None,
        durable_op_ids: durable_ids.iter().copied().collect(),
        expected,
        actual,
        minimized_op_ids: None,
        operation_trace: ops.iter().map(trace_op).collect(),
    })
}

/// Demonstrate that omitting the root directory flush violates acknowledgement.
///
/// Rename persistence is deliberately omitted from both cases. The only
/// difference is whether the later root namespace barrier persists the already
/// visible replacement, so this witness cannot accidentally treat rename as a
/// directory flush.
#[must_use]
pub fn lost_root_flush_witness(seed: u64) -> (FaultOracleReport, FaultOracleReport) {
    let phase = PublicationPhase::AfterRootFsync;
    let ids = PublicationIds::from_seed(seed);
    let ops = publication_ops(ids, phase);
    let replace_id = ops
        .iter()
        .find(|op| {
            matches!(
                &op.kind,
                PersistenceOpKind::AtomicReplace { path, .. } if path == CURRENT_FILE
            )
        })
        .map(|op| op.id)
        .expect("CURRENT replace op");

    let root_fsync_id = ops
        .iter()
        .find(|op| matches!(&op.kind, PersistenceOpKind::FsyncDir { path } if path == "."))
        .map(|op| op.id)
        .expect("root fsync op");

    let mut with_barrier = default_durable_ids(&ops, phase);
    with_barrier.remove(&replace_id);
    with_barrier.insert(root_fsync_id);
    let mut without_barrier = with_barrier.clone();
    without_barrier.remove(&root_fsync_id);

    let kept = simulate_crash(seed, phase, &with_barrier).expect("simulate");
    let lost = simulate_crash(seed, phase, &without_barrier).expect("simulate");
    (kept, lost)
}

/// Which metadata file receives torn bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TornTarget {
    /// Project-root `CURRENT`.
    Current,
    /// Selected generation `manifest.json`.
    Manifest,
}

/// Inject torn CURRENT or manifest bytes and classify reopen.
pub fn simulate_torn_bytes(seed: u64, target: TornTarget) -> Result<FaultOracleReport, GfError> {
    simulate_torn_bytes_for_profile(seed, target, DurabilityProfile::PosixDirectoryFsync)
}

/// Inject torn bytes under one admitted platform profile before API success.
pub fn simulate_torn_bytes_for_profile(
    seed: u64,
    target: TornTarget,
    profile: DurabilityProfile,
) -> Result<FaultOracleReport, GfError> {
    let ids = PublicationIds::from_seed(seed);
    let phase = PublicationPhase::AfterCurrentReplace;
    let mut ops = publication_ops_for_profile(ids, phase, profile);
    let next_id = ops.last().map_or(1, |op| op.id + 1);
    let path = match target {
        TornTarget::Current => CURRENT_FILE.to_owned(),
        TornTarget::Manifest => format!(
            "{GENERATIONS_DIR}/{}/{MANIFEST_FILE}",
            ids.new_generation.hyphenated()
        ),
    };
    ops.push(PersistenceOp {
        id: next_id,
        phase,
        kind: PersistenceOpKind::TearFile {
            path,
            bytes: b"{torn".to_vec(),
        },
    });

    let mut durable_ids = default_durable_ids(&ops, phase);
    durable_ids.insert(next_id);

    let mut media = Media::default();
    install_parent_baseline(&mut media, ids);
    media.apply_subset(&ops, &durable_ids, profile);
    let root = tempfile::tempdir().map_err(|error| io_err(&error))?;
    materialize_durable(&media, root.path())?;
    let resolved = resolve_project_generation(root.path());
    let actual = classify_resolution(&resolved, ids);

    Ok(FaultOracleReport {
        seed,
        phase,
        failpoint: phase.failpoint(),
        profile,
        acknowledged: false,
        injected_result: Some(InjectedOperationResult::TornBytes),
        durable_op_ids: durable_ids.iter().copied().collect(),
        expected: AuthorityClass::Corrupt,
        actual,
        minimized_op_ids: None,
        operation_trace: ops.iter().map(trace_op).collect(),
    })
}

/// Inject a typed operation non-success and reconcile authority without ack.
pub fn simulate_injected_operation(
    seed: u64,
    profile: DurabilityProfile,
    injected: InjectedOperationResult,
) -> Result<FaultOracleReport, GfError> {
    assert_ne!(
        injected,
        InjectedOperationResult::TornBytes,
        "use simulate_torn_bytes for byte corruption"
    );
    let phase = match injected {
        InjectedOperationResult::FileFlushError => PublicationPhase::AfterManifestFsync,
        InjectedOperationResult::NamespaceBarrierError => PublicationPhase::AfterRootFsync,
        InjectedOperationResult::ReplacementNotPerformed
        | InjectedOperationResult::ReplacementStateUnknownPrior
        | InjectedOperationResult::ReplacementStateUnknownNew => {
            PublicationPhase::AfterCurrentReplace
        }
        InjectedOperationResult::TornBytes => unreachable!(),
    };
    let ids = PublicationIds::from_seed(seed);
    let ops = publication_ops_for_profile(ids, phase, profile);
    let mut durable = default_durable_ids(&ops, phase);

    match injected {
        InjectedOperationResult::FileFlushError => {
            let failed = ops
                .iter()
                .find(|op| {
                    op.phase == PublicationPhase::AfterManifestFsync
                        && matches!(op.kind, PersistenceOpKind::FsyncFile { .. })
                })
                .expect("manifest flush");
            durable.remove(&failed.id);
        }
        InjectedOperationResult::NamespaceBarrierError => {
            for op in &ops {
                if op.phase == PublicationPhase::AfterRootFsync
                    && matches!(op.kind, PersistenceOpKind::FsyncDir { .. })
                {
                    durable.remove(&op.id);
                }
            }
        }
        InjectedOperationResult::ReplacementNotPerformed
        | InjectedOperationResult::ReplacementStateUnknownPrior => {
            for op in &ops {
                if op.phase == PublicationPhase::AfterCurrentReplace
                    && matches!(op.kind, PersistenceOpKind::AtomicReplace { .. })
                {
                    durable.remove(&op.id);
                }
            }
        }
        InjectedOperationResult::ReplacementStateUnknownNew => {}
        InjectedOperationResult::TornBytes => unreachable!(),
    }

    let mut report = simulate_crash_for_profile(seed, phase, &durable, profile)?;
    report.acknowledged = false;
    report.injected_result = Some(injected);
    Ok(report)
}

/// Shrink a durable-id set to a minimal subset that preserves `predicate`.
#[must_use]
pub fn minimize_durable_ids(
    seed: u64,
    phase: PublicationPhase,
    initial: &BTreeSet<u64>,
    predicate: impl Fn(&FaultOracleReport) -> bool,
) -> BTreeSet<u64> {
    let mut current = initial.clone();
    let mut changed = true;
    while changed {
        changed = false;
        for id in initial.iter().copied().collect::<Vec<_>>() {
            if !current.contains(&id) {
                continue;
            }
            let mut candidate = current.clone();
            candidate.remove(&id);
            let Ok(report) = simulate_crash(seed, phase, &candidate) else {
                continue;
            };
            if predicate(&report) {
                current = candidate;
                changed = true;
            }
        }
    }
    current
}

/// Shrink an omitted-operation fault set while preserving `predicate`.
///
/// The default successful history remains fixed; only the specified persistence
/// effects are removed. This avoids the vacuous empty-durable-set result that
/// does not explain which omissions are required to reproduce a failure.
#[must_use]
pub fn minimize_omitted_ids(
    seed: u64,
    phase: PublicationPhase,
    initial_omitted: &BTreeSet<u64>,
    predicate: impl Fn(&FaultOracleReport) -> bool,
) -> BTreeSet<u64> {
    minimize_omitted_ids_for_profile(
        seed,
        phase,
        initial_omitted,
        DurabilityProfile::PosixDirectoryFsync,
        predicate,
    )
}

/// Shrink omissions under one admitted platform profile.
#[must_use]
pub fn minimize_omitted_ids_for_profile(
    seed: u64,
    phase: PublicationPhase,
    initial_omitted: &BTreeSet<u64>,
    profile: DurabilityProfile,
    predicate: impl Fn(&FaultOracleReport) -> bool,
) -> BTreeSet<u64> {
    let ids = PublicationIds::from_seed(seed);
    let ops = publication_ops_for_profile(ids, phase, profile);
    let successful = default_durable_ids(&ops, phase);
    let mut current = initial_omitted.clone();
    let mut changed = true;
    while changed {
        changed = false;
        for id in current.iter().copied().collect::<Vec<_>>() {
            let mut candidate = current.clone();
            candidate.remove(&id);
            let durable = successful
                .difference(&candidate)
                .copied()
                .collect::<BTreeSet<_>>();
            let Ok(report) = simulate_crash_for_profile(seed, phase, &durable, profile) else {
                continue;
            };
            if predicate(&report) {
                current = candidate;
                changed = true;
            }
        }
    }
    current
}

/// Return a reproducing report carrying its stable, 1-minimal omitted trace.
#[must_use]
pub fn minimized_omission_report(
    seed: u64,
    phase: PublicationPhase,
    initial_omitted: &BTreeSet<u64>,
    predicate: impl Fn(&FaultOracleReport) -> bool + Copy,
) -> FaultOracleReport {
    minimized_omission_report_for_profile(
        seed,
        phase,
        initial_omitted,
        DurabilityProfile::PosixDirectoryFsync,
        predicate,
    )
}

/// Return a reproducing minimal omission report for one platform profile.
#[must_use]
pub fn minimized_omission_report_for_profile(
    seed: u64,
    phase: PublicationPhase,
    initial_omitted: &BTreeSet<u64>,
    profile: DurabilityProfile,
    predicate: impl Fn(&FaultOracleReport) -> bool + Copy,
) -> FaultOracleReport {
    let omitted =
        minimize_omitted_ids_for_profile(seed, phase, initial_omitted, profile, predicate);
    let ids = PublicationIds::from_seed(seed);
    let ops = publication_ops_for_profile(ids, phase, profile);
    let successful = default_durable_ids(&ops, phase);
    let durable = successful
        .difference(&omitted)
        .copied()
        .collect::<BTreeSet<_>>();
    let mut report = simulate_crash_for_profile(seed, phase, &durable, profile)
        .expect("minimized oracle report");
    assert!(predicate(&report), "minimized trace stopped reproducing");
    report.minimized_op_ids = Some(omitted.iter().copied().collect());
    report
}

/// Bounded CI history count; override with `GRAPHFORGE_FAULT_ORACLE_HISTORIES`.
#[must_use]
pub fn history_budget() -> usize {
    parse_history_budget(
        std::env::var("GRAPHFORGE_FAULT_ORACLE_HISTORIES")
            .ok()
            .as_deref(),
    )
}

const DEFAULT_HISTORY_BUDGET: usize = 8;
const MAX_HISTORY_BUDGET: usize = 4096;

fn parse_history_budget(value: Option<&str>) -> usize {
    value
        .and_then(|candidate| candidate.parse::<usize>().ok())
        .filter(|count| (1..=MAX_HISTORY_BUDGET).contains(count))
        .unwrap_or(DEFAULT_HISTORY_BUDGET)
}

/// Seeded enumeration of pending CURRENT-entry persistence before acknowledgement.
#[must_use]
pub fn enumerate_lost_root_flush_subsets(seed: u64) -> Vec<FaultOracleReport> {
    let phase = PublicationPhase::AfterCurrentReplace;
    let ids = PublicationIds::from_seed(seed);
    let ops = publication_ops(ids, phase);
    let replace_id = ops
        .iter()
        .find(|op| {
            matches!(
                &op.kind,
                PersistenceOpKind::AtomicReplace { path, .. } if path == CURRENT_FILE
            )
        })
        .map(|op| op.id)
        .expect("replace");

    let base = {
        let mut set = default_durable_ids(&ops, phase);
        for op in &ops {
            if matches!(&op.kind, PersistenceOpKind::FsyncDir { path } if path == ".") {
                set.remove(&op.id);
            }
        }
        set.remove(&replace_id);
        set
    };

    let mut reports = Vec::with_capacity(2);
    for include_replace in [false, true] {
        let mut durable = base.clone();
        if include_replace {
            durable.insert(replace_id);
        }
        reports.push(simulate_crash(seed, phase, &durable).expect("subset"));
    }
    reports
}

/// Seed causally ordered pre-ack persistence subsets for bounded CI histories.
#[must_use]
pub fn seed_pre_ack_persistence_subsets(seed: u64, count: usize) -> Vec<FaultOracleReport> {
    let count = count.clamp(1, MAX_HISTORY_BUDGET);
    let phase = PublicationPhase::AfterCurrentReplace;
    let ids = PublicationIds::from_seed(seed);
    let ops = publication_ops(ids, phase);
    let candidates = ops
        .iter()
        .filter(|op| {
            matches!(
                op.kind,
                PersistenceOpKind::FsyncFile { .. }
                    | PersistenceOpKind::FsyncDir { .. }
                    | PersistenceOpKind::AtomicReplace { .. }
                    | PersistenceOpKind::MkDir { .. }
            )
        })
        .map(|op| op.id)
        .collect::<Vec<_>>();
    let default = default_durable_ids(&ops, phase);
    (0..count)
        .map(|history| {
            let mut state = seed ^ (history as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let mut durable = default.clone();
            for id in &candidates {
                state ^= state >> 12;
                state ^= state << 25;
                state ^= state >> 27;
                if state.wrapping_mul(0x2545_f491_4f6c_dd1d) & 1 == 0 {
                    durable.remove(id);
                }
            }
            simulate_crash(seed ^ history as u64, phase, &durable).expect("seeded subset")
        })
        .collect()
}

/// Run default-durable simulations for every ADR publication phase.
pub fn simulate_all_phases(seed: u64) -> Result<Vec<PhaseOutcome>, GfError> {
    simulate_all_phases_for_profile(seed, DurabilityProfile::PosixDirectoryFsync)
}

/// Run every shared phase under one platform durability profile.
pub fn simulate_all_phases_for_profile(
    seed: u64,
    profile: DurabilityProfile,
) -> Result<Vec<PhaseOutcome>, GfError> {
    let ids = PublicationIds::from_seed(seed);
    let mut outcomes = Vec::new();
    for phase in PublicationPhase::all() {
        let ops = publication_ops_for_profile(ids, *phase, profile);
        let durable = default_durable_ids(&ops, *phase);
        let report = simulate_crash_for_profile(seed, *phase, &durable, profile)?;
        outcomes.push(PhaseOutcome {
            phase: *phase,
            failpoint: phase.failpoint(),
            acknowledged: report.acknowledged,
            expected: report.expected,
            actual: report.actual,
            selected_generation: match report.actual {
                AuthorityClass::NewGeneration => Some(ids.new_generation),
                AuthorityClass::PriorGeneration => Some(ids.parent_generation),
                AuthorityClass::Corrupt | AuthorityClass::Unexpected => None,
            },
        });
    }
    Ok(outcomes)
}

/// Project-relative generation manifest path for later M6 harness reuse.
#[must_use]
pub fn generation_manifest_path(generation: Uuid) -> PathBuf {
    PathBuf::from(GENERATIONS_DIR)
        .join(generation.hyphenated().to_string())
        .join(MANIFEST_FILE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_adr_publication_phase_has_modeled_authority_outcome() {
        let outcomes = simulate_all_phases(0x749).expect("phase sweep");
        assert_eq!(outcomes.len(), PublicationPhase::all().len());
        for outcome in &outcomes {
            assert_eq!(
                outcome.expected, outcome.actual,
                "phase {:?} failpoint {} expected={:?} actual={:?}",
                outcome.phase, outcome.failpoint, outcome.expected, outcome.actual
            );
            if outcome.acknowledged {
                assert_eq!(outcome.actual, AuthorityClass::NewGeneration);
            }
            if !outcome.phase.is_linearized() {
                assert_eq!(outcome.actual, AuthorityClass::PriorGeneration);
            }
            assert_ne!(outcome.actual, AuthorityClass::Unexpected);
        }
    }

    #[test]
    fn omitting_root_directory_flush_would_violate_acknowledgement_contract() {
        let (kept, lost) = lost_root_flush_witness(0x7490);
        assert_eq!(kept.actual, AuthorityClass::NewGeneration);
        assert_eq!(lost.actual, AuthorityClass::PriorGeneration);
        assert!(kept.acknowledged);
        assert!(!lost.acknowledged);
        assert_ne!(
            kept.actual, lost.actual,
            "root flush omission admits loss of the linearized CURRENT entry"
        );
    }

    #[test]
    fn lost_root_flush_subsets_are_exactly_prior_or_new() {
        for seed in 0..history_budget() as u64 {
            let reports = enumerate_lost_root_flush_subsets(seed.wrapping_mul(0x749));
            assert_eq!(reports.len(), 2);
            assert_eq!(reports[0].actual, AuthorityClass::PriorGeneration);
            assert_eq!(reports[1].actual, AuthorityClass::NewGeneration);
            for report in &reports {
                assert_ne!(report.actual, AuthorityClass::Corrupt);
            }
        }
    }

    #[test]
    fn torn_current_and_manifest_fail_closed() {
        for profile in [
            DurabilityProfile::PosixDirectoryFsync,
            DurabilityProfile::WindowsNtfsWriteThrough,
        ] {
            for target in [TornTarget::Current, TornTarget::Manifest] {
                let report =
                    simulate_torn_bytes_for_profile(0x7491, target, profile).expect("torn");
                assert_eq!(report.profile, profile);
                assert_eq!(report.expected, AuthorityClass::Corrupt);
                assert!(!report.acknowledged);
                assert_eq!(
                    report.injected_result,
                    Some(InjectedOperationResult::TornBytes)
                );
                assert_eq!(
                    report.actual,
                    AuthorityClass::Corrupt,
                    "torn bytes must surface GF_PROJECT_CORRUPT, not Unexpected"
                );
            }
        }
    }

    #[test]
    fn generated_failures_shrink_to_stable_minimal_trace() {
        let seed = 0x7492;
        let phase = PublicationPhase::AfterRootFsync;
        let ids = PublicationIds::from_seed(seed);
        let ops = publication_ops(ids, phase);
        let replace_id = ops
            .iter()
            .find(|op| {
                matches!(
                    &op.kind,
                    PersistenceOpKind::AtomicReplace { path, .. } if path == CURRENT_FILE
                )
            })
            .map(|op| op.id)
            .unwrap();
        let root_fsync_id = ops
            .iter()
            .find(|op| matches!(&op.kind, PersistenceOpKind::FsyncDir { path } if path == "."))
            .map(|op| op.id)
            .unwrap();
        let initial = BTreeSet::from([replace_id, root_fsync_id]);

        let minimized_report = minimized_omission_report(seed, phase, &initial, |report| {
            report.actual == AuthorityClass::PriorGeneration
        });
        let minimized = minimized_report
            .minimized_op_ids
            .clone()
            .expect("artifact records minimized omissions")
            .into_iter()
            .collect::<BTreeSet<_>>();
        let again = minimize_omitted_ids(seed, phase, &minimized, |report| {
            report.actual == AuthorityClass::PriorGeneration
        });
        assert_eq!(minimized, again, "minimizer must be idempotent");
        assert_eq!(minimized, initial, "both namespace effects are required");
        let successful = default_durable_ids(&ops, phase);
        let durable = successful
            .difference(&minimized)
            .copied()
            .collect::<BTreeSet<_>>();
        let report = simulate_crash(seed, phase, &durable).unwrap();
        assert_eq!(report.actual, AuthorityClass::PriorGeneration);
        for omitted in &minimized {
            let candidate = minimized
                .iter()
                .copied()
                .filter(|id| id != omitted)
                .collect::<BTreeSet<_>>();
            let durable = successful
                .difference(&candidate)
                .copied()
                .collect::<BTreeSet<_>>();
            let report = simulate_crash(seed, phase, &durable).unwrap();
            assert_eq!(
                report.actual,
                AuthorityClass::NewGeneration,
                "omission {omitted} was not necessary"
            );
        }
    }

    #[test]
    fn acknowledged_success_survives_restart_exactly() {
        let seed = 0x7494;
        let phase = PublicationPhase::AfterRootFsync;
        let ids = PublicationIds::from_seed(seed);
        let ops = publication_ops(ids, phase);
        let durable = default_durable_ids(&ops, phase);
        let report = simulate_crash(seed, phase, &durable).unwrap();
        assert_eq!(report.actual, AuthorityClass::NewGeneration);
        assert!(report.acknowledged);
    }

    #[test]
    fn harness_exposes_reusable_paths_and_reports() {
        let ids = PublicationIds::from_seed(1);
        let path = generation_manifest_path(ids.new_generation);
        assert!(path.ends_with(MANIFEST_FILE));
        let report = simulate_crash(
            1,
            PublicationPhase::BeforeCurrentReplace,
            &default_durable_ids(
                &publication_ops(ids, PublicationPhase::BeforeCurrentReplace),
                PublicationPhase::BeforeCurrentReplace,
            ),
        )
        .unwrap();
        let encoded = serde_json::to_string(&report).unwrap();
        assert!(encoded.contains("before_current_replace"));
        // Traces carry project-relative paths and lengths, never raw payloads.
        assert!(
            report
                .operation_trace
                .iter()
                .any(|line| line.contains("len="))
        );
        assert!(!encoded.contains("graph:nodes"));
        assert!(
            !report
                .operation_trace
                .iter()
                .any(|line| line.contains('\0'))
        );
    }

    #[test]
    fn no_hidden_failure_accepts_either_result_after_acknowledgement() {
        let seed = 0x7495;
        let phase = PublicationPhase::AfterJournalPublished;
        let ids = PublicationIds::from_seed(seed);
        let ops = publication_ops(ids, phase);
        let durable = default_durable_ids(&ops, phase);
        let report = simulate_crash(seed, phase, &durable).unwrap();
        assert_eq!(report.actual, AuthorityClass::NewGeneration);
        assert_eq!(report.expected, AuthorityClass::NewGeneration);
        assert!(report.acknowledged);
        assert!(matches!(
            (report.expected, report.actual),
            (AuthorityClass::NewGeneration, AuthorityClass::NewGeneration)
        ));
    }

    #[test]
    fn typed_operation_errors_reconcile_without_acknowledgement() {
        for profile in [
            DurabilityProfile::PosixDirectoryFsync,
            DurabilityProfile::WindowsNtfsWriteThrough,
        ] {
            for injected in [
                (InjectedOperationResult::FileFlushError),
                (InjectedOperationResult::NamespaceBarrierError),
                (InjectedOperationResult::ReplacementNotPerformed),
                (InjectedOperationResult::ReplacementStateUnknownPrior),
                (InjectedOperationResult::ReplacementStateUnknownNew),
            ] {
                let report = simulate_injected_operation(0x7496, profile, injected).unwrap();
                let expected = match injected {
                    InjectedOperationResult::NamespaceBarrierError
                    | InjectedOperationResult::ReplacementStateUnknownNew => {
                        AuthorityClass::NewGeneration
                    }
                    _ => AuthorityClass::PriorGeneration,
                };
                assert_eq!(report.injected_result, Some(injected));
                assert!(!report.acknowledged);
                assert_eq!(report.expected, expected);
                assert_eq!(report.actual, expected);
            }
        }
    }

    #[test]
    fn ntfs_profile_uses_write_through_handle_rename_as_acknowledgement() {
        let profile = DurabilityProfile::WindowsNtfsWriteThrough;
        let phase = PublicationPhase::AfterCurrentReplace;
        let ids = PublicationIds::from_seed(0x7497);
        let ops = publication_ops_for_profile(ids, phase, profile);
        let durable = default_durable_ids(&ops, phase);
        let report = simulate_crash_for_profile(0x7497, phase, &durable, profile).unwrap();
        assert!(report.acknowledged);
        assert_eq!(report.actual, AuthorityClass::NewGeneration);
        assert!(report.operation_trace.iter().any(|line| {
            line.contains("open_handle handle=current_stage") && line.contains("write_through=true")
        }));
        assert!(!ops.iter().any(|op| {
            op.phase == PublicationPhase::AfterRootFsync
                && matches!(op.kind, PersistenceOpKind::FsyncDir { .. })
        }));
    }

    #[test]
    fn bounded_seeded_subsets_never_select_an_unexpected_authority() {
        let reports = seed_pre_ack_persistence_subsets(0x7498, history_budget());
        assert_eq!(reports.len(), history_budget());
        assert!(reports.iter().all(|report| {
            matches!(
                report.actual,
                AuthorityClass::PriorGeneration
                    | AuthorityClass::NewGeneration
                    | AuthorityClass::Corrupt
            ) && report.actual == report.expected
                && !report.acknowledged
        }));
    }

    #[test]
    fn history_budget_is_positive_and_bounded() {
        assert_eq!(parse_history_budget(None), DEFAULT_HISTORY_BUDGET);
        assert_eq!(parse_history_budget(Some("0")), DEFAULT_HISTORY_BUDGET);
        assert_eq!(parse_history_budget(Some("4096")), MAX_HISTORY_BUDGET);
        assert_eq!(parse_history_budget(Some("4097")), DEFAULT_HISTORY_BUDGET);
        assert_eq!(
            parse_history_budget(Some("not-a-number")),
            DEFAULT_HISTORY_BUDGET
        );
    }

    #[test]
    fn directory_fsync_never_promotes_unflushed_file_bytes() {
        let mut media = Media::default();
        media.mkdir(".");
        media.fsync_dir(".");
        media.write_file("unflushed", b"volatile".to_vec());
        media.fsync_dir(".");
        media.crash();
        assert!(!media.durable_files.contains_key("unflushed"));
    }

    #[test]
    fn materialization_does_not_recreate_unreachable_namespace_ancestors() {
        let root = tempfile::tempdir().unwrap();
        let media = Media {
            durable_files: BTreeMap::from([("orphan/child/file".into(), b"bytes".to_vec())]),
            durable_dirs: BTreeMap::from([
                (".".into(), BTreeSet::new()),
                ("orphan".into(), BTreeSet::from(["child".into()])),
                ("orphan/child".into(), BTreeSet::from(["file".into()])),
            ]),
            ..Media::default()
        };
        materialize_durable(&media, root.path()).unwrap();
        assert!(!root.path().join("orphan").exists());
    }
}
