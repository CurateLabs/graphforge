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

    fn atomic_replace(&mut self, path: &str, bytes: Vec<u8>) {
        let parent = parent_path(path);
        let temp = format!("{parent}/.oracle-tmp-{}", file_name(path));
        self.write_file(&temp, bytes.clone());
        self.fsync_file(&temp);
        self.volatile_files.remove(&temp);
        self.unlink_volatile(&parent, file_name(&temp));
        self.durable_files.remove(&temp);
        // Flushed sibling proves data durability; the destination name remains
        // volatile until the parent directory flush promotes it.
        self.write_file(path, bytes);
    }

    fn fsync_dir(&mut self, path: &str) {
        let children = self.volatile_dirs.get(path).cloned().unwrap_or_default();
        self.durable_dirs.insert(path.to_owned(), children.clone());
        // Directory flush makes currently linked children's volatile bytes
        // durable when their content was already written.
        for name in children {
            let child = if path == "." {
                name
            } else {
                format!("{path}/{name}")
            };
            if let Some(bytes) = self.volatile_files.get(&child).cloned() {
                self.durable_files.insert(child, bytes);
            }
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
    }

    fn apply_subset(&mut self, ops: &[PersistenceOp], durable_ids: &BTreeSet<u64>) {
        let mut scratch = Self {
            durable_files: self.durable_files.clone(),
            durable_dirs: self.durable_dirs.clone(),
            volatile_files: self.durable_files.clone(),
            volatile_dirs: self.durable_dirs.clone(),
        };

        for op in ops {
            match &op.kind {
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
                PersistenceOpKind::AtomicReplace { path, bytes } => {
                    if durable_ids.contains(&op.id) {
                        scratch.atomic_replace(path, bytes.clone());
                        scratch.fsync_dir(&parent_path(path));
                    } else {
                        scratch.write_file(path, bytes.clone());
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
        PersistenceOpKind::WriteFile { path, bytes } => {
            format!("write_file path={path} len={}", bytes.len())
        }
        PersistenceOpKind::FsyncFile { path } => format!("fsync_file path={path}"),
        PersistenceOpKind::MkDir { path } => format!("mkdir path={path}"),
        PersistenceOpKind::AtomicReplace { path, bytes } => {
            format!("atomic_replace path={path} len={}", bytes.len())
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
        PersistenceOpKind::AtomicReplace {
            path: journal_path.clone(),
            bytes: b"{\"phase\":\"DURABLE\"}\n".to_vec(),
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
        },
    );
    push(
        PublicationPhase::AfterRootFsync,
        PersistenceOpKind::FsyncDir { path: ".".into() },
    );
    push(
        PublicationPhase::AfterJournalPublished,
        PersistenceOpKind::AtomicReplace {
            path: journal_path,
            bytes: b"{\"phase\":\"PUBLISHED\"}\n".to_vec(),
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
                    | PersistenceOpKind::WriteFile { .. }
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
    let mut dirs: Vec<_> = media.durable_dirs.keys().cloned().collect();
    dirs.sort_by_key(|path| path.matches('/').count());
    for dir in dirs {
        if dir == "." {
            continue;
        }
        std::fs::create_dir_all(root.join(&dir)).map_err(|error| io_err(&error))?;
    }
    for (path, bytes) in &media.durable_files {
        let full = root.join(path);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent).map_err(|error| io_err(&error))?;
        }
        std::fs::write(&full, bytes).map_err(|error| io_err(&error))?;
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
    phase: PublicationPhase,
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

    if phase.is_acknowledged() {
        if root_durable && replace_durable {
            AuthorityClass::NewGeneration
        } else if replace_durable {
            // Linearized but not acknowledged: filesystem may still present new.
            AuthorityClass::NewGeneration
        } else {
            AuthorityClass::PriorGeneration
        }
    } else if phase.is_linearized() {
        if replace_durable {
            AuthorityClass::NewGeneration
        } else {
            AuthorityClass::PriorGeneration
        }
    } else {
        AuthorityClass::PriorGeneration
    }
}

/// Run one simulated crash at `phase` with an explicit durable-op subset.
pub fn simulate_crash(
    seed: u64,
    phase: PublicationPhase,
    durable_ids: &BTreeSet<u64>,
) -> Result<FaultOracleReport, GfError> {
    let ids = PublicationIds::from_seed(seed);
    let ops = publication_ops(ids, phase);
    let mut media = Media::default();
    install_parent_baseline(&mut media, ids);
    media.apply_subset(&ops, durable_ids);

    let root = tempfile::tempdir().map_err(|error| io_err(&error))?;
    materialize_durable(&media, root.path())?;
    let resolved = resolve_project_generation(root.path());
    let actual = classify_resolution(&resolved, ids);
    let expected = expected_authority_for_subset(phase, &ops, durable_ids);

    Ok(FaultOracleReport {
        seed,
        phase,
        failpoint: phase.failpoint(),
        durable_op_ids: durable_ids.iter().copied().collect(),
        expected,
        actual,
        minimized_op_ids: None,
        operation_trace: ops.iter().map(trace_op).collect(),
    })
}

/// Demonstrate that omitting the root directory flush violates acknowledgement.
#[must_use]
pub fn lost_root_flush_witness(seed: u64) -> (FaultOracleReport, FaultOracleReport) {
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
        .expect("CURRENT replace op");

    let mut with_replace = default_durable_ids(&ops, phase);
    with_replace.insert(replace_id);
    for op in &ops {
        if matches!(&op.kind, PersistenceOpKind::FsyncDir { path } if path == ".") {
            with_replace.remove(&op.id);
        }
    }

    let mut without_replace = with_replace.clone();
    without_replace.remove(&replace_id);

    let kept = simulate_crash(seed, phase, &with_replace).expect("simulate");
    let lost = simulate_crash(seed, phase, &without_replace).expect("simulate");
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
    let ids = PublicationIds::from_seed(seed);
    let phase = PublicationPhase::AfterRootFsync;
    let mut ops = publication_ops(ids, phase);
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
    media.apply_subset(&ops, &durable_ids);
    let root = tempfile::tempdir().map_err(|error| io_err(&error))?;
    materialize_durable(&media, root.path())?;
    let resolved = resolve_project_generation(root.path());
    let actual = classify_resolution(&resolved, ids);

    Ok(FaultOracleReport {
        seed,
        phase,
        failpoint: phase.failpoint(),
        durable_op_ids: durable_ids.iter().copied().collect(),
        expected: AuthorityClass::Corrupt,
        actual,
        minimized_op_ids: None,
        operation_trace: ops.iter().map(trace_op).collect(),
    })
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

/// Bounded CI history count; override with `GRAPHFORGE_FAULT_ORACLE_HISTORIES`.
#[must_use]
pub fn history_budget() -> usize {
    std::env::var("GRAPHFORGE_FAULT_ORACLE_HISTORIES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8)
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

/// Shared-boundary authority classes matching native subprocess-kill matrices.
///
/// Process kill after a returned rename observes the new `CURRENT`. The oracle
/// default durable set mirrors that host-visibility model via
/// [`expected_authority`]. Power-loss lost-flush subsets are certified
/// separately and are not claimed to match process kill.
#[must_use]
pub fn native_shared_boundary_authority(phase: PublicationPhase) -> AuthorityClass {
    expected_authority(phase)
}

/// Run default-durable simulations for every ADR publication phase.
pub fn simulate_all_phases(seed: u64) -> Result<Vec<PhaseOutcome>, GfError> {
    let ids = PublicationIds::from_seed(seed);
    let mut outcomes = Vec::new();
    for phase in PublicationPhase::all() {
        let ops = publication_ops(ids, *phase);
        let durable = default_durable_ids(&ops, *phase);
        let report = simulate_crash(seed, *phase, &durable)?;
        outcomes.push(PhaseOutcome {
            phase: *phase,
            failpoint: phase.failpoint(),
            acknowledged: phase.is_acknowledged(),
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
        assert!(!PublicationPhase::AfterCurrentReplace.is_acknowledged());
        assert!(PublicationPhase::AfterRootFsync.is_acknowledged());
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
        for target in [TornTarget::Current, TornTarget::Manifest] {
            let report = simulate_torn_bytes(0x7491, target).expect("torn");
            assert_eq!(report.expected, AuthorityClass::Corrupt);
            assert_eq!(
                report.actual,
                AuthorityClass::Corrupt,
                "torn bytes must surface GF_PROJECT_CORRUPT, not Unexpected"
            );
        }
    }

    #[test]
    fn generated_failures_shrink_to_stable_minimal_trace() {
        let seed = 0x7492;
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
            .unwrap();
        let mut initial = default_durable_ids(&ops, phase);
        for op in &ops {
            if matches!(&op.kind, PersistenceOpKind::FsyncDir { path } if path == ".") {
                initial.remove(&op.id);
            }
        }
        initial.remove(&replace_id);

        let minimized = minimize_durable_ids(seed, phase, &initial, |report| {
            report.actual == AuthorityClass::PriorGeneration
        });
        let again = minimize_durable_ids(seed, phase, &minimized, |report| {
            report.actual == AuthorityClass::PriorGeneration
        });
        assert_eq!(minimized, again, "minimizer must be idempotent");
        let report = simulate_crash(seed, phase, &minimized).unwrap();
        assert_eq!(report.actual, AuthorityClass::PriorGeneration);
        assert!(
            !minimized.contains(&replace_id),
            "minimal prior-generation trace must omit durable CURRENT replace"
        );
    }

    #[test]
    fn native_and_simulated_results_agree_at_shared_phase_boundaries() {
        for outcome in simulate_all_phases(0x7493).unwrap() {
            let native = native_shared_boundary_authority(outcome.phase);
            assert_eq!(
                outcome.actual, native,
                "shared boundary mismatch at {}",
                outcome.failpoint
            );
            assert_ne!(outcome.actual, AuthorityClass::Unexpected);
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
        assert!(phase.is_acknowledged());
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
        assert!(matches!(
            (report.expected, report.actual),
            (AuthorityClass::NewGeneration, AuthorityClass::NewGeneration)
        ));
    }
}
