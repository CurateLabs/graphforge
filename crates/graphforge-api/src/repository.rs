//! Safe integration between a code repository and an embedded GraphForge project.

mod configuration;
mod skills;

pub use configuration::ProjectConfig;
use configuration::digest_definition_tree;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use fs4::FileExt;
use graphforge_core::{GfError, ProjectErrorCode};
use graphforge_storage as storage;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const CONFIG: &str = ".graphforge/graphforge.yaml";
const IGNORE_START: &str = "# graphforge: managed data (do not edit)";
const IGNORE_END: &str = "# graphforge: end managed data";
const MAX_PORTABLE_INTEGER: u64 = 9_007_199_254_740_991;
/// Four repository-local runtime patterns managed as one idempotent block.
/// The admission lock is a persistent sibling of `state`, not part of it.
const IGNORE_LINES: [&str; 4] = [
    "/.graphforge/state/",
    "/.graphforge/.graphforge-admission-*.lock",
    "/.graphforge/imports/",
    "/.graphforge/exports/",
];
const SKILLS_ROOT: &str = ".agents/skills";
const SKILLS_MANIFEST: &str = ".agents/skills/.graphforge-managed.json";
const SKILLS_LIFECYCLE_ROOT: &str = ".graphforge/imports/skills-lifecycle";
const SKILLS_TRANSACTION: &str = ".graphforge/imports/skills-lifecycle/transaction";
const SKILLS_LOCK: &str = ".graphforge/imports/skills-lifecycle/lock";
const SKILLS_STAGE: &str = ".graphforge/imports/skills-lifecycle/stage";
const SKILLS_BACKUP: &str = ".graphforge/imports/skills-lifecycle/backup";
const MANAGED_SKILL_NAMES: [&str; 2] = ["graphforge-bootstrap", "graphforge-build-knowledge"];

#[cfg(test)]
#[derive(Clone)]
struct RepositorySyncTestHook {
    root: PathBuf,
    barrier: std::sync::Arc<std::sync::Barrier>,
}

#[cfg(test)]
static REPOSITORY_SYNC_BEFORE_STAGE_HOOK: std::sync::OnceLock<
    std::sync::Mutex<Option<RepositorySyncTestHook>>,
> = std::sync::OnceLock::new();

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn idempotency_conflict(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::TransactionConflict,
        message: message.into(),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn repository_snapshot_to_participant(
    snapshot: graphforge_storage::ProjectParticipantSnapshot,
) -> Result<graphforge_storage::ProjectParticipant, GfError> {
    let encoding = match snapshot.encoding.as_str() {
        "parquet" => graphforge_storage::ProjectParticipantEncoding::Parquet,
        "arrow" => graphforge_storage::ProjectParticipantEncoding::Arrow,
        "json" => graphforge_storage::ProjectParticipantEncoding::Json,
        _ => {
            return Err(validation("committed participant has unsupported encoding"));
        }
    };
    Ok(graphforge_storage::ProjectParticipant {
        capability_id: snapshot.capability_id,
        capability_version: snapshot.capability_version,
        record_family_id: snapshot.record_family_id,
        record_version: snapshot.record_version,
        encoding,
        schema_fingerprint: snapshot.schema_fingerprint,
        row_count: snapshot.row_count,
        bytes: snapshot.bytes,
    })
}

fn repository_sync_generation_uuid(
    operation_uuid: Uuid,
    participants: &[graphforge_storage::ProjectParticipant],
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-repository-sync-generation/1");
    hasher.update(operation_uuid.as_bytes());
    for participant in participants {
        hasher.update(participant.capability_id.as_bytes());
        hasher.update([0]);
        hasher.update(participant.record_family_id.as_bytes());
        hasher.update([0]);
        hasher.update(Sha256::digest(&participant.bytes));
    }
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

fn validate_repository_snapshot_inventory(
    participants: &[graphforge_storage::StagedParticipant],
    expected_content_sha256: &str,
) -> Result<(), GfError> {
    let snapshots = participants
        .iter()
        .filter(|participant| {
            participant.capability_id == graphforge_storage::WORKSPACE_CAPABILITY_ID
                && participant.record_family_id
                    == graphforge_storage::WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY
        })
        .collect::<Vec<_>>();
    if snapshots.len() != 1 {
        return Err(validation(
            "workspace generation must contain exactly one repository snapshot",
        ));
    }
    let snapshot = snapshots[0];
    let expected_schema = encode_hex(&Sha256::digest("workspace/repository_snapshot@1"));
    if snapshot.capability_version != graphforge_storage::WORKSPACE_CAPABILITY_VERSION
        || snapshot.record_version != graphforge_storage::WORKSPACE_REPOSITORY_SNAPSHOT_VERSION
        || snapshot.encoding != "json"
        || snapshot.row_count != 1
        || snapshot.schema_fingerprint != expected_schema
        || snapshot.content_sha256 != expected_content_sha256
    {
        return Err(validation(
            "workspace repository snapshot metadata is invalid",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DesiredRepositorySnapshot {
    resolved_config_sha256: String,
    definitions: Vec<graphforge_storage::WorkspaceRepositoryDefinitionDigest>,
    sources: Vec<graphforge_storage::WorkspaceRepositorySourceDigest>,
    git: graphforge_storage::WorkspaceRepositoryGitProvenance,
}

impl DesiredRepositorySnapshot {
    fn matches(&self, snapshot: &graphforge_storage::WorkspaceRepositorySnapshot) -> bool {
        self.resolved_config_sha256 == snapshot.resolved_config_sha256
            && self.definitions == snapshot.definitions
            && self.sources == snapshot.sources
            && self.git == snapshot.git
    }

    fn into_snapshot(
        self,
        operation_uuid: Uuid,
        actor_uuid: Option<Uuid>,
    ) -> graphforge_storage::WorkspaceRepositorySnapshot {
        graphforge_storage::WorkspaceRepositorySnapshot {
            contract_version: graphforge_storage::WORKSPACE_REPOSITORY_SNAPSHOT_VERSION,
            resolved_config_sha256: self.resolved_config_sha256,
            definitions: self.definitions,
            sources: self.sources,
            git: self.git,
            operation_uuid,
            actor_uuid,
        }
    }
}

#[cfg(not(windows))]
fn sync_directory(path: &Path) -> Result<(), GfError> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| GfError::Storage(error.to_string()))
}

#[cfg(windows)]
fn sync_directory(path: &Path) -> Result<(), GfError> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    OpenOptions::new()
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| GfError::Storage(error.to_string()))
}

fn write_durable(path: &Path, bytes: &[u8]) -> Result<(), GfError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|error| GfError::Storage(error.to_string()))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| GfError::Storage(error.to_string()))?;
    sync_directory(
        path.parent()
            .ok_or_else(|| validation("durable file has no parent"))?,
    )
}

fn rename_durable(from: &Path, to: &Path) -> Result<(), GfError> {
    let from_parent = from
        .parent()
        .ok_or_else(|| validation("rename source has no parent"))?;
    let to_parent = to
        .parent()
        .ok_or_else(|| validation("rename destination has no parent"))?;
    fs::rename(from, to).map_err(|error| GfError::Storage(error.to_string()))?;
    sync_directory(from_parent)?;
    if to_parent != from_parent {
        sync_directory(to_parent)?;
    }
    Ok(())
}

fn create_dir_durable(path: &Path) -> Result<(), GfError> {
    fs::create_dir(path).map_err(|error| GfError::Storage(error.to_string()))?;
    sync_directory(path)?;
    sync_directory(
        path.parent()
            .ok_or_else(|| validation("directory has no parent"))?,
    )
}

fn remove_file_durable(path: &Path) -> Result<(), GfError> {
    fs::remove_file(path).map_err(|error| GfError::Storage(error.to_string()))?;
    sync_directory(
        path.parent()
            .ok_or_else(|| validation("removed file has no parent"))?,
    )
}

/// A safely discovered repository integration context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepositoryContext {
    /// Git worktree root, or the explicitly selected non-Git project directory.
    pub root: PathBuf,
    /// Canonical configuration path.
    pub config_path: PathBuf,
    /// Live embedded project path.
    pub state_path: PathBuf,
    /// Whether discovery found a Git worktree.
    pub git: bool,
}

impl RepositoryContext {
    /// Discover the nearest Git worktree from `start`, falling back to `start` itself.
    pub fn discover(start: impl AsRef<Path>) -> Result<Self, GfError> {
        let supplied = if start.as_ref().is_absolute() {
            start.as_ref().to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|error| GfError::Storage(error.to_string()))?
                .join(start)
        };
        reject_any_symlink(&supplied)?;
        let start = absolute_existing_dir(&supplied)?;
        let output = Command::new("git")
            .args(["-C"])
            .arg(&start)
            .args(["rev-parse", "--show-toplevel"])
            .output()
            .map_err(|error| GfError::Storage(error.to_string()))?;
        let (root, git) = if output.status.success() {
            let text = String::from_utf8(output.stdout)
                .map_err(|_| validation("Git returned a non-UTF-8 worktree path"))?;
            (absolute_existing_dir(Path::new(text.trim()))?, true)
        } else {
            (start, false)
        };
        reject_symlink_components(&root, &root)?;
        Ok(Self {
            config_path: root.join(CONFIG),
            state_path: root.join(".graphforge/state"),
            root,
            git,
        })
    }

    /// Resolve and validate a repository-relative path without following symlinks.
    pub fn contained_path(&self, relative: impl AsRef<Path>) -> Result<PathBuf, GfError> {
        let relative = relative.as_ref();
        if relative.is_absolute()
            || relative.components().any(|part| {
                matches!(
                    part,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(validation("path must be repository-relative and contained"));
        }
        let candidate = self.root.join(relative);
        reject_symlink_components(&self.root, &candidate)?;
        Ok(candidate)
    }

    /// Create the repository namespace, required definitions, ignore block, and live project.
    pub fn init(&self) -> Result<RepositoryInitReceipt, GfError> {
        self.init_without_skills()
    }

    /// Initialize repository-local GraphForge state without installing agent skills.
    ///
    /// The CLI's default `init` path calls this and then installs its verified
    /// embedded bundle. Keeping the filesystem lifecycle separate lets thin
    /// bindings provide the same canonical bundle without duplicating behavior.
    pub fn init_without_skills(&self) -> Result<RepositoryInitReceipt, GfError> {
        reject_symlink_components(&self.root, &self.root.join(".graphforge"))?;
        self.reject_tracked_data()?;
        // Complete every read-only check before the first mutation.
        let ignore = self.render_gitignore()?;
        let config = if self.config_path.exists() {
            self.load_config()?
        } else {
            let config: ProjectConfig = serde_yaml::from_str(DEFAULT_CONFIG)
                .map_err(|error| validation(format!("invalid built-in config: {error}")))?;
            config.validate(self)?;
            config
        };
        for path in config.project.paths() {
            fs::create_dir_all(self.contained_path(path)?)
                .map_err(|error| GfError::Storage(error.to_string()))?;
        }
        for name in ["imports", "exports"] {
            fs::create_dir_all(self.root.join(".graphforge").join(name))
                .map_err(|error| GfError::Storage(error.to_string()))?;
        }
        let created_config = if self.config_path.exists() {
            false
        } else {
            fs::write(&self.config_path, DEFAULT_CONFIG)
                .map_err(|error| GfError::Storage(error.to_string()))?;
            true
        };
        let ignore_changed = Self::write_gitignore(ignore)?;
        let state = self
            .state_path
            .to_str()
            .ok_or_else(|| validation("project path must be valid UTF-8"))?;
        // Storage admission creates or reopens the v1 container. Repository
        // setup must not create the final state target before that gate. Opening
        // it a second time proves that the published project resolves immediately.
        super::GraphForge::new(Some(state))?;
        super::GraphForge::new(Some(state))?;
        Ok(RepositoryInitReceipt {
            root: self.root.clone(),
            created_config,
            ignore_changed,
            state: self.state_path.clone(),
        })
    }

    /// Compare or atomically reconcile the declared repository snapshot.
    ///
    /// Check mode never initializes or mutates project state. Apply mode
    /// requires a caller-owned operation UUID only when drift exists.
    ///
    /// # Errors
    /// Returns a structured validation, idempotency, project, or storage error.
    pub fn sync(&self, request: RepositorySyncRequest) -> Result<RepositorySyncResult, GfError> {
        let desired = self.desired_repository_snapshot()?;
        let current = storage::resolve_project_generation(&self.state_path)?;
        current.validate_complete_participant_inventory()?;
        let current_snapshot = current
            .participant_snapshot(
                storage::WORKSPACE_CAPABILITY_ID,
                storage::WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY,
            )?
            .map(|snapshot| {
                storage::WorkspaceRepositorySnapshot::from_canonical_json(&snapshot.bytes)
            })
            .transpose()?;
        if current_snapshot
            .as_ref()
            .is_some_and(|snapshot| desired.matches(snapshot))
        {
            if !request.check
                && let (Some(requested_operation), Some(snapshot)) =
                    (request.operation_uuid, current_snapshot.as_ref())
                && requested_operation == snapshot.operation_uuid
                && request.actor_uuid != snapshot.actor_uuid
            {
                return Err(GfError::Project {
                    code: graphforge_core::ProjectErrorCode::TransactionConflict,
                    message:
                        "repository sync operation UUID was reused with a different actor identity"
                            .into(),
                });
            }
            let idempotent_replay = !request.check
                && request.operation_uuid.is_some_and(|operation| {
                    current_snapshot
                        .as_ref()
                        .is_some_and(|snapshot| snapshot.operation_uuid == operation)
                });
            return Ok(RepositorySyncResult {
                status: RepositorySyncStatus::InSync,
                prior_generation_uuid: current.generation_uuid(),
                generation_uuid: current.generation_uuid(),
                requested_operation_uuid: (!request.check)
                    .then_some(request.operation_uuid)
                    .flatten(),
                snapshot_operation_uuid: current_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.operation_uuid),
                snapshot_actor_uuid: current_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.actor_uuid),
                idempotent_replay,
                resolved_config_sha256: desired.resolved_config_sha256,
                definitions: desired
                    .definitions
                    .into_iter()
                    .map(RepositoryDefinitionDigest::from)
                    .collect(),
                sources: desired
                    .sources
                    .into_iter()
                    .map(RepositorySourceDigest::from)
                    .collect(),
                git: desired.git.into(),
            });
        }
        if request.check {
            return Ok(RepositorySyncResult {
                status: RepositorySyncStatus::Drift,
                prior_generation_uuid: current.generation_uuid(),
                generation_uuid: current.generation_uuid(),
                requested_operation_uuid: None,
                snapshot_operation_uuid: current_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.operation_uuid),
                snapshot_actor_uuid: current_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.actor_uuid),
                idempotent_replay: false,
                resolved_config_sha256: desired.resolved_config_sha256,
                definitions: desired
                    .definitions
                    .into_iter()
                    .map(RepositoryDefinitionDigest::from)
                    .collect(),
                sources: desired
                    .sources
                    .into_iter()
                    .map(RepositorySourceDigest::from)
                    .collect(),
                git: desired.git.into(),
            });
        }
        let operation_uuid = request
            .operation_uuid
            .ok_or_else(|| validation("sync drift requires an explicit operation UUID"))?;
        if operation_uuid.is_nil() {
            return Err(validation("sync operation UUID must not be nil"));
        }
        if request.actor_uuid.is_some_and(|actor| actor.is_nil()) {
            return Err(validation("sync actor UUID must not be nil"));
        }
        self.publish_repository_snapshot(&current, desired, operation_uuid, request.actor_uuid)
    }

    fn publish_repository_snapshot(
        &self,
        current: &storage::ResolvedProjectGeneration,
        desired: DesiredRepositorySnapshot,
        operation_uuid: Uuid,
        actor_uuid: Option<Uuid>,
    ) -> Result<RepositorySyncResult, GfError> {
        let expected_desired = desired.clone();
        let snapshot = desired.into_snapshot(operation_uuid, actor_uuid);
        let mut participants = current
            .participant_snapshots()?
            .into_iter()
            .filter(|participant| {
                !(participant.capability_id == storage::WORKSPACE_CAPABILITY_ID
                    && participant.record_family_id
                        == storage::WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY)
            })
            .map(repository_snapshot_to_participant)
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot_participant = snapshot.to_project_participant()?;
        let expected_snapshot_sha256 = encode_hex(&Sha256::digest(&snapshot_participant.bytes));
        participants.push(snapshot_participant);
        participants.sort_by(|left, right| {
            (&left.capability_id, &left.record_family_id)
                .cmp(&(&right.capability_id, &right.record_family_id))
        });
        let generation_uuid = repository_sync_generation_uuid(operation_uuid, &participants);
        let publication = storage::ProjectGenerationRequest {
            transaction_uuid: operation_uuid,
            generation_uuid,
            capabilities: current
                .capabilities()
                .into_iter()
                .map(|capability| storage::ProjectCapability {
                    capability_id: capability.capability_id,
                    capability_version: capability.capability_version,
                })
                .collect(),
            participants,
        };
        #[cfg(test)]
        if let Some(hook) = REPOSITORY_SYNC_BEFORE_STAGE_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("repository sync test hook lock poisoned")
            .as_ref()
            .filter(|hook| hook.root == self.root)
            .cloned()
        {
            hook.barrier.wait();
            hook.barrier.wait();
        }
        let expected_parent = current.generation_uuid();
        let receipt = match storage::stage_project_generation(&self.state_path, &publication)? {
            storage::ProjectStageOutcome::AlreadyPublished(receipt) => {
                if receipt.generation_uuid != expected_parent {
                    return Err(idempotency_conflict(
                        "repository sync operation identifies a generation that is no longer authoritative",
                    ));
                }
                receipt
            }
            storage::ProjectStageOutcome::Staged(staged) => staged
                .validate(
                    |participants| {
                        validate_repository_snapshot_inventory(
                            participants,
                            &expected_snapshot_sha256,
                        )
                    },
                    |actual_parent, _| {
                        if actual_parent.generation_uuid() != expected_parent {
                            return Err(validation(
                                "project generation changed before repository sync publication",
                            ));
                        }
                        if self.desired_repository_snapshot()? != expected_desired {
                            return Err(validation("repository definitions changed during sync"));
                        }
                        Ok(())
                    },
                )?
                .publish()?,
        };
        let evidence = expected_desired;
        Ok(RepositorySyncResult {
            status: RepositorySyncStatus::Published,
            prior_generation_uuid: expected_parent,
            generation_uuid: receipt.generation_uuid,
            requested_operation_uuid: Some(operation_uuid),
            snapshot_operation_uuid: Some(operation_uuid),
            snapshot_actor_uuid: actor_uuid,
            idempotent_replay: receipt.idempotent_replay,
            resolved_config_sha256: evidence.resolved_config_sha256,
            definitions: evidence
                .definitions
                .into_iter()
                .map(RepositoryDefinitionDigest::from)
                .collect(),
            sources: evidence
                .sources
                .into_iter()
                .map(RepositorySourceDigest::from)
                .collect(),
            git: evidence.git.into(),
        })
    }

    fn desired_repository_snapshot(&self) -> Result<DesiredRepositorySnapshot, GfError> {
        let config = self.load_config()?;
        let resolved_config = config.resolve()?;
        let resolved_config_sha256 = encode_hex(&Sha256::digest(
            serde_json::to_vec(&resolved_config)
                .map_err(|error| validation(format!("cannot encode resolved config: {error}")))?,
        ));
        let definitions = config
            .project
            .entries()
            .into_iter()
            .map(|(kind, path)| {
                let resolved = self.contained_path(path)?;
                if !resolved.is_dir() {
                    return Err(validation(format!(
                        "declared definition directory is missing: {path}"
                    )));
                }
                Ok(graphforge_storage::WorkspaceRepositoryDefinitionDigest {
                    definition_id: kind.id().into(),
                    sha256: digest_definition_tree(&resolved, kind)?,
                })
            })
            .collect::<Result<Vec<_>, GfError>>()?;
        let mut sources = config
            .sources
            .iter()
            .map(
                |source| graphforge_storage::WorkspaceRepositorySourceDigest {
                    source_id: source.id.clone(),
                    sha256: source.sha256.clone(),
                },
            )
            .collect::<Vec<_>>();
        sources.sort_by(|left, right| left.source_id.cmp(&right.source_id));
        let git = self.git_provenance()?;
        Ok(DesiredRepositorySnapshot {
            resolved_config_sha256,
            definitions,
            sources,
            git: graphforge_storage::WorkspaceRepositoryGitProvenance {
                commit_sha: git.sha,
                dirty: git.dirty,
            },
        })
    }

    /// Remove only ignored repository-local runtime state after explicit confirmation.
    pub fn remove(&self, confirmed: bool) -> Result<RepositoryRemoveReceipt, GfError> {
        if !confirmed {
            return Err(validation("remove requires explicit confirmation"));
        }
        self.reject_tracked_data()?;
        let target = self.contained_path(".graphforge/state")?;
        if target == self.root || target == self.root.join(".graphforge") {
            return Err(validation("refusing unsafe remove target"));
        }
        let removed = if target.exists() {
            reject_symlink_components(&self.root, &target)?;
            storage::remove_durable_project_root(&target)?;
            true
        } else {
            false
        };
        Ok(RepositoryRemoveReceipt { target, removed })
    }

    fn git_provenance(&self) -> Result<GitProvenance, GfError> {
        if !self.git {
            return Ok(GitProvenance {
                sha: None,
                dirty: false,
            });
        }
        let run = |args: &[&str]| -> Result<String, GfError> {
            let output = Command::new("git")
                .arg("-C")
                .arg(&self.root)
                .args(args)
                .output()
                .map_err(|error| GfError::Storage(error.to_string()))?;
            if !output.status.success() {
                return Err(validation("unable to inspect Git provenance"));
            }
            String::from_utf8(output.stdout).map_err(|_| validation("Git output is not UTF-8"))
        };
        let head = Command::new("git")
            .arg("-C")
            .arg(&self.root)
            .args(["rev-parse", "--verify", "HEAD"])
            .output()
            .map_err(|error| GfError::Storage(error.to_string()))?;
        let sha = if head.status.success() {
            Some(
                String::from_utf8(head.stdout)
                    .map_err(|_| validation("Git output is not UTF-8"))?
                    .trim()
                    .to_owned(),
            )
        } else {
            None
        };
        Ok(GitProvenance {
            sha,
            dirty: !run(&["status", "--porcelain=v1"])?.is_empty(),
        })
    }

    fn reject_tracked_data(&self) -> Result<(), GfError> {
        if !self.git {
            return Ok(());
        }
        let output = Command::new("git")
            .arg("-C")
            .arg(&self.root)
            .args(["ls-files", "-z"])
            .output()
            .map_err(|error| GfError::Storage(error.to_string()))?;
        if !output.status.success() {
            return Err(validation("unable to inspect the Git index"));
        }
        let unsafe_path = output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
            .any(|path| {
                let path = String::from_utf8_lossy(path).replace('\\', "/");
                path.starts_with(".graphforge/state/")
                    || path.starts_with(".graphforge/imports/")
                    || path.starts_with(".graphforge/exports/")
                    || path.starts_with(".graphforge/snapshots/")
                    || path.starts_with(".graphforge/seeds/materialized/")
                    || [
                        ".arrow", ".parquet", ".db", ".sqlite", ".sqlite3", ".duckdb",
                    ]
                    .iter()
                    .any(|extension| path.to_ascii_lowercase().ends_with(extension))
            });
        if unsafe_path {
            return Err(validation(
                "graph data is tracked by Git; untrack it before initialization",
            ));
        }
        Ok(())
    }

    fn render_gitignore(&self) -> Result<(PathBuf, String, String), GfError> {
        let path = self.root.join(".gitignore");
        reject_symlink_components(&self.root, &path)?;
        let original = match fs::read_to_string(&path) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(GfError::Storage(format!("cannot read .gitignore: {error}"))),
        };
        let newline = if original.contains("\r\n") {
            "\r\n"
        } else {
            "\n"
        };
        let mut lines: Vec<&str> = original.lines().collect();
        let starts: Vec<_> = lines
            .iter()
            .enumerate()
            .filter(|(_, line)| **line == IGNORE_START)
            .collect();
        let ends: Vec<_> = lines
            .iter()
            .enumerate()
            .filter(|(_, line)| **line == IGNORE_END)
            .collect();
        if starts.len() > 1 || ends.len() > 1 || starts.len() != ends.len() {
            return Err(validation("malformed GraphForge managed .gitignore block"));
        }
        if let (Some((start, _)), Some((end, _))) = (starts.first(), ends.first()) {
            if start >= end {
                return Err(validation("malformed GraphForge managed .gitignore block"));
            }
            lines.drain(*start..=*end);
        }
        while lines.last().is_some_and(|line| line.is_empty()) {
            lines.pop();
        }
        let mut next = lines.join(newline);
        if !next.is_empty() {
            next.push_str(newline);
            next.push_str(newline);
        }
        next.push_str(IGNORE_START);
        next.push_str(newline);
        next.push_str(&IGNORE_LINES.join(newline));
        next.push_str(newline);
        next.push_str(IGNORE_END);
        next.push_str(newline);
        Ok((path, original, next))
    }

    fn write_gitignore(rendered: (PathBuf, String, String)) -> Result<bool, GfError> {
        let (path, original, next) = rendered;
        if next == original {
            return Ok(false);
        }
        let mut staged = tempfile::NamedTempFile::new_in(
            path.parent()
                .ok_or_else(|| validation(".gitignore has no parent"))?,
        )
        .map_err(|error| GfError::Storage(error.to_string()))?;
        staged
            .write_all(next.as_bytes())
            .map_err(|error| GfError::Storage(error.to_string()))?;
        if let Ok(metadata) = fs::metadata(&path) {
            staged
                .as_file()
                .set_permissions(metadata.permissions())
                .map_err(|error| GfError::Storage(error.to_string()))?;
        }
        staged
            .as_file()
            .sync_all()
            .map_err(|error| GfError::Storage(error.to_string()))?;
        staged
            .persist(&path)
            .map_err(|error| GfError::Storage(error.error.to_string()))?;
        Ok(true)
    }
}

fn absolute_existing_dir(path: &Path) -> Result<PathBuf, GfError> {
    let absolute = path
        .canonicalize()
        .map_err(|error| GfError::Storage(error.to_string()))?;
    if !absolute.is_dir() {
        return Err(validation("project directory must be a directory"));
    }
    Ok(absolute)
}

fn reject_any_symlink(path: &Path) -> Result<(), GfError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| GfError::Storage(error.to_string()))?;
    if metadata.file_type().is_symlink() {
        return Err(validation(
            "the project directory itself must not be a symlink",
        ));
    }
    Ok(())
}

fn reject_symlink_components(root: &Path, path: &Path) -> Result<(), GfError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| validation("path escapes repository root"))?;
    let mut cursor = root.to_path_buf();
    for component in relative.components() {
        cursor.push(component);
        match fs::symlink_metadata(&cursor) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(validation("symlinks are not allowed in managed paths"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(GfError::Storage(error.to_string())),
        }
    }
    Ok(())
}

fn stable_id(value: &str) -> Result<(), GfError> {
    let bytes = value.as_bytes();
    let valid = bytes.first().is_some_and(u8::is_ascii_lowercase)
        && bytes.len() <= 64
        && bytes[1..]
            .iter()
            .try_fold(false, |separator, byte| {
                if byte.is_ascii_lowercase() || byte.is_ascii_digit() {
                    Some(false)
                } else if matches!(byte, b'-' | b'_') && !separator {
                    Some(true)
                } else {
                    None
                }
            })
            .is_some_and(|trailing_separator| !trailing_separator);
    if valid {
        Ok(())
    } else {
        Err(validation("invalid stable id"))
    }
}
fn digest(value: &str) -> Result<(), GfError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(validation("invalid sha256 digest"))
    }
}

fn bounded(value: &str, minimum: usize, maximum: usize, name: &str) -> Result<(), GfError> {
    if (minimum..=maximum).contains(&value.len()) {
        Ok(())
    } else {
        Err(validation(format!("{name} exceeds contract bounds")))
    }
}

fn uri_has_inline_credentials(value: &str) -> bool {
    value.split_once("://").is_some_and(|(_, remainder)| {
        remainder
            .split('/')
            .next()
            .is_some_and(|host| host.contains('@'))
    })
}

/// One immutable file from a packaged project-skill bundle.
#[derive(Debug, Clone, Copy)]
pub struct SkillBundleFile<'a> {
    /// Slash-separated path relative to the bundle and `.agents/skills/`.
    pub path: &'a str,
    /// Exact packaged bytes.
    pub bytes: &'a [u8],
}

/// A packaged project-skill bundle supplied by the CLI or a thin binding.
#[derive(Debug, Clone, Copy)]
pub struct SkillBundle<'a> {
    /// Exact canonical `project-skills/manifest.json` bytes.
    pub manifest: &'a [u8],
    /// Exact payload files named by the manifest.
    pub files: &'a [SkillBundleFile<'a>],
}

/// Stable project-local skill lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillStatus {
    /// No managed files or manifest are installed.
    Missing,
    /// Installed files match the packaged bundle.
    Current,
    /// Installed files are unedited but describe another bundle version.
    Outdated,
    /// User-owned or edited files overlap the managed bundle.
    Conflict,
}

/// Result of inspecting project-local managed skills.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkillStatusReceipt {
    /// Current lifecycle state.
    pub status: SkillStatus,
    /// Installed bundle version, when a valid managed manifest exists.
    pub bundle_version: Option<u32>,
    /// Version packaged with this CLI.
    pub expected_bundle_version: u32,
    /// Sorted managed paths that no longer match their recorded hashes.
    pub edited_files: Vec<String>,
}

/// Result of a project-local skill mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkillMutationReceipt {
    /// Whether the command changed the repository.
    pub changed: bool,
    /// Bundle version affected by the command.
    pub bundle_version: Option<u32>,
    /// Number of currently installed managed files.
    pub installed_files: usize,
}

/// Bounded repository provenance recorded by sync.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GitProvenance {
    /// Exact Git commit when discovery occurs, when applicable.
    pub sha: Option<String>,
    /// Whether tracked files differ from the selected commit.
    pub dirty: bool,
}

/// Versioned, deterministic result of static target validation.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InfraValidationResult {
    /// Frozen receipt contract.
    pub contract: &'static str,
    /// SHA-256 of canonical resolved configuration JSON.
    pub resolved_config_sha256: String,
    /// Selected resolved target; contains references and requirements, never payloads.
    pub target: Value,
    /// Structural and semantic configuration validity.
    pub static_validity: InfraStaticValidity,
    /// Provider-neutral infrastructure intent validated without provisioning.
    pub planned_infrastructure: InfraPlan,
    /// Live transport state, deliberately not checked by static validation.
    pub connectivity: InfraNotChecked,
    /// Live health/readiness state, deliberately not checked by static validation.
    pub readiness: InfraNotChecked,
    /// Declared capability requirements; runtime compatibility remains unverified.
    pub capability_compatibility: InfraCapabilityCompatibility,
}

/// Static-validity state.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InfraStaticValidity {
    /// Always `valid` for a successful receipt.
    pub status: &'static str,
}

/// Provider-neutral planned infrastructure state.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InfraPlan {
    /// Always `validated` for a successful receipt.
    pub status: &'static str,
    /// Always `none`; this operation never provisions.
    pub mutation: &'static str,
    /// Embedded, local, or separately owned external deployment.
    pub ownership: String,
    /// Embedded/service/worker/job/host target role.
    pub kind: String,
    /// Process/container/host execution topology.
    pub execution: String,
    /// Long-running or on-demand scheduling topology.
    pub scheduling: String,
    /// Declared provider-neutral replica count.
    pub replicas: u64,
    /// Pinned artifact kind, version, and checksum.
    pub artifact: Value,
}

/// State that static validation deliberately does not inspect.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InfraNotChecked {
    /// Always `not_checked`.
    pub status: &'static str,
}

/// Declared capability requirements, distinct from live compatibility.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct InfraCapabilityCompatibility {
    /// Always `requirements_declared`.
    pub status: &'static str,
    /// Ordered `{id, version}` requirements from resolved configuration.
    pub requirements: Vec<Value>,
}

/// Successful initialization output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepositoryInitReceipt {
    /// Discovered worktree or project root.
    pub root: PathBuf,
    /// Whether initialization created the default configuration.
    pub created_config: bool,
    /// Whether initialization changed `.gitignore`.
    pub ignore_changed: bool,
    /// Live embedded project location.
    pub state: PathBuf,
}
/// Digest evidence for one declared repository definition family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepositoryDefinitionDigest {
    /// Stable definition family identifier; never a repository path.
    pub definition_id: String,
    /// Canonical SHA-256 digest of the validated definition tree.
    pub sha256: String,
}

impl From<graphforge_storage::WorkspaceRepositoryDefinitionDigest> for RepositoryDefinitionDigest {
    fn from(value: graphforge_storage::WorkspaceRepositoryDefinitionDigest) -> Self {
        Self {
            definition_id: value.definition_id,
            sha256: value.sha256,
        }
    }
}

/// Digest evidence for one explicitly declared external source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepositorySourceDigest {
    /// Stable source identifier; never a source URI or path.
    pub source_id: String,
    /// Caller-declared canonical SHA-256 digest.
    pub sha256: String,
}

impl From<graphforge_storage::WorkspaceRepositorySourceDigest> for RepositorySourceDigest {
    fn from(value: graphforge_storage::WorkspaceRepositorySourceDigest) -> Self {
        Self {
            source_id: value.source_id,
            sha256: value.sha256,
        }
    }
}

impl From<graphforge_storage::WorkspaceRepositoryGitProvenance> for GitProvenance {
    fn from(value: graphforge_storage::WorkspaceRepositoryGitProvenance) -> Self {
        Self {
            sha: value.commit_sha,
            dirty: value.dirty,
        }
    }
}

/// Repository reconciliation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepositorySyncRequest {
    /// Validate and compare without publishing.
    pub check: bool,
    /// Caller-owned idempotency identity, required only when applying drift.
    pub operation_uuid: Option<Uuid>,
    /// Optional caller-owned actor identity.
    pub actor_uuid: Option<Uuid>,
}

/// Deterministic repository reconciliation state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepositorySyncStatus {
    /// Desired and authoritative repository snapshots match.
    InSync,
    /// Check mode found a difference and did not mutate state.
    Drift,
    /// Apply mode atomically published one complete generation.
    Published,
}

/// Repository reconciliation result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepositorySyncResult {
    /// Deterministic reconciliation outcome.
    pub status: RepositorySyncStatus,
    /// Generation authoritative before this call.
    pub prior_generation_uuid: Uuid,
    /// Generation authoritative after this call.
    pub generation_uuid: Uuid,
    /// Operation requested for this mutating call; always absent in check mode.
    pub requested_operation_uuid: Option<Uuid>,
    /// Operation identity actually recorded in the authoritative snapshot.
    pub snapshot_operation_uuid: Option<Uuid>,
    /// Actor identity actually recorded in the authoritative snapshot.
    pub snapshot_actor_uuid: Option<Uuid>,
    /// Whether the current desired state was already published by this operation.
    pub idempotent_replay: bool,
    /// SHA-256 digest of the canonical, secret-free resolved configuration.
    pub resolved_config_sha256: String,
    /// Ordered digest evidence for validated definition families.
    pub definitions: Vec<RepositoryDefinitionDigest>,
    /// Ordered identifiers and digests for explicitly declared external sources.
    pub sources: Vec<RepositorySourceDigest>,
    /// Bounded Git provenance; never repository contents or unrestricted paths.
    pub git: GitProvenance,
}
/// Successful local-state removal output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepositoryRemoveReceipt {
    /// Validated state-only deletion target.
    pub target: PathBuf,
    /// Whether an existing state directory was removed.
    pub removed: bool,
}

const DEFAULT_CONFIG: &str = "schema_version: 1\nproject:\n  ontology: .graphforge/ontology\n  schemas: .graphforge/schemas\n  seeds: .graphforge/seeds\n  migrations: .graphforge/migrations\ntargets:\n  local:\n    kind: embedded\n    artifact:\n      kind: native_binary\n      version: 0.5.0-dev\n      sha256: 0000000000000000000000000000000000000000000000000000000000000000\n    write: { mode: single_writer }\n    storage: { kind: local, persistent: true }\n";

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    /// Assert `writer.lock` and `checkpoints.lock` are free for exclusive acquire.
    ///
    /// Does not touch generation `lease.lock` files; a live GraphForge may hold those.
    fn assert_project_mutation_locks_free(state_path: &Path, phase: &str) {
        use fs4::TryLockError;

        let lock_root = state_path.join("locks");
        for name in ["writer.lock", "checkpoints.lock"] {
            let path = lock_root.join(name);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .unwrap_or_else(|error| panic!("{phase}: open {name}: {error}"));
            let acquired = match FileExt::try_lock(&file) {
                Ok(()) => true,
                Err(TryLockError::WouldBlock) => false,
                Err(TryLockError::Error(error)) => {
                    panic!("{phase}: try_lock {name}: {error}")
                }
            };
            assert!(
                acquired,
                "{phase}: {name} was still held (issue #275 probe)"
            );
            FileExt::unlock(&file)
                .unwrap_or_else(|error| panic!("{phase}: unlock {name}: {error}"));
        }
    }

    fn staged_repository_snapshot(content_sha256: &str) -> graphforge_storage::StagedParticipant {
        graphforge_storage::StagedParticipant {
            capability_id: graphforge_storage::WORKSPACE_CAPABILITY_ID.into(),
            capability_version: graphforge_storage::WORKSPACE_CAPABILITY_VERSION,
            record_family_id: graphforge_storage::WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY.into(),
            record_version: graphforge_storage::WORKSPACE_REPOSITORY_SNAPSHOT_VERSION,
            relative_path: "workspace/repository_snapshot.json".into(),
            encoding: "json".into(),
            byte_length: 1,
            row_count: 1,
            schema_fingerprint: encode_hex(&Sha256::digest("workspace/repository_snapshot@1")),
            content_sha256: content_sha256.into(),
        }
    }

    #[test]
    fn repository_snapshot_inventory_rejects_cardinality_and_metadata_drift() {
        assert_eq!(
            validate_repository_snapshot_inventory(&[], "content")
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        let valid = staged_repository_snapshot("content");
        assert_eq!(
            validate_repository_snapshot_inventory(&[valid.clone(), valid.clone()], "content")
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        for malformed in [
            graphforge_storage::StagedParticipant {
                capability_version: u32::MAX,
                ..valid.clone()
            },
            graphforge_storage::StagedParticipant {
                encoding: "arrow".into(),
                ..valid.clone()
            },
            graphforge_storage::StagedParticipant {
                content_sha256: "wrong".into(),
                ..valid
            },
        ] {
            assert_eq!(
                validate_repository_snapshot_inventory(&[malformed], "content")
                    .unwrap_err()
                    .code(),
                "GF_VALIDATION"
            );
        }
    }

    #[test]
    fn relative_discovery_and_source_digest_conversion_preserve_inputs() {
        let root = tempdir().unwrap();
        assert!(
            Command::new("git")
                .arg("init")
                .arg("-q")
                .arg(root.path())
                .status()
                .unwrap()
                .success()
        );
        let previous = std::env::current_dir().unwrap();
        std::env::set_current_dir(root.path()).unwrap();
        let relative = tempfile::tempdir_in(".").unwrap();
        let name = relative.path().file_name().unwrap().to_os_string();
        let discovery = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let context = RepositoryContext::discover(&name).unwrap();
            assert!(context.git);
            assert!(relative.path().starts_with(&context.root));
            context
        }));
        std::env::set_current_dir(&previous).unwrap();
        discovery.unwrap();
        let converted =
            RepositorySourceDigest::from(graphforge_storage::WorkspaceRepositorySourceDigest {
                source_id: "catalog".into(),
                sha256: "a".repeat(64),
            });
        assert_eq!(converted.source_id, "catalog");
        assert_eq!(converted.sha256, "a".repeat(64));
    }

    #[test]
    fn desired_snapshot_rejects_a_declared_missing_definition_directory() {
        let dir = tempdir().unwrap();
        let context = RepositoryContext::discover(dir.path()).unwrap();
        context.init_without_skills().unwrap();
        fs::remove_dir_all(dir.path().join(".graphforge/ontology")).unwrap();
        let error = context.desired_repository_snapshot().unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION");
        assert!(
            error
                .to_string()
                .contains("declared definition directory is missing")
        );
    }

    #[test]
    fn init_is_idempotent_and_preserves_gitignore() {
        let root = tempdir().unwrap();
        fs::write(root.path().join(".gitignore"), "target/\n").unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        assert!(context.init().unwrap().ignore_changed);
        assert!(!context.init().unwrap().ignore_changed);
        let ignore = fs::read_to_string(root.path().join(".gitignore")).unwrap();
        assert!(ignore.starts_with("target/\n"));
        assert_eq!(IGNORE_LINES.len(), 4);
        for line in IGNORE_LINES {
            assert_eq!(ignore.matches(line).count(), 1);
        }
        assert!(root.path().join(".graphforge/graphforge.yaml").is_file());
    }

    #[test]
    fn admission_lock_persists_without_dirtying_git_after_init_or_remove() {
        let root = tempdir().unwrap();
        assert!(
            Command::new("git")
                .arg("init")
                .arg("-q")
                .arg(root.path())
                .status()
                .unwrap()
                .success()
        );
        let context = RepositoryContext::discover(root.path()).unwrap();
        context.init_without_skills().unwrap();

        let admission_locks = || {
            fs::read_dir(root.path().join(".graphforge"))
                .unwrap()
                .filter_map(|entry| {
                    let entry = entry.unwrap();
                    let name = entry.file_name().into_string().ok()?;
                    (name.starts_with(".graphforge-admission-") && name.ends_with(".lock"))
                        .then_some(entry.path())
                })
                .collect::<Vec<_>>()
        };
        let locks = admission_locks();
        assert_eq!(locks.len(), 1);
        let lock = locks[0].clone();
        assert!(lock.is_file());
        let ignored = Command::new("git")
            .arg("-C")
            .arg(root.path())
            .args(["check-ignore", "-v"])
            .arg(&lock)
            .output()
            .unwrap();
        assert!(ignored.status.success());
        assert!(
            String::from_utf8(ignored.stdout)
                .unwrap()
                .contains("/.graphforge/.graphforge-admission-*.lock")
        );

        assert!(
            Command::new("git")
                .arg("-C")
                .arg(root.path())
                .args(["add", "-A"])
                .status()
                .unwrap()
                .success()
        );
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(root.path())
                .args([
                    "-c",
                    "user.name=GraphForge Test",
                    "-c",
                    "user.email=test@example.invalid",
                    "commit",
                    "-qm",
                    "fixture",
                ])
                .status()
                .unwrap()
                .success()
        );
        let git_status = || {
            Command::new("git")
                .arg("-C")
                .arg(root.path())
                .args(["status", "--porcelain=v1"])
                .output()
                .unwrap()
                .stdout
        };
        assert!(git_status().is_empty());

        assert!(!context.init_without_skills().unwrap().ignore_changed);
        assert_eq!(admission_locks(), [lock.clone()]);
        assert!(git_status().is_empty());

        assert!(context.remove(true).unwrap().removed);
        assert!(!context.state_path.exists());
        assert_eq!(admission_locks(), [lock]);
        assert!(git_status().is_empty());
    }

    #[test]
    fn repository_init_delegates_state_target_creation_to_storage_admission() {
        let root = tempdir().unwrap();
        let mut context = RepositoryContext::discover(root.path()).unwrap();
        fs::create_dir_all(root.path().join(".graphforge/hop")).unwrap();
        context.state_path = root.path().join(".graphforge/hop/../state");

        let error = context.init_without_skills().unwrap_err();

        assert_eq!(error.code(), "GF_UNSUPPORTED_FILESYSTEM");
        assert!(!root.path().join(".graphforge/state").exists());
    }

    #[test]
    fn containment_and_symlinks_fail_closed() {
        let root = tempdir().unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        for path in ["../outside", "/absolute"] {
            let error = context.contained_path(path).unwrap_err();
            assert_eq!(error.code(), "GF_VALIDATION");
            assert_eq!(
                error.to_string(),
                "validation error: path must be repository-relative and contained"
            );
        }
        let missing = context.load_config().unwrap_err();
        assert_eq!(missing.code(), "GF_IO");
        assert!(missing.to_string().contains("cannot read"));
        fs::create_dir_all(root.path().join(".graphforge")).unwrap();
        fs::write(root.path().join(".graphforge/graphforge.yaml"), "[").unwrap();
        let malformed = context.load_config().unwrap_err();
        assert_eq!(malformed.code(), "GF_VALIDATION");
        assert!(malformed.to_string().contains("invalid graphforge.yaml"));
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(root.path(), root.path().join("linked")).unwrap();
            assert!(context.contained_path("linked/value").is_err());
            let parent = tempdir().unwrap();
            std::os::unix::fs::symlink(root.path(), parent.path().join("repo-link")).unwrap();
            assert!(RepositoryContext::discover(parent.path().join("repo-link")).is_err());
        }
    }

    #[test]
    fn remove_never_deletes_tracked_definitions() {
        let root = tempdir().unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        context.init().unwrap();
        fs::write(root.path().join(".graphforge/ontology/keep.yaml"), "keep").unwrap();
        assert!(context.remove(false).is_err());
        assert!(context.remove(true).unwrap().removed);
        assert!(root.path().join(".graphforge/ontology/keep.yaml").is_file());
        assert!(!root.path().join(".graphforge/state").exists());
    }

    #[test]
    fn remove_rejects_traversal_without_deleting_the_project() {
        let root = tempdir().unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        context.init_without_skills().unwrap();
        let current = fs::read(context.state_path.join(storage::CURRENT_FILE)).unwrap();
        fs::create_dir(root.path().join("hop")).unwrap();
        let mut traversed = context.clone();
        traversed.root = context.root.join("hop/..");

        let error = traversed.remove(true).unwrap_err();

        assert_eq!(error.code(), "GF_UNSUPPORTED_FILESYSTEM");
        assert_eq!(
            fs::read(context.state_path.join(storage::CURRENT_FILE)).unwrap(),
            current
        );
    }

    #[test]
    fn discovery_uses_the_nearest_git_worktree_and_rejects_tracked_data() {
        let root = tempdir().unwrap();
        assert!(
            Command::new("git")
                .arg("init")
                .arg("-q")
                .arg(root.path())
                .status()
                .unwrap()
                .success()
        );
        fs::create_dir_all(root.path().join("nested/repo path")).unwrap();
        let context = RepositoryContext::discover(root.path().join("nested/repo path")).unwrap();
        assert_eq!(context.root, root.path().canonicalize().unwrap());
        fs::create_dir_all(root.path().join(".graphforge/state")).unwrap();
        fs::write(root.path().join(".graphforge/state/data.parquet"), "data").unwrap();
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(root.path())
                .args(["add", "-f", ".graphforge/state/data.parquet"])
                .status()
                .unwrap()
                .success()
        );
        assert!(context.init().is_err());
        assert!(context.remove(true).is_err());
    }

    #[test]
    fn non_utf8_gitignore_fails_before_mutation() {
        let root = tempdir().unwrap();
        fs::write(root.path().join(".gitignore"), [0xff, 0xfe]).unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        assert!(context.init().is_err());
        assert!(!root.path().join(".graphforge").exists());
        assert_eq!(
            fs::read(root.path().join(".gitignore")).unwrap(),
            [0xff, 0xfe]
        );
    }

    #[test]
    fn crlf_gitignore_is_preserved() {
        let root = tempdir().unwrap();
        fs::write(root.path().join(".gitignore"), "target/\r\n").unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        context.init().unwrap();
        let bytes = fs::read(root.path().join(".gitignore")).unwrap();
        assert!(bytes.windows(2).any(|pair| pair == b"\r\n"));
        assert!(!bytes.windows(2).any(|pair| pair == b"\n\n"));
    }

    #[test]
    fn unborn_git_repository_has_null_sha_and_dirty_definitions() {
        let root = tempdir().unwrap();
        assert!(
            Command::new("git")
                .arg("init")
                .arg("-q")
                .arg(root.path())
                .status()
                .unwrap()
                .success()
        );
        let context = RepositoryContext::discover(root.path()).unwrap();
        context.init().unwrap();
        let result = context
            .sync(RepositorySyncRequest {
                check: true,
                operation_uuid: None,
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(result.git.sha, None);
        assert!(result.git.dirty);
        assert_eq!(result.definitions.len(), 4);
    }

    #[test]
    fn repository_sync_checks_applies_replays_conflicts_and_preserves_authority() {
        let root = tempdir().unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        context.init_without_skills().unwrap();
        let initial = graphforge_storage::resolve_project_generation(&context.state_path).unwrap();
        let initial_uuid = initial.generation_uuid();
        let initial_participants = initial.participant_snapshots().unwrap();
        let initial_ontology = initial
            .participant_snapshot(
                graphforge_storage::WORKSPACE_CAPABILITY_ID,
                graphforge_storage::WORKSPACE_ONTOLOGY_FAMILY,
            )
            .unwrap()
            .unwrap()
            .bytes;
        let initial_configuration = initial
            .participant_snapshot(
                graphforge_storage::WORKSPACE_CAPABILITY_ID,
                graphforge_storage::WORKSPACE_CONFIGURATION_FAMILY,
            )
            .unwrap()
            .unwrap()
            .bytes;
        let generation_count = || {
            fs::read_dir(context.state_path.join("generations"))
                .unwrap()
                .count()
        };
        let initial_count = generation_count();

        let check = context
            .sync(RepositorySyncRequest {
                check: true,
                operation_uuid: None,
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(check.status, RepositorySyncStatus::Drift);
        assert_eq!(check.generation_uuid, initial_uuid);
        assert_eq!(check.requested_operation_uuid, None);
        assert_eq!(check.snapshot_operation_uuid, None);
        assert_eq!(check.resolved_config_sha256.len(), 64);
        assert_eq!(
            check
                .definitions
                .iter()
                .map(|definition| definition.definition_id.as_str())
                .collect::<Vec<_>>(),
            ["migrations", "ontology", "schemas", "seeds"]
        );
        assert!(check.sources.is_empty());
        assert_eq!(
            check.git,
            GitProvenance {
                sha: None,
                dirty: false
            }
        );
        assert_eq!(generation_count(), initial_count);
        assert_eq!(
            context
                .sync(RepositorySyncRequest {
                    check: false,
                    operation_uuid: None,
                    actor_uuid: None,
                })
                .unwrap_err()
                .code(),
            "GF_VALIDATION"
        );
        assert_eq!(generation_count(), initial_count);

        let operation = Uuid::from_bytes([41; 16]);
        let actor = Uuid::from_bytes([42; 16]);
        let applied = context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(operation),
                actor_uuid: Some(actor),
            })
            .unwrap();
        assert_eq!(applied.status, RepositorySyncStatus::Published);
        assert_eq!(applied.requested_operation_uuid, Some(operation));
        assert_eq!(applied.snapshot_operation_uuid, Some(operation));
        assert_eq!(applied.snapshot_actor_uuid, Some(actor));
        assert_eq!(applied.resolved_config_sha256, check.resolved_config_sha256);
        assert_eq!(applied.definitions, check.definitions);
        assert_eq!(applied.sources, check.sources);
        assert_eq!(applied.git, check.git);
        assert_ne!(applied.generation_uuid, initial_uuid);
        assert_eq!(generation_count(), initial_count + 1);

        let published =
            graphforge_storage::resolve_project_generation(&context.state_path).unwrap();
        assert_eq!(published.generation_uuid(), applied.generation_uuid);
        assert_eq!(
            published
                .participant_snapshot(
                    graphforge_storage::WORKSPACE_CAPABILITY_ID,
                    graphforge_storage::WORKSPACE_ONTOLOGY_FAMILY,
                )
                .unwrap()
                .unwrap()
                .bytes,
            initial_ontology
        );
        assert_eq!(
            published
                .participant_snapshot(
                    graphforge_storage::WORKSPACE_CAPABILITY_ID,
                    graphforge_storage::WORKSPACE_CONFIGURATION_FAMILY,
                )
                .unwrap()
                .unwrap()
                .bytes,
            initial_configuration
        );
        let stored_snapshot = published
            .participant_snapshot(
                graphforge_storage::WORKSPACE_CAPABILITY_ID,
                graphforge_storage::WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY,
            )
            .unwrap()
            .unwrap();
        let stored_snapshot = graphforge_storage::WorkspaceRepositorySnapshot::from_canonical_json(
            &stored_snapshot.bytes,
        )
        .unwrap();
        assert_eq!(stored_snapshot.operation_uuid, operation);
        assert_eq!(stored_snapshot.actor_uuid, Some(actor));
        let carried = published
            .participant_snapshots()
            .unwrap()
            .into_iter()
            .filter(|participant| {
                participant.record_family_id
                    != graphforge_storage::WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY
            })
            .collect::<Vec<_>>();
        assert_eq!(carried, initial_participants);

        let replay = context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(operation),
                actor_uuid: Some(actor),
            })
            .unwrap();
        assert_eq!(replay.status, RepositorySyncStatus::InSync);
        assert!(replay.idempotent_replay);
        assert_eq!(replay.requested_operation_uuid, Some(operation));
        assert_eq!(replay.snapshot_operation_uuid, Some(operation));
        assert_eq!(replay.generation_uuid, applied.generation_uuid);
        assert_eq!(generation_count(), initial_count + 1);
        let actor_conflict = context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(operation),
                actor_uuid: Some(Uuid::from_bytes([46; 16])),
            })
            .unwrap_err();
        assert_eq!(actor_conflict.code(), "GF_IDEMPOTENCY_CONFLICT");
        assert_eq!(generation_count(), initial_count + 1);

        let unused_operation = Uuid::from_bytes([44; 16]);
        let unused_actor = Uuid::from_bytes([45; 16]);
        let no_op = context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(unused_operation),
                actor_uuid: Some(unused_actor),
            })
            .unwrap();
        assert_eq!(no_op.status, RepositorySyncStatus::InSync);
        assert!(!no_op.idempotent_replay);
        assert_eq!(no_op.requested_operation_uuid, Some(unused_operation));
        assert_eq!(no_op.snapshot_operation_uuid, Some(operation));
        assert_eq!(no_op.snapshot_actor_uuid, Some(actor));
        assert_eq!(generation_count(), initial_count + 1);
        let check_with_ignored_identities = context
            .sync(RepositorySyncRequest {
                check: true,
                operation_uuid: Some(unused_operation),
                actor_uuid: Some(unused_actor),
            })
            .unwrap();
        assert_eq!(
            check_with_ignored_identities.status,
            RepositorySyncStatus::InSync
        );
        assert_eq!(check_with_ignored_identities.requested_operation_uuid, None);
        assert!(!check_with_ignored_identities.idempotent_replay);
        assert_eq!(
            check_with_ignored_identities.snapshot_operation_uuid,
            Some(operation)
        );

        fs::write(
            root.path()
                .join(".graphforge/ontology")
                .join("repository-sync-test.yaml"),
            "version: 1\n",
        )
        .unwrap();
        let drift = context
            .sync(RepositorySyncRequest {
                check: true,
                operation_uuid: None,
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(drift.status, RepositorySyncStatus::Drift);
        assert_eq!(generation_count(), initial_count + 1);
        let conflict = context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(operation),
                actor_uuid: Some(actor),
            })
            .unwrap_err();
        assert_eq!(conflict.code(), "GF_IDEMPOTENCY_CONFLICT");
        assert_eq!(
            graphforge_storage::resolve_project_generation(&context.state_path)
                .unwrap()
                .generation_uuid(),
            applied.generation_uuid
        );

        let second = context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(Uuid::from_bytes([43; 16])),
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(second.status, RepositorySyncStatus::Published);
        fs::remove_file(
            root.path()
                .join(".graphforge/ontology")
                .join("repository-sync-test.yaml"),
        )
        .unwrap();
        let stale_replay = context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(operation),
                actor_uuid: Some(actor),
            })
            .unwrap_err();
        assert_eq!(stale_replay.code(), "GF_IDEMPOTENCY_CONFLICT");
        assert_eq!(
            graphforge_storage::resolve_project_generation(&context.state_path)
                .unwrap()
                .generation_uuid(),
            second.generation_uuid
        );
        assert_eq!(
            context
                .sync(RepositorySyncRequest {
                    check: true,
                    operation_uuid: None,
                    actor_uuid: None,
                })
                .unwrap()
                .status,
            RepositorySyncStatus::Drift
        );
        fs::write(
            root.path()
                .join(".graphforge/ontology")
                .join("repository-sync-test.yaml"),
            "version: 1\n",
        )
        .unwrap();
        let reopened = RepositoryContext::discover(root.path()).unwrap();
        let reopened_check = reopened
            .sync(RepositorySyncRequest {
                check: true,
                operation_uuid: None,
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(reopened_check.status, RepositorySyncStatus::InSync);
        assert_eq!(reopened_check.generation_uuid, second.generation_uuid);
    }

    #[test]
    fn repository_sync_tracks_ontology_definitions_without_changing_authority() {
        let root = tempdir().unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        context.init_without_skills().unwrap();
        let ontology_path = root.path().join(".graphforge/ontology/authority.yaml");
        fs::write(
            &ontology_path,
            "ontology_id: repository-authority\nversion: \"1\"\nentity_types:\n  - name: Person\n    abstract: false\nrelation_types: []\n",
        )
        .unwrap();
        let mut graph = crate::GraphForge::new(Some(context.state_path.to_str().unwrap())).unwrap();
        graph
            .adopt_ontology(crate::AdoptOntologyRequest {
                context: crate::WriteContext {
                    operation_uuid: crate::OperationId(Uuid::from_u128(101)),
                    actor_uuid: None,
                },
                path: ontology_path.clone(),
                mode: graphforge_core::OntologyMode::Strict,
            })
            .unwrap();
        let authoritative = graph.workspace_ontology().unwrap();
        drop(graph);

        fs::write(
            &ontology_path,
            "ontology_id: repository-authority\nversion: \"2\"\nentity_types:\n  - name: Organization\n    abstract: false\nrelation_types: []\n",
        )
        .unwrap();
        let synced = context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(Uuid::from_u128(102)),
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(synced.status, RepositorySyncStatus::Published);
        assert!(
            synced
                .definitions
                .iter()
                .any(|definition| definition.definition_id == "ontology")
        );

        let reopened = crate::GraphForge::new(Some(context.state_path.to_str().unwrap())).unwrap();
        assert_eq!(
            reopened.ontology_mode(),
            graphforge_core::OntologyMode::Strict
        );
        assert_eq!(reopened.workspace_ontology().unwrap(), authoritative);

        drop(reopened);
        let mut graph = crate::GraphForge::new(Some(context.state_path.to_str().unwrap())).unwrap();
        graph
            .clear_ontology(crate::ClearOntologyRequest {
                context: crate::WriteContext {
                    operation_uuid: crate::OperationId(Uuid::from_u128(103)),
                    actor_uuid: None,
                },
            })
            .unwrap();
        let cleared = graph.workspace_ontology().unwrap();
        drop(graph);
        fs::write(&ontology_path, "not: an ontology\n").unwrap();
        context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(Uuid::from_u128(104)),
                actor_uuid: None,
            })
            .unwrap();

        let reopened_clear =
            crate::GraphForge::new(Some(context.state_path.to_str().unwrap())).unwrap();
        assert_eq!(
            reopened_clear.ontology_mode(),
            graphforge_core::OntologyMode::Exploratory
        );
        assert_eq!(reopened_clear.workspace_ontology().unwrap(), cleared);
    }

    #[test]
    fn repository_snapshot_checkpoint_revert_reopens_and_replays() {
        let root = tempdir().unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        context.init_without_skills().unwrap();

        let first_operation = Uuid::from_bytes([71; 16]);
        let first = context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(first_operation),
                actor_uuid: Some(Uuid::from_bytes([72; 16])),
            })
            .unwrap();
        assert_eq!(first.status, RepositorySyncStatus::Published);

        let graph = crate::GraphForge::new(Some(context.state_path.to_str().unwrap())).unwrap();
        graph
            .checkpoint(crate::CheckpointRequest {
                name: "Before definition change".into(),
                description: None,
                idempotency_key: crate::OperationId(Uuid::from_bytes([73; 16])),
                actor_uuid: None,
            })
            .unwrap();
        drop(graph);

        fs::write(
            root.path().join(".graphforge/ontology/changed.yaml"),
            "version: 1\n",
        )
        .unwrap();
        let second = context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(Uuid::from_bytes([74; 16])),
                actor_uuid: None,
            })
            .unwrap();
        assert_eq!(second.status, RepositorySyncStatus::Published);
        assert_ne!(second.generation_uuid, first.generation_uuid);

        let mut graph = crate::GraphForge::new(Some(context.state_path.to_str().unwrap())).unwrap();
        let diff = graph
            .diff_checkpoints(crate::DiffCheckpointsRequest {
                from: crate::CheckpointSelector::Named("Before definition change".into()),
                to: crate::CheckpointSelector::Current,
                scope: crate::CheckpointDiffScope::All,
                detail: crate::CheckpointDiffDetail::Records,
                page: crate::PageRequest::default(),
            })
            .unwrap();
        assert_eq!(diff.batches[0].num_rows(), 1);

        let preview = crate::GraphForge::preview_revert_to_checkpoint(
            &context.state_path,
            crate::PreviewRevertCheckpointRequest {
                name: "Before definition change".into(),
            },
        )
        .unwrap();
        assert_eq!(preview.source_generation_uuid, first.generation_uuid);
        assert_eq!(preview.current_generation_uuid, second.generation_uuid);

        // Issue #275: if WriterBusy fires on an isolated project, distinguish a
        // pre-existing held lock (after diff/preview) from an acquire-path bug.
        assert_project_mutation_locks_free(
            &context.state_path,
            "after diff_checkpoints and preview_revert_to_checkpoint",
        );

        let generation_count = || {
            fs::read_dir(context.state_path.join("generations"))
                .unwrap()
                .count()
        };
        let before_revert_count = generation_count();
        let request = crate::RevertCheckpointRequest {
            name: "Before definition change".into(),
            reason: "restore repository snapshot".into(),
            idempotency_key: crate::OperationId(Uuid::from_bytes([75; 16])),
            actor_uuid: Some(Uuid::from_bytes([76; 16])),
        };
        let first_receipt = graph.revert_to_checkpoint(request.clone()).unwrap();
        let reverted = graphforge_storage::resolve_project_generation(&context.state_path).unwrap();
        assert_ne!(reverted.generation_uuid(), second.generation_uuid);
        assert_eq!(generation_count(), before_revert_count + 1);
        let snapshot = reverted
            .participant_snapshot(
                graphforge_storage::WORKSPACE_CAPABILITY_ID,
                graphforge_storage::WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY,
            )
            .unwrap()
            .unwrap();
        let snapshot =
            graphforge_storage::WorkspaceRepositorySnapshot::from_canonical_json(&snapshot.bytes)
                .unwrap();
        assert_eq!(snapshot.operation_uuid, first_operation);

        assert_project_mutation_locks_free(&context.state_path, "after first revert_to_checkpoint");

        let replay_receipt = graph.revert_to_checkpoint(request).unwrap();
        assert_eq!(replay_receipt.schema, first_receipt.schema);
        assert_eq!(replay_receipt.batches[0].num_rows(), 1);
        assert_eq!(generation_count(), before_revert_count + 1);
        drop(graph);

        assert_project_mutation_locks_free(
            &context.state_path,
            "after drop of GraphForge that performed revert",
        );

        let reopened = crate::GraphForge::new(Some(context.state_path.to_str().unwrap())).unwrap();
        let checkpoints = reopened
            .list_checkpoints(crate::ListCheckpointsRequest::default())
            .unwrap();
        assert_eq!(checkpoints.batches[0].num_rows(), 1);
        let reopened_snapshot = reopened
            .generation_for_read()
            .unwrap()
            .participant_snapshot(
                graphforge_storage::WORKSPACE_CAPABILITY_ID,
                graphforge_storage::WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY,
            )
            .unwrap()
            .unwrap();
        graphforge_storage::WorkspaceRepositorySnapshot::from_canonical_json(
            &reopened_snapshot.bytes,
        )
        .unwrap();
    }

    #[test]
    fn repository_sync_rechecks_definitions_at_publication_boundary() {
        let root = tempdir().unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        context.init_without_skills().unwrap();
        context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(Uuid::from_bytes([61; 16])),
                actor_uuid: None,
            })
            .unwrap();
        let prior = graphforge_storage::resolve_project_generation(&context.state_path)
            .unwrap()
            .generation_uuid();
        let definition = root.path().join(".graphforge/ontology/concurrent.yaml");
        fs::write(&definition, "version: 1\n").unwrap();

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        *REPOSITORY_SYNC_BEFORE_STAGE_HOOK
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .unwrap() = Some(RepositorySyncTestHook {
            root: context.root.clone(),
            barrier: barrier.clone(),
        });
        let mutation = std::thread::spawn(move || {
            barrier.wait();
            fs::write(definition, "version: 2\n").unwrap();
            barrier.wait();
        });
        let error = context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(Uuid::from_bytes([62; 16])),
                actor_uuid: None,
            })
            .unwrap_err();
        mutation.join().unwrap();
        *REPOSITORY_SYNC_BEFORE_STAGE_HOOK
            .get()
            .unwrap()
            .lock()
            .unwrap() = None;
        assert_eq!(error.code(), "GF_VALIDATION");
        assert!(
            error
                .to_string()
                .contains("definitions changed during sync")
        );
        assert_eq!(
            graphforge_storage::resolve_project_generation(&context.state_path)
                .unwrap()
                .generation_uuid(),
            prior
        );
    }

    #[test]
    fn repository_definitions_reject_data_and_renamed_binary_files() {
        for (name, bytes) in [
            ("data.csv", b"id,name\n1,Ada\n".as_slice()),
            ("data.jsonl", b"{\"id\":1}\n".as_slice()),
            ("data.ndjson", b"{\"id\":1}\n".as_slice()),
            ("data.ipc", b"ARROW1binary".as_slice()),
            ("data.feather", b"FEA1binary".as_slice()),
            ("data.avro", b"Obj\x01binary".as_slice()),
            ("renamed.yaml", b"\0\xff\x10binary".as_slice()),
            ("renamed.json", b"\0\xff\x10binary".as_slice()),
        ] {
            let root = tempdir().unwrap();
            let context = RepositoryContext::discover(root.path()).unwrap();
            context.init_without_skills().unwrap();
            let before = fs::read_dir(context.state_path.join("generations"))
                .unwrap()
                .count();
            fs::write(root.path().join(".graphforge/seeds").join(name), bytes).unwrap();
            let error = context
                .sync(RepositorySyncRequest {
                    check: true,
                    operation_uuid: None,
                    actor_uuid: None,
                })
                .unwrap_err();
            assert_eq!(error.code(), "GF_VALIDATION", "{name}");
            assert_eq!(
                fs::read_dir(context.state_path.join("generations"))
                    .unwrap()
                    .count(),
                before,
                "{name}"
            );
        }
    }

    #[test]
    fn repository_sync_failpoint_child() {
        let Ok(root) = std::env::var("GRAPHFORGE_REPOSITORY_SYNC_TEST_ROOT") else {
            return;
        };
        let context = RepositoryContext::discover(root).unwrap();
        context
            .sync(RepositorySyncRequest {
                check: false,
                operation_uuid: Some(Uuid::from_bytes([51; 16])),
                actor_uuid: Some(Uuid::from_bytes([52; 16])),
            })
            .unwrap();
    }

    #[test]
    fn repository_sync_failure_boundaries_recover_to_one_authoritative_state() {
        for (failpoint, expect_published) in [
            ("project.after_domain_validation", false),
            ("project.after_current_replace", true),
        ] {
            let root = tempdir().unwrap();
            let context = RepositoryContext::discover(root.path()).unwrap();
            context.init_without_skills().unwrap();
            let initial = graphforge_storage::resolve_project_generation(&context.state_path)
                .unwrap()
                .generation_uuid();
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "repository::tests::repository_sync_failpoint_child",
                    "--nocapture",
                ])
                .env(
                    "GRAPHFORGE_REPOSITORY_SYNC_TEST_ROOT",
                    root.path().as_os_str(),
                )
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "{failpoint}");

            graphforge_storage::recover_project_transactions(&context.state_path).unwrap();
            let current =
                graphforge_storage::resolve_project_generation(&context.state_path).unwrap();
            current.validate_complete_participant_inventory().unwrap();
            let check = context
                .sync(RepositorySyncRequest {
                    check: true,
                    operation_uuid: None,
                    actor_uuid: None,
                })
                .unwrap();
            if expect_published {
                assert_ne!(current.generation_uuid(), initial, "{failpoint}");
                assert_eq!(check.status, RepositorySyncStatus::InSync, "{failpoint}");
            } else {
                assert_eq!(current.generation_uuid(), initial, "{failpoint}");
                assert_eq!(check.status, RepositorySyncStatus::Drift, "{failpoint}");
            }
        }
    }

    #[test]
    fn malformed_managed_ignore_block_fails_without_changes() {
        let root = tempdir().unwrap();
        let original = format!("keep\n{IGNORE_START}\n");
        fs::write(root.path().join(".gitignore"), &original).unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        assert!(context.init().is_err());
        assert_eq!(
            fs::read_to_string(root.path().join(".gitignore")).unwrap(),
            original
        );
        assert!(!root.path().join(".graphforge").exists());
    }

    #[test]
    fn discovery_resolves_a_linked_git_worktree_not_the_common_directory() {
        let source = tempdir().unwrap();
        assert!(
            Command::new("git")
                .arg("init")
                .arg("-q")
                .arg(source.path())
                .status()
                .unwrap()
                .success()
        );
        for args in [
            ["config", "user.email", "test@example.invalid"],
            ["config", "user.name", "GraphForge Test"],
        ] {
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(source.path())
                    .args(args)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(source.path())
                .args(["commit", "--allow-empty", "-qm", "initial"])
                .status()
                .unwrap()
                .success()
        );
        let parent = tempdir().unwrap();
        let linked = parent.path().join("linked worktree ü");
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(source.path())
                .args(["worktree", "add", "-q", "-b", "fixture"])
                .arg(&linked)
                .status()
                .unwrap()
                .success()
        );
        let nested = linked.join("nested");
        fs::create_dir(&nested).unwrap();
        let context = RepositoryContext::discover(&nested).unwrap();
        assert_eq!(context.root, linked.canonicalize().unwrap());
        assert!(context.git);
    }
}
