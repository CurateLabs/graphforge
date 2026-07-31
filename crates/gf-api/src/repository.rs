//! Safe integration between a code repository and an embedded GraphForge project.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use fs4::fs_std::FileExt;
use gf_core::GfError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const CONFIG: &str = ".graphforge/graphforge.yaml";
const IGNORE_START: &str = "# graphforge: managed data (do not edit)";
const IGNORE_END: &str = "# graphforge: end managed data";
const IGNORE_LINES: [&str; 3] = [
    "/.graphforge/state/",
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

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
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

    /// Parse and semantically validate the closed project configuration.
    pub fn load_config(&self) -> Result<ProjectConfig, GfError> {
        let bytes = fs::read(&self.config_path).map_err(|error| {
            GfError::Storage(format!(
                "cannot read {}: {error}",
                self.config_path.display()
            ))
        })?;
        let config: ProjectConfig = serde_yaml::from_slice(&bytes)
            .map_err(|error| validation(format!("invalid graphforge.yaml: {error}")))?;
        config.validate(self)?;
        Ok(config)
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
        fs::create_dir_all(&self.state_path)
            .map_err(|error| GfError::Storage(error.to_string()))?;
        let state = self
            .state_path
            .to_str()
            .ok_or_else(|| validation("project path must be valid UTF-8"))?;
        // GraphForge::new creates or reopens the v1 container. Opening it a second
        // time proves that the published project can be resolved immediately.
        super::GraphForge::new(Some(state))?;
        super::GraphForge::new(Some(state))?;
        Ok(RepositoryInitReceipt {
            root: self.root.clone(),
            created_config,
            ignore_changed,
            state: self.state_path.clone(),
        })
    }

    /// Inspect the installed project-local skill bundle without changing it.
    pub fn skills_status(&self, bundle: &SkillBundle<'_>) -> Result<SkillStatusReceipt, GfError> {
        let expected = validate_skill_bundle(bundle)?;
        let _lock = self.lock_skills()?;
        self.recover_skill_transaction()?;
        self.skills_status_locked(&expected)
    }

    fn skills_status_locked(
        &self,
        expected: &CanonicalSkillManifest,
    ) -> Result<SkillStatusReceipt, GfError> {
        self.reject_skill_symlinks()?;
        let manifest_path = self.contained_path(SKILLS_MANIFEST)?;
        if !manifest_path.exists() {
            let occupied = MANAGED_SKILL_NAMES
                .iter()
                .any(|name| self.root.join(SKILLS_ROOT).join(name).exists());
            return Ok(SkillStatusReceipt {
                status: if occupied {
                    SkillStatus::Conflict
                } else {
                    SkillStatus::Missing
                },
                bundle_version: None,
                expected_bundle_version: expected.bundle_version,
                edited_files: Vec::new(),
            });
        }
        let Ok(installed) = read_installed_manifest(&manifest_path) else {
            return Ok(SkillStatusReceipt {
                status: SkillStatus::Conflict,
                bundle_version: None,
                expected_bundle_version: expected.bundle_version,
                edited_files: vec![".graphforge-managed.json".to_owned()],
            });
        };
        let edited_files = verify_installed_files(self, &installed)?;
        let status = if !edited_files.is_empty()
            || installed.graphforge_compatibility != expected.graphforge_compatibility
            || installed.source != "graphforge-packaged-bundle"
        {
            SkillStatus::Conflict
        } else if installed.bundle_version == expected.bundle_version
            && installed.files == expected.files
        {
            SkillStatus::Current
        } else {
            SkillStatus::Outdated
        };
        Ok(SkillStatusReceipt {
            status,
            bundle_version: Some(installed.bundle_version),
            expected_bundle_version: expected.bundle_version,
            edited_files,
        })
    }

    /// Atomically install the verified project-local skill bundle.
    pub fn skills_install(
        &self,
        bundle: &SkillBundle<'_>,
        force: bool,
    ) -> Result<SkillMutationReceipt, GfError> {
        self.install_or_update_skills(bundle, force, SkillMutation::Install)
    }

    /// Atomically update the managed project-local skill bundle.
    pub fn skills_update(
        &self,
        bundle: &SkillBundle<'_>,
        force: bool,
    ) -> Result<SkillMutationReceipt, GfError> {
        self.install_or_update_skills(bundle, force, SkillMutation::Update)
    }

    /// Remove managed skill namespaces, preserving edits unless `force` explicitly resolves them.
    pub fn skills_remove(&self, force: bool) -> Result<SkillMutationReceipt, GfError> {
        let _lock = self.lock_skills()?;
        self.recover_skill_transaction()?;
        self.reject_skill_symlinks()?;
        let manifest_path = self.contained_path(SKILLS_MANIFEST)?;
        if !manifest_path.exists() {
            return Ok(SkillMutationReceipt {
                changed: false,
                bundle_version: None,
                installed_files: 0,
            });
        }
        let installed = match read_installed_manifest(&manifest_path) {
            Ok(installed) => Some(installed),
            Err(error) if !force => {
                return Err(validation(format!(
                    "managed skill manifest conflicts with the supported contract: {error}; rerun with --force to resolve the conflict"
                )));
            }
            Err(_) => None,
        };
        let edited = installed
            .as_ref()
            .map(|manifest| verify_installed_files(self, manifest))
            .transpose()?
            .unwrap_or_default();
        if !edited.is_empty() && !force {
            return Err(validation(format!(
                "managed skill files were edited: {}; rerun with --force to resolve the conflict",
                edited.join(", ")
            )));
        }
        let backup = self.contained_path(SKILLS_BACKUP)?;
        if backup.exists() {
            return Err(validation("stale managed skill transaction path"));
        }
        self.begin_skill_transaction()?;
        if let Err(error) = create_dir_durable(&backup) {
            self.rollback_skill_transaction()?;
            return Err(error);
        }
        let publish = (|| -> Result<(), GfError> {
            for name in MANAGED_SKILL_NAMES {
                let target = self.contained_path(Path::new(SKILLS_ROOT).join(name))?;
                if target.exists() {
                    rename_durable(&target, &backup.join(name))?;
                }
            }
            rename_durable(&manifest_path, &backup.join(".graphforge-managed.json"))
        })();
        if let Err(error) = publish {
            self.rollback_skill_transaction()?;
            return Err(error);
        }
        if let Err(error) = remove_file_durable(&self.contained_path(SKILLS_TRANSACTION)?) {
            self.rollback_skill_transaction()?;
            return Err(error);
        }
        fs::remove_dir_all(backup).map_err(|error| GfError::Storage(error.to_string()))?;
        sync_directory(&self.contained_path(SKILLS_LIFECYCLE_ROOT)?)?;
        Ok(SkillMutationReceipt {
            changed: true,
            bundle_version: installed.map(|manifest| manifest.bundle_version),
            installed_files: 0,
        })
    }

    fn install_or_update_skills(
        &self,
        bundle: &SkillBundle<'_>,
        force: bool,
        mutation: SkillMutation,
    ) -> Result<SkillMutationReceipt, GfError> {
        let expected = validate_skill_bundle(bundle)?;
        let _lock = self.lock_skills()?;
        self.recover_skill_transaction()?;
        self.reject_skill_symlinks()?;
        let status = self.skills_status_locked(&expected)?;
        if status.status == SkillStatus::Current {
            return Ok(SkillMutationReceipt {
                changed: false,
                bundle_version: Some(expected.bundle_version),
                installed_files: expected.files.len(),
            });
        }
        if status.status == SkillStatus::Conflict && !force {
            let action = match mutation {
                SkillMutation::Install => "install",
                SkillMutation::Update => "update",
            };
            return Err(validation(format!(
                "cannot {action}: project-local skill files conflict with the managed bundle; rerun with --force to resolve the conflict"
            )));
        }

        let skills_root = self.contained_path(SKILLS_ROOT)?;
        fs::create_dir_all(&skills_root).map_err(|error| GfError::Storage(error.to_string()))?;
        let lifecycle_root = self.contained_path(SKILLS_LIFECYCLE_ROOT)?;
        fs::create_dir_all(&lifecycle_root).map_err(|error| GfError::Storage(error.to_string()))?;
        let stage = self.contained_path(SKILLS_STAGE)?;
        let backup = self.contained_path(SKILLS_BACKUP)?;
        for path in [&stage, &backup] {
            if path.exists() {
                return Err(validation("stale managed skill transaction path"));
            }
        }
        create_dir_durable(&stage)?;
        for file in bundle.files {
            let target = stage.join(file.path);
            let parent = target
                .parent()
                .ok_or_else(|| validation("skill file has no parent"))?;
            fs::create_dir_all(parent).map_err(|error| GfError::Storage(error.to_string()))?;
            write_durable(&target, file.bytes)?;
        }
        sync_directory(&stage)?;
        let installed = InstalledSkillManifest {
            schema_version: expected.schema_version,
            bundle_version: expected.bundle_version,
            graphforge_compatibility: expected.graphforge_compatibility,
            source: "graphforge-packaged-bundle".to_owned(),
            files: expected.files,
        };
        let manifest_bytes = serde_json::to_vec_pretty(&installed)
            .map_err(|error| GfError::Storage(error.to_string()))?;
        write_durable(
            &stage.join(".graphforge-managed.json"),
            &[&manifest_bytes[..], b"\n"].concat(),
        )?;
        self.begin_skill_transaction()?;
        if let Err(error) = create_dir_durable(&backup) {
            self.rollback_skill_transaction()?;
            return Err(error);
        }

        let publish = (|| -> Result<(), GfError> {
            let manifest = self.contained_path(SKILLS_MANIFEST)?;
            if manifest.exists() {
                rename_durable(&manifest, &backup.join(".graphforge-managed.json"))?;
            }
            for name in MANAGED_SKILL_NAMES {
                let target = skills_root.join(name);
                if target.exists() {
                    rename_durable(&target, &backup.join(name))?;
                } else {
                    write_durable(&backup.join(format!(".missing-{name}")), &[])?;
                }
                rename_durable(&stage.join(name), &target)?;
            }
            rename_durable(&stage.join(".graphforge-managed.json"), &manifest)?;
            Ok(())
        })();
        if let Err(error) = publish {
            self.rollback_skill_transaction()?;
            return Err(error);
        }
        if let Err(error) = remove_file_durable(&self.contained_path(SKILLS_TRANSACTION)?) {
            self.rollback_skill_transaction()?;
            return Err(error);
        }
        fs::remove_dir_all(&stage).map_err(|error| GfError::Storage(error.to_string()))?;
        fs::remove_dir_all(&backup).map_err(|error| GfError::Storage(error.to_string()))?;
        sync_directory(&lifecycle_root)?;
        Ok(SkillMutationReceipt {
            changed: true,
            bundle_version: Some(installed.bundle_version),
            installed_files: installed.files.len(),
        })
    }

    fn reject_skill_symlinks(&self) -> Result<(), GfError> {
        for relative in [
            SKILLS_ROOT,
            SKILLS_MANIFEST,
            SKILLS_TRANSACTION,
            SKILLS_LOCK,
            SKILLS_LIFECYCLE_ROOT,
            SKILLS_STAGE,
            SKILLS_BACKUP,
            ".agents/skills/graphforge-bootstrap",
            ".agents/skills/graphforge-build-knowledge",
        ] {
            reject_symlink_components(&self.root, &self.root.join(relative))?;
        }
        Ok(())
    }

    fn lock_skills(&self) -> Result<fs::File, GfError> {
        let root = self.contained_path(SKILLS_LIFECYCLE_ROOT)?;
        fs::create_dir_all(&root).map_err(|error| GfError::Storage(error.to_string()))?;
        sync_directory(&root)?;
        sync_directory(
            root.parent()
                .ok_or_else(|| validation("skills lifecycle root has no parent"))?,
        )?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.contained_path(SKILLS_LOCK)?)
            .map_err(|error| GfError::Storage(error.to_string()))?;
        FileExt::lock_exclusive(&lock).map_err(|error| GfError::Storage(error.to_string()))?;
        Ok(lock)
    }

    fn begin_skill_transaction(&self) -> Result<(), GfError> {
        let path = self.contained_path(SKILLS_TRANSACTION)?;
        let mut marker = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    validation("another managed skill transaction is active")
                } else {
                    GfError::Storage(error.to_string())
                }
            })?;
        marker
            .write_all(b"graphforge-skills/1\n")
            .map_err(|error| GfError::Storage(error.to_string()))?;
        marker
            .sync_all()
            .map_err(|error| GfError::Storage(error.to_string()))?;
        sync_directory(
            path.parent()
                .ok_or_else(|| validation("transaction marker has no parent"))?,
        )
    }

    fn recover_skill_transaction(&self) -> Result<(), GfError> {
        let transaction = self.contained_path(SKILLS_TRANSACTION)?;
        if transaction.exists() {
            self.rollback_skill_transaction()?;
        } else {
            for path in [
                self.contained_path(SKILLS_STAGE)?,
                self.contained_path(SKILLS_BACKUP)?,
            ] {
                if path.exists() {
                    fs::remove_dir_all(path)
                        .map_err(|error| GfError::Storage(error.to_string()))?;
                    sync_directory(&self.contained_path(SKILLS_LIFECYCLE_ROOT)?)?;
                }
            }
        }
        Ok(())
    }

    fn rollback_skill_transaction(&self) -> Result<(), GfError> {
        let skills_root = self.contained_path(SKILLS_ROOT)?;
        let backup = self.contained_path(SKILLS_BACKUP)?;
        for name in MANAGED_SKILL_NAMES {
            let target = skills_root.join(name);
            let saved = backup.join(name);
            if saved.exists() {
                if target.exists() {
                    fs::remove_dir_all(&target)
                        .map_err(|error| GfError::Storage(error.to_string()))?;
                    sync_directory(&skills_root)?;
                }
                rename_durable(&saved, &target)?;
            } else if backup.join(format!(".missing-{name}")).exists() && target.exists() {
                fs::remove_dir_all(target).map_err(|error| GfError::Storage(error.to_string()))?;
                sync_directory(&skills_root)?;
            }
        }
        let saved_manifest = backup.join(".graphforge-managed.json");
        if saved_manifest.exists() {
            let manifest = self.contained_path(SKILLS_MANIFEST)?;
            if manifest.exists() {
                fs::remove_file(&manifest).map_err(|error| GfError::Storage(error.to_string()))?;
                sync_directory(&skills_root)?;
            }
            rename_durable(&saved_manifest, &manifest)?;
        }
        for path in [self.contained_path(SKILLS_STAGE)?, backup] {
            if path.exists() {
                fs::remove_dir_all(path).map_err(|error| GfError::Storage(error.to_string()))?;
                sync_directory(&self.contained_path(SKILLS_LIFECYCLE_ROOT)?)?;
            }
        }
        let transaction = self.contained_path(SKILLS_TRANSACTION)?;
        if transaction.exists() {
            remove_file_durable(&transaction)?;
        }
        Ok(())
    }

    /// Resolve the config to deterministic, secret-free JSON with explicit defaults.
    pub fn resolve_config(&self) -> Result<Value, GfError> {
        self.load_config()?.resolve()
    }

    /// Record bounded Git provenance after validating only declared inputs.
    pub fn sync(&self) -> Result<RepositorySyncReceipt, GfError> {
        let config = self.load_config()?;
        let mut definition_digests = BTreeMap::new();
        for path in config.project.paths() {
            let resolved = self.contained_path(path)?;
            if !resolved.is_dir() {
                return Err(validation(format!(
                    "declared definition directory is missing: {path}"
                )));
            }
            definition_digests.insert(path.to_owned(), digest_definition_tree(&resolved)?);
        }
        let provenance = self.git_provenance()?;
        Ok(RepositorySyncReceipt {
            definitions: config.project.paths().map(str::to_owned).collect(),
            definition_digests,
            source_digests: config
                .sources
                .iter()
                .map(|source| source.sha256.clone())
                .collect(),
            provenance,
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
            fs::remove_dir_all(&target).map_err(|error| GfError::Storage(error.to_string()))?;
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

fn digest_definition_tree(root: &Path) -> Result<String, GfError> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    let mut total = 0_u64;
    while let Some(directory) = pending.pop() {
        for entry in
            fs::read_dir(&directory).map_err(|error| GfError::Storage(error.to_string()))?
        {
            let entry = entry.map_err(|error| GfError::Storage(error.to_string()))?;
            let kind = entry
                .file_type()
                .map_err(|error| GfError::Storage(error.to_string()))?;
            if kind.is_symlink() {
                return Err(validation("symlinks are not allowed in definitions"));
            }
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                files.push(entry.path());
            }
            if files.len() + pending.len() > 10_000 {
                return Err(validation("definition tree exceeds file bound"));
            }
        }
    }
    files.sort();
    let mut hash = Sha256::new();
    for path in files {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| validation("definition path escaped its root"))?;
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        if matches!(
            extension,
            "arrow" | "parquet" | "db" | "sqlite" | "sqlite3" | "duckdb"
        ) {
            return Err(validation(
                "materialized graph or database data is not a definition",
            ));
        }
        let bytes = fs::read(&path).map_err(|error| GfError::Storage(error.to_string()))?;
        total = total.saturating_add(bytes.len() as u64);
        if total > 64 * 1024 * 1024 {
            return Err(validation("definition tree exceeds byte bound"));
        }
        hash.update(relative.to_string_lossy().replace('\\', "/").as_bytes());
        hash.update([0]);
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(&bytes);
    }
    Ok(format!("{:x}", hash.finalize()))
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

/// Closed repository configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectConfig {
    schema_version: u32,
    project: DefinitionPaths,
    #[serde(default)]
    sources: Vec<Source>,
    #[serde(default)]
    secrets: Vec<SecretReference>,
    targets: BTreeMap<String, Target>,
}

impl ProjectConfig {
    fn validate(&self, context: &RepositoryContext) -> Result<(), GfError> {
        if self.schema_version != 1 {
            return Err(validation("unsupported schema_version"));
        }
        if self.sources.len() > 256 || self.secrets.len() > 128 || self.targets.len() > 64 {
            return Err(validation(
                "configuration collection exceeds contract bounds",
            ));
        }
        let mut source_ids = BTreeSet::new();
        for source in &self.sources {
            stable_id(&source.id)?;
            if !source_ids.insert(&source.id) {
                return Err(validation("duplicate source id"));
            }
            digest(&source.sha256)?;
            if uri_has_inline_credentials(&source.uri) {
                return Err(validation("source URI must not contain inline credentials"));
            }
            bounded(&source.uri, 1, 2048, "source URI")?;
            if let Some(media_type) = &source.media_type {
                bounded(media_type, 1, 128, "media type")?;
            }
        }
        let mut secret_ids = BTreeSet::new();
        for secret in &self.secrets {
            stable_id(&secret.id)?;
            if !secret_ids.insert(&secret.id) {
                return Err(validation("duplicate secret id"));
            }
        }
        if self.targets.is_empty() {
            return Err(validation("at least one target is required"));
        }
        for (id, target) in &self.targets {
            stable_id(id)?;
            digest(&target.artifact.sha256)?;
            bounded(&target.artifact.version, 1, 128, "artifact version")?;
            if target.source_ids.len() > 256 || target.secret_ids.len() > 128 {
                return Err(validation(
                    "target reference collection exceeds contract bounds",
                ));
            }
            if target.source_ids.iter().collect::<BTreeSet<_>>().len() != target.source_ids.len()
                || target.secret_ids.iter().collect::<BTreeSet<_>>().len()
                    != target.secret_ids.len()
            {
                return Err(validation("target references must be unique"));
            }
            target.validate_bounds()?;
            if target.source_ids.iter().any(|id| !source_ids.contains(id)) {
                return Err(validation("target references an unknown source"));
            }
            if target.secret_ids.iter().any(|id| !secret_ids.contains(id)) {
                return Err(validation("target references an unknown secret"));
            }
        }
        for path in self.project.paths() {
            bounded(path, 1, 1024, "definition path")?;
            if path.contains('\\') {
                return Err(validation("definition paths must use '/' separators"));
            }
            context.contained_path(path)?;
        }
        Ok(())
    }

    fn resolve(&self) -> Result<Value, GfError> {
        let mut targets = Vec::new();
        for (id, target) in &self.targets {
            let mut value =
                serde_json::to_value(target).map_err(|error| validation(error.to_string()))?;
            let object = value.as_object_mut().expect("target serializes as object");
            object.insert("id".into(), json!(id));
            object.entry("resources").or_insert_with(|| json!({}));
            object
                .entry("network")
                .or_insert_with(|| json!({"exposure":"none","tls_required":false}));
            object
                .entry("health")
                .or_insert_with(|| json!({"timeout_seconds":30}));
            object
                .entry("observability")
                .or_insert_with(|| json!({"logs":true,"metrics":false,"traces":false}));
            object
                .entry("backup")
                .or_insert_with(|| json!({"checkpoints":false}));
            fill_target_defaults(object);
            targets.push(value);
        }
        let mut sources = self.sources.clone();
        sources.sort_by(|left, right| left.id.cmp(&right.id));
        let mut secrets = self.secrets.clone();
        secrets.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(json!({
            "contract": "graphforge-resolved-config/1",
            "project": {
                "integration_root": ".graphforge", "state": ".graphforge/state",
                "imports": ".graphforge/imports", "exports": ".graphforge/exports",
                "ontology": self.project.ontology, "schemas": self.project.schemas,
                "seeds": self.project.seeds, "migrations": self.project.migrations
            },
            "sources": sources, "secrets": secrets, "targets": targets
        }))
    }
}

fn fill_target_defaults(object: &mut serde_json::Map<String, Value>) {
    insert_defaults(
        object,
        "network",
        &[("exposure", json!("none")), ("tls_required", json!(false))],
    );
    insert_defaults(
        object,
        "observability",
        &[
            ("logs", json!(true)),
            ("metrics", json!(false)),
            ("traces", json!(false)),
        ],
    );
    insert_defaults(object, "backup", &[("checkpoints", json!(false))]);
}

fn insert_defaults(
    object: &mut serde_json::Map<String, Value>,
    section: &str,
    fields: &[(&str, Value)],
) {
    if let Some(Value::Object(values)) = object.get_mut(section) {
        for (name, default) in fields {
            values
                .entry((*name).to_owned())
                .or_insert_with(|| default.clone());
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DefinitionPaths {
    ontology: String,
    schemas: String,
    seeds: String,
    migrations: String,
}
impl DefinitionPaths {
    fn paths(&self) -> impl Iterator<Item = &str> {
        [
            &*self.ontology,
            &*self.schemas,
            &*self.seeds,
            &*self.migrations,
        ]
        .into_iter()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Source {
    id: String,
    uri: String,
    sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    media_type: Option<String>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SecretReference {
    id: String,
    source: SecretSource,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum SecretSource {
    Environment,
    Pulumi,
    Terraform,
    SecretManager,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    kind: ArtifactKind,
    version: String,
    sha256: String,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ArtifactKind {
    PythonWheel,
    NodePackage,
    NativeBinary,
    OciImage,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Target {
    kind: TargetKind,
    artifact: Artifact,
    write: WriteConfig,
    storage: StorageConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    resources: Option<Resources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    network: Option<Network>,
    #[serde(skip_serializing_if = "Option::is_none")]
    health: Option<Health>,
    #[serde(skip_serializing_if = "Option::is_none")]
    observability: Option<Observability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backup: Option<Backup>,
    #[serde(default)]
    source_ids: Vec<String>,
    #[serde(default)]
    secret_ids: Vec<String>,
}
impl Target {
    fn validate_bounds(&self) -> Result<(), GfError> {
        if self.write.queue_capacity == Some(0)
            || self
                .write
                .queue_capacity
                .is_some_and(|value| value > 65_536)
            || self
                .write
                .max_rebase_attempts
                .is_some_and(|value| value > 64)
            || self.storage.capacity_bytes == Some(0)
            || self
                .resources
                .as_ref()
                .is_some_and(|value| value.cpu_millis == Some(0) || value.memory_bytes == Some(0))
            || self
                .health
                .as_ref()
                .is_some_and(|value| value.timeout_seconds == 0 || value.timeout_seconds > 300)
            || self
                .backup
                .as_ref()
                .and_then(|value| value.retention_count)
                .is_some_and(|value| value == 0 || value > 1024)
        {
            return Err(validation("target value exceeds contract bounds"));
        }
        if let Some(class) = &self.storage.class {
            bounded(class, 1, 128, "storage class")?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum TargetKind {
    Embedded,
    Service,
    Worker,
    Job,
    Host,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WriteConfig {
    mode: WriteMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_capacity: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_rebase_attempts: Option<u8>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
enum WriteMode {
    #[serde(rename = "single_writer")]
    Single,
    #[serde(rename = "queued_writer")]
    Queued,
    #[serde(rename = "optimistic_multi_writer")]
    OptimisticMulti,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StorageConfig {
    kind: StorageKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    persistent: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    capacity_bytes: Option<u64>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum StorageKind {
    Local,
    Volume,
    Object,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Resources {
    #[serde(skip_serializing_if = "Option::is_none")]
    cpu_millis: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    memory_bytes: Option<u64>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Network {
    #[serde(skip_serializing_if = "Option::is_none")]
    exposure: Option<Exposure>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls_required: Option<bool>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Exposure {
    None,
    Private,
    Public,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Health {
    timeout_seconds: u16,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Observability {
    #[serde(skip_serializing_if = "Option::is_none")]
    logs: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    traces: Option<bool>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Backup {
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoints: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retention_count: Option<u16>,
}

fn stable_id(value: &str) -> Result<(), GfError> {
    let valid = !value.is_empty()
        && value.len() <= 64
        && value.bytes().enumerate().all(|(i, byte)| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit() && i > 0
                || i > 0 && matches!(byte, b'-' | b'_')
        });
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
            .is_some_and(|authority| authority.contains('@'))
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalSkillManifest {
    schema_version: u32,
    bundle_version: u32,
    #[serde(alias = "compatibility")]
    graphforge_compatibility: String,
    skills: Vec<String>,
    files: Vec<SkillFileDigest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SkillFileDigest {
    path: String,
    sha256: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InstalledSkillManifest {
    schema_version: u32,
    bundle_version: u32,
    graphforge_compatibility: String,
    source: String,
    files: Vec<SkillFileDigest>,
}

fn validate_skill_bundle(bundle: &SkillBundle<'_>) -> Result<CanonicalSkillManifest, GfError> {
    let manifest: CanonicalSkillManifest = serde_json::from_slice(bundle.manifest)
        .map_err(|error| validation(format!("invalid project skill manifest: {error}")))?;
    if manifest.schema_version != 1 || manifest.bundle_version == 0 {
        return Err(validation("unsupported project skill manifest version"));
    }
    if manifest.graphforge_compatibility != ">=0.5.0 <0.6.0" {
        return Err(validation(
            "project skill bundle is incompatible with this GraphForge release",
        ));
    }
    if manifest.skills
        != MANAGED_SKILL_NAMES
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    {
        return Err(validation(
            "project skill manifest must name the supported managed directories",
        ));
    }
    if manifest.files.len() != bundle.files.len() || manifest.files.is_empty() {
        return Err(validation(
            "project skill manifest does not match packaged files",
        ));
    }
    let mut previous: Option<&str> = None;
    for (expected, actual) in manifest.files.iter().zip(bundle.files) {
        validate_skill_path(&expected.path)?;
        if previous.is_some_and(|value| value >= expected.path.as_str()) {
            return Err(validation(
                "project skill manifest files must be unique and sorted",
            ));
        }
        previous = Some(&expected.path);
        if expected.path != actual.path {
            return Err(validation(
                "project skill manifest does not match packaged file paths",
            ));
        }
        digest(&expected.sha256)?;
        let actual_digest = format!("{:x}", Sha256::digest(actual.bytes));
        if actual_digest != expected.sha256 {
            return Err(validation(format!(
                "packaged project skill digest mismatch: {}",
                expected.path
            )));
        }
    }
    for required in [
        "graphforge-bootstrap/SKILL.md",
        "graphforge-build-knowledge/SKILL.md",
    ] {
        if !manifest.files.iter().any(|file| file.path == required) {
            return Err(validation(format!(
                "project skill bundle is missing {required}"
            )));
        }
    }
    Ok(manifest)
}

fn validate_skill_path(value: &str) -> Result<(), GfError> {
    let path = Path::new(value);
    let mut parts = path.components();
    let first = parts
        .next()
        .ok_or_else(|| validation("project skill file path is empty"))?;
    let Component::Normal(first) = first else {
        return Err(validation("project skill file path is not contained"));
    };
    let remaining: Vec<_> = parts.collect();
    if !MANAGED_SKILL_NAMES
        .iter()
        .any(|name| first == std::ffi::OsStr::new(name))
        || value.contains('\\')
        || remaining.is_empty()
        || remaining
            .iter()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(validation("project skill file path is not contained"));
    }
    Ok(())
}

fn read_installed_manifest(path: &Path) -> Result<InstalledSkillManifest, GfError> {
    let bytes = fs::read(path).map_err(|error| GfError::Storage(error.to_string()))?;
    let manifest: InstalledSkillManifest = serde_json::from_slice(&bytes)
        .map_err(|error| validation(format!("invalid managed skill manifest: {error}")))?;
    if manifest.schema_version != 1 || manifest.bundle_version == 0 {
        return Err(validation("unsupported managed skill manifest version"));
    }
    if manifest.graphforge_compatibility != ">=0.5.0 <0.6.0"
        || manifest.source != "graphforge-packaged-bundle"
    {
        return Err(validation("invalid managed skill provenance"));
    }
    if manifest.files.is_empty() {
        return Err(validation("managed skill manifest files are required"));
    }
    let mut previous: Option<&str> = None;
    for file in &manifest.files {
        validate_skill_path(&file.path)?;
        digest(&file.sha256)?;
        if previous.is_some_and(|value| value >= file.path.as_str()) {
            return Err(validation(
                "managed skill manifest files must be unique and sorted",
            ));
        }
        previous = Some(&file.path);
    }
    Ok(manifest)
}

fn verify_installed_files(
    context: &RepositoryContext,
    manifest: &InstalledSkillManifest,
) -> Result<Vec<String>, GfError> {
    let mut edited = Vec::new();
    let expected: BTreeSet<_> = manifest
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect();
    for file in &manifest.files {
        let path = context.contained_path(Path::new(SKILLS_ROOT).join(&file.path))?;
        match fs::read(&path) {
            Ok(bytes) if format!("{:x}", Sha256::digest(&bytes)) == file.sha256 => {}
            Ok(_) => edited.push(file.path.clone()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                edited.push(file.path.clone());
            }
            Err(error) => return Err(GfError::Storage(error.to_string())),
        }
    }
    for name in MANAGED_SKILL_NAMES {
        let root = context.contained_path(Path::new(SKILLS_ROOT).join(name))?;
        if !root.exists() {
            continue;
        }
        let mut pending = vec![root.clone()];
        while let Some(directory) = pending.pop() {
            for entry in
                fs::read_dir(directory).map_err(|error| GfError::Storage(error.to_string()))?
            {
                let entry = entry.map_err(|error| GfError::Storage(error.to_string()))?;
                let kind = entry
                    .file_type()
                    .map_err(|error| GfError::Storage(error.to_string()))?;
                if kind.is_symlink() {
                    return Err(validation("symlinks are not allowed in managed skills"));
                }
                if kind.is_dir() {
                    pending.push(entry.path());
                } else if kind.is_file() {
                    let relative = entry
                        .path()
                        .strip_prefix(context.root.join(SKILLS_ROOT))
                        .map_err(|_| validation("managed skill path escaped its root"))?
                        .to_string_lossy()
                        .replace('\\', "/");
                    if !expected.contains(relative.as_str()) {
                        edited.push(relative);
                    }
                }
            }
        }
    }
    edited.sort();
    edited.dedup();
    Ok(edited)
}

#[derive(Debug, Clone, Copy)]
enum SkillMutation {
    Install,
    Update,
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
/// Successful sync validation output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepositorySyncReceipt {
    /// Explicitly declared definition paths that were validated.
    pub definitions: Vec<String>,
    /// Canonical SHA-256 digest for each declared definition tree.
    pub definition_digests: BTreeMap<String, String>,
    /// Digests of explicitly declared external sources.
    pub source_digests: Vec<String>,
    /// Bounded Git provenance; no repository data is scanned.
    pub provenance: GitProvenance,
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
    use std::sync::mpsc;
    use tempfile::tempdir;

    fn test_skill_manifest() -> (Vec<u8>, [SkillBundleFile<'static>; 2]) {
        let files = [
            SkillBundleFile {
                path: "graphforge-bootstrap/SKILL.md",
                bytes: b"bootstrap",
            },
            SkillBundleFile {
                path: "graphforge-build-knowledge/SKILL.md",
                bytes: b"knowledge",
            },
        ];
        let manifest = serde_json::to_vec(&json!({
            "schema_version": 1,
            "bundle_version": 1,
            "graphforge_compatibility": ">=0.5.0 <0.6.0",
            "skills": MANAGED_SKILL_NAMES,
            "files": files.iter().map(|file| json!({
                "path": file.path,
                "sha256": format!("{:x}", Sha256::digest(file.bytes))
            })).collect::<Vec<_>>()
        }))
        .unwrap();
        (manifest, files)
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
        for line in IGNORE_LINES {
            assert_eq!(ignore.matches(line).count(), 1);
        }
        assert!(root.path().join(".graphforge/graphforge.yaml").is_file());
    }

    #[test]
    fn containment_and_symlinks_fail_closed() {
        let root = tempdir().unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        assert!(context.contained_path("../outside").is_err());
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
        let receipt = context.sync().unwrap();
        assert_eq!(receipt.provenance.sha, None);
        assert!(receipt.provenance.dirty);
        assert_eq!(receipt.definition_digests.len(), 4);
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

    #[test]
    fn checked_in_fixture_resolves_to_the_contract_golden() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join(".graphforge")).unwrap();
        fs::write(
            root.path().join(CONFIG),
            include_str!("../../../docs/contracts/examples/graphforge-v1.yaml"),
        )
        .unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        assert_eq!(context.load_config().unwrap().schema_version, 1);
        let actual = context.resolve_config().unwrap();
        let expected: Value = serde_json::from_str(include_str!(
            "../../../docs/contracts/examples/graphforge-resolved-v1.json"
        ))
        .unwrap();
        assert_eq!(actual, expected);
        let canonical = serde_json::to_string(&actual).unwrap() + "\n";
        assert_eq!(
            canonical,
            include_str!("../../../docs/contracts/examples/graphforge-resolved-v1.json")
        );
    }

    #[test]
    fn status_waits_for_the_writer_lock_and_recovers_before_reading() {
        let root = tempdir().unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        let (manifest, files) = test_skill_manifest();
        let bundle = SkillBundle {
            manifest: &manifest,
            files: &files,
        };
        context.skills_install(&bundle, false).unwrap();
        let writer = context.lock_skills().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let thread_root = root.path().to_path_buf();
        let reader = std::thread::spawn(move || {
            let context = RepositoryContext::discover(thread_root).unwrap();
            let (manifest, files) = test_skill_manifest();
            let bundle = SkillBundle {
                manifest: &manifest,
                files: &files,
            };
            started_tx.send(()).unwrap();
            let result = context.skills_status(&bundle);
            finished_tx.send(result).unwrap();
        });
        started_rx.recv().unwrap();
        assert!(matches!(
            finished_rx.recv_timeout(std::time::Duration::from_millis(250)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(writer);
        assert_eq!(
            finished_rx.recv().unwrap().unwrap().status,
            SkillStatus::Current
        );
        reader.join().unwrap();
    }

    #[test]
    fn malformed_managed_manifest_is_a_force_resolvable_conflict() {
        let root = tempdir().unwrap();
        let context = RepositoryContext::discover(root.path()).unwrap();
        let (manifest, files) = test_skill_manifest();
        let bundle = SkillBundle {
            manifest: &manifest,
            files: &files,
        };
        context.skills_install(&bundle, false).unwrap();
        fs::write(context.contained_path(SKILLS_MANIFEST).unwrap(), b"{}").unwrap();
        assert_eq!(
            context.skills_status(&bundle).unwrap().status,
            SkillStatus::Conflict
        );
        assert!(context.skills_install(&bundle, false).is_err());
        assert!(context.skills_install(&bundle, true).unwrap().changed);
        fs::write(context.contained_path(SKILLS_MANIFEST).unwrap(), b"{}").unwrap();
        assert!(context.skills_remove(false).is_err());
        assert!(context.skills_remove(true).unwrap().changed);
    }

    #[test]
    fn bare_managed_directory_is_not_a_valid_manifest_file() {
        assert!(validate_skill_path("graphforge-bootstrap").is_err());
        assert!(validate_skill_path("graphforge-bootstrap/SKILL.md").is_ok());
    }
}
