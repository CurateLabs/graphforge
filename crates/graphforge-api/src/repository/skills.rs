//! Repository skills ownership.

use super::{
    BTreeSet, Component, Deserialize, Digest, FileExt, GfError, MANAGED_SKILL_NAMES, OpenOptions,
    Path, RepositoryContext, SKILLS_BACKUP, SKILLS_LIFECYCLE_ROOT, SKILLS_LOCK, SKILLS_MANIFEST,
    SKILLS_ROOT, SKILLS_STAGE, SKILLS_TRANSACTION, Serialize, Sha256, SkillBundle,
    SkillMutationReceipt, SkillStatus, SkillStatusReceipt, Write, create_dir_durable, digest,
    encode_hex, fs, reject_symlink_components, remove_file_durable, rename_durable, sync_directory,
    validation, write_durable,
};

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
        let actual_digest = encode_hex(&Sha256::digest(actual.bytes));
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
            Ok(bytes) if encode_hex(&Sha256::digest(&bytes)) == file.sha256 => {}
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

impl RepositoryContext {
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
        FileExt::lock(&lock).map_err(|error| GfError::Storage(error.to_string()))?;
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
}

#[cfg(test)]
mod tests;
