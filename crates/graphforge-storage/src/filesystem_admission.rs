//! Native filesystem admission for the immutable-generation durability model.
//!
//! The probe is deliberately independent of project contents. It operates in
//! one private sibling below the canonical target parent and proves the same
//! replacement primitive used by durable publication. Resolution opens and
//! retains the project parent as the storage boundary. Unix uses
//! handle-relative, no-follow access for the project root and durable children;
//! Windows retains identities and rejects reparse points around named opens.
//!
//! This admission check is a capability probe, not a sandbox boundary against
//! another process already running as the same OS principal. Private directory
//! permissions and the per-target parent lock coordinate GraphForge processes;
//! retained directory identity plus post-operation reconciliation ensure a
//! concurrent namespace change cannot produce a successful admission.

use std::fs::File;
#[cfg(any(test, windows))]
use std::fs::OpenOptions;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::Instant;

use graphforge_core::{GfError, ProjectErrorCode};
use sha2::{Digest as _, Sha256};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use sysinfo::Disks;

const PROBE_BYTES_A: &[u8] = b"graphforge-filesystem-probe/a\n";
const PROBE_BYTES_B: &[u8] = b"graphforge-filesystem-probe/b\n";
const MAX_PROBE_FILES: u64 = 3;
const MAX_PROBE_BYTES: u64 = (PROBE_BYTES_A.len() * 2 + PROBE_BYTES_B.len()) as u64;

/// Content-free evidence from one successful native durability admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesystemAdmissionEvidence {
    /// Stable, non-device-specific filesystem class (`apfs`, `ext4`, ...).
    pub filesystem_class: String,
    /// Number of private regular files created by the probe.
    pub files_created: u64,
    /// Maximum fixed payload bytes written by the probe.
    pub bytes_written: u64,
    /// Wall-clock duration used only for safe operational diagnostics.
    pub elapsed_ms: u64,
}

/// Whether a project lifecycle requires the durable-filesystem contract.
///
/// Ephemeral mode is an explicit escape hatch for in-memory instances whose
/// temporary workspace is not presented as durable project storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectLifecycleMode {
    /// Probe and retain the supported durable-filesystem identity.
    Durable,
    /// Skip durability probing while retaining link and identity checks.
    Ephemeral,
}

/// Whether lifecycle admission may create an absent final project directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectRootRequirement {
    /// The final project directory must already exist.
    Existing,
    /// Create the final project directory after successful durable probing.
    CreateIfMissing,
}

/// Short-lived, storage-owned admission guard for one project lifecycle.
///
/// Durable guards own the persistent parent-scoped creation lock until drop.
/// The lock file itself is intentionally never unlinked. Both durable and
/// ephemeral guards retain opened parent/root identities so callers can
/// revalidate the namespace immediately before mutation.
#[derive(Debug)]
pub struct ProjectLifecycleAdmission {
    mode: ProjectLifecycleMode,
    root: PathBuf,
    parent: LifecycleDirectory,
    project: LifecycleDirectory,
    lifecycle_lock: Option<LifecycleLock>,
    evidence: Option<FilesystemAdmissionEvidence>,
    created_root: bool,
}

/// Retained identity for an admitted project root without a lifecycle lock.
///
/// This token is suitable for optimistic work that must not serialize every
/// stager. Call [`Self::readmit`] before durable publication to reacquire the
/// lifecycle lock and prove the namespace still names the retained root.
#[derive(Debug)]
pub struct ProjectRootIdentity {
    mode: ProjectLifecycleMode,
    root: PathBuf,
    parent: LifecycleDirectory,
    project: LifecycleDirectory,
}

impl ProjectLifecycleAdmission {
    /// Canonical project-root path admitted by this guard.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Lifecycle mode selected by the caller.
    #[must_use]
    pub const fn mode(&self) -> ProjectLifecycleMode {
        self.mode
    }

    /// Native durability evidence, present only for durable admission.
    #[must_use]
    pub fn evidence(&self) -> Option<&FilesystemAdmissionEvidence> {
        self.evidence.as_ref()
    }

    /// Whether this admission created the previously absent final directory.
    #[must_use]
    pub const fn created_root(&self) -> bool {
        self.created_root
    }

    /// Revalidate the retained parent, persistent lock, and project identity.
    ///
    /// # Errors
    /// Returns `GF_UNSUPPORTED_FILESYSTEM` when any named object no longer
    /// resolves to the exact opened identity retained by this guard.
    pub fn revalidate_identity(&self) -> Result<(), GfError> {
        self.parent
            .revalidate("IDENTITY", "parent_identity_changed")?;
        if let Some(lock) = &self.lifecycle_lock {
            lock.revalidate()?;
        }
        self.project
            .revalidate("IDENTITY", "project_identity_changed")?;
        if self.project.identity.volume_serial != self.parent.identity.volume_serial {
            return Err(unsupported("IDENTITY", "project_cross_volume"));
        }
        Ok(())
    }

    /// Retain parent/root identity while releasing the lifecycle lock.
    ///
    /// # Errors
    /// Returns `GF_UNSUPPORTED_FILESYSTEM` if namespace identity changed
    /// before the lock-release transition.
    pub fn into_identity(self) -> Result<ProjectRootIdentity, GfError> {
        self.revalidate_identity()?;
        let Self {
            mode,
            root,
            parent,
            project,
            lifecycle_lock,
            ..
        } = self;
        drop(lifecycle_lock);
        let identity = ProjectRootIdentity {
            mode,
            root,
            parent,
            project,
        };
        identity.revalidate_identity()?;
        Ok(identity)
    }

    /// Remove the exact project root retained by this admission.
    ///
    /// The lifecycle lock remains held while the root identity is checked and
    /// removed. The retained root handle is released only after that check so
    /// Windows can delete a directory opened without delete sharing. A second
    /// named-identity check immediately before removal prevents deleting a
    /// replacement root.
    ///
    /// # Errors
    /// Returns `GF_UNSUPPORTED_FILESYSTEM` if the admitted namespace identity
    /// changed or the exact root cannot be removed durably.
    pub fn remove_project_root(self) -> Result<(), GfError> {
        self.revalidate_identity()?;
        let Self {
            root,
            parent,
            project,
            lifecycle_lock,
            ..
        } = self;
        let project_identity = project.identity;
        drop(project);

        let named = std::fs::symlink_metadata(&root)
            .map_err(|_| unsupported("REMOVE", "project_identity_unavailable"))?;
        if is_link_or_reparse(&named)
            || !named.is_dir()
            || graphforge_filesystem::path_identity(&root)
                .map_err(|_| unsupported("REMOVE", "project_identity_unavailable"))?
                != project_identity
        {
            return Err(unsupported("REMOVE", "project_identity_changed"));
        }
        std::fs::remove_dir_all(&root)
            .map_err(|_| unsupported("REMOVE", "project_remove_failed"))?;
        complete_namespace_barrier(&parent.path)
            .map_err(|_| unsupported("REMOVE", "parent_namespace_barrier_failed"))?;
        parent.revalidate("REMOVE", "parent_identity_changed")?;
        if let Some(lock) = &lifecycle_lock {
            lock.revalidate()?;
        }
        Ok(())
    }
}

impl ProjectRootIdentity {
    /// Canonical project-root path retained by this token.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Revalidate the retained parent and project identities.
    ///
    /// # Errors
    /// Returns `GF_UNSUPPORTED_FILESYSTEM` when the named root or parent no
    /// longer resolves to the exact opened identity retained by this token.
    pub fn revalidate_identity(&self) -> Result<(), GfError> {
        self.parent
            .revalidate("IDENTITY", "parent_identity_changed")?;
        self.project
            .revalidate("IDENTITY", "project_identity_changed")?;
        if self.project.identity.volume_serial != self.parent.identity.volume_serial {
            return Err(unsupported("IDENTITY", "project_cross_volume"));
        }
        Ok(())
    }

    /// Reacquire lifecycle admission in the token's original mode for the
    /// exact retained root.
    ///
    /// The newly opened parent/root identities must equal this token's
    /// identities. A namespace replacement therefore fails closed even when
    /// it occurs while the lifecycle lock was intentionally released.
    ///
    /// # Errors
    /// Returns `GF_UNSUPPORTED_FILESYSTEM` if readmission fails or the retained
    /// namespace identity changed.
    pub fn readmit(self) -> Result<ProjectLifecycleAdmission, GfError> {
        self.revalidate_identity()?;
        let admission =
            admit_project_lifecycle(&self.root, self.mode, ProjectRootRequirement::Existing)?;
        if admission.parent.identity != self.parent.identity
            || admission.project.identity != self.project.identity
        {
            return Err(unsupported("IDENTITY", "project_identity_changed"));
        }
        admission.revalidate_identity()?;
        Ok(admission)
    }
}

/// Admit one project lifecycle before initialization, recovery, or mutation.
///
/// Durable admission creates and exclusively owns a deterministic lock file in
/// the canonical target parent, runs the native publication probe, and only
/// then creates an absent final project directory. The lock file persists after
/// the guard releases its kernel lock so crash/retry and independent processes
/// always rendezvous on the same inode.
///
/// # Errors
/// Returns `GF_UNSUPPORTED_FILESYSTEM` before final-root creation when the
/// durable contract, link policy, namespace identity, or target shape cannot
/// be proven.
pub fn admit_project_lifecycle(
    proposed_project_root: impl AsRef<Path>,
    mode: ProjectLifecycleMode,
    requirement: ProjectRootRequirement,
) -> Result<ProjectLifecycleAdmission, GfError> {
    admit_project_lifecycle_inner(
        proposed_project_root.as_ref(),
        mode,
        requirement,
        ProbeFault::None,
    )
}

fn admit_project_lifecycle_inner(
    proposed_project_root: &Path,
    mode: ProjectLifecycleMode,
    requirement: ProjectRootRequirement,
    fault: ProbeFault,
) -> Result<ProjectLifecycleAdmission, GfError> {
    let ResolvedProjectPath {
        parent,
        target_name,
        root,
    } = resolve_project_path(proposed_project_root)?;
    let lifecycle_lock = match mode {
        ProjectLifecycleMode::Durable => Some(LifecycleLock::acquire(&parent, &target_name)?),
        ProjectLifecycleMode::Ephemeral => None,
    };
    crate::project_failpoint::hit(
        "filesystem_admission.after_lifecycle_lock",
        None,
        None,
        "LIFECYCLE_LOCK",
        false,
    )?;
    parent.revalidate("LOCK", "parent_identity_changed")?;

    let evidence = match mode {
        ProjectLifecycleMode::Durable => Some(filesystem_durability_preflight_resolved(
            &parent,
            &target_name,
            fault,
        )?),
        ProjectLifecycleMode::Ephemeral => None,
    };
    crate::project_failpoint::hit(
        "filesystem_admission.after_probe",
        None,
        None,
        "PROBE",
        false,
    )?;
    parent.revalidate("IDENTITY", "parent_identity_changed")?;
    if let Some(lock) = &lifecycle_lock {
        lock.revalidate()?;
    }

    let mut created_root = false;
    match child_metadata(&parent, &target_name) {
        Ok(metadata) => {
            if is_link_or_reparse(&metadata) || !metadata.is_dir() {
                return Err(unsupported("IDENTITY", "target_link_or_special"));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if requirement == ProjectRootRequirement::Existing {
                return Err(unsupported("IDENTITY", "target_missing"));
            }
            created_root = create_missing_project_root_with(
                &parent,
                &target_name,
                || create_private_child_directory(&parent, &target_name, &root),
                || complete_namespace_barrier_handle(&parent),
            )?;
        }
        Err(_) => return Err(unsupported("IDENTITY", "target_metadata_unavailable")),
    }

    let project = LifecycleDirectory::open_child(
        &parent,
        &target_name,
        &root,
        "IDENTITY",
        "project_identity_unavailable",
    )?;
    let admission = ProjectLifecycleAdmission {
        mode,
        root,
        parent,
        project,
        lifecycle_lock,
        evidence,
        created_root,
    };
    admission.revalidate_identity()?;
    crate::project_failpoint::hit(
        "filesystem_admission.after_root_identity",
        None,
        None,
        "ROOT_IDENTITY",
        false,
    )?;
    Ok(admission)
}

fn create_missing_project_root_with<Create, Barrier>(
    parent: &LifecycleDirectory,
    target_name: &std::ffi::OsStr,
    create: Create,
    creator_barrier: Barrier,
) -> Result<bool, GfError>
where
    Create: FnOnce() -> std::io::Result<()>,
    Barrier: FnOnce() -> std::io::Result<()>,
{
    match create() {
        Ok(()) => {
            creator_barrier()
                .map_err(|_| unsupported("CREATE", "parent_namespace_barrier_failed"))?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = child_metadata(parent, target_name)
                .map_err(|_| unsupported("IDENTITY", "target_metadata_unavailable"))?;
            if is_link_or_reparse(&metadata) || !metadata.is_dir() {
                return Err(unsupported("IDENTITY", "target_link_or_special"));
            }
            Ok(false)
        }
        Err(_) => Err(unsupported("CREATE", "project_directory_create_failed")),
    }
}

#[derive(Debug)]
struct LifecycleDirectory {
    path: PathBuf,
    handle: File,
    identity: graphforge_filesystem::FileIdentity,
    ancestors: Vec<RetainedDirectory>,
}

#[derive(Debug)]
struct RetainedDirectory {
    path: PathBuf,
    handle: File,
    identity: graphforge_filesystem::FileIdentity,
}

impl LifecycleDirectory {
    fn from_handle(
        path: PathBuf,
        handle: File,
        phase: &'static str,
        cause: &'static str,
    ) -> Result<Self, GfError> {
        let opened = handle.metadata().map_err(|_| unsupported(phase, cause))?;
        let identity = file_identity(&handle)?;
        if !opened.is_dir() {
            return Err(unsupported(phase, cause));
        }
        let directory = Self {
            path,
            handle,
            identity,
            ancestors: Vec::new(),
        };
        directory.revalidate(phase, cause)?;
        Ok(directory)
    }

    fn open(path: &Path, phase: &'static str, cause: &'static str) -> Result<Self, GfError> {
        let named = std::fs::symlink_metadata(path).map_err(|_| unsupported(phase, cause))?;
        if is_link_or_reparse(&named) || !named.is_dir() {
            return Err(unsupported(phase, cause));
        }
        let handle = open_directory_handle(path).map_err(|_| unsupported(phase, cause))?;
        let identity =
            graphforge_filesystem::path_identity(path).map_err(|_| unsupported(phase, cause))?;
        let opened = handle.metadata().map_err(|_| unsupported(phase, cause))?;
        if !opened.is_dir() || file_identity(&handle)? != identity {
            return Err(unsupported(phase, cause));
        }
        Self::from_handle(path.to_path_buf(), handle, phase, cause)
    }

    fn open_child(
        parent: &Self,
        name: &std::ffi::OsStr,
        path: &Path,
        phase: &'static str,
        cause: &'static str,
    ) -> Result<Self, GfError> {
        let handle = open_child_directory_handle(parent, name, path)
            .map_err(|_| unsupported(phase, cause))?;
        let opened = handle.metadata().map_err(|_| unsupported(phase, cause))?;
        let identity = file_identity(&handle)?;
        if !opened.is_dir() {
            return Err(unsupported(phase, cause));
        }
        let directory = Self {
            path: path.to_path_buf(),
            handle,
            identity,
            ancestors: parent.retained_ancestry(phase, cause)?,
        };
        directory.revalidate(phase, cause)?;
        parent.revalidate(phase, "ancestor_identity_changed")?;
        if directory.identity.volume_serial != parent.identity.volume_serial {
            return Err(unsupported(phase, "child_cross_volume"));
        }
        Ok(directory)
    }

    fn revalidate(&self, phase: &'static str, cause: &'static str) -> Result<(), GfError> {
        for ancestor in &self.ancestors {
            ancestor.revalidate(phase, "ancestor_identity_changed")?;
        }
        let named = std::fs::symlink_metadata(&self.path).map_err(|_| unsupported(phase, cause))?;
        let opened = self
            .handle
            .metadata()
            .map_err(|_| unsupported(phase, cause))?;
        if is_link_or_reparse(&named)
            || !named.is_dir()
            || !opened.is_dir()
            || graphforge_filesystem::path_identity(&self.path)
                .map_err(|_| unsupported(phase, cause))?
                != self.identity
            || file_identity(&self.handle)? != self.identity
        {
            return Err(unsupported(phase, cause));
        }
        Ok(())
    }

    fn retained_ancestry(
        &self,
        phase: &'static str,
        cause: &'static str,
    ) -> Result<Vec<RetainedDirectory>, GfError> {
        let mut retained = Vec::with_capacity(self.ancestors.len() + 1);
        for ancestor in &self.ancestors {
            retained.push(ancestor.try_clone(phase, cause)?);
        }
        retained.push(RetainedDirectory {
            path: self.path.clone(),
            handle: self
                .handle
                .try_clone()
                .map_err(|_| unsupported(phase, cause))?,
            identity: self.identity,
        });
        Ok(retained)
    }
}

impl RetainedDirectory {
    fn try_clone(&self, phase: &'static str, cause: &'static str) -> Result<Self, GfError> {
        Ok(Self {
            path: self.path.clone(),
            handle: self
                .handle
                .try_clone()
                .map_err(|_| unsupported(phase, cause))?,
            identity: self.identity,
        })
    }

    fn revalidate(&self, phase: &'static str, cause: &'static str) -> Result<(), GfError> {
        let named = std::fs::symlink_metadata(&self.path).map_err(|_| unsupported(phase, cause))?;
        let opened = self
            .handle
            .metadata()
            .map_err(|_| unsupported(phase, cause))?;
        if is_link_or_reparse(&named)
            || !named.is_dir()
            || !opened.is_dir()
            || graphforge_filesystem::path_identity(&self.path)
                .map_err(|_| unsupported(phase, cause))?
                != self.identity
            || file_identity(&self.handle)? != self.identity
        {
            return Err(unsupported(phase, cause));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ResolvedProjectPath {
    parent: LifecycleDirectory,
    target_name: std::ffi::OsString,
    root: PathBuf,
}

#[derive(Debug)]
struct LifecycleLock {
    path: PathBuf,
    file: File,
    identity: graphforge_filesystem::FileIdentity,
    parent_identity: graphforge_filesystem::FileIdentity,
}

impl LifecycleLock {
    fn acquire(
        parent: &LifecycleDirectory,
        target_name: &std::ffi::OsStr,
    ) -> Result<Self, GfError> {
        let name = lifecycle_lock_name(&parent.path, target_name);
        let path = parent.path.join(&name);
        let file = open_lifecycle_lock_file(parent, &name)
            .map_err(|_| unsupported("LOCK", "lifecycle_lock_open_failed"))?;
        file.sync_all()
            .map_err(|_| unsupported("LOCK", "lifecycle_lock_flush_failed"))?;
        complete_namespace_barrier(&parent.path)
            .map_err(|_| unsupported("LOCK", "parent_namespace_barrier_failed"))?;
        crate::file_lock::lock_exclusive(&file)
            .map_err(|_| unsupported("LOCK", "lifecycle_lock_failed"))?;
        let identity = graphforge_filesystem::file_identity(&file)
            .map_err(|_| unsupported("LOCK", "lifecycle_lock_identity_unavailable"))?;
        let lock = Self {
            path,
            file,
            identity,
            parent_identity: parent.identity,
        };
        lock.revalidate()?;
        Ok(lock)
    }

    fn revalidate(&self) -> Result<(), GfError> {
        let named = std::fs::symlink_metadata(&self.path)
            .map_err(|_| unsupported("LOCK", "lifecycle_lock_missing"))?;
        let opened = self
            .file
            .metadata()
            .map_err(|_| unsupported("LOCK", "lifecycle_lock_unreadable"))?;
        if is_link_or_reparse(&named)
            || !named.is_file()
            || !opened.is_file()
            || graphforge_filesystem::path_link_count(&self.path)
                .map_err(|_| unsupported("LOCK", "lifecycle_lock_link_count_unavailable"))?
                != 1
            || graphforge_filesystem::file_link_count(&self.file)
                .map_err(|_| unsupported("LOCK", "lifecycle_lock_link_count_unavailable"))?
                != 1
            || graphforge_filesystem::path_identity(&self.path)
                .map_err(|_| unsupported("LOCK", "lifecycle_lock_identity_unavailable"))?
                != self.identity
            || graphforge_filesystem::file_identity(&self.file)
                .map_err(|_| unsupported("LOCK", "lifecycle_lock_identity_unavailable"))?
                != self.identity
            || self.identity.volume_serial != self.parent_identity.volume_serial
        {
            return Err(unsupported("LOCK", "lifecycle_lock_identity_changed"));
        }
        Ok(())
    }
}

impl Drop for LifecycleLock {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.file);
    }
}

fn lifecycle_lock_name(parent: &Path, target_name: &std::ffi::OsStr) -> String {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-project-lifecycle-lock/v1\0");
    digest.update(path_bytes(parent.as_os_str()));
    digest.update([0]);
    digest.update(path_bytes(target_name));
    let digest: [u8; 32] = digest.finalize().into();
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    format!(".graphforge-admission-{encoded}.lock")
}

#[cfg(unix)]
fn open_lifecycle_lock_file(parent: &LifecycleDirectory, name: &str) -> std::io::Result<File> {
    use rustix::fs::{Mode, OFlags, openat};

    let open_existing = || {
        openat(
            &parent.handle,
            name,
            OFlags::RDWR | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map(File::from)
        .map_err(std::io::Error::from)
    };
    match open_existing() {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => match openat(
            &parent.handle,
            name,
            OFlags::RDWR
                | OFlags::CREATE
                | OFlags::EXCL
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        ) {
            Ok(file) => Ok(File::from(file)),
            Err(error) if error == rustix::io::Errno::EXIST => open_existing(),
            Err(error) => Err(std::io::Error::from(error)),
        },
        Err(error) => Err(error),
    }
}

#[cfg(windows)]
fn open_lifecycle_lock_file(parent: &LifecycleDirectory, name: &str) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_WRITE_THROUGH)
        .open(parent.path.join(name))
}

#[cfg(all(not(unix), not(windows)))]
fn open_lifecycle_lock_file(_parent: &LifecycleDirectory, _name: &str) -> std::io::Result<File> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "persistent lifecycle locks are unsupported",
    ))
}

/// Prove that the proposed project location provides GraphForge's required
/// local publication primitives.
///
/// The target itself is never created or mutated. The nearest parent must
/// already exist. Caller-controlled final-component links and traversal
/// components are rejected. Namespace above the project parent is outside the
/// GraphForge storage contract, so the parent may be a mounted persistent
/// volume beneath a different process-root filesystem. The fixed macOS `/var`
/// and `/tmp` system aliases are normalized to `/private/var` and
/// `/private/tmp` first.
///
/// # Errors
/// Every inability to prove the contract returns
/// `GF_UNSUPPORTED_FILESYSTEM`. The diagnostic contains only a phase and safe
/// cause class, never the supplied path.
pub fn filesystem_durability_preflight(
    proposed_project_root: impl AsRef<Path>,
) -> Result<FilesystemAdmissionEvidence, GfError> {
    filesystem_durability_preflight_inner(proposed_project_root.as_ref(), ProbeFault::None)
}

fn filesystem_durability_preflight_inner(
    proposed_project_root: &Path,
    fault: ProbeFault,
) -> Result<FilesystemAdmissionEvidence, GfError> {
    let ResolvedProjectPath {
        parent,
        target_name,
        ..
    } = resolve_project_path(proposed_project_root)?;
    filesystem_durability_preflight_resolved(&parent, &target_name, fault)
}

fn filesystem_durability_preflight_resolved(
    parent: &LifecycleDirectory,
    target_name: &std::ffi::OsStr,
    fault: ProbeFault,
) -> Result<FilesystemAdmissionEvidence, GfError> {
    let started = Instant::now();
    let _probe_lock = lock_probe_parent(parent, target_name)?;
    let parent_metadata = parent
        .handle
        .metadata()
        .map_err(|_| unsupported("CLASSIFY", "parent_metadata_unavailable"))?;
    if !parent_metadata.is_dir() {
        return Err(unsupported("CLASSIFY", "parent_not_directory"));
    }
    if let Ok(target_metadata) = child_metadata(parent, target_name) {
        if is_link_or_reparse(&target_metadata) || !target_metadata.is_dir() {
            return Err(unsupported("CLASSIFY", "target_link_or_special"));
        }
        let target = LifecycleDirectory::open_child(
            parent,
            target_name,
            &parent.path.join(target_name),
            "CLASSIFY",
            "target_identity_unavailable",
        )?;
        if target.identity.volume_serial != parent.identity.volume_serial {
            return Err(unsupported("CLASSIFY", "target_cross_volume"));
        }
    }
    hit(fault, ProbeFault::Classify, "CLASSIFY")?;
    let filesystem_class = classify_supported_local_volume(parent)?;

    let probe_name = stable_probe_name(&parent.path, target_name);
    let probe_root = parent.path.join(&probe_name);
    if child_metadata(parent, std::ffi::OsStr::new(&probe_name)).is_ok() {
        let stale = open_probe_directory(parent, &probe_name, &probe_root)?;
        cleanup_probe(parent, stale, ProbeFault::None)?;
    }
    let probe = match create_private_probe_directory(parent, &probe_name, &probe_root) {
        Ok(probe) => probe,
        Err(create_error) => {
            if child_metadata(parent, std::ffi::OsStr::new(&probe_name)).is_ok() {
                let partial = open_probe_directory(parent, &probe_name, &probe_root)?;
                cleanup_probe(parent, partial, ProbeFault::None)?;
            }
            return Err(create_error);
        }
    };

    let probe_result = run_probe(parent, &probe, fault);
    let cleanup_result = cleanup_probe(parent, probe, fault);
    probe_result?;
    cleanup_result?;

    Ok(FilesystemAdmissionEvidence {
        filesystem_class,
        files_created: MAX_PROBE_FILES,
        bytes_written: MAX_PROBE_BYTES,
        elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    })
}

fn stable_probe_name(parent: &Path, target_name: &std::ffi::OsStr) -> String {
    let mut digest = Sha256::new();
    digest.update(path_bytes(parent.as_os_str()));
    digest.update([0]);
    digest.update(path_bytes(target_name));
    let digest: [u8; 32] = digest.finalize().into();
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    format!(".graphforge-probe-{encoded}")
}

#[cfg(unix)]
fn path_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt as _;
    value.as_bytes().to_vec()
}

#[cfg(windows)]
fn path_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt as _;
    value
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>()
}

#[cfg(all(not(unix), not(windows)))]
fn path_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
    value.to_string_lossy().as_bytes().to_vec()
}

#[cfg(unix)]
struct ProbeParentLock(File);

#[cfg(unix)]
impl Drop for ProbeParentLock {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.0);
    }
}

#[cfg(unix)]
fn lock_probe_parent(
    parent: &LifecycleDirectory,
    _target_name: &std::ffi::OsStr,
) -> Result<ProbeParentLock, GfError> {
    let handle = parent
        .handle
        .try_clone()
        .map_err(|_| unsupported("LOCK", "parent_open_failed"))?;
    crate::file_lock::lock_exclusive(&handle)
        .map_err(|_| unsupported("LOCK", "parent_lock_failed"))?;
    Ok(ProbeParentLock(handle))
}

#[cfg(windows)]
fn lock_probe_parent(
    parent: &LifecycleDirectory,
    target_name: &std::ffi::OsStr,
) -> Result<named_lock::NamedLockGuard, GfError> {
    let name = stable_probe_name(&parent.path, target_name);
    let lock = named_lock::NamedLock::create(&format!("GraphForge.{name}"))
        .map_err(|_| unsupported("LOCK", "parent_lock_create_failed"))?;
    lock.lock()
        .map_err(|_| unsupported("LOCK", "parent_lock_failed"))
}

#[cfg(all(not(unix), not(windows)))]
fn lock_probe_parent(
    _parent: &LifecycleDirectory,
    _target_name: &std::ffi::OsStr,
) -> Result<(), GfError> {
    Err(unsupported("LOCK", "parent_lock_unsupported"))
}

fn resolve_project_path(root: &Path) -> Result<ResolvedProjectPath, GfError> {
    let target_name = validate_plain_path(root)?;
    let absolute = if root.is_absolute() {
        #[cfg(target_os = "macos")]
        let root = normalize_trusted_system_alias(root)?;
        #[cfg(not(target_os = "macos"))]
        let root = root.to_path_buf();
        root
    } else {
        std::env::current_dir()
            .map_err(|_| unsupported("CLASSIFY", "working_directory_unavailable"))?
            .join(root)
    };
    let parent_path = absolute
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| unsupported("CLASSIFY", "parent_unavailable"))?;

    // The durable-storage boundary begins at the project parent. Container and
    // VM deployments routinely mount that directory on a different filesystem
    // from the process root; namespace ancestors above it are not mutated by
    // GraphForge and are therefore outside the durability contract.
    let parent = LifecycleDirectory::open(parent_path, "CLASSIFY", "parent_identity_unavailable")?;
    let root = parent.path.join(&target_name);
    if let Ok(metadata) = child_metadata(&parent, &target_name)
        && is_link_or_reparse(&metadata)
    {
        return Err(unsupported("CLASSIFY", "target_link"));
    }
    parent.revalidate("CLASSIFY", "parent_identity_changed")?;
    Ok(ResolvedProjectPath {
        parent,
        target_name,
        root,
    })
}

fn validate_plain_path(root: &Path) -> Result<std::ffi::OsString, GfError> {
    if root.components().any(|component| {
        matches!(
            component,
            std::path::Component::CurDir | std::path::Component::ParentDir
        )
    }) {
        return Err(unsupported("CLASSIFY", "path_traversal"));
    }
    Ok(root
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| unsupported("CLASSIFY", "target_name_invalid"))?
        .to_owned())
}

#[cfg(target_os = "macos")]
fn normalize_trusted_system_alias(path: &Path) -> Result<PathBuf, GfError> {
    let (alias, replacement, expected_link) = if path.starts_with("/var") {
        (
            Path::new("/var"),
            Path::new("/private/var"),
            Path::new("private/var"),
        )
    } else if path.starts_with("/tmp") {
        (
            Path::new("/tmp"),
            Path::new("/private/tmp"),
            Path::new("private/tmp"),
        )
    } else {
        return Ok(path.to_path_buf());
    };
    let metadata = std::fs::symlink_metadata(alias)
        .map_err(|_| unsupported("CLASSIFY", "system_alias_unavailable"))?;
    let link = std::fs::read_link(alias)
        .map_err(|_| unsupported("CLASSIFY", "system_alias_unavailable"))?;
    if !metadata.file_type().is_symlink() || link != expected_link {
        return Err(unsupported("CLASSIFY", "system_alias_changed"));
    }
    Ok(replacement.join(path.strip_prefix(alias).expect("prefix was checked")))
}

fn classify_supported_local_volume(parent: &LifecycleDirectory) -> Result<String, GfError> {
    classify_supported_local_volume_platform(parent)
}

#[cfg(target_os = "macos")]
fn classify_supported_local_volume_platform(
    parent: &LifecycleDirectory,
) -> Result<String, GfError> {
    let stat = rustix::fs::fstatfs(&parent.handle)
        .map_err(|_| unsupported("CLASSIFY", "native_volume_query_failed"))?;
    let class = stat
        .f_fstypename
        .iter()
        .copied()
        .take_while(|byte| *byte != 0)
        .map(|byte| u8::try_from(byte).unwrap_or_default())
        .collect::<Vec<_>>();
    let class = std::str::from_utf8(&class)
        .map_err(|_| unsupported("CLASSIFY", "filesystem_class_invalid"))?
        .to_ascii_lowercase();
    if class != "apfs" {
        return Err(unsupported("CLASSIFY", "filesystem_class_unproven"));
    }
    let flags = stat.f_flags;
    if (flags & u32::try_from(libc::MNT_LOCAL).unwrap_or(u32::MAX)) == 0 {
        return Err(unsupported("CLASSIFY", "volume_not_local"));
    }
    if (flags & u32::try_from(libc::MNT_RDONLY).unwrap_or(u32::MAX)) != 0 {
        return Err(unsupported("CLASSIFY", "volume_read_only"));
    }
    reject_removable_volume(&parent.path)?;
    Ok(class)
}

#[cfg(target_os = "linux")]
fn classify_supported_local_volume_platform(
    parent: &LifecycleDirectory,
) -> Result<String, GfError> {
    let stat = rustix::fs::fstatfs(&parent.handle)
        .map_err(|_| unsupported("CLASSIFY", "native_volume_query_failed"))?;
    let class = match u64::try_from(stat.f_type).unwrap_or_default() {
        0xEF53 => "ext",
        0x5846_5342 => "xfs",
        0x9123_683E => "btrfs",
        _ => return Err(unsupported("CLASSIFY", "filesystem_class_unproven")),
    };
    let vfs = rustix::fs::fstatvfs(&parent.handle)
        .map_err(|_| unsupported("CLASSIFY", "native_volume_query_failed"))?;
    if vfs.f_flag.contains(rustix::fs::StatVfsMountFlags::RDONLY) {
        return Err(unsupported("CLASSIFY", "volume_read_only"));
    }
    reject_removable_volume(&parent.path)?;
    Ok(class.into())
}

#[cfg(target_os = "windows")]
fn classify_supported_local_volume_platform(
    parent: &LifecycleDirectory,
) -> Result<String, GfError> {
    let information = graphforge_filesystem::windows_volume_information(&parent.path)
        .map_err(|_| unsupported("CLASSIFY", "native_volume_query_failed"))?;
    classify_windows_volume(
        &information.filesystem_name,
        information.read_only,
        information.fixed,
    )
}

#[cfg(any(test, target_os = "windows"))]
fn classify_windows_volume(
    filesystem_name: &str,
    read_only: bool,
    fixed: bool,
) -> Result<String, GfError> {
    if read_only {
        return Err(unsupported("CLASSIFY", "volume_read_only"));
    }
    if !fixed {
        return Err(unsupported("CLASSIFY", "volume_not_fixed_local"));
    }
    let class = filesystem_name.to_ascii_lowercase();
    if class != "ntfs" {
        return Err(unsupported("CLASSIFY", "filesystem_class_unproven"));
    }
    Ok(class)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn classify_supported_local_volume_platform(
    _parent: &LifecycleDirectory,
) -> Result<String, GfError> {
    Err(unsupported("CLASSIFY", "platform_unsupported"))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn reject_removable_volume(parent: &Path) -> Result<(), GfError> {
    let disks = Disks::new_with_refreshed_list();
    let disk = disks
        .list()
        .iter()
        .filter(|disk| parent.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().components().count())
        .ok_or_else(|| unsupported("CLASSIFY", "device_identity_unknown"))?;
    if disk.is_removable() {
        return Err(unsupported("CLASSIFY", "volume_removable"));
    }
    Ok(())
}

struct ProbeDirectory {
    path: PathBuf,
    handle: File,
    identity: graphforge_filesystem::FileIdentity,
}

impl ProbeDirectory {
    fn revalidate(&self, phase: &'static str) -> Result<(), GfError> {
        let named = std::fs::symlink_metadata(&self.path)
            .map_err(|_| unsupported(phase, "private_directory_missing"))?;
        let opened = self
            .handle
            .metadata()
            .map_err(|_| unsupported(phase, "private_directory_handle_unreadable"))?;
        if is_link_or_reparse(&named)
            || !named.is_dir()
            || !opened.is_dir()
            || graphforge_filesystem::path_identity(&self.path)
                .map_err(|_| unsupported(phase, "private_directory_identity_unavailable"))?
                != self.identity
            || file_identity(&self.handle)? != self.identity
        {
            return Err(unsupported(phase, "private_directory_identity_changed"));
        }
        Ok(())
    }
}

fn open_probe_directory(
    parent: &LifecycleDirectory,
    name: &str,
    path: &Path,
) -> Result<ProbeDirectory, GfError> {
    let named = child_metadata(parent, std::ffi::OsStr::new(name))
        .map_err(|_| unsupported("CREATE", "private_directory_missing"))?;
    if is_link_or_reparse(&named) || !named.is_dir() {
        return Err(unsupported("CREATE", "private_directory_substituted"));
    }
    let handle = open_child_directory_handle(parent, std::ffi::OsStr::new(name), path)
        .map_err(|_| unsupported("CREATE", "private_directory_open_failed"))?;
    let opened = handle
        .metadata()
        .map_err(|_| unsupported("CREATE", "private_directory_handle_unreadable"))?;
    let identity = graphforge_filesystem::path_identity(path)
        .map_err(|_| unsupported("CREATE", "private_directory_identity_unavailable"))?;
    if !opened.is_dir() || file_identity(&handle)? != identity {
        return Err(unsupported(
            "CREATE",
            "private_directory_substituted_during_open",
        ));
    }
    parent.revalidate("CREATE", "parent_identity_changed")?;
    Ok(ProbeDirectory {
        path: path.to_path_buf(),
        handle,
        identity,
    })
}

#[cfg(unix)]
fn open_child_directory_handle(
    parent: &LifecycleDirectory,
    name: &std::ffi::OsStr,
    _path: &Path,
) -> std::io::Result<File> {
    use rustix::fs::{Mode, OFlags, openat};

    openat(
        &parent.handle,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(std::io::Error::from)
}

#[cfg(windows)]
fn open_child_directory_handle(
    _parent: &LifecycleDirectory,
    _name: &std::ffi::OsStr,
    path: &Path,
) -> std::io::Result<File> {
    open_directory_handle(path)
}

#[cfg(all(not(unix), not(windows)))]
fn open_child_directory_handle(
    _parent: &LifecycleDirectory,
    _name: &std::ffi::OsStr,
    _path: &Path,
) -> std::io::Result<File> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "child directory handles are unsupported",
    ))
}

#[cfg(unix)]
fn child_metadata(
    parent: &LifecycleDirectory,
    name: &std::ffi::OsStr,
) -> std::io::Result<std::fs::Metadata> {
    use rustix::fs::{AtFlags, statat};
    use std::os::unix::fs::MetadataExt as _;

    let stat =
        statat(&parent.handle, name, AtFlags::SYMLINK_NOFOLLOW).map_err(std::io::Error::from)?;
    let path_metadata = std::fs::symlink_metadata(parent.path.join(name))?;
    #[cfg(target_os = "linux")]
    let stat_device = stat.st_dev;
    #[cfg(not(target_os = "linux"))]
    let stat_device = u64::try_from(stat.st_dev).unwrap_or(u64::MAX);
    if path_metadata.dev() != stat_device || path_metadata.ino() != stat.st_ino {
        return Err(std::io::Error::other("child identity changed"));
    }
    Ok(path_metadata)
}

#[cfg(not(unix))]
fn child_metadata(
    parent: &LifecycleDirectory,
    name: &std::ffi::OsStr,
) -> std::io::Result<std::fs::Metadata> {
    std::fs::symlink_metadata(parent.path.join(name))
}

#[cfg(unix)]
fn create_private_child_directory(
    parent: &LifecycleDirectory,
    name: &std::ffi::OsStr,
    _path: &Path,
) -> std::io::Result<()> {
    use rustix::fs::{Mode, mkdirat};

    mkdirat(&parent.handle, name, Mode::RUSR | Mode::WUSR | Mode::XUSR)
        .map_err(std::io::Error::from)
}

#[cfg(not(unix))]
fn create_private_child_directory(
    parent: &LifecycleDirectory,
    name: &std::ffi::OsStr,
    path: &Path,
) -> std::io::Result<()> {
    parent
        .revalidate("CREATE", "parent_identity_changed")
        .map_err(std::io::Error::other)?;
    graphforge_filesystem::create_private_directory(path)?;
    parent
        .revalidate("CREATE", "parent_identity_changed")
        .map_err(std::io::Error::other)?;
    let metadata = child_metadata(parent, name)?;
    if is_link_or_reparse(&metadata) || !metadata.is_dir() {
        return Err(std::io::Error::other("created child is linked or special"));
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn open_directory_handle(path: &Path) -> std::io::Result<File> {
    use rustix::fs::{Mode, OFlags, open};

    let handle = open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    Ok(File::from(handle))
}

#[cfg(windows)]
pub(crate) fn open_directory_handle(path: &Path) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
}

#[cfg(all(not(unix), not(windows)))]
pub(crate) fn open_directory_handle(_path: &Path) -> std::io::Result<File> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "directory identity handles are unsupported",
    ))
}

fn create_private_probe_directory(
    parent: &LifecycleDirectory,
    probe_name: &str,
    probe_root: &Path,
) -> Result<ProbeDirectory, GfError> {
    create_private_child_directory(parent, std::ffi::OsStr::new(probe_name), probe_root)
        .map_err(|_| unsupported("CREATE", "private_directory_create_failed"))?;
    let probe = open_probe_directory(parent, probe_name, probe_root)?;
    complete_namespace_barrier_handle(parent)
        .map_err(|_| unsupported("CREATE", "parent_namespace_barrier_failed"))?;
    probe.revalidate("CREATE")?;
    Ok(probe)
}

fn run_probe(
    parent: &LifecycleDirectory,
    probe: &ProbeDirectory,
    fault: ProbeFault,
) -> Result<(), GfError> {
    probe.revalidate("CREATE")?;
    if parent.identity.volume_serial != probe.identity.volume_serial {
        return Err(unsupported("CREATE", "private_directory_cross_volume"));
    }

    let lock_path = probe.path.join("lock");
    let mut lock = create_new_file(probe, "lock")?;
    lock.write_all(PROBE_BYTES_A)
        .map_err(|_| unsupported("WRITE", "lock_file_write_failed"))?;
    hit(fault, ProbeFault::Write, "WRITE")?;
    lock.sync_all()
        .map_err(|_| unsupported("FILE_FLUSH", "lock_file_flush_failed"))?;
    hit(fault, ProbeFault::FileFlush, "FILE_FLUSH")?;
    verify_stable_identity(&lock, &lock_path, &parent.path)?;

    crate::file_lock::lock_exclusive(&lock)
        .map_err(|_| unsupported("LOCK", "exclusive_lock_failed"))?;
    let contender = open_regular_non_link(probe, "lock")?;
    if crate::file_lock::try_lock_shared(&contender)
        .map_err(|_| unsupported("LOCK", "contention_check_failed"))?
    {
        let _ = crate::file_lock::unlock(&contender);
        return Err(unsupported("LOCK", "exclusive_lock_not_enforced"));
    }
    verify_stable_identity(&lock, &lock_path, &parent.path)?;
    crate::file_lock::unlock(&lock).map_err(|_| unsupported("LOCK", "exclusive_unlock_failed"))?;

    crate::file_lock::lock_shared(&lock).map_err(|_| unsupported("LOCK", "shared_lock_failed"))?;
    crate::file_lock::lock_shared(&contender)
        .map_err(|_| unsupported("LOCK", "second_shared_lock_failed"))?;
    let exclusive_contender = open_regular_non_link(probe, "lock")?;
    if crate::file_lock::try_lock_exclusive(&exclusive_contender)
        .map_err(|_| unsupported("LOCK", "shared_contention_check_failed"))?
    {
        let _ = crate::file_lock::unlock(&exclusive_contender);
        return Err(unsupported("LOCK", "shared_lock_not_enforced"));
    }
    crate::file_lock::unlock(&contender)
        .and_then(|()| crate::file_lock::unlock(&lock))
        .map_err(|_| unsupported("LOCK", "shared_unlock_failed"))?;
    hit(fault, ProbeFault::Lock, "LOCK")?;

    let (target, target_identity, target_path) = replace_probe_file(probe, fault)?;
    probe.revalidate("NAMESPACE_DURABILITY")?;
    complete_namespace_barrier(&probe.path)
        .map_err(|_| unsupported("NAMESPACE_DURABILITY", "probe_namespace_barrier_failed"))?;
    hit(
        fault,
        ProbeFault::NamespaceDurability,
        "NAMESPACE_DURABILITY",
    )?;

    // The open handle must keep the old identity while the pathname now names
    // the replacement. This proves stable locked/open file identity across the
    // exact replacement primitive publication will consume.
    probe.revalidate("IDENTITY")?;
    hit(fault, ProbeFault::Identity, "IDENTITY")?;
    if file_identity(&target)? != target_identity {
        return Err(unsupported("IDENTITY", "open_identity_changed"));
    }
    let mut published = open_regular_non_link(probe, "published")?;
    if file_identity(&published)? == target_identity {
        return Err(unsupported("IDENTITY", "pathname_identity_not_replaced"));
    }
    let mut bytes = Vec::new();
    published
        .read_to_end(&mut bytes)
        .map_err(|_| unsupported("IDENTITY", "replacement_read_failed"))?;
    if bytes != PROBE_BYTES_B {
        return Err(unsupported("IDENTITY", "replacement_bytes_mismatch"));
    }
    verify_stable_identity(&published, &target_path, &parent.path)?;
    drop(target);
    complete_namespace_barrier_handle(parent)
        .map_err(|_| unsupported("NAMESPACE_DURABILITY", "parent_namespace_barrier_failed"))
}

fn replace_probe_file(
    probe: &ProbeDirectory,
    fault: ProbeFault,
) -> Result<(File, graphforge_filesystem::FileIdentity, PathBuf), GfError> {
    probe.revalidate("REPLACE")?;
    let mut initial = create_new_file(probe, "initial")?;
    initial
        .write_all(PROBE_BYTES_A)
        .and_then(|()| initial.sync_all())
        .map_err(|_| unsupported("REPLACE", "initial_file_flush_failed"))?;
    drop(initial);
    graphforge_filesystem::install_new_file(
        &probe.handle,
        std::ffi::OsStr::new("initial"),
        std::ffi::OsStr::new("published"),
    )
    .map_err(|_| unsupported("REPLACE", "atomic_create_failed"))?;

    let target_path = probe.path.join("published");
    let target = open_regular_non_link(probe, "published")?;
    let target_identity = file_identity(&target)?;
    let replacement_path = probe.path.join("replacement");
    let mut replacement = create_new_file(probe, "replacement")?;
    replacement
        .write_all(PROBE_BYTES_B)
        .and_then(|()| replacement.sync_all())
        .map_err(|_| unsupported("REPLACE", "replacement_file_flush_failed"))?;
    drop(replacement);
    hit(fault, ProbeFault::Replace, "REPLACE")?;

    let replacement_result = if fault == ProbeFault::ReplaceUnknown {
        let source_before = graphforge_filesystem::path_identity(&replacement_path)
            .map_err(|_| unsupported("REPLACE", "source_identity_unavailable"))?;
        let target_before = graphforge_filesystem::path_identity(&target_path)
            .map_err(|_| unsupported("REPLACE", "target_identity_unavailable"))?;
        graphforge_filesystem::replace_file(
            &probe.handle,
            std::ffi::OsStr::new("replacement"),
            std::ffi::OsStr::new("published"),
        )
        .map_err(|_| unsupported("REPLACE", "fault_setup_replace_failed"))?;
        Err(graphforge_filesystem::classify_failed_replacement(
            std::io::Error::other("injected OS failure after namespace mutation"),
            source_before,
            target_before,
            graphforge_filesystem::path_identity(&replacement_path).ok(),
            graphforge_filesystem::path_identity(&target_path).ok(),
        ))
    } else {
        graphforge_filesystem::replace_file(
            &probe.handle,
            std::ffi::OsStr::new("replacement"),
            std::ffi::OsStr::new("published"),
        )
    };
    match replacement_result {
        Ok(()) => Ok((target, target_identity, target_path)),
        Err(graphforge_filesystem::ReplaceFileError::NotReplaced(_)) => {
            Err(unsupported("REPLACE", "atomic_replace_not_applied"))
        }
        Err(graphforge_filesystem::ReplaceFileError::StateUnknown(_)) => {
            Err(unsupported("REPLACE", "atomic_replace_state_unknown"))
        }
    }
}

fn create_new_file(probe: &ProbeDirectory, name: &str) -> Result<File, GfError> {
    let file = open_probe_file(probe, name, true)
        .map_err(|_| unsupported("CREATE", "exclusive_file_create_failed"))?;
    let metadata = file
        .metadata()
        .map_err(|_| unsupported("CREATE", "created_file_metadata_failed"))?;
    if !metadata.is_file()
        || graphforge_filesystem::file_link_count(&file)
            .map_err(|_| unsupported("CREATE", "created_file_link_count_unavailable"))?
            != 1
    {
        return Err(unsupported("CREATE", "created_file_identity_invalid"));
    }
    Ok(file)
}

fn open_regular_non_link(probe: &ProbeDirectory, name: &str) -> Result<File, GfError> {
    let path = probe.path.join(name);
    let metadata = std::fs::symlink_metadata(&path)
        .map_err(|_| unsupported("IDENTITY", "path_metadata_failed"))?;
    if is_link_or_reparse(&metadata)
        || !metadata.is_file()
        || graphforge_filesystem::path_link_count(&path)
            .map_err(|_| unsupported("IDENTITY", "path_link_count_unavailable"))?
            != 1
    {
        return Err(unsupported("IDENTITY", "path_link_or_special"));
    }
    let file = open_probe_file(probe, name, false)
        .map_err(|_| unsupported("IDENTITY", "file_open_failed"))?;
    if file_identity(&file)?
        != graphforge_filesystem::path_identity(&path)
            .map_err(|_| unsupported("IDENTITY", "path_identity_unavailable"))?
    {
        return Err(unsupported("IDENTITY", "file_substituted_during_open"));
    }
    Ok(file)
}

#[cfg(unix)]
fn open_probe_file(probe: &ProbeDirectory, name: &str, create: bool) -> std::io::Result<File> {
    use rustix::fs::{Mode, OFlags, openat};

    let mut flags = OFlags::RDWR | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
    if create {
        flags |= OFlags::CREATE | OFlags::EXCL;
    }
    openat(&probe.handle, name, flags, Mode::RUSR | Mode::WUSR)
        .map(File::from)
        .map_err(std::io::Error::from)
}

#[cfg(windows)]
fn open_probe_file(probe: &ProbeDirectory, name: &str, create: bool) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    const FILE_FLAG_WRITE_THROUGH: u32 = 0x8000_0000;
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_WRITE_THROUGH);
    if create {
        options.create_new(true);
    }
    options.open(probe.path.join(name))
}

#[cfg(all(not(unix), not(windows)))]
fn open_probe_file(_probe: &ProbeDirectory, _name: &str, _create: bool) -> std::io::Result<File> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "probe files are unsupported",
    ))
}

fn verify_stable_identity(file: &File, path: &Path, parent: &Path) -> Result<(), GfError> {
    let named = std::fs::symlink_metadata(path)
        .map_err(|_| unsupported("IDENTITY", "named_metadata_failed"))?;
    if is_link_or_reparse(&named)
        || !named.is_file()
        || graphforge_filesystem::file_link_count(file)
            .map_err(|_| unsupported("IDENTITY", "opened_link_count_unavailable"))?
            != 1
        || graphforge_filesystem::path_link_count(path)
            .map_err(|_| unsupported("IDENTITY", "named_link_count_unavailable"))?
            != 1
        || file_identity(file)?
            != graphforge_filesystem::path_identity(path)
                .map_err(|_| unsupported("IDENTITY", "path_identity_unavailable"))?
        || !same_volume_paths(parent, path)?
    {
        return Err(unsupported("IDENTITY", "stable_identity_unproven"));
    }
    Ok(())
}

fn cleanup_probe(
    parent: &LifecycleDirectory,
    probe: ProbeDirectory,
    fault: ProbeFault,
) -> Result<(), GfError> {
    hit(fault, ProbeFault::Cleanup, "CLEANUP")?;
    probe.revalidate("CLEANUP")?;
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(&probe.path)
        .map_err(|_| unsupported("CLEANUP", "private_directory_unreadable"))?
    {
        if entries.len() >= usize::try_from(MAX_PROBE_FILES).unwrap_or(usize::MAX) {
            return Err(unsupported("CLEANUP", "private_entry_limit_exceeded"));
        }
        entries.push(entry.map_err(|_| unsupported("CLEANUP", "private_entry_unreadable"))?);
    }
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let name = entry.file_name();
        if name != "lock" && name != "initial" && name != "published" && name != "replacement" {
            return Err(unsupported("CLEANUP", "private_entry_unknown"));
        }
        let metadata = std::fs::symlink_metadata(entry.path())
            .map_err(|_| unsupported("CLEANUP", "private_entry_metadata_failed"))?;
        if is_link_or_reparse(&metadata)
            || !metadata.is_file()
            || graphforge_filesystem::path_link_count(&entry.path())
                .map_err(|_| unsupported("CLEANUP", "entry_link_count_unavailable"))?
                != 1
        {
            return Err(unsupported("CLEANUP", "private_entry_link_or_special"));
        }
        let size_limit = if name == "lock" || name == "initial" {
            PROBE_BYTES_A.len()
        } else if name == "replacement" {
            PROBE_BYTES_B.len()
        } else {
            PROBE_BYTES_A.len().max(PROBE_BYTES_B.len())
        };
        if metadata.len() > u64::try_from(size_limit).unwrap_or(u64::MAX) {
            return Err(unsupported("CLEANUP", "private_entry_size_exceeded"));
        }
        std::fs::remove_file(entry.path())
            .map_err(|_| unsupported("CLEANUP", "private_entry_remove_failed"))?;
    }
    probe.revalidate("CLEANUP")?;
    let path = probe.path.clone();
    drop(probe.handle);
    std::fs::remove_dir(path)
        .map_err(|_| unsupported("CLEANUP", "private_directory_remove_failed"))?;
    complete_namespace_barrier_handle(parent)
        .map_err(|_| unsupported("CLEANUP", "parent_namespace_barrier_failed"))
}

fn complete_namespace_barrier_handle(parent: &LifecycleDirectory) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        parent.handle.sync_all()
    }
    #[cfg(not(unix))]
    {
        complete_namespace_barrier(&parent.path)
    }
}

#[cfg(unix)]
fn complete_namespace_barrier(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(windows)]
fn complete_namespace_barrier(path: &Path) -> std::io::Result<()> {
    // NTFS persists rename metadata through the write-through staging handle.
    // Directory FlushFileBuffers is not a documented Windows durability
    // barrier; here we only revalidate that the namespace parent is ordinary.
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.is_dir() && !is_link_or_reparse(&metadata) {
        Ok(())
    } else {
        Err(std::io::Error::other(
            "namespace parent is linked or not a directory",
        ))
    }
}

#[cfg(all(not(unix), not(windows)))]
fn complete_namespace_barrier(_path: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "namespace durability barrier is unsupported",
    ))
}

#[cfg(windows)]
fn is_link_or_reparse(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_type().is_symlink()
        || (metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0
}

#[cfg(not(windows))]
fn is_link_or_reparse(metadata: &std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

fn file_identity(file: &File) -> Result<graphforge_filesystem::FileIdentity, GfError> {
    graphforge_filesystem::file_identity(file)
        .map_err(|_| unsupported("IDENTITY", "opened_identity_unavailable"))
}

fn same_volume_paths(left: &Path, right: &Path) -> Result<bool, GfError> {
    let left = graphforge_filesystem::path_identity(left)
        .map_err(|_| unsupported("IDENTITY", "left_volume_identity_unavailable"))?;
    let right = graphforge_filesystem::path_identity(right)
        .map_err(|_| unsupported("IDENTITY", "right_volume_identity_unavailable"))?;
    Ok(left.volume_serial == right.volume_serial)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeFault {
    None,
    Classify,
    Lock,
    Write,
    FileFlush,
    Replace,
    ReplaceUnknown,
    NamespaceDurability,
    Identity,
    Cleanup,
}

fn hit(actual: ProbeFault, expected: ProbeFault, phase: &'static str) -> Result<(), GfError> {
    if actual == expected {
        Err(unsupported(phase, "injected_failure"))
    } else {
        Ok(())
    }
}

fn unsupported(phase: &'static str, cause: &'static str) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::UnsupportedFilesystem,
        message: format!("phase={phase} outcome=rejected cause={cause}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod directory_capability_conformance {
        include!("directory_capability_conformance.rs");
    }

    fn canonical_tempdir() -> tempfile::TempDir {
        let base = std::env::temp_dir().canonicalize().unwrap();
        tempfile::tempdir_in(base).unwrap()
    }

    #[test]
    fn native_probe_is_bounded_content_free_and_cleans_up() {
        let parent = canonical_tempdir();
        let target = parent.path().join("project");
        let evidence = filesystem_durability_preflight(&target).unwrap();
        assert!(matches!(
            evidence.filesystem_class.as_str(),
            "apfs" | "ext" | "ext2" | "ext3" | "ext4" | "xfs" | "btrfs" | "ntfs"
        ));
        assert_eq!(evidence.files_created, 3);
        assert_eq!(evidence.bytes_written, MAX_PROBE_BYTES);
        assert!(!target.exists());
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn durable_lifecycle_rejects_before_target_mutation_and_retries_on_persistent_lock() {
        let parent = canonical_tempdir();
        let target = parent.path().join("project");
        let target_name = target.file_name().unwrap();
        let lock = parent
            .path()
            .join(lifecycle_lock_name(parent.path(), target_name));

        let error = admit_project_lifecycle_inner(
            &target,
            ProjectLifecycleMode::Durable,
            ProjectRootRequirement::CreateIfMissing,
            ProbeFault::Classify,
        )
        .unwrap_err();
        assert_eq!(error.code(), "GF_UNSUPPORTED_FILESYSTEM");
        assert!(
            !target.exists(),
            "failed admission must not create the root"
        );
        assert!(lock.is_file(), "the rendezvous lock persists after failure");
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 1);

        let admission = admit_project_lifecycle(
            &target,
            ProjectLifecycleMode::Durable,
            ProjectRootRequirement::CreateIfMissing,
        )
        .unwrap();
        assert!(admission.created_root());
        assert!(admission.evidence().is_some());
        admission.revalidate_identity().unwrap();
        drop(admission);
        assert!(target.is_dir());
        assert!(lock.is_file(), "unlock must never unlink the lock file");
    }

    #[test]
    fn durable_lifecycle_preserves_existing_current_bytes() {
        let parent = canonical_tempdir();
        let target = parent.path().join("project");
        graphforge_filesystem::create_private_directory(&target).unwrap();
        let current = target.join("CURRENT");
        let expected = b"existing-current-authority\n";
        std::fs::write(&current, expected).unwrap();

        let admission = admit_project_lifecycle(
            &target,
            ProjectLifecycleMode::Durable,
            ProjectRootRequirement::Existing,
        )
        .unwrap();
        assert!(!admission.created_root());
        admission.revalidate_identity().unwrap();
        assert_eq!(std::fs::read(&current).unwrap(), expected);
    }

    #[test]
    fn concurrent_first_admissions_create_exactly_one_root_and_share_identity() {
        use std::sync::{Arc, Barrier};

        let parent = canonical_tempdir();
        let target = parent.path().join("project");
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let target = target.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                let admission = admit_project_lifecycle(
                    &target,
                    ProjectLifecycleMode::Durable,
                    ProjectRootRequirement::CreateIfMissing,
                )
                .unwrap();
                admission.revalidate_identity().unwrap();
                (admission.created_root(), admission.project.identity)
            }));
        }
        barrier.wait();
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|(created, _)| *created).count(), 1);
        assert_eq!(results[0].1, results[1].1);
        assert_eq!(
            graphforge_filesystem::path_identity(&target).unwrap(),
            results[0].1
        );
        let lock = parent.path().join(lifecycle_lock_name(
            parent.path(),
            target.file_name().unwrap(),
        ));
        assert!(lock.is_file());
    }

    #[test]
    fn concurrent_already_exists_directory_is_reopened_without_creator_credit() {
        let parent = canonical_tempdir();
        let target = parent.path().join("project");
        let ResolvedProjectPath {
            parent,
            target_name,
            root,
        } = resolve_project_path(&target).unwrap();

        let created_root = create_missing_project_root_with(
            &parent,
            &target_name,
            || {
                graphforge_filesystem::create_private_directory(&root).unwrap();
                Err(std::io::ErrorKind::AlreadyExists.into())
            },
            || panic!("a concurrent creator owns the namespace barrier"),
        )
        .unwrap();

        assert!(!created_root);
        let project = LifecycleDirectory::open_child(
            &parent,
            &target_name,
            &root,
            "IDENTITY",
            "project_identity_unavailable",
        )
        .unwrap();
        project
            .revalidate("IDENTITY", "project_identity_changed")
            .unwrap();
    }

    #[test]
    fn ephemeral_lifecycle_is_an_explicit_probe_and_lock_bypass() {
        let parent = canonical_tempdir();
        let target = parent.path().join("ephemeral");
        let admission = admit_project_lifecycle_inner(
            &target,
            ProjectLifecycleMode::Ephemeral,
            ProjectRootRequirement::CreateIfMissing,
            ProbeFault::Classify,
        )
        .unwrap();
        assert_eq!(admission.mode(), ProjectLifecycleMode::Ephemeral);
        assert!(admission.evidence().is_none());
        assert!(admission.created_root());
        admission.revalidate_identity().unwrap();
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 1);
    }

    const LIFECYCLE_TEST_COOKIE: &str = "graphforge-780-lifecycle-test";

    #[test]
    fn subprocess_lifecycle_admission() {
        if std::env::var("GF_780_LIFECYCLE_COOKIE").as_deref() != Ok(LIFECYCLE_TEST_COOKIE) {
            return;
        }
        let root = PathBuf::from(std::env::var_os("GF_780_PROJECT_ROOT").unwrap());
        let admission = admit_project_lifecycle(
            root,
            ProjectLifecycleMode::Durable,
            ProjectRootRequirement::CreateIfMissing,
        )
        .unwrap();
        admission.revalidate_identity().unwrap();
    }

    #[test]
    fn lifecycle_phase_crashes_retry_to_one_bounded_root_and_lock() {
        for phase in [
            "filesystem_admission.after_lifecycle_lock",
            "filesystem_admission.after_probe",
            "filesystem_admission.after_root_identity",
        ] {
            let parent = canonical_tempdir();
            let root = parent.path().join("project");
            let lock = parent.path().join(lifecycle_lock_name(
                parent.path(),
                root.file_name().unwrap(),
            ));
            let crashed = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "filesystem_admission::tests::subprocess_lifecycle_admission",
                    "--nocapture",
                ])
                .env("GF_780_LIFECYCLE_COOKIE", LIFECYCLE_TEST_COOKIE)
                .env("GF_780_PROJECT_ROOT", &root)
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", phase)
                .status()
                .unwrap();
            assert_eq!(
                crashed.code(),
                Some(crate::project_failpoint::exit_code()),
                "{phase}"
            );
            assert!(lock.is_file(), "{phase}");
            assert!(
                std::fs::read_dir(parent.path()).unwrap().count() <= 2,
                "{phase} left unbounded admission artifacts"
            );

            let retry = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "filesystem_admission::tests::subprocess_lifecycle_admission",
                    "--nocapture",
                ])
                .env("GF_780_LIFECYCLE_COOKIE", LIFECYCLE_TEST_COOKIE)
                .env("GF_780_PROJECT_ROOT", &root)
                .status()
                .unwrap();
            assert!(retry.success(), "retry after {phase} failed: {retry}");

            let admission = admit_project_lifecycle(
                &root,
                ProjectLifecycleMode::Durable,
                ProjectRootRequirement::Existing,
            )
            .unwrap();
            admission.revalidate_identity().unwrap();
            assert!(root.is_dir(), "{phase}");
            assert!(lock.is_file(), "{phase}");
            assert_eq!(
                std::fs::read_dir(parent.path()).unwrap().count(),
                2,
                "{phase} did not converge to exactly one root and one lock"
            );
        }
    }

    #[test]
    fn admitted_root_can_be_removed_without_unlinking_the_lifecycle_lock() {
        let parent = canonical_tempdir();
        let root = parent.path().join("project");
        let admission = admit_project_lifecycle(
            &root,
            ProjectLifecycleMode::Durable,
            ProjectRootRequirement::CreateIfMissing,
        )
        .unwrap();
        let lock = parent.path().join(lifecycle_lock_name(
            parent.path(),
            root.file_name().unwrap(),
        ));
        std::fs::write(root.join("owned"), b"data").unwrap();

        admission.remove_project_root().unwrap();

        assert!(!root.exists());
        assert!(lock.is_file());
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn root_removal_rejects_namespace_substitution_without_deleting_either_tree() {
        let parent = canonical_tempdir();
        let root = parent.path().join("project");
        let moved = parent.path().join("moved");
        let admission = admit_project_lifecycle(
            &root,
            ProjectLifecycleMode::Ephemeral,
            ProjectRootRequirement::CreateIfMissing,
        )
        .unwrap();
        std::fs::rename(&root, &moved).unwrap();
        graphforge_filesystem::create_private_directory(&root).unwrap();

        let error = admission.remove_project_root().unwrap_err();

        assert_eq!(error.code(), "GF_UNSUPPORTED_FILESYSTEM");
        assert!(root.is_dir());
        assert!(moved.is_dir());
    }

    #[test]
    fn existing_ephemeral_root_retains_canonical_identity_before_ancestor_policy() {
        let root = tempfile::tempdir().unwrap();
        let admission = admit_project_lifecycle(
            root.path(),
            ProjectLifecycleMode::Ephemeral,
            ProjectRootRequirement::Existing,
        )
        .unwrap();
        assert_eq!(
            graphforge_filesystem::path_identity(admission.root()).unwrap(),
            graphforge_filesystem::path_identity(root.path()).unwrap()
        );
        admission.revalidate_identity().unwrap();
    }

    #[test]
    fn identity_token_releases_and_readmits_the_same_durable_root() {
        let parent = canonical_tempdir();
        let root = parent.path().join("project");
        let admission = admit_project_lifecycle(
            &root,
            ProjectLifecycleMode::Durable,
            ProjectRootRequirement::CreateIfMissing,
        )
        .unwrap();
        let expected = admission.project.identity;

        let identity = admission.into_identity().unwrap();
        identity.revalidate_identity().unwrap();
        assert_eq!(identity.root(), root);
        let readmitted = identity.readmit().unwrap();

        assert_eq!(readmitted.project.identity, expected);
        readmitted.revalidate_identity().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn identity_token_readmission_rejects_a_replacement_root() {
        let parent = canonical_tempdir();
        let root = parent.path().join("project");
        let moved = parent.path().join("moved");
        let identity = admit_project_lifecycle(
            &root,
            ProjectLifecycleMode::Durable,
            ProjectRootRequirement::CreateIfMissing,
        )
        .unwrap()
        .into_identity()
        .unwrap();
        std::fs::rename(&root, &moved).unwrap();
        graphforge_filesystem::create_private_directory(&root).unwrap();

        let error = identity.readmit().unwrap_err();

        assert_eq!(error.code(), "GF_UNSUPPORTED_FILESYSTEM");
        assert!(root.is_dir());
        assert!(moved.is_dir());
    }

    #[test]
    fn windows_classifier_accepts_only_fixed_writable_ntfs() {
        assert_eq!(
            classify_windows_volume("NTFS", false, true).unwrap(),
            "ntfs"
        );
        for (class, read_only, fixed, cause) in [
            ("ReFS", false, true, "filesystem_class_unproven"),
            ("FAT32", false, true, "filesystem_class_unproven"),
            ("NTFS", true, true, "volume_read_only"),
            ("NTFS", false, false, "volume_not_fixed_local"),
        ] {
            let error = classify_windows_volume(class, read_only, fixed).unwrap_err();
            assert_eq!(error.code(), "GF_UNSUPPORTED_FILESYSTEM");
            assert!(error.to_string().contains(cause), "{error}");
        }
    }

    #[test]
    fn every_injected_phase_is_typed_and_never_mutates_target() {
        for fault in [
            ProbeFault::Classify,
            ProbeFault::Lock,
            ProbeFault::Write,
            ProbeFault::FileFlush,
            ProbeFault::Replace,
            ProbeFault::ReplaceUnknown,
            ProbeFault::NamespaceDurability,
            ProbeFault::Identity,
            ProbeFault::Cleanup,
        ] {
            let parent = canonical_tempdir();
            let target = parent.path().join("project");
            let error = filesystem_durability_preflight_inner(&target, fault).unwrap_err();
            assert_eq!(error.code(), "GF_UNSUPPORTED_FILESYSTEM", "{fault:?}");
            assert!(!target.exists(), "{fault:?}");
            let entries = std::fs::read_dir(parent.path()).unwrap().count();
            if fault == ProbeFault::Cleanup {
                assert_eq!(entries, 1, "cleanup failure retains one bounded sibling");
                filesystem_durability_preflight(&target).unwrap();
                assert_eq!(
                    std::fs::read_dir(parent.path()).unwrap().count(),
                    0,
                    "a later admission reconciles the bounded stale probe"
                );
            } else {
                assert_eq!(entries, 0, "{fault:?}");
            }
        }
    }

    const LOCK_TEST_COOKIE: &str = "graphforge-779-native-lock-test";

    #[test]
    fn subprocess_lock_contender() {
        if std::env::var("GF_779_LOCK_COOKIE").as_deref() != Ok(LOCK_TEST_COOKIE) {
            return;
        }
        let path = PathBuf::from(std::env::var_os("GF_779_LOCK_PATH").unwrap());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        assert!(!crate::file_lock::try_lock_shared(&file).unwrap());
    }

    #[test]
    fn subprocess_crash_lock_holder() {
        if std::env::var("GF_779_CRASH_COOKIE").as_deref() != Ok(LOCK_TEST_COOKIE) {
            return;
        }
        let path = PathBuf::from(std::env::var_os("GF_779_LOCK_PATH").unwrap());
        let ready = PathBuf::from(std::env::var_os("GF_779_READY_PATH").unwrap());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        crate::file_lock::lock_exclusive(&file).unwrap();
        let mut signal = File::create(ready).unwrap();
        signal.write_all(b"locked").unwrap();
        signal.sync_all().unwrap();
        std::process::abort();
    }

    #[test]
    fn exclusive_lock_excludes_a_separate_process() {
        let directory = canonical_tempdir();
        let path = directory.path().join("lock");
        let file = File::create(&path).unwrap();
        crate::file_lock::lock_exclusive(&file).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "filesystem_admission::tests::subprocess_lock_contender",
                "--nocapture",
            ])
            .env("GF_779_LOCK_COOKIE", LOCK_TEST_COOKIE)
            .env("GF_779_LOCK_PATH", &path)
            .status()
            .unwrap();
        assert!(status.success());
        crate::file_lock::unlock(&file).unwrap();
    }

    #[test]
    fn operating_system_releases_lock_after_process_crash() {
        use wait_timeout::ChildExt as _;

        let directory = canonical_tempdir();
        let path = directory.path().join("lock");
        let ready = directory.path().join("ready");
        File::create(&path).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "filesystem_admission::tests::subprocess_crash_lock_holder",
                "--nocapture",
            ])
            .env("GF_779_CRASH_COOKIE", LOCK_TEST_COOKIE)
            .env("GF_779_LOCK_PATH", &path)
            .env("GF_779_READY_PATH", &ready)
            .spawn()
            .unwrap();
        let status = child
            .wait_timeout(std::time::Duration::from_secs(10))
            .unwrap()
            .expect("crash helper must terminate");
        assert!(!status.success());
        assert_eq!(std::fs::read(ready).unwrap(), b"locked");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        assert!(crate::file_lock::try_lock_exclusive(&file).unwrap());
        crate::file_lock::unlock(&file).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn direct_target_link_is_rejected_without_touching_its_destination() {
        use std::os::unix::fs::symlink;

        let parent = canonical_tempdir();
        let destination = parent.path().join("destination");
        std::fs::create_dir(&destination).unwrap();
        let target = parent.path().join("project");
        symlink(&destination, &target).unwrap();
        let error = filesystem_durability_preflight(&target).unwrap_err();
        assert_eq!(error.code(), "GF_UNSUPPORTED_FILESYSTEM");
        assert_eq!(std::fs::read_dir(&destination).unwrap().count(), 0);
    }

    #[test]
    fn project_parent_is_the_durable_namespace_boundary() {
        let base = canonical_tempdir();
        let storage_parent = base.path().join("container/mounted-volume/projects");
        std::fs::create_dir_all(&storage_parent).unwrap();
        let target = storage_parent.join("project");

        let resolved = resolve_project_path(&target).unwrap();

        assert_eq!(resolved.parent.path, storage_parent);
        assert!(
            resolved.parent.ancestors.is_empty(),
            "namespace above the storage parent is outside GraphForge's contract"
        );
        assert_eq!(resolved.root, target);
    }

    #[cfg(windows)]
    #[test]
    fn canonical_extended_drive_path_completes_full_native_admission() {
        use std::path::{Component, Prefix};

        let parent = canonical_tempdir();
        assert!(matches!(
            parent.path().components().next(),
            Some(Component::Prefix(prefix))
                if matches!(prefix.kind(), Prefix::VerbatimDisk(_))
        ));
        let target = parent.path().join("project");
        let evidence = filesystem_durability_preflight(&target).unwrap();
        assert_eq!(evidence.filesystem_class, "ntfs");
        assert!(!target.exists());
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn ambiguous_replacement_state_is_typed_and_reconciled_by_cleanup() {
        let parent = canonical_tempdir();
        let target = parent.path().join("project");
        let error =
            filesystem_durability_preflight_inner(&target, ProbeFault::ReplaceUnknown).unwrap_err();
        assert_eq!(error.code(), "GF_UNSUPPORTED_FILESYSTEM");
        assert!(error.to_string().contains("atomic_replace_state_unknown"));
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
        filesystem_durability_preflight(&target).unwrap();
    }

    #[test]
    fn admitted_filesystem_class_selects_the_matching_fault_oracle_profile() {
        let parent = canonical_tempdir();
        let target = parent.path().join("project");
        let evidence = filesystem_durability_preflight(&target).unwrap();
        let profile =
            crate::project_fault_oracle::DurabilityProfile::for_admitted_filesystem_class(
                &evidence.filesystem_class,
            )
            .expect("every admitted durable filesystem has oracle semantics");
        let outcomes =
            crate::project_fault_oracle::simulate_all_phases_for_profile(0x749a, profile)
                .expect("admission profile phase sweep");
        assert_eq!(
            outcomes.len(),
            crate::project_fault_oracle::PublicationPhase::all().len()
        );
        assert!(outcomes.iter().all(|outcome| {
            outcome.actual == outcome.expected
                && outcome.actual != crate::project_fault_oracle::AuthorityClass::Unexpected
                && (!outcome.acknowledged
                    || outcome.actual == crate::project_fault_oracle::AuthorityClass::NewGeneration)
        }));
    }
}
