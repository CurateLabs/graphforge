//! Audited native filesystem primitives used by GraphForge's durability
//! protocol.
//!
//! Cache-release I/O and platform operations have private owners; common
//! capability types and public entrypoints remain at the crate root.

#![deny(unsafe_code)]

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

mod cache_io;
mod platform;
#[cfg(windows)]
#[allow(unsafe_code)]
mod windows;
#[cfg(windows)]
mod windows_cas;

pub use cache_io::DEFAULT_CACHE_RELEASE_WINDOW_BYTES;
pub use cache_io::DurableFileCacheWriter;
pub use cache_io::FileCacheReleaseEvidence;
pub use cache_io::FileCacheReleaseOutcome;
pub use cache_io::FileCacheReleaseTracker;
pub use cache_io::FileCacheReleasingReader;
pub use cache_io::cache_release_window_for_streams;
pub use cache_io::release_file_cache;
pub use cache_io::validate_cache_release_operation_windows;
use platform::create_private_directory_platform;
use platform::file_identity_platform;
use platform::file_link_count_platform;
use platform::file_space_usage_platform;
use platform::install_new_file_platform;
pub use platform::is_link_or_reparse;
use platform::link_count;
use platform::path_identity_platform;
use platform::path_link_count_platform;
use platform::rename_no_replace_platform;
use platform::replace_file_platform;
use platform::stable_child_names;
use platform::stable_child_names_bounded;
use platform::stable_create_child_directory;
use platform::stable_link_child;
use platform::stable_open_child_directory;
use platform::stable_open_child_file;
use platform::stable_open_directory;
#[cfg(windows)]
use platform::stable_open_directory_for_sync;
use platform::stable_open_or_create_child_file;
use platform::stable_open_replaceable_child_file;
use platform::stable_remove_child_directory_if_identity;
use platform::stable_unlink_child_if_identity;
use platform::visit_regular_files_platform;
#[cfg(windows)]
pub use windows_cas::{WindowsCasWriter, WindowsLegacyCasAdopter, WindowsSealedCasFile};

/// Stable filesystem identity suitable for Windows and Unix filesystems.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileIdentity {
    /// Native volume/device identity.
    pub volume_serial: u64,
    /// Full native file identity (128-bit on Windows; zero-extended inode on Unix).
    pub file_id: [u8; 16],
}

/// Logical and physically allocated byte counts for one retained file handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileSpaceUsage {
    /// Logical end-of-file length visible to readers.
    pub logical_bytes: u64,
    /// Physical filesystem allocation charged to the file.
    pub allocated_bytes: u64,
}

/// Stage of a failed retained-directory validation. Policy owners may preserve
/// their own diagnostics without duplicating native identity checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectoryValidationStage {
    /// Named path metadata could not be read.
    NamedMetadata,
    /// Retained handle metadata could not be read.
    RetainedMetadata,
    /// Named path identity could not be read.
    NamedIdentity,
    /// Retained handle identity could not be read.
    RetainedIdentity,
    /// A path/handle is not an ordinary directory or its identity changed.
    IdentityChanged,
}

/// Native validation failure with a typed stage and original I/O diagnostic.
#[derive(Debug)]
pub struct DirectoryValidationError {
    stage: DirectoryValidationStage,
    source: io::Error,
}

impl DirectoryValidationError {
    fn new(stage: DirectoryValidationStage, source: io::Error) -> Self {
        Self { stage, source }
    }

    /// Return the failing native operation for a caller's policy mapping.
    #[must_use]
    pub fn stage(&self) -> DirectoryValidationStage {
        self.stage
    }

    /// Preserve the existing native I/O error and kind for generic consumers.
    #[must_use]
    pub fn into_io_error(self) -> io::Error {
        self.source
    }
}

impl std::fmt::Display for DirectoryValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.source, formatter)
    }
}

impl std::error::Error for DirectoryValidationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Directory handle opened with this crate's native no-follow and sharing policy.
/// Private construction prevents an arbitrary `File` from claiming that policy.
#[derive(Debug)]
pub struct OpenedDirectoryHandle {
    file: File,
}

impl OpenedDirectoryHandle {
    /// Borrow the native handle for metadata, volume queries, or cooperative locks.
    #[must_use]
    pub fn as_file(&self) -> &File {
        &self.file
    }

    /// Transfer the raw handle to a caller that owns its validation policy.
    #[must_use]
    pub fn into_file(self) -> File {
        self.file
    }
}

/// Retained directory capability whose children are opened without following
/// links or reparse points.
#[derive(Debug)]
pub struct StableDirectory {
    path: PathBuf,
    file: File,
    identity: FileIdentity,
}

/// Single-owner capability for one exclusively created, unpublished child.
///
/// The guard follows the owned inode across an atomic rename and removes any
/// still-uncommitted name on explicit cleanup or drop. Callers must retain the
/// guard until every higher-level publication invariant, including parent
/// directory synchronization and manifest/receipt inclusion, is complete.
#[derive(Debug)]
pub struct UnpublishedArtifactGuard {
    directory: StableDirectory,
    candidate_names: Vec<OsString>,
    identity: Option<FileIdentity>,
    file: Option<File>,
    published: bool,
    parent_synced: bool,
    armed: bool,
}

impl UnpublishedArtifactGuard {
    /// Return the immutable identity captured during descriptor setup.
    ///
    /// # Errors
    /// Returns an error before descriptor identity has been initialized.
    pub fn identity(&self) -> io::Result<FileIdentity> {
        self.identity
            .ok_or_else(|| io::Error::other("unpublished artifact identity is not initialized"))
    }

    /// Re-read the retained descriptor identity and run a setup check before
    /// the descriptor is transferred to a writer.
    ///
    /// # Errors
    /// Returns an error when identity observation, `setup_check`, or the
    /// identity comparison fails.
    pub fn verify_identity_with(
        &mut self,
        setup_check: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<FileIdentity> {
        let file = self
            .file
            .as_ref()
            .ok_or_else(|| io::Error::other("unpublished artifact descriptor was transferred"))?;
        let observed = file_identity(file)?;
        self.identity = Some(observed);
        setup_check()?;
        Ok(observed)
    }

    /// Open one sibling through the retained no-follow parent capability.
    ///
    /// # Errors
    /// Returns an error when the child is absent, special, linked, or the
    /// retained directory authority changed.
    pub fn open_sibling(&self, name: &OsStr) -> io::Result<File> {
        self.directory.open_child_file(name)
    }

    /// Transfer the retained data descriptor into its durable writer.
    ///
    /// # Errors
    /// Returns an error if identity observation fails or the descriptor was
    /// already transferred.
    pub fn take_file(&mut self) -> io::Result<File> {
        if self.identity.is_none() {
            let file = self
                .file
                .as_ref()
                .ok_or_else(|| io::Error::other("unpublished artifact file already transferred"))?;
            self.identity = Some(file_identity(file)?);
        }
        self.file
            .take()
            .ok_or_else(|| io::Error::other("unpublished artifact file already transferred"))
    }

    /// Atomically install `target` while retaining cleanup ownership.
    ///
    /// # Errors
    /// Returns an error when identity-safe installation fails.
    pub fn install_child(&mut self, target: &OsStr) -> io::Result<()> {
        validate_child_name(target)?;
        let temporary = self
            .candidate_names
            .first()
            .ok_or_else(|| io::Error::other("unpublished artifact has no temporary name"))?
            .clone();
        if !self.candidate_names.iter().any(|name| name == target) {
            self.candidate_names.push(target.to_owned());
        }
        self.directory
            .install_child(&temporary, self.identity()?, target)?;
        self.published = true;
        self.parent_synced = false;
        Ok(())
    }

    /// Synchronize the retained parent directory while remaining armed.
    ///
    /// # Errors
    /// Returns an error when the directory durability barrier fails.
    pub fn sync_parent(&mut self) -> io::Result<()> {
        self.directory.sync()?;
        self.parent_synced = true;
        Ok(())
    }

    /// Disarm cleanup after every publication invariant is complete.
    ///
    /// # Errors
    /// Returns an error if the descriptor is still held by the guard or the
    /// atomic publication and parent barrier have not both completed.
    pub fn commit(mut self) -> io::Result<()> {
        if self.file.is_some() || !self.published || !self.parent_synced {
            return Err(io::Error::other(
                "unpublished artifact commit invariants are incomplete",
            ));
        }
        self.armed = false;
        Ok(())
    }

    /// Remove every possible uncommitted name and synchronize the parent.
    ///
    /// Names that are absent or no longer identify the exclusively created
    /// inode are left untouched.
    ///
    /// # Errors
    /// Returns sanitized cleanup failure context after attempting every unlink
    /// and the final parent-directory barrier.
    pub fn cleanup(&mut self) -> io::Result<()> {
        if !self.armed {
            return Ok(());
        }
        let mut cleanup = Ok(());
        let identity = match self.identity {
            Some(identity) => Some(identity),
            None => match self
                .file
                .as_ref()
                .ok_or_else(|| io::Error::other("unpublished artifact descriptor is unavailable"))
                .and_then(file_identity)
            {
                Ok(identity) => {
                    self.identity = Some(identity);
                    Some(identity)
                }
                Err(error) => {
                    cleanup = append_sanitized_cleanup(
                        cleanup,
                        &error,
                        "unpublished artifact identity recovery failed",
                    );
                    None
                }
            },
        };
        self.file.take();
        for name in &self.candidate_names {
            let Some(identity) = identity else {
                break;
            };
            let removed = self.directory.open_child_file(name).and_then(|file| {
                if file_identity(&file)? == identity {
                    drop(file);
                    self.directory.unlink_child_if_identity(name, identity)?;
                }
                Ok(())
            });
            match removed {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    cleanup = append_sanitized_cleanup(
                        cleanup,
                        &error,
                        "unpublished artifact unlink failed",
                    );
                }
            }
        }
        cleanup = match self.directory.sync() {
            Ok(()) => cleanup,
            Err(error) => append_sanitized_cleanup(
                cleanup,
                &error,
                "unpublished artifact directory synchronization failed",
            ),
        };
        self.armed = false;
        cleanup
    }

    /// Perform full identity-safe cleanup and its parent barrier, then run one
    /// caller-supplied finalization check.
    ///
    /// # Errors
    /// Returns sanitized cleanup or finalization context after both have been
    /// attempted.
    pub fn cleanup_checked(
        &mut self,
        finalization_check: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        let cleanup = self.cleanup();
        match finalization_check() {
            Ok(()) => cleanup,
            Err(error) => append_sanitized_cleanup(
                cleanup,
                &error,
                "unpublished artifact cleanup finalization failed",
            ),
        }
    }
}

impl Drop for UnpublishedArtifactGuard {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

fn append_sanitized_cleanup(
    primary: io::Result<()>,
    cleanup: &io::Error,
    context: &'static str,
) -> io::Result<()> {
    let cleanup = io::Error::new(cleanup.kind(), context);
    match primary {
        Ok(()) => Err(cleanup),
        Err(primary) => Err(io::Error::new(
            primary.kind(),
            format!("{primary}; {cleanup}"),
        )),
    }
}

impl StableDirectory {
    /// Duplicate this retained directory capability without resolving its path again.
    ///
    /// # Errors
    /// Returns an error if the retained directory identity changed or the OS
    /// cannot duplicate its handle.
    pub fn try_clone(&self) -> io::Result<Self> {
        self.revalidate_named()?;
        let clone = Self {
            path: self.path.clone(),
            file: self.file.try_clone()?,
            identity: self.identity,
        };
        clone.revalidate_named()?;
        Ok(clone)
    }

    /// Acquire a cooperative shared lock on this retained Unix directory inode.
    ///
    /// Windows directory handles cannot be byte-range locked; callers use a
    /// retained regular coordination file there instead.
    #[cfg(unix)]
    pub fn lock_shared(&self) -> io::Result<()> {
        <File as fs4::FileExt>::lock_shared(&self.file)
    }

    /// Acquire a cooperative exclusive lock on this retained Unix directory inode.
    #[cfg(unix)]
    pub fn lock_exclusive(&self) -> io::Result<()> {
        <File as fs4::FileExt>::lock(&self.file)
    }

    /// Try to acquire a cooperative exclusive lock on this retained Unix directory inode.
    #[cfg(unix)]
    pub fn try_lock_exclusive(&self) -> io::Result<bool> {
        match <File as fs4::FileExt>::try_lock(&self.file) {
            Ok(()) => Ok(true),
            Err(fs4::TryLockError::WouldBlock) => Ok(false),
            Err(fs4::TryLockError::Error(error)) => Err(error),
        }
    }

    /// Release this retained Unix directory inode's cooperative lock.
    #[cfg(unix)]
    pub fn unlock(&self) -> io::Result<()> {
        <File as fs4::FileExt>::unlock(&self.file)
    }

    /// Open and retain a real directory at `path`.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = stable_open_directory(path)?;
        let identity = file_identity(&file)?;
        let directory = Self {
            path: path.to_path_buf(),
            file,
            identity,
        };
        directory.revalidate_named()?;
        Ok(directory)
    }

    /// Adopt an already retained handle and expected identity, validating both
    /// the named path and the handle before issuing a directory capability.
    pub fn from_retained_handle(
        path: PathBuf,
        handle: OpenedDirectoryHandle,
        identity: FileIdentity,
    ) -> Result<Self, DirectoryValidationError> {
        let directory = Self {
            path,
            file: handle.file,
            identity,
        };
        directory.revalidate_named_detailed()?;
        Ok(directory)
    }

    /// Borrow the retained handle for native volume queries and cooperative locks.
    /// Callers must revalidate around namespace-sensitive operations.
    #[must_use]
    pub fn as_file(&self) -> &File {
        &self.file
    }

    /// Return the named path associated with this capability.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Transfer the policy-proven native handle for adoption by another capability.
    #[must_use]
    pub fn into_handle(self) -> OpenedDirectoryHandle {
        OpenedDirectoryHandle { file: self.file }
    }

    /// Require that the named path and retained handle still identify this
    /// ordinary directory, returning a typed failure stage for policy adapters.
    pub fn revalidate_named_detailed(&self) -> Result<(), DirectoryValidationError> {
        use DirectoryValidationStage as Stage;
        let named = std::fs::symlink_metadata(&self.path)
            .map_err(|error| DirectoryValidationError::new(Stage::NamedMetadata, error))?;
        let retained = self
            .file
            .metadata()
            .map_err(|error| DirectoryValidationError::new(Stage::RetainedMetadata, error))?;
        if !named.is_dir() || is_link_or_reparse(&named) || !retained.is_dir() {
            return Err(DirectoryValidationError::new(
                Stage::IdentityChanged,
                io::Error::other("stable directory path is linked or special"),
            ));
        }
        let named_identity = path_identity(&self.path)
            .map_err(|error| DirectoryValidationError::new(Stage::NamedIdentity, error))?;
        if named_identity != self.identity {
            return Err(DirectoryValidationError::new(
                Stage::IdentityChanged,
                io::Error::other("stable directory identity changed"),
            ));
        }
        let retained_identity = file_identity(&self.file)
            .map_err(|error| DirectoryValidationError::new(Stage::RetainedIdentity, error))?;
        if retained_identity != self.identity {
            return Err(DirectoryValidationError::new(
                Stage::IdentityChanged,
                io::Error::other("stable directory identity changed"),
            ));
        }
        Ok(())
    }

    /// Require that the named path still identifies this retained directory.
    pub fn revalidate_named(&self) -> io::Result<()> {
        self.revalidate_named_detailed()
            .map_err(DirectoryValidationError::into_io_error)
    }

    /// Open one real child directory relative to this capability.
    pub fn open_child_directory(&self, name: &OsStr) -> io::Result<Self> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        let path = self.path.join(name);
        let file = stable_open_child_directory(&self.file, &path, name)?;
        let child = Self {
            identity: file_identity(&file)?,
            path,
            file,
        };
        child.revalidate_named()?;
        self.revalidate_named()?;
        Ok(child)
    }

    /// Create one child directory if absent, then retain it.
    pub fn create_child_directory(&self, name: &OsStr) -> io::Result<Self> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        stable_create_child_directory(&self.file, &self.path.join(name), name)?;
        self.open_child_directory(name)
    }

    /// Open one regular child without following links or reparse points.
    pub fn open_child_file(&self, name: &OsStr) -> io::Result<File> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        let path = self.path.join(name);
        let file = stable_open_child_file(&self.file, &path, name, false)?;
        validate_stable_child_file(&file, &path)?;
        self.revalidate_named()?;
        Ok(file)
    }

    /// Visit regular descendants through retained, no-follow directory handles.
    ///
    /// Every child is opened relative to its retained parent and revalidated
    /// before it reaches `visit`. Links and non-regular objects are skipped.
    ///
    /// # Errors
    /// Returns an error if traversal exceeds `remaining`, a retained identity
    /// changes, directory enumeration fails, or `visit` fails. Targets without
    /// descriptor-relative directory enumeration return `Unsupported`.
    pub fn visit_regular_files(
        &self,
        remaining: &mut usize,
        visit: &mut impl FnMut(&File) -> io::Result<()>,
    ) -> io::Result<()> {
        self.revalidate_named()?;
        visit_regular_files_platform(self, remaining, visit)?;
        self.revalidate_named()
    }

    /// Create one new regular child without following links or reparse points.
    pub fn create_child_file(&self, name: &OsStr) -> io::Result<File> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        let path = self.path.join(name);
        let file = stable_open_child_file(&self.file, &path, name, true)?;
        validate_stable_child_file(&file, &path)?;
        self.revalidate_named()?;
        Ok(file)
    }

    /// Create a Windows CAS child with exclusive data-write authority.
    ///
    /// Readers may coexist, but no second writer can be admitted. The handle
    /// retains the native authority needed for the irreversible seal.
    #[cfg(windows)]
    pub fn create_cas_child_file(&self, name: &OsStr) -> io::Result<WindowsCasWriter> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        let path = self.path.join(name);
        let file = windows::create_cas_writer(&path)?;
        validate_stable_child_file(&file, &path)?;
        self.revalidate_named()?;
        Ok(WindowsCasWriter {
            identity: file_identity(&file)?,
            file,
        })
    }

    /// Convert an exact Windows CAS writer into an identity-matched sealed reader.
    #[cfg(windows)]
    pub fn seal_cas_child_file(
        &self,
        name: &OsStr,
        writer: WindowsCasWriter,
    ) -> io::Result<WindowsSealedCasFile> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        let path = self.path.join(name);
        let expected = writer.identity;
        if file_identity(&writer.file)? != expected || path_identity(&path)? != expected {
            return Err(io::Error::other(
                "CAS writer identity changed before sealing",
            ));
        }
        windows::seal_cas_writer(&writer.file)?;
        if file_identity(&writer.file)? != expected || path_identity(&path)? != expected {
            return Err(io::Error::other(
                "CAS writer identity changed while sealing",
            ));
        }
        let bridge = windows::open_cas_bridge(&path)?;
        if file_identity(&bridge)? != expected {
            return Err(io::Error::other(
                "CAS bridge identity changed while sealing",
            ));
        }
        drop(writer.file);
        let reader = windows::open_sealed_cas_reader(&path)?;
        validate_stable_child_file(&reader, &path)?;
        if file_identity(&reader)? != expected || path_identity(&path)? != expected {
            return Err(io::Error::other(
                "CAS identity changed while reopening sealed reader",
            ));
        }
        drop(bridge);
        self.revalidate_named()?;
        Ok(WindowsSealedCasFile(reader))
    }

    /// Open a canonically sealed Windows CAS child while excluding writers.
    #[cfg(windows)]
    pub fn open_cas_child_file(&self, name: &OsStr) -> io::Result<WindowsSealedCasFile> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        let path = self.path.join(name);
        let reader = windows::open_sealed_cas_reader(&path)?;
        validate_stable_child_file(&reader, &path)?;
        if !reader.metadata()?.permissions().readonly()
            || !windows::has_canonical_cas_dacl(&reader)?
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "CAS child is not canonically sealed",
            ));
        }
        self.revalidate_named()?;
        Ok(WindowsSealedCasFile(reader))
    }

    /// Retain an exclusive metadata handle for authenticating a legacy sealed CAS child.
    #[cfg(windows)]
    pub fn open_legacy_cas_child_for_adoption(
        &self,
        name: &OsStr,
    ) -> io::Result<WindowsLegacyCasAdopter> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        let path = self.path.join(name);
        let file = windows::open_legacy_cas_adopter(&path)?;
        validate_stable_child_file(&file, &path)?;
        let identity = file_identity(&file)?;
        if path_identity(&path)? != identity {
            return Err(io::Error::other("legacy CAS identity changed during open"));
        }
        self.revalidate_named()?;
        Ok(WindowsLegacyCasAdopter { file, identity })
    }

    /// Canonically seal a retained legacy CAS child after caller authentication.
    #[cfg(windows)]
    pub fn adopt_legacy_cas_child(
        &self,
        name: &OsStr,
        adopter: WindowsLegacyCasAdopter,
    ) -> io::Result<WindowsSealedCasFile> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        let path = self.path.join(name);
        windows::set_canonical_cas_dacl(&adopter.file)?;
        let bridge = windows::open_cas_bridge(&path)?;
        if file_identity(&bridge)? != adopter.identity {
            return Err(io::Error::other("legacy CAS bridge identity changed"));
        }
        drop(adopter.file);
        let reader = windows::open_sealed_cas_reader(&path)?;
        if file_identity(&reader)? != adopter.identity || path_identity(&path)? != adopter.identity
        {
            return Err(io::Error::other(
                "legacy CAS identity changed during adoption",
            ));
        }
        drop(bridge);
        self.revalidate_named()?;
        Ok(WindowsSealedCasFile(reader))
    }

    /// Create a new regular child whose retained handle permits an atomic
    /// namespace replacement while it remains open.
    pub fn create_replaceable_child_file(&self, name: &OsStr) -> io::Result<File> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        let path = self.path.join(name);
        let file = stable_open_replaceable_child_file(&self.file, &path, name)?;
        validate_stable_child_file(&file, &path)?;
        self.revalidate_named()?;
        Ok(file)
    }

    /// Exclusively create one replaceable child and immediately bind cleanup
    /// ownership to its retained descriptor identity.
    ///
    /// # Errors
    /// Returns an error when creation, identity capture, or directory-capability
    /// cloning fails. Setup failure removes the exact created inode when safe.
    pub fn create_unpublished_replaceable_child(
        &self,
        name: &OsStr,
    ) -> io::Result<UnpublishedArtifactGuard> {
        // Clone the parent authority before creating the child. From the
        // instant exclusive creation succeeds, no fallible setup remains
        // outside the armed guard.
        let directory = self.try_clone()?;
        let file = self.create_replaceable_child_file(name)?;
        Ok(UnpublishedArtifactGuard {
            directory,
            candidate_names: vec![name.to_owned()],
            identity: None,
            file: Some(file),
            published: false,
            parent_synced: false,
            armed: true,
        })
    }

    /// Open an existing regular child for read/write, or create it once.
    pub fn open_or_create_child_file(&self, name: &OsStr) -> io::Result<File> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        let path = self.path.join(name);
        let file = stable_open_or_create_child_file(&self.file, &path, name)?;
        validate_stable_child_file(&file, &path)?;
        self.revalidate_named()?;
        Ok(file)
    }

    /// Enumerate child names while retaining this directory capability.
    pub fn child_names(&self) -> io::Result<Vec<std::ffi::OsString>> {
        self.revalidate_named()?;
        stable_child_names(&self.file, &self.path)
    }

    /// Enumerate no more than `limit` child names from this retained directory.
    /// Returns `InvalidData` instead of materializing an attacker-sized sibling
    /// inventory when the bound is exceeded.
    pub fn child_names_bounded(&self, limit: usize) -> io::Result<Vec<std::ffi::OsString>> {
        self.revalidate_named()?;
        stable_child_names_bounded(&self.file, &self.path, limit)
    }

    /// Create a hard link between retained source and destination directories.
    pub fn link_child_into(
        &self,
        source_name: &OsStr,
        source: &File,
        expected_source: FileIdentity,
        destination: &Self,
        destination_name: &OsStr,
    ) -> io::Result<(File, FileIdentity)> {
        validate_child_name(source_name)?;
        validate_child_name(destination_name)?;
        self.revalidate_named()?;
        destination.revalidate_named()?;
        validate_stable_child_file(source, &self.path.join(source_name))?;
        if file_identity(source)? != expected_source {
            return Err(io::Error::other("hard-link source identity changed"));
        }
        stable_link_child(
            &self.file,
            &self.path,
            source_name,
            &destination.file,
            &destination.path,
            destination_name,
        )?;
        self.revalidate_named()?;
        destination.revalidate_named()?;
        let installed = destination.open_child_file(destination_name)?;
        let installed_identity = file_identity(&installed)?;
        if installed_identity != expected_source {
            return Err(io::Error::other("hard-link destination identity mismatch"));
        }
        Ok((installed, installed_identity))
    }

    /// Remove a child under the caller's held cooperative exclusive lifecycle
    /// guard, only while its current named identity matches `expected`.
    pub fn unlink_child_if_identity(&self, name: &OsStr, expected: FileIdentity) -> io::Result<()> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        stable_unlink_child_if_identity(&self.file, &self.path, name, expected)?;
        self.revalidate_named()
    }

    /// Remove one empty child directory only while its retained and named
    /// identities still match. Callers must first authenticate and empty the
    /// directory through the returned child capability.
    pub fn remove_child_directory_if_identity(
        &self,
        name: &OsStr,
        expected: FileIdentity,
    ) -> io::Result<()> {
        validate_child_name(name)?;
        self.revalidate_named()?;
        stable_remove_child_directory_if_identity(&self.file, &self.path, name, expected)?;
        self.revalidate_named()
    }

    /// Atomically publish a retained temporary child as `target` within this
    /// retained directory. Cooperative publishers must serialize the target.
    pub fn replace_child(
        &self,
        temporary: &OsStr,
        expected_temporary: FileIdentity,
        target: &OsStr,
    ) -> io::Result<()> {
        validate_child_name(temporary)?;
        validate_child_name(target)?;
        self.revalidate_named()?;
        let temporary_file = self.open_child_file(temporary)?;
        if file_identity(&temporary_file)? != expected_temporary
            || file_link_count(&temporary_file)? != 1
        {
            return Err(io::Error::other(
                "atomic temporary child identity or link count changed",
            ));
        }
        drop(temporary_file);
        let target_exists = match self.open_child_file(target) {
            Ok(target_file) => {
                drop(target_file);
                true
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(error),
        };
        let result = if target_exists {
            replace_file_platform(
                &self.file,
                temporary,
                target,
                Some(expected_temporary),
                None,
            )
            .map_err(|error| io::Error::other(error.to_string()))
        } else {
            install_new_file_platform(&self.file, temporary, target, Some(expected_temporary))
        };
        result?;
        self.revalidate_named()?;
        self.open_child_file(target).map(|_| ())
    }

    /// Atomically replace an authenticated prior child, which may share its inode
    /// with immutable payloads or active snapshots. The source must be private.
    /// Callers must authenticate the prior contents and serialize publishers;
    /// this operation checks identity and never writes to the prior inode.
    pub fn replace_authenticated_child(
        &self,
        temporary: &OsStr,
        expected_temporary: FileIdentity,
        target: &OsStr,
        expected_target: FileIdentity,
    ) -> io::Result<()> {
        validate_child_name(temporary)?;
        validate_child_name(target)?;
        self.revalidate_named()?;
        replace_file_platform(
            &self.file,
            temporary,
            target,
            Some(expected_temporary),
            Some(expected_target),
        )
        .map_err(|error| io::Error::other(error.to_string()))?;
        self.revalidate_named()?;
        let installed = self.open_child_file(target)?;
        if file_identity(&installed)? != expected_temporary || file_link_count(&installed)? != 1 {
            return Err(io::Error::other(
                "authenticated replacement identity changed",
            ));
        }
        Ok(())
    }

    /// Atomically install a retained temporary child without replacing an
    /// existing target. This is the creation authority for durable control
    /// records whose first publication must never overwrite competing state.
    ///
    /// # Errors
    /// Returns an I/O error when either name is invalid, the retained source
    /// identity changed, the target already exists, or durable installation
    /// and identity revalidation fail.
    pub fn install_child(
        &self,
        temporary: &OsStr,
        expected_temporary: FileIdentity,
        target: &OsStr,
    ) -> io::Result<()> {
        validate_child_name(temporary)?;
        validate_child_name(target)?;
        self.revalidate_named()?;
        let temporary_file = self.open_child_file(temporary)?;
        if file_identity(&temporary_file)? != expected_temporary
            || file_link_count(&temporary_file)? != 1
        {
            return Err(io::Error::other(
                "atomic temporary child identity or link count changed",
            ));
        }
        drop(temporary_file);
        install_new_file_platform(&self.file, temporary, target, Some(expected_temporary))?;
        self.revalidate_named()?;
        self.open_child_file(target).map(|_| ())
    }

    /// Flush this retained directory capability.
    pub fn sync(&self) -> io::Result<()> {
        self.revalidate_named()?;
        #[cfg(windows)]
        {
            let directory = stable_open_directory_for_sync(&self.path)?;
            if file_identity(&directory)? != self.identity {
                return Err(io::Error::other(
                    "stable directory identity changed before sync",
                ));
            }
            directory.sync_all()?;
            self.revalidate_named()
        }
        #[cfg(not(windows))]
        {
            self.file.sync_all()?;
            self.revalidate_named()
        }
    }

    /// Return the retained native identity.
    #[must_use]
    pub fn identity(&self) -> FileIdentity {
        self.identity
    }
}

/// Open a directory handle using the platform's no-follow and sharing policy.
/// The caller must validate directory kind and retained/named identity before
/// treating it as authority; prefer `StableDirectory::open` for a capability.
pub fn open_directory_handle(path: &Path) -> io::Result<OpenedDirectoryHandle> {
    stable_open_directory(path).map(|file| OpenedDirectoryHandle { file })
}

fn validate_child_name(name: &OsStr) -> io::Result<()> {
    let path = Path::new(name);
    if name.is_empty()
        || path.is_absolute()
        || path.components().count() != 1
        || matches!(name.to_str(), Some("." | ".."))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid child name",
        ));
    }
    Ok(())
}

fn validate_stable_child_file(file: &File, path: &Path) -> io::Result<()> {
    let metadata = file.metadata()?;
    let named = std::fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || !named.is_file()
        || is_link_or_reparse(&named)
        || file_identity(file)? != path_identity(path)?
    {
        return Err(io::Error::other(
            "stable child is linked, special, or substituted",
        ));
    }
    Ok(())
}

/// Native Windows volume facts needed by the durability admission policy.
#[cfg(windows)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowsVolumeInformation {
    /// Filesystem name reported by the mounted volume (`NTFS`, `ReFS`, ...).
    pub filesystem_name: String,
    /// Whether the volume reports the read-only filesystem flag.
    pub read_only: bool,
    /// Whether Windows classifies the volume root as a fixed local drive.
    pub fixed: bool,
}

/// Create a durability-probe directory that is private to the current user.
///
/// Unix uses mode `0700`. Windows installs a protected DACL that grants full
/// access only to the owner, LocalSystem, and local administrators.
pub fn create_private_directory(path: &Path) -> io::Result<()> {
    create_private_directory_platform(path)
}

/// Return the stable native volume/file identity of an open handle.
pub fn file_identity(file: &File) -> io::Result<FileIdentity> {
    file_identity_platform(file)
}

/// Return logical and physically allocated bytes for a retained regular-file handle.
///
/// The descriptor is the sole authority: this function never resolves or reopens a
/// pathname. Unsupported platforms and native values that cannot be represented
/// safely fail closed.
pub fn file_space_usage(file: &File) -> io::Result<FileSpaceUsage> {
    file_space_usage_platform(file)
}

/// Return the stable native volume/file identity of a non-followed path.
pub fn path_identity(path: &Path) -> io::Result<FileIdentity> {
    path_identity_platform(path)
}

/// Return the native hard-link count of an open file handle.
pub fn file_link_count(file: &File) -> io::Result<u64> {
    file_link_count_platform(file)
}

/// Return the native hard-link count of a non-followed path.
pub fn path_link_count(path: &Path) -> io::Result<u64> {
    path_link_count_platform(path)
}

/// Query Windows volume facts from the native mount root containing `path`.
///
/// This accepts canonical extended-length paths such as `\\?\C:\...` and
/// follows mount-point boundaries through `GetVolumePathNameW`.
#[cfg(windows)]
pub fn windows_volume_information(path: &Path) -> io::Result<WindowsVolumeInformation> {
    windows::volume_information(path)
}

/// Failure classification for an attempted atomic replacement.
#[derive(Debug)]
pub enum ReplaceFileError {
    /// The operating system rejected the operation and the open source handle
    /// plus both named identities were verified unchanged.
    NotReplaced(io::Error),
    /// The operating system reported failure after it may have moved or
    /// modified one of the named files. The caller must reconcile from
    /// authoritative persisted state.
    StateUnknown(io::Error),
}

impl std::fmt::Display for ReplaceFileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotReplaced(error) => write!(formatter, "file was not replaced: {error}"),
            Self::StateUnknown(error) => {
                write!(
                    formatter,
                    "replacement state requires reconciliation: {error}"
                )
            }
        }
    }
}

impl std::error::Error for ReplaceFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::NotReplaced(error) | Self::StateUnknown(error) => error,
        })
    }
}

/// Classify an OS-reported failed replacement from reconciled identities.
///
/// This is public for the deterministic fault oracle; publication callers use
/// [`replace_file`] directly.
#[doc(hidden)]
#[must_use]
pub fn classify_failed_replacement(
    error: io::Error,
    source_before: FileIdentity,
    target_before: FileIdentity,
    source_after: Option<FileIdentity>,
    target_after: Option<FileIdentity>,
) -> ReplaceFileError {
    if source_after == Some(source_before) && target_after == Some(target_before) {
        ReplaceFileError::NotReplaced(error)
    } else {
        ReplaceFileError::StateUnknown(error)
    }
}

/// Atomically replace an existing regular file with another regular file in
/// the same directory.
///
/// Source contents must already be written. On Windows the implementation
/// reopens and flushes the source through a write-through handle before issuing
/// the NTFS namespace rename through that same handle. On POSIX the caller
/// remains responsible for the containing-directory durability barrier.
pub fn replace_file(
    directory: &File,
    source_name: &OsStr,
    target_name: &OsStr,
) -> Result<(), ReplaceFileError> {
    verify_single_component(source_name).map_err(ReplaceFileError::NotReplaced)?;
    verify_single_component(target_name).map_err(ReplaceFileError::NotReplaced)?;
    replace_file_platform(directory, source_name, target_name, None, None)
}

/// Atomically install a new regular file without replacing an existing entry.
///
/// Windows uses the same flushed write-through source handle for the NTFS
/// namespace rename. POSIX callers remain responsible for directory `fsync`.
pub fn install_new_file(
    directory: &File,
    source_name: &OsStr,
    target_name: &OsStr,
) -> io::Result<()> {
    verify_single_component(source_name)?;
    verify_single_component(target_name)?;
    install_new_file_platform(directory, source_name, target_name, None)
}

/// Atomically move a file or directory without replacing any existing destination.
///
/// This never copies across volumes. The caller owns source admission and the
/// containing-directory durability barrier after a successful namespace change.
pub fn rename_no_replace(source: &Path, destination: &Path) -> io::Result<()> {
    rename_no_replace_platform(source, destination)
}

fn verify_single_component(name: &OsStr) -> io::Result<()> {
    let mut components = Path::new(name).components();
    if !matches!(components.next(), Some(std::path::Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "filesystem operation requires one plain name",
        ));
    }
    Ok(())
}

fn verify_regular_metadata(metadata: &std::fs::Metadata) -> io::Result<()> {
    if is_link_or_reparse(metadata) || !metadata.is_file() || link_count(metadata) != 1 {
        return Err(io::Error::other(
            "replacement path is not a regular non-link file",
        ));
    }
    Ok(())
}

fn verify_space_usage_metadata(metadata: &std::fs::Metadata) -> io::Result<()> {
    if is_link_or_reparse(metadata) || !metadata.is_file() {
        return Err(io::Error::other(
            "space usage handle is not a regular non-reparse file",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
