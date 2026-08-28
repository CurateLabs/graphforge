//! Audited native filesystem primitives used by GraphForge's durability
//! protocol.

#![deny(unsafe_code)]

use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

/// Stable filesystem identity suitable for Windows and Unix filesystems.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileIdentity {
    /// Native volume/device identity.
    pub volume_serial: u64,
    /// Full native file identity (128-bit on Windows; zero-extended inode on Unix).
    pub file_id: [u8; 16],
}

/// Exclusive Windows capability used while constructing one CAS object.
#[cfg(windows)]
#[derive(Debug)]
pub struct WindowsCasWriter {
    file: File,
    identity: FileIdentity,
}

#[cfg(windows)]
impl io::Write for WindowsCasWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        io::Write::write(&mut self.file, buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        io::Write::flush(&mut self.file)
    }
}

#[cfg(windows)]
impl io::Read for WindowsCasWriter {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        io::Read::read(&mut self.file, buffer)
    }
}

#[cfg(windows)]
impl io::Seek for WindowsCasWriter {
    fn seek(&mut self, position: io::SeekFrom) -> io::Result<u64> {
        io::Seek::seek(&mut self.file, position)
    }
}

#[cfg(windows)]
impl WindowsCasWriter {
    /// Flush the exact retained writer.
    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Return the immutable identity captured at exclusive creation.
    #[must_use]
    pub fn identity(&self) -> FileIdentity {
        self.identity
    }
}

/// Read-only Windows capability for a canonically sealed CAS object.
#[cfg(windows)]
#[derive(Debug)]
pub struct WindowsSealedCasFile(File);

#[cfg(windows)]
impl WindowsSealedCasFile {
    /// Consume the sealed capability as a standard read-only file handle.
    #[must_use]
    pub fn into_file(self) -> File {
        self.0
    }
}

/// Exclusive retained handle for authenticating a released legacy Windows CAS object.
#[cfg(windows)]
#[derive(Debug)]
pub struct WindowsLegacyCasAdopter {
    file: File,
    identity: FileIdentity,
}

#[cfg(windows)]
impl io::Read for WindowsLegacyCasAdopter {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        io::Read::read(&mut self.file, buffer)
    }
}

#[cfg(windows)]
impl io::Seek for WindowsLegacyCasAdopter {
    fn seek(&mut self, position: io::SeekFrom) -> io::Result<u64> {
        io::Seek::seek(&mut self.file, position)
    }
}

/// Logical and physically allocated byte counts for one retained file handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileSpaceUsage {
    /// Logical end-of-file length visible to readers.
    pub logical_bytes: u64,
    /// Physical filesystem allocation charged to the file.
    pub allocated_bytes: u64,
}

/// Retained directory capability whose children are opened without following
/// links or reparse points.
#[derive(Debug)]
pub struct StableDirectory {
    path: PathBuf,
    file: File,
    identity: FileIdentity,
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

    /// Require that the named path still identifies this retained directory.
    pub fn revalidate_named(&self) -> io::Result<()> {
        let metadata = std::fs::symlink_metadata(&self.path)?;
        if !metadata.is_dir() || is_link_or_reparse(&metadata) {
            return Err(io::Error::other(
                "stable directory path is linked or special",
            ));
        }
        if path_identity(&self.path)? != self.identity {
            return Err(io::Error::other("stable directory identity changed"));
        }
        Ok(())
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
            replace_file_platform(&self.file, temporary, target, Some(expected_temporary))
                .map_err(|error| io::Error::other(error.to_string()))
        } else {
            install_new_file_platform(&self.file, temporary, target, Some(expected_temporary))
        };
        result?;
        self.revalidate_named()?;
        self.open_child_file(target).map(|_| ())
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

#[cfg(unix)]
fn stable_open_directory(path: &Path) -> io::Result<File> {
    use rustix::fs::{Mode, OFlags};
    rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)
}

#[cfg(unix)]
fn stable_open_child_directory(parent: &File, _path: &Path, name: &OsStr) -> io::Result<File> {
    use rustix::fs::{Mode, OFlags};
    rustix::fs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)
}

#[cfg(unix)]
fn stable_create_child_directory(parent: &File, _path: &Path, name: &OsStr) -> io::Result<()> {
    use rustix::fs::Mode;
    match rustix::fs::mkdirat(parent, name, Mode::from_bits_truncate(0o700)) {
        Ok(()) | Err(rustix::io::Errno::EXIST) => Ok(()),
        Err(error) => Err(io::Error::from(error)),
    }
}

#[cfg(unix)]
fn stable_open_child_file(
    parent: &File,
    _path: &Path,
    name: &OsStr,
    create_new: bool,
) -> io::Result<File> {
    use rustix::fs::{Mode, OFlags};
    let flags = if create_new {
        OFlags::RDWR | OFlags::CREATE | OFlags::EXCL
    } else {
        OFlags::RDONLY
    } | OFlags::NOFOLLOW
        | OFlags::NONBLOCK
        | OFlags::CLOEXEC;
    rustix::fs::openat(parent, name, flags, Mode::from_bits_truncate(0o600))
        .map(File::from)
        .map_err(io::Error::from)
}

#[cfg(unix)]
fn stable_open_replaceable_child_file(
    parent: &File,
    path: &Path,
    name: &OsStr,
) -> io::Result<File> {
    stable_open_child_file(parent, path, name, true)
}

#[cfg(unix)]
fn stable_open_or_create_child_file(parent: &File, path: &Path, name: &OsStr) -> io::Result<File> {
    match stable_open_child_file(parent, path, name, true) {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            use rustix::fs::{Mode, OFlags};
            rustix::fs::openat(
                parent,
                name,
                OFlags::RDWR | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map(File::from)
            .map_err(io::Error::from)
        }
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn stable_child_names(parent: &File, path: &Path) -> io::Result<Vec<std::ffi::OsString>> {
    stable_child_names_bounded(parent, path, usize::MAX)
}

#[cfg(unix)]
fn stable_child_names_bounded(
    parent: &File,
    _path: &Path,
    limit: usize,
) -> io::Result<Vec<std::ffi::OsString>> {
    use std::os::unix::ffi::OsStrExt as _;
    let directory = rustix::fs::Dir::read_from(parent).map_err(io::Error::from)?;
    let names = directory
        .map(|entry| {
            let entry = entry.map_err(io::Error::from)?;
            let name = OsStr::from_bytes(entry.file_name().to_bytes());
            Ok(name.to_os_string())
        })
        .filter(|entry| {
            !matches!(entry, Ok(name) if name == OsStr::new(".") || name == OsStr::new(".."))
        })
        .take(limit.saturating_add(1))
        .collect::<io::Result<Vec<_>>>()?;
    if names.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "stable directory child count exceeds bound",
        ));
    }
    Ok(names)
}

#[cfg(unix)]
fn stable_link_child(
    source: &File,
    _source_path: &Path,
    source_name: &OsStr,
    destination: &File,
    _destination_path: &Path,
    destination_name: &OsStr,
) -> io::Result<()> {
    rustix::fs::linkat(
        source,
        source_name,
        destination,
        destination_name,
        rustix::fs::AtFlags::empty(),
    )
    .map_err(io::Error::from)
}

#[cfg(unix)]
fn stable_unlink_child_if_identity(
    parent: &File,
    _path: &Path,
    name: &OsStr,
    expected: FileIdentity,
) -> io::Result<()> {
    use rustix::fs::{AtFlags, Mode, OFlags};
    let opened = rustix::fs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)?;
    if !opened.metadata()?.is_file() {
        return Err(io::Error::other("child is not a regular file"));
    }
    if file_identity(&opened)? != expected {
        return Err(io::Error::other("child identity changed before unlink"));
    }
    let named =
        rustix::fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW).map_err(io::Error::from)?;
    // `dev_t` is an opaque device bit pattern. It is unsigned on Linux and
    // signed on Darwin, so preserving the bits requires a target-dependent
    // no-op/sign cast that Clippy cannot express portably without allowances.
    #[allow(clippy::cast_sign_loss, clippy::unnecessary_cast)]
    let volume_serial = named.st_dev as u64;
    let named_identity = FileIdentity {
        volume_serial,
        file_id: u128::from(named.st_ino).to_le_bytes(),
    };
    if named_identity != expected {
        return Err(io::Error::other("child identity changed before unlink"));
    }
    rustix::fs::unlinkat(parent, name, AtFlags::empty()).map_err(io::Error::from)
}

#[cfg(unix)]
fn stable_remove_child_directory_if_identity(
    parent: &File,
    _path: &Path,
    name: &OsStr,
    expected: FileIdentity,
) -> io::Result<()> {
    use rustix::fs::{AtFlags, Mode, OFlags};
    let opened = rustix::fs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)?;
    if file_identity(&opened)? != expected {
        return Err(io::Error::other(
            "child directory identity changed before removal",
        ));
    }
    let named =
        rustix::fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW).map_err(io::Error::from)?;
    #[allow(clippy::cast_sign_loss, clippy::unnecessary_cast)]
    let volume_serial = named.st_dev as u64;
    let named_identity = FileIdentity {
        volume_serial,
        file_id: u128::from(named.st_ino).to_le_bytes(),
    };
    if named_identity != expected {
        return Err(io::Error::other(
            "child directory identity changed before removal",
        ));
    }
    rustix::fs::unlinkat(parent, name, AtFlags::REMOVEDIR).map_err(io::Error::from)
}

#[cfg(windows)]
fn stable_open_directory(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
}

#[cfg(windows)]
fn stable_open_directory_for_sync(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    std::fs::OpenOptions::new()
        .write(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
}

#[cfg(windows)]
fn stable_open_child_directory(_parent: &File, path: &Path, _name: &OsStr) -> io::Result<File> {
    stable_open_directory(path)
}

#[cfg(windows)]
fn stable_create_child_directory(_parent: &File, path: &Path, _name: &OsStr) -> io::Result<()> {
    match create_private_directory(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(windows)]
fn stable_open_child_file(
    _parent: &File,
    path: &Path,
    _name: &OsStr,
    create_new: bool,
) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .write(create_new)
        .create_new(create_new)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    options.open(path)
}

#[cfg(windows)]
fn stable_open_replaceable_child_file(
    _parent: &File,
    path: &Path,
    _name: &OsStr,
) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
}

#[cfg(windows)]
fn stable_open_or_create_child_file(parent: &File, path: &Path, name: &OsStr) -> io::Result<File> {
    match stable_open_child_file(parent, path, name, true) {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            use std::os::windows::fs::OpenOptionsExt as _;
            const FILE_SHARE_READ: u32 = 0x0000_0001;
            const FILE_SHARE_WRITE: u32 = 0x0000_0002;
            const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
                .open(path)
        }
        Err(error) => Err(error),
    }
}

#[cfg(windows)]
fn stable_child_names(parent: &File, path: &Path) -> io::Result<Vec<std::ffi::OsString>> {
    stable_child_names_bounded(parent, path, usize::MAX)
}

#[cfg(windows)]
fn stable_child_names_bounded(
    _parent: &File,
    path: &Path,
    limit: usize,
) -> io::Result<Vec<std::ffi::OsString>> {
    let names = std::fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .take(limit.saturating_add(1))
        .collect::<io::Result<Vec<_>>>()?;
    if names.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "stable directory child count exceeds bound",
        ));
    }
    Ok(names)
}

#[cfg(windows)]
fn stable_link_child(
    _source: &File,
    source_path: &Path,
    source_name: &OsStr,
    _destination: &File,
    destination_path: &Path,
    destination_name: &OsStr,
) -> io::Result<()> {
    std::fs::hard_link(
        source_path.join(source_name),
        destination_path.join(destination_name),
    )
}

#[cfg(windows)]
fn stable_unlink_child_if_identity(
    _parent: &File,
    path: &Path,
    name: &OsStr,
    expected: FileIdentity,
) -> io::Result<()> {
    let child = path.join(name);
    windows::delete_file_by_handle(&child, expected)
}

#[cfg(windows)]
fn stable_remove_child_directory_if_identity(
    _parent: &File,
    path: &Path,
    name: &OsStr,
    expected: FileIdentity,
) -> io::Result<()> {
    let child = path.join(name);
    let retained = stable_open_directory(&child)?;
    if file_identity(&retained)? != expected || path_identity(&child)? != expected {
        return Err(io::Error::other(
            "child directory identity changed before removal",
        ));
    }
    drop(retained);
    std::fs::remove_dir(child)
}

#[cfg(all(not(unix), not(windows)))]
fn stable_open_directory(_path: &Path) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stable directories unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
fn stable_open_child_directory(_parent: &File, _path: &Path, _name: &OsStr) -> io::Result<File> {
    stable_open_directory(Path::new(""))
}

#[cfg(all(not(unix), not(windows)))]
fn stable_open_or_create_child_file(
    _parent: &File,
    _path: &Path,
    _name: &OsStr,
) -> io::Result<File> {
    stable_open_directory(Path::new(""))
}

#[cfg(all(not(unix), not(windows)))]
fn stable_open_replaceable_child_file(
    _parent: &File,
    _path: &Path,
    _name: &OsStr,
) -> io::Result<File> {
    stable_open_directory(Path::new(""))
}

#[cfg(all(not(unix), not(windows)))]
fn stable_child_names(_parent: &File, _path: &Path) -> io::Result<Vec<std::ffi::OsString>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stable directories unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
fn stable_child_names_bounded(
    parent: &File,
    path: &Path,
    _limit: usize,
) -> io::Result<Vec<std::ffi::OsString>> {
    stable_child_names(parent, path)
}

#[cfg(all(not(unix), not(windows)))]
fn stable_link_child(
    _source: &File,
    _source_path: &Path,
    _source_name: &OsStr,
    _destination: &File,
    _destination_path: &Path,
    _destination_name: &OsStr,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stable directories unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
fn stable_unlink_child_if_identity(
    _parent: &File,
    _path: &Path,
    _name: &OsStr,
    _expected: FileIdentity,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stable directories unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
fn stable_remove_child_directory_if_identity(
    _parent: &File,
    _path: &Path,
    _name: &OsStr,
    _expected: FileIdentity,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stable directories unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
fn stable_create_child_directory(_parent: &File, _path: &Path, _name: &OsStr) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stable directories unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
fn stable_open_child_file(
    _parent: &File,
    _path: &Path,
    _name: &OsStr,
    _create_new: bool,
) -> io::Result<File> {
    stable_open_directory(Path::new(""))
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
    replace_file_platform(directory, source_name, target_name, None)
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

#[cfg(unix)]
fn link_count(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt as _;
    metadata.nlink()
}

#[cfg(windows)]
fn link_count(metadata: &std::fs::Metadata) -> u64 {
    let _ = metadata;
    1
}

#[cfg(all(not(unix), not(windows)))]
fn link_count(_metadata: &std::fs::Metadata) -> u64 {
    0
}

#[cfg(unix)]
fn replace_file_platform(
    directory: &File,
    source_name: &OsStr,
    target_name: &OsStr,
    expected_source: Option<FileIdentity>,
) -> Result<(), ReplaceFileError> {
    use rustix::fs::{AtFlags, Mode, OFlags, openat, renameat, statat};

    let source = openat(
        directory,
        source_name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)
    .map_err(ReplaceFileError::NotReplaced)?;
    let target = openat(
        directory,
        target_name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)
    .map_err(ReplaceFileError::NotReplaced)?;
    verify_regular_metadata(&source.metadata().map_err(ReplaceFileError::NotReplaced)?)
        .map_err(ReplaceFileError::NotReplaced)?;
    verify_regular_metadata(&target.metadata().map_err(ReplaceFileError::NotReplaced)?)
        .map_err(ReplaceFileError::NotReplaced)?;
    let source_identity = unix_identity(&source).map_err(ReplaceFileError::NotReplaced)?;
    if expected_source.is_some_and(|expected| expected != source_identity) {
        return Err(ReplaceFileError::NotReplaced(io::Error::other(
            "rename source differs from the retained expected identity",
        )));
    }
    renameat(directory, source_name, directory, target_name)
        .map_err(io::Error::from)
        .map_err(ReplaceFileError::NotReplaced)?;
    let replaced = openat(
        directory,
        target_name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)
    .map_err(ReplaceFileError::StateUnknown)?;
    verify_regular_metadata(
        &replaced
            .metadata()
            .map_err(ReplaceFileError::StateUnknown)?,
    )
    .map_err(ReplaceFileError::StateUnknown)?;
    if unix_identity(&replaced).map_err(ReplaceFileError::StateUnknown)? != source_identity
        || statat(directory, source_name, AtFlags::SYMLINK_NOFOLLOW).is_ok()
    {
        return Err(ReplaceFileError::StateUnknown(io::Error::other(
            "replacement success state did not reconcile",
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn unix_identity(file: &File) -> io::Result<FileIdentity> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = file.metadata()?;
    Ok(FileIdentity {
        volume_serial: metadata.dev(),
        file_id: u128::from(metadata.ino()).to_le_bytes(),
    })
}

#[cfg(unix)]
fn file_identity_platform(file: &File) -> io::Result<FileIdentity> {
    unix_identity(file)
}

#[cfg(unix)]
fn file_space_usage_platform(file: &File) -> io::Result<FileSpaceUsage> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = file.metadata()?;
    verify_space_usage_metadata(&metadata)?;
    let allocated_bytes = metadata
        .blocks()
        .checked_mul(512)
        .ok_or_else(|| io::Error::other("allocated file byte count overflowed u64"))?;
    Ok(FileSpaceUsage {
        logical_bytes: metadata.len(),
        allocated_bytes,
    })
}

#[cfg(unix)]
fn path_identity_platform(path: &Path) -> io::Result<FileIdentity> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = std::fs::symlink_metadata(path)?;
    Ok(FileIdentity {
        volume_serial: metadata.dev(),
        file_id: u128::from(metadata.ino()).to_le_bytes(),
    })
}

#[cfg(unix)]
fn file_link_count_platform(file: &File) -> io::Result<u64> {
    use std::os::unix::fs::MetadataExt as _;
    Ok(file.metadata()?.nlink())
}

#[cfg(unix)]
fn path_link_count_platform(path: &Path) -> io::Result<u64> {
    use std::os::unix::fs::MetadataExt as _;
    Ok(std::fs::symlink_metadata(path)?.nlink())
}

#[cfg(unix)]
fn create_private_directory_platform(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt as _;

    let mut builder = std::fs::DirBuilder::new();
    builder.mode(0o700).create(path)
}

#[cfg(windows)]
fn file_identity_platform(file: &File) -> io::Result<FileIdentity> {
    windows::file_identity(file)
}

#[cfg(windows)]
fn file_space_usage_platform(file: &File) -> io::Result<FileSpaceUsage> {
    windows::file_space_usage(file)
}

#[cfg(windows)]
fn path_identity_platform(path: &Path) -> io::Result<FileIdentity> {
    windows::identity(path)
}

#[cfg(windows)]
fn file_link_count_platform(file: &File) -> io::Result<u64> {
    windows::link_count(file)
}

#[cfg(windows)]
fn path_link_count_platform(path: &Path) -> io::Result<u64> {
    windows::path_link_count(path)
}

#[cfg(unix)]
fn install_new_file_platform(
    directory: &File,
    source_name: &OsStr,
    target_name: &OsStr,
    expected_source: Option<FileIdentity>,
) -> io::Result<()> {
    use rustix::fs::{Mode, OFlags, RenameFlags, openat, renameat_with};

    let source = openat(
        directory,
        source_name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)?;
    verify_regular_metadata(&source.metadata()?)?;
    let source_identity = unix_identity(&source)?;
    if expected_source.is_some_and(|expected| expected != source_identity) {
        return Err(io::Error::other(
            "rename source differs from the retained expected identity",
        ));
    }
    renameat_with(
        directory,
        source_name,
        directory,
        target_name,
        RenameFlags::NOREPLACE,
    )
    .map_err(io::Error::from)?;
    let installed = openat(
        directory,
        target_name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)?;
    verify_regular_metadata(&installed.metadata()?)?;
    if unix_identity(&installed)? != source_identity {
        return Err(io::Error::other("atomic creation state did not reconcile"));
    }
    Ok(())
}

#[cfg(windows)]
fn replace_file_platform(
    directory: &File,
    source_name: &OsStr,
    target_name: &OsStr,
    expected_source: Option<FileIdentity>,
) -> Result<(), ReplaceFileError> {
    windows::replace_file(directory, source_name, target_name, expected_source)
}

#[cfg(windows)]
fn install_new_file_platform(
    directory: &File,
    source_name: &OsStr,
    target_name: &OsStr,
    expected_source: Option<FileIdentity>,
) -> io::Result<()> {
    // The native handle-scoped rename is the race-free no-replace authority;
    // identity reconciliation supplies a stable AlreadyExists class.
    windows::install_new_file(directory, source_name, target_name, expected_source)
}

#[cfg(windows)]
fn create_private_directory_platform(path: &Path) -> io::Result<()> {
    windows::create_private_directory(path)
}

#[cfg(all(not(unix), not(windows)))]
fn replace_file_platform(
    _directory: &File,
    _source_name: &OsStr,
    _target_name: &OsStr,
    _expected_source: Option<FileIdentity>,
) -> Result<(), ReplaceFileError> {
    Err(ReplaceFileError::NotReplaced(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic replacement is unsupported on this platform",
    )))
}

#[cfg(all(not(unix), not(windows)))]
fn install_new_file_platform(
    _directory: &File,
    _source_name: &OsStr,
    _target_name: &OsStr,
    _expected_source: Option<FileIdentity>,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic creation is unsupported on this platform",
    ))
}

#[cfg(all(not(unix), not(windows)))]
fn create_private_directory_platform(_path: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "private directory creation is unsupported on this platform",
    ))
}

#[cfg(all(not(unix), not(windows)))]
fn file_identity_platform(_file: &File) -> io::Result<FileIdentity> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "identity unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
fn file_space_usage_platform(_file: &File) -> io::Result<FileSpaceUsage> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "allocated-byte measurement is unsupported on this platform",
    ))
}

#[cfg(all(not(unix), not(windows)))]
fn path_identity_platform(_path: &Path) -> io::Result<FileIdentity> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "identity unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
fn file_link_count_platform(_file: &File) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "link count unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
fn path_link_count_platform(_path: &Path) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "link count unsupported",
    ))
}

#[cfg(windows)]
#[allow(unsafe_code)]
mod windows {
    use std::ffi::OsStr;
    use std::fs::File;
    use std::io;
    use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
    use std::os::windows::fs::OpenOptionsExt as _;
    use std::os::windows::io::AsRawHandle as _;
    use std::path::{Path, PathBuf};

    use windows_sys::Win32::Foundation::{
        ERROR_INVALID_FUNCTION, ERROR_INVALID_PARAMETER, ERROR_NOT_SUPPORTED, GENERIC_WRITE,
        LocalFree,
    };
    #[cfg(test)]
    use windows_sys::Win32::Security::Authorization::{
        ConvertSecurityDescriptorToStringSecurityDescriptorW, GetNamedSecurityInfoW, SE_FILE_OBJECT,
    };
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    #[cfg(test)]
    use windows_sys::Win32::Security::OWNER_SECURITY_INFORMATION;
    use windows_sys::Win32::Security::{
        ACL, ACL_SIZE_INFORMATION, AclSizeInformation, DACL_SECURITY_INFORMATION,
        GetAclInformation, GetSecurityDescriptorControl, GetSecurityDescriptorDacl,
        PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, SE_DACL_PROTECTED,
        SECURITY_ATTRIBUTES,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, CreateDirectoryW, DELETE, FILE_ATTRIBUTE_READONLY,
        FILE_BASIC_INFO, FILE_DISPOSITION_FLAG_DELETE,
        FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE, FILE_DISPOSITION_INFO_EX,
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_FLAG_WRITE_THROUGH,
        FILE_ID_INFO, FILE_NAME_NORMALIZED, FILE_READ_ATTRIBUTES, FILE_RENAME_INFO,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_STANDARD_INFO,
        FILE_WRITE_ATTRIBUTES, FileBasicInfo, FileDispositionInfoEx, FileIdInfo, FileRenameInfo,
        FileRenameInfoEx, FileStandardInfo, GetDriveTypeW, GetFileInformationByHandle,
        GetFileInformationByHandleEx, GetFinalPathNameByHandleW, GetVolumeInformationW,
        GetVolumePathNameW, SetFileInformationByHandle, VOLUME_NAME_DOS,
    };

    #[cfg(test)]
    use super::classify_failed_replacement;
    use super::{
        FileIdentity, FileSpaceUsage, ReplaceFileError, WindowsVolumeInformation,
        is_link_or_reparse, verify_regular_metadata, verify_space_usage_metadata,
    };

    const DRIVE_FIXED: u32 = 3;
    const FILE_READ_ONLY_VOLUME: u32 = 0x0008_0000;
    const EXTENDED_PATH_CAPACITY: usize = 32_768;
    const FILESYSTEM_NAME_CAPACITY: usize = 256;
    const FILE_RENAME_REPLACE_IF_EXISTS_FLAG: u32 = 0x0000_0001;
    const FILE_RENAME_POSIX_SEMANTICS_FLAG: u32 = 0x0000_0002;

    pub(super) fn create_cas_writer(path: &Path) -> io::Result<File> {
        use windows_sys::Win32::Foundation::{GENERIC_READ, GENERIC_WRITE};
        use windows_sys::Win32::Storage::FileSystem::WRITE_DAC;
        std::fs::OpenOptions::new()
            // `access_mode` supplies the exact native rights below, but the
            // standard library still requires a write intent when a create
            // disposition is requested.
            .write(true)
            .create_new(true)
            .access_mode(GENERIC_READ | GENERIC_WRITE | WRITE_DAC)
            .share_mode(FILE_SHARE_READ)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_WRITE_THROUGH)
            .open(path)
    }

    pub(super) fn seal_cas_writer(writer: &File) -> io::Result<()> {
        writer.sync_all()?;
        let current_attributes = information(writer)?.dwFileAttributes;
        let mut basic = FILE_BASIC_INFO {
            CreationTime: 0,
            LastAccessTime: 0,
            LastWriteTime: 0,
            ChangeTime: 0,
            FileAttributes: current_attributes | FILE_ATTRIBUTE_READONLY,
        };
        // SAFETY: `writer` is live and the fixed-size input is valid.
        if unsafe {
            SetFileInformationByHandle(
                writer.as_raw_handle(),
                FileBasicInfo,
                (&raw mut basic).cast(),
                u32::try_from(std::mem::size_of::<FILE_BASIC_INFO>())
                    .expect("FILE_BASIC_INFO size fits u32"),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }

        set_canonical_cas_dacl(writer)?;
        writer.sync_all()
    }

    fn canonical_cas_descriptor() -> io::Result<PSECURITY_DESCRIPTOR> {
        // The owner is the ordinary (non-administrator) GraphForge process.
        // Keep the sealed payload non-writable while retaining exactly the
        // metadata right required by FileDispositionInfoEx's
        // IGNORE_READONLY_ATTRIBUTE deletion path: FILE_GENERIC_READ | DELETE
        // | FILE_WRITE_ATTRIBUTES. Use the file-specific mapped mask so the
        // stored ACE has no generic bits. In particular, this grants neither
        // data write/append nor WRITE_DAC.
        cas_descriptor("D:P(A;;0x00130189;;;OW)(A;;FA;;;SY)(A;;FA;;;BA)")
    }

    fn cas_descriptor(sddl: &str) -> io::Result<PSECURITY_DESCRIPTOR> {
        let descriptor_text = wide(OsStr::new(sddl))?;
        let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: the SDDL input and descriptor output are valid.
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                descriptor_text.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                std::ptr::null_mut(),
            )
        } == 0
        {
            Err(io::Error::last_os_error())
        } else {
            Ok(descriptor)
        }
    }

    pub(super) fn set_canonical_cas_dacl(writer: &File) -> io::Result<()> {
        let descriptor = canonical_cas_descriptor()?;
        set_cas_dacl(writer, descriptor)
    }

    fn set_cas_dacl(writer: &File, descriptor: PSECURITY_DESCRIPTOR) -> io::Result<()> {
        let mut present = 0;
        let mut defaulted = 0;
        let mut dacl = std::ptr::null_mut();
        // SAFETY: the converted descriptor is live and outputs are valid.
        let got_dacl = unsafe {
            GetSecurityDescriptorDacl(descriptor, &mut present, &mut dacl, &mut defaulted)
        };
        let operation = if got_dacl == 0 || present == 0 {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: the writer owns WRITE_DAC and the DACL remains live.
            let status = unsafe {
                windows_sys::Win32::Security::Authorization::SetSecurityInfo(
                    writer.as_raw_handle(),
                    windows_sys::Win32::Security::Authorization::SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    dacl,
                    std::ptr::null_mut(),
                )
            };
            if status == 0 {
                Ok(())
            } else {
                Err(io::Error::from_raw_os_error(
                    i32::try_from(status).unwrap_or(i32::MAX),
                ))
            }
        };
        // SAFETY: conversion allocated this descriptor with LocalAlloc.
        unsafe { LocalFree(descriptor.cast()) };
        operation
    }

    #[cfg(test)]
    pub(super) fn replace_with_owner_only_cas_dacl(path: &Path) -> io::Result<()> {
        use windows_sys::Win32::Foundation::GENERIC_READ;
        use windows_sys::Win32::Storage::FileSystem::WRITE_DAC;
        let file = std::fs::OpenOptions::new()
            .access_mode(GENERIC_READ | WRITE_DAC)
            .share_mode(FILE_SHARE_READ)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)?;
        let descriptor = cas_descriptor("D:P(A;;0x00130189;;;OW)")?;
        set_cas_dacl(&file, descriptor)
    }

    pub(super) fn open_sealed_cas_reader(path: &Path) -> io::Result<File> {
        std::fs::OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)
    }

    pub(super) fn open_cas_bridge(path: &Path) -> io::Result<File> {
        std::fs::OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)
    }

    pub(super) fn open_legacy_cas_adopter(path: &Path) -> io::Result<File> {
        use windows_sys::Win32::Foundation::GENERIC_READ;
        use windows_sys::Win32::Storage::FileSystem::WRITE_DAC;
        // Released Windows objects carry the read-only attribute but inherited
        // DACLs. This exclusive metadata handle safely upgrades only those
        // objects. Writable planted files and files with a pre-opened writer
        // are rejected rather than blessed as CAS authority.
        let writer = std::fs::OpenOptions::new()
            .access_mode(GENERIC_READ | WRITE_DAC | FILE_WRITE_ATTRIBUTES)
            .share_mode(FILE_SHARE_READ)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_WRITE_THROUGH)
            .open(path)?;
        if information(&writer)?.dwFileAttributes & FILE_ATTRIBUTE_READONLY == 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "unsealed CAS child cannot be adopted",
            ));
        }
        Ok(writer)
    }

    pub(super) fn has_canonical_cas_dacl(file: &File) -> io::Result<bool> {
        use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};

        let expected_descriptor = canonical_cas_descriptor()?;
        let mut actual: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: all optional component outputs are null and the descriptor
        // output is released below.
        let status = unsafe {
            GetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &raw mut actual,
            )
        };
        if status != 0 {
            // SAFETY: canonical descriptor was allocated with LocalAlloc.
            unsafe { LocalFree(expected_descriptor.cast()) };
            return Err(io::Error::from_raw_os_error(
                i32::try_from(status).unwrap_or(i32::MAX),
            ));
        }
        let result = descriptors_have_same_protected_dacl(actual, expected_descriptor);
        // SAFETY: actual was allocated by GetSecurityInfo.
        unsafe { LocalFree(actual.cast()) };
        // SAFETY: canonical descriptor was allocated with LocalAlloc.
        unsafe { LocalFree(expected_descriptor.cast()) };
        result
    }

    fn descriptors_have_same_protected_dacl(
        actual: PSECURITY_DESCRIPTOR,
        expected: PSECURITY_DESCRIPTOR,
    ) -> io::Result<bool> {
        let mut control = 0;
        let mut revision = 0;
        // The protection bit is the security property we require. Other
        // descriptor-control bits (notably SE_DACL_AUTO_INHERITED) describe
        // provenance and may legitimately differ after SetSecurityInfo.
        if unsafe { GetSecurityDescriptorControl(actual, &raw mut control, &raw mut revision) } == 0
        {
            return Err(io::Error::last_os_error());
        }
        if control & SE_DACL_PROTECTED == 0 {
            return Ok(false);
        }

        let actual_dacl = descriptor_dacl(actual)?;
        let expected_dacl = descriptor_dacl(expected)?;
        let actual_bytes = acl_bytes_in_use(actual_dacl)?;
        let expected_bytes = acl_bytes_in_use(expected_dacl)?;
        if actual_bytes != expected_bytes {
            return Ok(false);
        }
        // SAFETY: both ACL pointers remain owned by their live descriptors and
        // GetAclInformation proved the byte ranges occupied by their ACEs.
        Ok(unsafe {
            std::slice::from_raw_parts(actual_dacl.cast::<u8>(), actual_bytes)
                == std::slice::from_raw_parts(expected_dacl.cast::<u8>(), expected_bytes)
        })
    }

    fn descriptor_dacl(descriptor: PSECURITY_DESCRIPTOR) -> io::Result<*mut ACL> {
        let mut present = 0;
        let mut defaulted = 0;
        let mut dacl = std::ptr::null_mut();
        if unsafe {
            GetSecurityDescriptorDacl(
                descriptor,
                &raw mut present,
                &raw mut dacl,
                &raw mut defaulted,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        if present == 0 || dacl.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "security descriptor has no explicit DACL",
            ));
        }
        Ok(dacl)
    }

    fn acl_bytes_in_use(dacl: *const ACL) -> io::Result<usize> {
        let mut information = ACL_SIZE_INFORMATION::default();
        if unsafe {
            GetAclInformation(
                dacl,
                (&raw mut information).cast(),
                u32::try_from(std::mem::size_of::<ACL_SIZE_INFORMATION>())
                    .expect("ACL_SIZE_INFORMATION size fits u32"),
                AclSizeInformation,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        usize::try_from(information.AclBytesInUse)
            .map_err(|_| io::Error::other("ACL byte length does not fit usize"))
    }

    pub(super) fn replace_file(
        directory: &File,
        source_name: &OsStr,
        target_name: &OsStr,
        expected_source: Option<FileIdentity>,
    ) -> Result<(), ReplaceFileError> {
        let (_directory_guard, directory_path) =
            guarded_directory_path(directory).map_err(ReplaceFileError::NotReplaced)?;
        let source_path = directory_path.join(source_name);
        let target_path = directory_path.join(target_name);
        let source = open_rename_handle(&source_path).map_err(ReplaceFileError::NotReplaced)?;
        verify_open_regular(&source).map_err(ReplaceFileError::NotReplaced)?;
        source.sync_all().map_err(ReplaceFileError::NotReplaced)?;
        let source_before = file_identity(&source).map_err(ReplaceFileError::NotReplaced)?;
        if expected_source.is_some_and(|expected| expected != source_before) {
            return Err(ReplaceFileError::NotReplaced(io::Error::other(
                "rename source differs from the retained expected identity",
            )));
        }
        if identity(&source_path).map_err(ReplaceFileError::NotReplaced)? != source_before {
            return Err(ReplaceFileError::NotReplaced(io::Error::other(
                "rename source identity changed during open",
            )));
        }

        let target = open_identity_handle(&target_path).map_err(ReplaceFileError::NotReplaced)?;
        verify_open_regular(&target).map_err(ReplaceFileError::NotReplaced)?;
        let target_before = file_identity(&target).map_err(ReplaceFileError::NotReplaced)?;
        if identity(&target_path).map_err(ReplaceFileError::NotReplaced)? != target_before {
            return Err(ReplaceFileError::NotReplaced(io::Error::other(
                "rename target identity changed during open",
            )));
        }

        let result = rename_handle(&source, target_path.as_os_str(), true);
        let opened_source_after = file_identity(&source).ok();
        let opened_target_after = file_identity(&target).ok();
        let source_after = identity(&source_path).ok();
        let target_after = identity(&target_path).ok();
        if result.is_ok()
            && opened_source_after == Some(source_before)
            && opened_target_after == Some(target_before)
            && source_after.is_none()
            && target_after == Some(source_before)
        {
            return Ok(());
        }
        if result.is_ok() {
            return Err(ReplaceFileError::StateUnknown(io::Error::other(
                "replacement success state did not reconcile",
            )));
        }
        let error = result.expect_err("failed rename result was checked");
        if opened_source_after != Some(source_before) || opened_target_after != Some(target_before)
        {
            return Err(ReplaceFileError::StateUnknown(error));
        }
        Err(super::classify_failed_replacement(
            error,
            source_before,
            target_before,
            source_after,
            target_after,
        ))
    }

    pub(super) fn install_new_file(
        directory: &File,
        source_name: &OsStr,
        target_name: &OsStr,
        expected_source: Option<FileIdentity>,
    ) -> io::Result<()> {
        install_new_file_before_rename(directory, source_name, target_name, expected_source, || {})
    }

    fn install_new_file_before_rename(
        directory: &File,
        source_name: &OsStr,
        target_name: &OsStr,
        expected_source: Option<FileIdentity>,
        before_rename: impl FnOnce(),
    ) -> io::Result<()> {
        let (_directory_guard, directory_path) = guarded_directory_path(directory)?;
        let source_path = directory_path.join(source_name);
        let target_path = directory_path.join(target_name);
        let source = open_rename_handle(&source_path)?;
        verify_open_regular(&source)?;
        source.sync_all()?;
        let source_identity = file_identity(&source)?;
        if expected_source.is_some_and(|expected| expected != source_identity) {
            return Err(io::Error::other(
                "rename source differs from the retained expected identity",
            ));
        }
        if identity(&source_path)? != source_identity {
            return Err(io::Error::other(
                "rename source identity changed during open",
            ));
        }

        before_rename();
        let result = rename_handle(&source, target_path.as_os_str(), false);
        let opened_after = file_identity(&source).ok();
        let source_after = identity(&source_path).ok();
        let target_after = identity(&target_path).ok();
        if result.is_ok()
            && opened_after == Some(source_identity)
            && source_after.is_none()
            && target_after == Some(source_identity)
        {
            return Ok(());
        }
        if result.is_ok() {
            return Err(io::Error::other("atomic creation state did not reconcile"));
        }
        if opened_after != Some(source_identity) || source_after != Some(source_identity) {
            return Err(io::Error::other(
                "atomic creation failure state requires reconciliation",
            ));
        }
        let error = result.expect_err("failed rename result was checked");
        if matches!(error.raw_os_error(), Some(80) | Some(183)) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "target exists",
            ));
        }
        Err(error)
    }

    fn guarded_directory_path(directory: &File) -> io::Result<(File, PathBuf)> {
        let supplied_identity = file_identity(directory)?;
        let supplied_path = directory_path(directory)?;
        let guard = std::fs::OpenOptions::new()
            .access_mode(FILE_READ_ATTRIBUTES)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&supplied_path)?;
        let guard_metadata = guard.metadata()?;
        if is_link_or_reparse(&guard_metadata)
            || !guard_metadata.is_dir()
            || file_identity(&guard)? != supplied_identity
        {
            return Err(io::Error::other(
                "publication directory identity changed while acquiring guard",
            ));
        }
        let guarded_path = directory_path(&guard)?;
        Ok((guard, guarded_path))
    }

    fn open_rename_handle(path: &Path) -> io::Result<File> {
        std::fs::OpenOptions::new()
            .access_mode(GENERIC_WRITE | DELETE | FILE_READ_ATTRIBUTES)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_WRITE_THROUGH)
            .open(path)
    }

    pub(super) fn delete_file_by_handle(path: &Path, expected: FileIdentity) -> io::Result<()> {
        let file = std::fs::OpenOptions::new()
            // IGNORE_READONLY_ATTRIBUTE requires FILE_WRITE_ATTRIBUTES even
            // though the operation does not clear the shared attribute.
            .access_mode(DELETE | FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_WRITE_THROUGH)
            .open(path)?;
        if file_identity(&file)? != expected || identity(path)? != expected {
            return Err(io::Error::other("child identity changed before unlink"));
        }
        let mut disposition = FILE_DISPOSITION_INFO_EX {
            Flags: FILE_DISPOSITION_FLAG_DELETE | FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE,
        };
        // SAFETY: `file` is an exact retained handle opened with DELETE access,
        // and `disposition` is the initialized structure required by
        // FileDispositionInfoEx for the duration of the call. Ignoring the
        // readonly attribute removes only this name without mutating attributes
        // shared by another hard link to the same file.
        let deleted = unsafe {
            SetFileInformationByHandle(
                file.as_raw_handle(),
                FileDispositionInfoEx,
                (&mut disposition as *mut FILE_DISPOSITION_INFO_EX).cast(),
                u32::try_from(std::mem::size_of::<FILE_DISPOSITION_INFO_EX>())
                    .expect("FILE_DISPOSITION_INFO_EX size fits u32"),
            )
        };
        if deleted == 0 {
            Err(readonly_safe_delete_error(io::Error::last_os_error()))
        } else {
            Ok(())
        }
    }

    fn readonly_safe_delete_error(error: io::Error) -> io::Error {
        let unsupported = [
            ERROR_INVALID_FUNCTION as i32,
            ERROR_NOT_SUPPORTED as i32,
            ERROR_INVALID_PARAMETER as i32,
        ];
        if error
            .raw_os_error()
            .is_some_and(|code| unsupported.contains(&code))
        {
            io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "readonly-safe child deletion requires Windows 10 version 1709 or Windows Server version 1709 or newer: {error}"
                ),
            )
        } else {
            error
        }
    }

    fn rename_handle(
        source: &File,
        target_path: &OsStr,
        replace_if_exists: bool,
    ) -> io::Result<()> {
        let mut rename = RenameInformation::new(target_path, replace_if_exists)?;
        // SAFETY: `rename` owns an aligned, initialized FILE_RENAME_INFO buffer
        // for the duration of the call. The absolute target path was resolved
        // from the retained directory handle, and the source was opened with
        // FILE_FLAG_WRITE_THROUGH, so on NTFS the rename metadata uses the
        // documented write-through path. Replacement uses POSIX semantics so
        // the retained old-target handle remains valid while new name opens
        // resolve to the replacement.
        let information_class = if replace_if_exists {
            FileRenameInfoEx
        } else {
            FileRenameInfo
        };
        let renamed = unsafe {
            SetFileInformationByHandle(
                source.as_raw_handle(),
                information_class,
                rename.as_mut_ptr().cast(),
                rename.byte_len,
            )
        };
        if renamed == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    struct RenameInformation {
        words: Vec<usize>,
        byte_len: u32,
    }

    impl RenameInformation {
        fn new(target_name: &OsStr, replace_if_exists: bool) -> io::Result<Self> {
            let target = target_name.encode_wide().collect::<Vec<_>>();
            if target.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "target name is empty",
                ));
            }
            let name_bytes = target
                .len()
                .checked_mul(std::mem::size_of::<u16>())
                .and_then(|length| u32::try_from(length).ok())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "target name too long")
                })?;
            let required_bytes = std::mem::size_of::<FILE_RENAME_INFO>()
                .checked_add(usize::try_from(name_bytes).unwrap_or(usize::MAX))
                .and_then(|length| length.checked_add(std::mem::size_of::<u16>()))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "target name too long")
                })?;
            let word_bytes = std::mem::size_of::<usize>();
            let mut words = vec![0usize; required_bytes.div_ceil(word_bytes)];
            let allocated_bytes = words
                .len()
                .checked_mul(word_bytes)
                .and_then(|length| u32::try_from(length).ok())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "target name too long")
                })?;
            let information = words.as_mut_ptr().cast::<FILE_RENAME_INFO>();
            // SAFETY: `words` is zero-initialized, pointer-aligned, and at
            // least sizeof(FILE_RENAME_INFO) plus the UTF-16 name and its NUL.
            // FileNameLength excludes the retained zero terminator.
            unsafe {
                if replace_if_exists {
                    (*information).Anonymous.Flags =
                        FILE_RENAME_REPLACE_IF_EXISTS_FLAG | FILE_RENAME_POSIX_SEMANTICS_FLAG;
                } else {
                    (*information).Anonymous.ReplaceIfExists = false;
                }
                // SetFileInformationByHandle resolves a Win32 relative path
                // against the process current directory. Use the absolute
                // target path derived from the retained directory handle.
                (*information).RootDirectory = std::ptr::null_mut();
                (*information).FileNameLength = name_bytes;
                std::ptr::copy_nonoverlapping(
                    target.as_ptr(),
                    std::ptr::addr_of_mut!((*information).FileName).cast::<u16>(),
                    target.len(),
                );
            }
            Ok(Self {
                words,
                byte_len: allocated_bytes,
            })
        }

        fn as_mut_ptr(&mut self) -> *mut FILE_RENAME_INFO {
            self.words.as_mut_ptr().cast()
        }
    }

    pub(super) fn directory_path(directory: &File) -> io::Result<PathBuf> {
        let handle = directory.as_raw_handle();
        // SAFETY: this is a live owned directory handle. A null output buffer
        // with length zero is the documented size query.
        let required = unsafe {
            GetFinalPathNameByHandleW(
                handle,
                std::ptr::null_mut(),
                0,
                FILE_NAME_NORMALIZED | VOLUME_NAME_DOS,
            )
        };
        if required == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut buffer = vec![0u16; usize::try_from(required).unwrap_or(usize::MAX) + 1];
        // SAFETY: the buffer is writable for its advertised size and the
        // directory handle stays live through the call.
        let written = unsafe {
            GetFinalPathNameByHandleW(
                handle,
                buffer.as_mut_ptr(),
                u32::try_from(buffer.len()).unwrap_or(u32::MAX),
                FILE_NAME_NORMALIZED | VOLUME_NAME_DOS,
            )
        };
        if written == 0 {
            return Err(io::Error::last_os_error());
        }
        if usize::try_from(written).unwrap_or(usize::MAX) >= buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "normalized directory path exceeded its allocated buffer",
            ));
        }
        buffer.truncate(usize::try_from(written).unwrap_or_default());
        Ok(PathBuf::from(std::ffi::OsString::from_wide(&buffer)))
    }

    pub(super) fn create_private_directory(path: &Path) -> io::Result<()> {
        let path = wide(path.as_os_str())?;
        // Protected DACL: owner, LocalSystem, and local Administrators only.
        let descriptor_text = wide(OsStr::new("D:P(A;;FA;;;OW)(A;;FA;;;SY)(A;;FA;;;BA)"))?;
        let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: the input is a valid NUL-terminated SDDL buffer and the
        // output pointer is valid for the duration of the call.
        let converted = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                descriptor_text.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                std::ptr::null_mut(),
            )
        };
        if converted == 0 {
            return Err(io::Error::last_os_error());
        }
        let attributes = SECURITY_ATTRIBUTES {
            nLength: u32::try_from(std::mem::size_of::<SECURITY_ATTRIBUTES>())
                .expect("SECURITY_ATTRIBUTES size fits u32"),
            lpSecurityDescriptor: descriptor,
            bInheritHandle: 0,
        };
        // SAFETY: `path` and the descriptor backing `attributes` remain live
        // through the call. The descriptor is released exactly once below.
        let succeeded = unsafe { CreateDirectoryW(path.as_ptr(), &attributes) };
        let operation_error = if succeeded == 0 {
            Some(io::Error::last_os_error())
        } else {
            None
        };
        // SAFETY: successful conversion allocated this descriptor with
        // LocalAlloc; LocalFree is the documented matching release function.
        let free_result = unsafe { LocalFree(descriptor.cast()) };
        if let Some(error) = operation_error {
            return Err(error);
        }
        if !free_result.is_null() {
            return Err(io::Error::other(
                "private directory security descriptor release failed",
            ));
        }
        Ok(())
    }

    pub(super) fn volume_information(path: &Path) -> io::Result<WindowsVolumeInformation> {
        let path = wide(path.as_os_str())?;
        let mut volume_root = vec![0u16; EXTENDED_PATH_CAPACITY];
        // SAFETY: `path` is a NUL-terminated UTF-16 input and `volume_root`
        // is writable for the exact capacity supplied to the native call.
        let found = unsafe {
            GetVolumePathNameW(
                path.as_ptr(),
                volume_root.as_mut_ptr(),
                u32::try_from(volume_root.len()).expect("extended path capacity fits u32"),
            )
        };
        if found == 0 {
            return Err(io::Error::last_os_error());
        }
        let root_length = volume_root
            .iter()
            .position(|unit| *unit == 0)
            .ok_or_else(|| io::Error::other("native volume root was not terminated"))?;
        volume_root.truncate(root_length + 1);

        let mut filesystem_flags = 0u32;
        let mut filesystem_name = vec![0u16; FILESYSTEM_NAME_CAPACITY];
        // SAFETY: `volume_root` is the NUL-terminated mount root returned by
        // Windows. Optional outputs are null and both supplied outputs point
        // to initialized writable storage of the advertised sizes.
        let described = unsafe {
            GetVolumeInformationW(
                volume_root.as_ptr(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut filesystem_flags,
                filesystem_name.as_mut_ptr(),
                u32::try_from(filesystem_name.len()).expect("filesystem name capacity fits u32"),
            )
        };
        if described == 0 {
            return Err(io::Error::last_os_error());
        }
        let name_length = filesystem_name
            .iter()
            .position(|unit| *unit == 0)
            .ok_or_else(|| io::Error::other("native filesystem name was not terminated"))?;
        let filesystem_name = String::from_utf16(&filesystem_name[..name_length])
            .map_err(|_| io::Error::other("native filesystem name was invalid UTF-16"))?;
        // SAFETY: `volume_root` remains a valid NUL-terminated root path.
        let drive_type = unsafe { GetDriveTypeW(volume_root.as_ptr()) };
        Ok(WindowsVolumeInformation {
            filesystem_name,
            read_only: (filesystem_flags & FILE_READ_ONLY_VOLUME) != 0,
            fixed: drive_type == DRIVE_FIXED,
        })
    }

    fn verify_open_regular(file: &File) -> io::Result<()> {
        verify_space_usage_metadata(&file.metadata()?)?;
        let information = information(file)?;
        if information.nNumberOfLinks != 1 {
            return Err(io::Error::other("replacement path is hard linked"));
        }
        Ok(())
    }

    pub(super) fn identity(path: &Path) -> io::Result<FileIdentity> {
        file_identity(&open_identity_handle(path)?)
    }

    fn open_identity_handle(path: &Path) -> io::Result<File> {
        std::fs::OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)
    }

    pub(super) fn file_identity(file: &File) -> io::Result<FileIdentity> {
        let mut information = FILE_ID_INFO::default();
        // SAFETY: the handle is live and the output buffer has exactly the
        // FILE_ID_INFO size required by FileIdInfo.
        let succeeded = unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle(),
                FileIdInfo,
                (&mut information as *mut FILE_ID_INFO).cast(),
                u32::try_from(std::mem::size_of::<FILE_ID_INFO>())
                    .expect("FILE_ID_INFO size fits u32"),
            )
        };
        if succeeded == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(FileIdentity {
            volume_serial: information.VolumeSerialNumber,
            file_id: information.FileId.Identifier,
        })
    }

    pub(super) fn file_space_usage(file: &File) -> io::Result<FileSpaceUsage> {
        verify_regular_metadata(&file.metadata()?)?;
        let mut information = FILE_STANDARD_INFO::default();
        // SAFETY: the retained handle remains live and the output buffer has
        // exactly the size required by FileStandardInfo.
        let succeeded = unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle(),
                FileStandardInfo,
                (&raw mut information).cast(),
                u32::try_from(std::mem::size_of::<FILE_STANDARD_INFO>())
                    .expect("FILE_STANDARD_INFO size fits u32"),
            )
        };
        if succeeded == 0 {
            return Err(io::Error::last_os_error());
        }
        let logical_bytes = u64::try_from(information.EndOfFile)
            .map_err(|_| io::Error::other("native logical file length was negative"))?;
        let allocated_bytes = u64::try_from(information.AllocationSize)
            .map_err(|_| io::Error::other("native allocated file length was negative"))?;
        Ok(FileSpaceUsage {
            logical_bytes,
            allocated_bytes,
        })
    }

    pub(super) fn link_count(file: &File) -> io::Result<u64> {
        Ok(u64::from(information(file)?.nNumberOfLinks))
    }

    pub(super) fn path_link_count(path: &Path) -> io::Result<u64> {
        link_count(&open_identity_handle(path)?)
    }

    fn information(file: &File) -> io::Result<BY_HANDLE_FILE_INFORMATION> {
        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        // SAFETY: the file owns a live handle and the output points to a fully
        // allocated structure for the duration of the call.
        let succeeded =
            unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut information) };
        if succeeded == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(information)
        }
    }

    fn wide(value: &OsStr) -> io::Result<Vec<u16>> {
        let mut encoded = value.encode_wide().collect::<Vec<_>>();
        if encoded.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "filesystem path contains NUL",
            ));
        }
        encoded.push(0);
        Ok(encoded)
    }

    #[cfg(test)]
    fn security_descriptor_sddl(path: &Path) -> io::Result<String> {
        let path = wide(path.as_os_str())?;
        let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: all optional component outputs are null; the descriptor
        // output is valid and released below with LocalFree.
        let status = unsafe {
            GetNamedSecurityInfoW(
                path.as_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        if status != 0 {
            return Err(io::Error::from_raw_os_error(
                i32::try_from(status).unwrap_or(i32::MAX),
            ));
        }
        let mut text = std::ptr::null_mut();
        let mut length = 0;
        // SAFETY: the descriptor was returned by GetNamedSecurityInfoW and
        // the output pointer/length are valid for this call.
        let converted = unsafe {
            ConvertSecurityDescriptorToStringSecurityDescriptorW(
                descriptor,
                SDDL_REVISION_1,
                OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                &mut text,
                &mut length,
            )
        };
        if converted == 0 {
            // SAFETY: descriptor ownership is ours after the successful query.
            unsafe { LocalFree(descriptor.cast()) };
            return Err(io::Error::last_os_error());
        }
        // SAFETY: conversion returns `length` initialized UTF-16 code units.
        let result = String::from_utf16_lossy(unsafe {
            std::slice::from_raw_parts(text, usize::try_from(length).unwrap_or_default())
        });
        // SAFETY: both allocations use LocalAlloc and are released once.
        unsafe {
            LocalFree(text.cast());
            LocalFree(descriptor.cast());
        }
        Ok(result)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn id(volume_serial: u64, low: u64) -> FileIdentity {
            FileIdentity {
                volume_serial,
                file_id: u128::from(low).to_le_bytes(),
            }
        }

        #[test]
        fn failed_replace_is_unknown_unless_both_identities_are_unchanged() {
            let unchanged = classify_failed_replacement(
                io::Error::other("injected"),
                id(1, 2),
                id(1, 3),
                Some(id(1, 2)),
                Some(id(1, 3)),
            );
            assert!(matches!(unchanged, ReplaceFileError::NotReplaced(_)));

            for (source_after, target_after) in [
                (None, Some(id(1, 3))),
                (Some(id(1, 2)), None),
                (Some(id(1, 4)), Some(id(1, 3))),
                (Some(id(1, 2)), Some(id(1, 4))),
            ] {
                assert!(matches!(
                    classify_failed_replacement(
                        io::Error::other("injected"),
                        id(1, 2),
                        id(1, 3),
                        source_after,
                        target_after,
                    ),
                    ReplaceFileError::StateUnknown(_)
                ));
            }

            let mut high_bits_changed = id(1, 2);
            high_bits_changed.file_id[15] = 1;
            assert!(matches!(
                classify_failed_replacement(
                    io::Error::other("injected"),
                    id(1, 2),
                    id(1, 3),
                    Some(high_bits_changed),
                    Some(id(1, 3)),
                ),
                ReplaceFileError::StateUnknown(_)
            ));
        }

        #[test]
        fn readonly_safe_delete_maps_unsupported_platform_errors_actionably() {
            let unsupported = readonly_safe_delete_error(io::Error::from_raw_os_error(
                ERROR_NOT_SUPPORTED as i32,
            ));
            assert_eq!(unsupported.kind(), io::ErrorKind::Unsupported);
            assert!(unsupported.to_string().contains("Windows 10 version 1709"));

            let denied = readonly_safe_delete_error(io::Error::from_raw_os_error(5));
            assert_eq!(denied.kind(), io::ErrorKind::PermissionDenied);
            assert_eq!(denied.raw_os_error(), Some(5));
        }

        #[test]
        fn private_directory_dacl_is_protected_and_has_no_public_trustee() {
            let parent = tempfile::tempdir().unwrap();
            let path = parent.path().join("private");
            create_private_directory(&path).unwrap();
            let sddl = security_descriptor_sddl(&path).unwrap();
            assert!(sddl.contains("D:P"), "{sddl}");
            assert!(!sddl.contains(";;;WD)"), "{sddl}");
            assert!(!sddl.contains(";;;AU)"), "{sddl}");
            assert!(!sddl.contains(";;;BU)"), "{sddl}");
            assert_eq!(sddl.matches("(A;").count(), 3, "{sddl}");
        }

        #[test]
        fn junction_reparse_directory_is_detected_fail_closed() {
            let parent = tempfile::tempdir().unwrap();
            let target = parent.path().join("target");
            let junction = parent.path().join("junction");
            std::fs::create_dir(&target).unwrap();
            let status = std::process::Command::new("cmd")
                .args(["/C", "mklink", "/J"])
                .arg(&junction)
                .arg(&target)
                .status()
                .unwrap();
            assert!(status.success());
            let metadata = std::fs::symlink_metadata(&junction).unwrap();
            assert!(super::super::is_link_or_reparse(&metadata));
        }

        #[test]
        fn canonical_extended_drive_path_reports_native_volume_information() {
            use std::path::{Component, Prefix};

            let parent = tempfile::tempdir().unwrap();
            let canonical = parent.path().canonicalize().unwrap();
            assert!(matches!(
                canonical.components().next(),
                Some(Component::Prefix(prefix))
                    if matches!(prefix.kind(), Prefix::VerbatimDisk(_))
            ));
            let information = volume_information(&canonical).unwrap();
            assert!(information.fixed);
            assert!(!information.read_only);
            assert!(!information.filesystem_name.is_empty());
        }

        #[test]
        fn write_through_source_handle_performs_replacement_rename() {
            let directory = tempfile::tempdir().unwrap();
            let source_path = directory.path().join("source");
            let target_path = directory.path().join("target");
            std::fs::write(&source_path, b"new").unwrap();
            std::fs::write(&target_path, b"old").unwrap();
            let source = open_rename_handle(&source_path).unwrap();
            source.sync_all().unwrap();
            let source_identity = file_identity(&source).unwrap();
            let old_target = open_identity_handle(&target_path).unwrap();
            let old_target_identity = file_identity(&old_target).unwrap();

            rename_handle(&source, target_path.as_os_str(), true).unwrap();

            assert_eq!(file_identity(&source).unwrap(), source_identity);
            assert_eq!(file_identity(&old_target).unwrap(), old_target_identity);
            assert!(!source_path.exists());
            assert_eq!(identity(&target_path).unwrap(), source_identity);
            assert_eq!(std::fs::read(target_path).unwrap(), b"new");
        }

        #[test]
        fn rename_information_buffer_meets_win32_layout_contract() {
            let target_path = OsStr::new(r"C:\durability-probe\published");
            let target = target_path.encode_wide().collect::<Vec<_>>();
            let mut rename = RenameInformation::new(target_path, false).unwrap();
            let information = rename.as_mut_ptr();

            assert_eq!(
                information.addr() % std::mem::align_of::<FILE_RENAME_INFO>(),
                0
            );
            assert!(
                usize::try_from(rename.byte_len).unwrap()
                    >= std::mem::size_of::<FILE_RENAME_INFO>()
                        + target.len() * std::mem::size_of::<u16>()
                        + std::mem::size_of::<u16>()
            );
            // SAFETY: `rename` owns the initialized buffer and the assertion
            // above proves room for the encoded name plus its zero terminator.
            unsafe {
                assert_eq!((*information).FileNameLength as usize, target.len() * 2);
                assert!((*information).RootDirectory.is_null());
                assert!(!(*information).Anonymous.ReplaceIfExists);
                let file_name = std::ptr::addr_of!((*information).FileName).cast::<u16>();
                assert_eq!(std::slice::from_raw_parts(file_name, target.len()), target);
                assert_eq!(*file_name.add(target.len()), 0);
            }

            let mut replacement = RenameInformation::new(target_path, true).unwrap();
            // SAFETY: `replacement` owns a live initialized buffer.
            unsafe {
                assert_eq!(
                    (*replacement.as_mut_ptr()).Anonymous.Flags,
                    FILE_RENAME_REPLACE_IF_EXISTS_FLAG | FILE_RENAME_POSIX_SEMANTICS_FLAG
                );
            }
        }

        #[test]
        fn contender_created_after_source_open_is_never_replaced() {
            let directory = tempfile::tempdir().unwrap();
            let source = directory.path().join("source");
            let target = directory.path().join("target");
            std::fs::write(&source, b"source").unwrap();
            let source_before = identity(&source).unwrap();
            let handle = super::super::tests::directory_handle(directory.path());
            let error = install_new_file_before_rename(
                &handle,
                OsStr::new("source"),
                OsStr::new("target"),
                None,
                || std::fs::write(&target, b"contender").unwrap(),
            )
            .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
            assert_eq!(std::fs::read(&source).unwrap(), b"source");
            assert_eq!(identity(&source).unwrap(), source_before);
            assert_eq!(std::fs::read(&target).unwrap(), b"contender");
            assert_ne!(identity(&target).unwrap(), source_before);
        }

        #[test]
        fn absolute_target_does_not_resolve_against_process_current_directory() {
            let directory = tempfile::tempdir().unwrap();
            let source = directory.path().join("source");
            std::fs::write(&source, b"source").unwrap();
            let cwd_decoy = tempfile::Builder::new()
                .prefix("graphforge-rename-decoy-")
                .tempdir_in(std::env::current_dir().unwrap())
                .unwrap();
            let target_name = cwd_decoy.path().file_name().unwrap();
            let target = directory.path().join(target_name);
            let handle = super::super::tests::directory_handle(directory.path());

            install_new_file(&handle, OsStr::new("source"), target_name, None).unwrap();

            assert_eq!(std::fs::read(target).unwrap(), b"source");
            assert!(cwd_decoy.path().is_dir());
        }

        #[test]
        fn internal_directory_guard_blocks_anchor_rename() {
            let parent = tempfile::tempdir().unwrap();
            let directory = parent.path().join("probe");
            let moved = parent.path().join("moved");
            std::fs::create_dir(&directory).unwrap();
            std::fs::write(directory.join("source"), b"source").unwrap();
            let caller = std::fs::OpenOptions::new()
                .access_mode(FILE_READ_ATTRIBUTES)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
                .open(&directory)
                .unwrap();

            install_new_file_before_rename(
                &caller,
                OsStr::new("source"),
                OsStr::new("target"),
                None,
                || assert!(std::fs::rename(&directory, &moved).is_err()),
            )
            .unwrap();

            assert_eq!(std::fs::read(directory.join("target")).unwrap(), b"source");
            assert!(!moved.exists());
        }

        #[test]
        fn directory_guard_rejects_junction_before_publication() {
            let parent = tempfile::tempdir().unwrap();
            let target_directory = parent.path().join("target-directory");
            let junction = parent.path().join("junction");
            std::fs::create_dir(&target_directory).unwrap();
            let source = target_directory.join("source");
            let published = target_directory.join("published");
            std::fs::write(&source, b"source").unwrap();
            let status = std::process::Command::new("cmd")
                .args(["/C", "mklink", "/J"])
                .arg(&junction)
                .arg(&target_directory)
                .status()
                .unwrap();
            assert!(status.success());
            let caller = std::fs::OpenOptions::new()
                .access_mode(FILE_READ_ATTRIBUTES)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
                .open(&junction)
                .unwrap();

            let error =
                install_new_file(&caller, OsStr::new("source"), OsStr::new("published"), None)
                    .unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::Other);
            assert_eq!(std::fs::read(source).unwrap(), b"source");
            assert!(!published.exists());
        }

        #[test]
        fn install_rejects_source_substituted_after_expected_identity_was_recorded() {
            let directory = tempfile::tempdir().unwrap();
            let source = directory.path().join("source");
            let original = directory.path().join("original");
            let target = directory.path().join("target");
            std::fs::write(&source, b"authenticated").unwrap();
            let expected = identity(&source).unwrap();
            std::fs::rename(&source, &original).unwrap();
            std::fs::write(&source, b"substitute").unwrap();
            let handle = super::super::tests::directory_handle(directory.path());

            let error = install_new_file(
                &handle,
                OsStr::new("source"),
                OsStr::new("target"),
                Some(expected),
            )
            .unwrap_err();

            assert_eq!(error.kind(), io::ErrorKind::Other);
            assert_eq!(std::fs::read(source).unwrap(), b"substitute");
            assert_eq!(std::fs::read(original).unwrap(), b"authenticated");
            assert!(!target.exists());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    #[allow(unsafe_code)]
    fn mark_sparse(file: &File) {
        use std::os::windows::io::AsRawHandle as _;
        use windows_sys::Win32::System::IO::DeviceIoControl;
        use windows_sys::Win32::System::Ioctl::FSCTL_SET_SPARSE;

        let mut returned = 0;
        // SAFETY: `file` retains a live file handle; this control code has no
        // input or output buffer, and `returned` remains live for the call.
        let succeeded = unsafe {
            DeviceIoControl(
                file.as_raw_handle(),
                FSCTL_SET_SPARSE,
                std::ptr::null(),
                0,
                std::ptr::null_mut(),
                0,
                &raw mut returned,
                std::ptr::null_mut(),
            )
        };
        assert_ne!(succeeded, 0, "{}", io::Error::last_os_error());
    }

    #[cfg(unix)]
    fn mark_sparse(_file: &File) {}

    #[cfg(any(unix, windows))]
    #[test]
    fn retained_handle_reports_sparse_logical_and_allocated_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sparse.bin");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        mark_sparse(&file);
        file.set_len(64 * 1024 * 1024).unwrap();
        file.sync_all().unwrap();

        let usage = file_space_usage(&file).unwrap();
        assert_eq!(usage.logical_bytes, 64 * 1024 * 1024);
        assert!(
            usage.allocated_bytes < usage.logical_bytes,
            "sparse allocation must be physical, not a logical-length proxy: {usage:?}"
        );
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn retained_hard_link_handles_share_identity_and_space_usage() {
        let directory = tempfile::tempdir().unwrap();
        let source_path = directory.path().join("source.bin");
        let alias_path = directory.path().join("alias.bin");
        let source = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&source_path)
            .unwrap();
        mark_sparse(&source);
        source.set_len(32 * 1024 * 1024).unwrap();
        source.sync_all().unwrap();
        std::fs::hard_link(&source_path, &alias_path).unwrap();
        let alias = File::open(&alias_path).unwrap();

        assert_eq!(
            file_identity(&source).unwrap(),
            file_identity(&alias).unwrap()
        );
        assert_eq!(
            file_space_usage(&source).unwrap(),
            file_space_usage(&alias).unwrap()
        );

        std::fs::remove_file(&source_path).unwrap();
        assert_eq!(
            file_identity(&source).unwrap(),
            file_identity(&alias).unwrap()
        );
        assert_eq!(
            file_space_usage(&source).unwrap(),
            file_space_usage(&alias).unwrap()
        );
    }

    #[cfg(unix)]
    const FIFO_CHILD_ENV: &str = "GRAPHFORGE_FILESYSTEM_FIFO_CHILD";

    #[cfg(unix)]
    const FIFO_ROOT_ENV: &str = "GRAPHFORGE_FILESYSTEM_FIFO_ROOT";

    pub(super) fn directory_handle(path: &Path) -> File {
        #[cfg(unix)]
        return File::open(path).unwrap();

        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
            const FILE_SHARE_READ: u32 = 0x0000_0001;
            const FILE_SHARE_WRITE: u32 = 0x0000_0002;
            return std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .open(path)
                .unwrap();
        }
    }

    #[test]
    fn concurrent_open_or_create_has_one_stable_named_identity() {
        let root = tempfile::tempdir().unwrap();
        let root = std::sync::Arc::new(root);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(9));
        let workers = (0..8)
            .map(|_| {
                let root = std::sync::Arc::clone(&root);
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let directory = StableDirectory::open(root.path()).unwrap();
                    barrier.wait();
                    let file = directory
                        .open_or_create_child_file(OsStr::new("lifecycle.lock"))
                        .unwrap();
                    file_identity(&file).unwrap()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let identities = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert!(identities.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(
            file_link_count(&File::open(root.path().join("lifecycle.lock")).unwrap()).unwrap(),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn peerless_fifo_child() {
        let Ok(operation) = std::env::var(FIFO_CHILD_ENV) else {
            return;
        };
        let root = PathBuf::from(std::env::var_os(FIFO_ROOT_ENV).expect("FIFO test root"));
        let stable = StableDirectory::open(&root).unwrap();
        let fifo_identity = path_identity(&root.join("fifo")).unwrap();
        let regular_identity = path_identity(&root.join("regular")).unwrap();
        let result = match operation.as_str() {
            "open" => stable.open_child_file(OsStr::new("fifo")).map(drop),
            "open-or-create" => stable
                .open_or_create_child_file(OsStr::new("fifo"))
                .map(drop),
            "unlink" => stable.unlink_child_if_identity(OsStr::new("fifo"), fifo_identity),
            "replace-source" => {
                stable.replace_child(OsStr::new("fifo"), fifo_identity, OsStr::new("regular"))
            }
            "replace-target" => {
                stable.replace_child(OsStr::new("regular"), regular_identity, OsStr::new("fifo"))
            }
            "native-replace-source" => replace_file(
                &directory_handle(&root),
                OsStr::new("fifo"),
                OsStr::new("regular"),
            )
            .map_err(|error| io::Error::other(error.to_string())),
            "native-replace-target" => replace_file(
                &directory_handle(&root),
                OsStr::new("regular"),
                OsStr::new("fifo"),
            )
            .map_err(|error| io::Error::other(error.to_string())),
            "native-install-source" => install_new_file(
                &directory_handle(&root),
                OsStr::new("fifo"),
                OsStr::new("absent"),
            ),
            other => panic!("unknown FIFO operation {other}"),
        };
        assert!(
            result.is_err(),
            "peerless FIFO must fail closed: {operation}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn every_regular_child_operation_rejects_peerless_fifo_without_blocking() {
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};

        for operation in [
            "open",
            "open-or-create",
            "unlink",
            "replace-source",
            "replace-target",
            "native-replace-source",
            "native-replace-target",
            "native-install-source",
        ] {
            let root = tempfile::tempdir().unwrap();
            std::fs::write(root.path().join("regular"), b"regular").unwrap();
            let status = Command::new("mkfifo")
                .arg(root.path().join("fifo"))
                .status()
                .unwrap();
            assert!(status.success(), "mkfifo failed for {operation}");
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "tests::peerless_fifo_child", "--nocapture"])
                .env(FIFO_CHILD_ENV, operation)
                .env(FIFO_ROOT_ENV, root.path())
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let started = Instant::now();
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    assert!(
                        status.success(),
                        "FIFO child failed for {operation}: {status}"
                    );
                    break;
                }
                if started.elapsed() >= Duration::from_secs(2) {
                    child.kill().unwrap();
                    let _ = child.wait();
                    panic!("regular-child operation blocked on peerless FIFO: {operation}");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn regular_child_operations_reject_symlinks_and_unix_sockets() {
        use std::os::unix::fs::symlink;
        use std::os::unix::net::UnixDatagram;

        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("regular"), b"regular").unwrap();
        symlink("regular", root.path().join("linked")).unwrap();
        let _socket = UnixDatagram::bind(root.path().join("socket")).unwrap();
        let stable = StableDirectory::open(root.path()).unwrap();

        for special in ["linked", "socket"] {
            assert!(stable.open_child_file(OsStr::new(special)).is_err());
            assert!(
                stable
                    .open_or_create_child_file(OsStr::new(special))
                    .is_err()
            );
            let identity = path_identity(&root.path().join(special)).unwrap();
            assert!(
                stable
                    .unlink_child_if_identity(OsStr::new(special), identity)
                    .is_err()
            );
            assert!(root.path().join(special).exists());
        }
    }

    #[test]
    fn replacement_changes_exact_bytes_and_consumes_source() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        let target = directory.path().join("target");
        std::fs::write(&source, b"new").unwrap();
        std::fs::write(&target, b"old").unwrap();
        let handle = directory_handle(directory.path());
        replace_file(&handle, OsStr::new("source"), OsStr::new("target")).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
        assert!(!source.exists());
    }

    #[test]
    fn new_install_never_replaces_an_existing_entry() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        let target = directory.path().join("target");
        std::fs::write(&source, b"new").unwrap();
        let handle = directory_handle(directory.path());
        install_new_file(&handle, OsStr::new("source"), OsStr::new("target")).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"new");

        let second = directory.path().join("second");
        std::fs::write(&second, b"other").unwrap();
        let error =
            install_new_file(&handle, OsStr::new("second"), OsStr::new("target")).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
        assert_eq!(std::fs::read(&second).unwrap(), b"other");
    }

    #[cfg(windows)]
    #[test]
    fn concurrent_no_replace_install_has_exactly_one_winner() {
        use std::sync::{Arc, Barrier};

        let directory = tempfile::tempdir().unwrap();
        let first = directory.path().join("first");
        let second = directory.path().join("second");
        let target = directory.path().join("target");
        std::fs::write(&first, b"first").unwrap();
        std::fs::write(&second, b"second").unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let mut contenders = Vec::new();
        for name in ["first", "second"] {
            let path = directory.path().to_path_buf();
            let barrier = Arc::clone(&barrier);
            contenders.push(std::thread::spawn(move || {
                let handle = directory_handle(&path);
                barrier.wait();
                install_new_file(&handle, OsStr::new(name), OsStr::new("target"))
            }));
        }
        barrier.wait();
        let results = contenders
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| {
                    result
                        .as_ref()
                        .is_err_and(|error| error.kind() == io::ErrorKind::AlreadyExists)
                })
                .count(),
            1
        );
        let target_bytes = std::fs::read(&target).unwrap();
        assert!(target_bytes == b"first" || target_bytes == b"second");
        let loser = if target_bytes == b"first" {
            second
        } else {
            first
        };
        assert!(loser.exists());
    }

    #[test]
    fn hard_linked_inputs_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        let alias = directory.path().join("alias");
        let target = directory.path().join("target");
        std::fs::write(&source, b"new").unwrap();
        std::fs::hard_link(&source, &alias).unwrap();
        std::fs::write(&target, b"old").unwrap();
        assert!(matches!(
            replace_file(
                &directory_handle(directory.path()),
                OsStr::new("source"),
                OsStr::new("target")
            ),
            Err(ReplaceFileError::NotReplaced(_))
        ));
        assert_eq!(std::fs::read(&target).unwrap(), b"old");
    }

    #[test]
    fn private_directory_is_created_without_inheriting_public_access() {
        let parent = tempfile::tempdir().unwrap();
        let directory = parent.path().join("private");
        create_private_directory(&directory).unwrap();
        assert!(directory.is_dir());
        let identity = path_identity(&directory).unwrap();
        assert_ne!(identity.file_id, [0; 16]);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(directory).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn stable_directory_fails_closed_after_named_child_substitution() {
        let root = tempfile::tempdir().unwrap();
        let stable_root = StableDirectory::open(root.path()).unwrap();
        let child = stable_root
            .create_child_directory(OsStr::new("objects"))
            .unwrap();
        let file = child.create_child_file(OsStr::new("payload")).unwrap();
        drop(file);
        let displaced = root.path().join("displaced");
        std::fs::rename(root.path().join("objects"), &displaced).unwrap();
        std::fs::create_dir(root.path().join("objects")).unwrap();

        assert!(child.revalidate_named().is_err());
        assert!(child.open_child_file(OsStr::new("payload")).is_err());
        assert_eq!(std::fs::read(displaced.join("payload")).unwrap(), b"");
    }

    #[cfg(unix)]
    #[test]
    fn stable_directory_rejects_symlink_child() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), root.path().join("objects")).unwrap();
        let stable_root = StableDirectory::open(root.path()).unwrap();
        assert!(
            stable_root
                .open_child_directory(OsStr::new("objects"))
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn stable_directory_enumerates_and_links_only_retained_regular_source() {
        use std::io::Write as _;

        let root = tempfile::tempdir().unwrap();
        let stable = StableDirectory::open(root.path()).unwrap();
        let source_dir = stable.create_child_directory(OsStr::new("source")).unwrap();
        let destination = stable
            .create_child_directory(OsStr::new("destination"))
            .unwrap();
        let mut source = source_dir.create_child_file(OsStr::new("payload")).unwrap();
        source.write_all(b"payload").unwrap();
        let identity = file_identity(&source).unwrap();
        assert_eq!(
            source_dir.child_names().unwrap(),
            [std::ffi::OsString::from("payload")]
        );
        let (installed, installed_identity) = source_dir
            .link_child_into(
                OsStr::new("payload"),
                &source,
                identity,
                &destination,
                OsStr::new("copy"),
            )
            .unwrap();
        assert_eq!(installed_identity, identity);
        assert_eq!(file_identity(&installed).unwrap(), identity);

        std::fs::rename(
            root.path().join("source/payload"),
            root.path().join("source/old"),
        )
        .unwrap();
        std::fs::write(root.path().join("source/payload"), b"replacement").unwrap();
        assert!(
            source_dir
                .link_child_into(
                    OsStr::new("payload"),
                    &source,
                    identity,
                    &destination,
                    OsStr::new("bad")
                )
                .is_err()
        );
        assert!(!root.path().join("destination/bad").exists());
    }

    #[test]
    fn stable_directory_rejects_non_child_names_and_identity_mismatch_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let stable = StableDirectory::open(root.path()).unwrap();
        assert!(stable.open_child_file(OsStr::new("../escape")).is_err());
        let file = stable.create_child_file(OsStr::new("payload")).unwrap();
        let identity = file_identity(&file).unwrap();
        drop(file);
        std::fs::rename(root.path().join("payload"), root.path().join("old")).unwrap();
        std::fs::write(root.path().join("payload"), b"replacement").unwrap();
        assert!(
            stable
                .unlink_child_if_identity(OsStr::new("payload"), identity)
                .is_err()
        );
        assert_eq!(
            std::fs::read(root.path().join("payload")).unwrap(),
            b"replacement"
        );
    }

    #[cfg(windows)]
    #[test]
    fn stable_directory_adopts_readonly_legacy_cas_without_data_write_authority() {
        use std::io::Read as _;
        use std::os::windows::fs::OpenOptionsExt as _;

        const FILE_SHARE_READ: u32 = 0x0000_0001;
        const FILE_SHARE_WRITE: u32 = 0x0000_0002;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("legacy");
        let payload = b"authenticated legacy payload";
        std::fs::write(&path, payload).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&path, permissions).unwrap();
        let stable = StableDirectory::open(root.path()).unwrap();
        let expected = path_identity(&path).unwrap();

        let mut adopter = stable
            .open_legacy_cas_child_for_adoption(OsStr::new("legacy"))
            .unwrap();
        let mut authenticated = Vec::new();
        adopter.read_to_end(&mut authenticated).unwrap();
        assert_eq!(authenticated, payload);
        assert!(
            std::fs::OpenOptions::new()
                .write(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .open(&path)
                .is_err()
        );

        let adopted = stable
            .adopt_legacy_cas_child(OsStr::new("legacy"), adopter)
            .unwrap();
        assert_eq!(file_identity(&adopted.0).unwrap(), expected);
        assert_eq!(std::fs::read(&path).unwrap(), payload);
        drop(adopted);
        assert!(stable.open_cas_child_file(OsStr::new("legacy")).is_ok());
        assert!(
            std::fs::OpenOptions::new()
                .write(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .open(&path)
                .is_err()
        );
    }

    #[cfg(windows)]
    #[test]
    fn stable_directory_unlinks_canonical_cas_child_without_unsealing_hard_link() {
        use std::io::Write as _;

        let root = tempfile::tempdir().unwrap();
        let stable = StableDirectory::open(root.path()).unwrap();
        let temporary_path = root.path().join("temporary");
        let installed_path = root.path().join("installed");
        let mut temporary = stable
            .create_cas_child_file(OsStr::new("temporary"))
            .unwrap();
        temporary.write_all(b"sealed").unwrap();
        temporary.sync_all().unwrap();
        let identity = temporary.identity();
        let temporary = stable
            .seal_cas_child_file(OsStr::new("temporary"), temporary)
            .unwrap()
            .into_file();
        assert!(
            stable.open_cas_child_file(OsStr::new("temporary")).is_ok(),
            "a freshly sealed CAS child must pass canonical reopen"
        );
        std::fs::hard_link(&temporary_path, &installed_path).unwrap();
        assert_eq!(path_identity(&installed_path).unwrap(), identity);
        assert!(temporary.metadata().unwrap().permissions().readonly());
        assert!(
            std::fs::metadata(&installed_path)
                .unwrap()
                .permissions()
                .readonly()
        );
        drop(temporary);
        windows::replace_with_owner_only_cas_dacl(&temporary_path).unwrap();
        assert_eq!(path_identity(&temporary_path).unwrap(), identity);

        stable
            .unlink_child_if_identity(OsStr::new("temporary"), identity)
            .unwrap();

        assert!(!temporary_path.exists());
        assert_eq!(std::fs::read(&installed_path).unwrap(), b"sealed");
        assert_eq!(path_identity(&installed_path).unwrap(), identity);
        let installed = File::open(&installed_path).unwrap();
        assert!(installed.metadata().unwrap().permissions().readonly());
        assert_eq!(file_link_count(&installed).unwrap(), 1);
    }

    #[test]
    fn stable_directory_rejects_replaced_atomic_temporary_child() {
        use std::io::Write as _;

        let root = tempfile::tempdir().unwrap();
        let stable = StableDirectory::open(root.path()).unwrap();
        let mut temporary = stable
            .create_replaceable_child_file(OsStr::new("temporary"))
            .unwrap();
        temporary.write_all(b"authenticated").unwrap();
        temporary.sync_all().unwrap();
        let expected = file_identity(&temporary).unwrap();
        drop(temporary);
        std::fs::rename(root.path().join("temporary"), root.path().join("original")).unwrap();
        std::fs::write(root.path().join("temporary"), b"substitute").unwrap();

        assert!(
            stable
                .replace_child(OsStr::new("temporary"), expected, OsStr::new("CURRENT"))
                .is_err()
        );
        assert_eq!(
            std::fs::read(root.path().join("temporary")).unwrap(),
            b"substitute"
        );
        assert_eq!(
            std::fs::read(root.path().join("original")).unwrap(),
            b"authenticated"
        );
        assert!(!root.path().join("CURRENT").exists());
    }

    #[cfg(windows)]
    #[test]
    fn stable_directory_sync_uses_an_identity_checked_write_handle() {
        let root = tempfile::tempdir().unwrap();
        let stable = StableDirectory::open(root.path()).unwrap();
        stable.sync().unwrap();
    }
}
