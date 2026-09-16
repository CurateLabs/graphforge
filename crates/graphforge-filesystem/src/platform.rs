//! Platform-specific retained-directory, identity, and atomic-file operations.

use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::path::Path;

#[cfg(any(unix, windows))]
use super::file_identity;
use super::{FileIdentity, FileSpaceUsage, ReplaceFileError, StableDirectory};
#[cfg(windows)]
use super::{path_identity, windows};
#[cfg(unix)]
use super::{verify_regular_metadata, verify_space_usage_metadata};

#[cfg(unix)]
pub(super) fn stable_open_directory(path: &Path) -> io::Result<File> {
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
pub(super) fn visit_regular_files_platform(
    directory: &StableDirectory,
    remaining: &mut usize,
    visit: &mut impl FnMut(&File) -> io::Result<()>,
) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;

    use rustix::fs::{AtFlags, Dir, FileType};

    let mut entries = Dir::read_from(&directory.file).map_err(io::Error::from)?;
    while let Some(entry) = entries.read() {
        let entry = entry.map_err(io::Error::from)?;
        let raw_name = entry.file_name().to_bytes();
        if matches!(raw_name, b"." | b"..") {
            continue;
        }
        if *remaining == 0 {
            return Err(io::Error::other(
                "descriptor-relative traversal exceeds entry bound",
            ));
        }
        *remaining -= 1;
        let name = OsStr::from_bytes(raw_name);
        let mut file_type = entry.file_type();
        if file_type == FileType::Unknown {
            let stat = rustix::fs::statat(&directory.file, name, AtFlags::SYMLINK_NOFOLLOW)
                .map_err(io::Error::from)?;
            file_type = FileType::from_raw_mode(stat.st_mode);
        }
        if file_type.is_file() {
            let file = directory.open_child_file(name)?;
            visit(&file)?;
        } else if file_type.is_dir() {
            let child = directory.open_child_directory(name)?;
            visit_regular_files_platform(&child, remaining, visit)?;
        }
    }
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn visit_regular_files_platform(
    _directory: &StableDirectory,
    _remaining: &mut usize,
    _visit: &mut impl FnMut(&File) -> io::Result<()>,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "descriptor-relative directory traversal is unsupported",
    ))
}

#[cfg(unix)]
pub(super) fn stable_open_child_directory(
    parent: &File,
    _path: &Path,
    name: &OsStr,
) -> io::Result<File> {
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
pub(super) fn stable_create_child_directory(
    parent: &File,
    _path: &Path,
    name: &OsStr,
) -> io::Result<()> {
    use rustix::fs::Mode;
    match rustix::fs::mkdirat(parent, name, Mode::from_bits_truncate(0o700)) {
        Ok(()) | Err(rustix::io::Errno::EXIST) => Ok(()),
        Err(error) => Err(io::Error::from(error)),
    }
}

#[cfg(unix)]
pub(super) fn stable_open_child_file(
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
pub(super) fn stable_open_replaceable_child_file(
    parent: &File,
    path: &Path,
    name: &OsStr,
) -> io::Result<File> {
    stable_open_child_file(parent, path, name, true)
}

#[cfg(unix)]
pub(super) fn stable_open_or_create_child_file(
    parent: &File,
    path: &Path,
    name: &OsStr,
) -> io::Result<File> {
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
pub(super) fn stable_child_names(
    parent: &File,
    path: &Path,
) -> io::Result<Vec<std::ffi::OsString>> {
    stable_child_names_bounded(parent, path, usize::MAX)
}

#[cfg(unix)]
pub(super) fn stable_child_names_bounded(
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
pub(super) fn stable_link_child(
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
pub(super) fn stable_unlink_child_if_identity(
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
pub(super) fn stable_remove_child_directory_if_identity(
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
pub(super) fn stable_open_directory(path: &Path) -> io::Result<File> {
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
pub(super) fn stable_open_directory_for_sync(path: &Path) -> io::Result<File> {
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
pub(super) fn stable_open_child_directory(
    _parent: &File,
    path: &Path,
    _name: &OsStr,
) -> io::Result<File> {
    stable_open_directory(path)
}

#[cfg(windows)]
pub(super) fn stable_create_child_directory(
    _parent: &File,
    path: &Path,
    _name: &OsStr,
) -> io::Result<()> {
    match create_private_directory(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(windows)]
pub(super) fn stable_open_child_file(
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
pub(super) fn stable_open_replaceable_child_file(
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
pub(super) fn stable_open_or_create_child_file(
    parent: &File,
    path: &Path,
    name: &OsStr,
) -> io::Result<File> {
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
pub(super) fn stable_child_names(
    parent: &File,
    path: &Path,
) -> io::Result<Vec<std::ffi::OsString>> {
    stable_child_names_bounded(parent, path, usize::MAX)
}

#[cfg(windows)]
pub(super) fn stable_child_names_bounded(
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
pub(super) fn stable_link_child(
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
pub(super) fn stable_unlink_child_if_identity(
    _parent: &File,
    path: &Path,
    name: &OsStr,
    expected: FileIdentity,
) -> io::Result<()> {
    let child = path.join(name);
    windows::delete_file_by_handle(&child, expected)
}

#[cfg(windows)]
pub(super) fn stable_remove_child_directory_if_identity(
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
pub(super) fn stable_open_directory(_path: &Path) -> io::Result<File> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stable directories unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn stable_open_child_directory(
    _parent: &File,
    _path: &Path,
    _name: &OsStr,
) -> io::Result<File> {
    stable_open_directory(Path::new(""))
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn stable_open_or_create_child_file(
    _parent: &File,
    _path: &Path,
    _name: &OsStr,
) -> io::Result<File> {
    stable_open_directory(Path::new(""))
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn stable_open_replaceable_child_file(
    _parent: &File,
    _path: &Path,
    _name: &OsStr,
) -> io::Result<File> {
    stable_open_directory(Path::new(""))
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn stable_child_names(
    _parent: &File,
    _path: &Path,
) -> io::Result<Vec<std::ffi::OsString>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stable directories unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn stable_child_names_bounded(
    parent: &File,
    path: &Path,
    _limit: usize,
) -> io::Result<Vec<std::ffi::OsString>> {
    stable_child_names(parent, path)
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn stable_link_child(
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
pub(super) fn stable_unlink_child_if_identity(
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
pub(super) fn stable_remove_child_directory_if_identity(
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
pub(super) fn stable_create_child_directory(
    _parent: &File,
    _path: &Path,
    _name: &OsStr,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stable directories unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn stable_open_child_file(
    _parent: &File,
    _path: &Path,
    _name: &OsStr,
    _create_new: bool,
) -> io::Result<File> {
    stable_open_directory(Path::new(""))
}

#[cfg(any(target_vendor = "apple", target_os = "linux", target_os = "redox"))]
pub(super) fn rename_no_replace_platform(source: &Path, destination: &Path) -> io::Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        source,
        rustix::fs::CWD,
        destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )?;
    Ok(())
}

#[cfg(windows)]
pub(super) fn rename_no_replace_platform(source: &Path, destination: &Path) -> io::Result<()> {
    windows::rename_no_replace(source, destination)
}

#[cfg(not(any(
    target_vendor = "apple",
    target_os = "linux",
    target_os = "redox",
    windows
)))]
pub(super) fn rename_no_replace_platform(_source: &Path, _destination: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic no-replace rename is unsupported",
    ))
}

/// Whether metadata denotes a symlink or native reparse point.
#[cfg(windows)]
#[must_use]
pub fn is_link_or_reparse(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_type().is_symlink()
        || (metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0
}

/// Whether metadata denotes a symlink or native reparse point.
#[cfg(not(windows))]
#[must_use]
pub fn is_link_or_reparse(metadata: &std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(unix)]
pub(super) fn link_count(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt as _;
    metadata.nlink()
}

#[cfg(windows)]
pub(super) fn link_count(metadata: &std::fs::Metadata) -> u64 {
    let _ = metadata;
    1
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn link_count(_metadata: &std::fs::Metadata) -> u64 {
    0
}

#[cfg(unix)]
pub(super) fn replace_file_platform(
    directory: &File,
    source_name: &OsStr,
    target_name: &OsStr,
    expected_source: Option<FileIdentity>,
    expected_target: Option<FileIdentity>,
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
    let target_metadata = target.metadata().map_err(ReplaceFileError::NotReplaced)?;
    if let Some(expected) = expected_target {
        verify_space_usage_metadata(&target_metadata).map_err(ReplaceFileError::NotReplaced)?;
        if unix_identity(&target).map_err(ReplaceFileError::NotReplaced)? != expected {
            return Err(ReplaceFileError::NotReplaced(io::Error::other(
                "rename target differs from the authenticated expected identity",
            )));
        }
    } else {
        verify_regular_metadata(&target_metadata).map_err(ReplaceFileError::NotReplaced)?;
    }
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
pub(super) fn file_identity_platform(file: &File) -> io::Result<FileIdentity> {
    unix_identity(file)
}

#[cfg(unix)]
pub(super) fn file_space_usage_platform(file: &File) -> io::Result<FileSpaceUsage> {
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
pub(super) fn path_identity_platform(path: &Path) -> io::Result<FileIdentity> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = std::fs::symlink_metadata(path)?;
    Ok(FileIdentity {
        volume_serial: metadata.dev(),
        file_id: u128::from(metadata.ino()).to_le_bytes(),
    })
}

#[cfg(unix)]
pub(super) fn file_link_count_platform(file: &File) -> io::Result<u64> {
    use std::os::unix::fs::MetadataExt as _;
    Ok(file.metadata()?.nlink())
}

#[cfg(unix)]
pub(super) fn path_link_count_platform(path: &Path) -> io::Result<u64> {
    use std::os::unix::fs::MetadataExt as _;
    Ok(std::fs::symlink_metadata(path)?.nlink())
}

#[cfg(unix)]
pub(super) fn create_private_directory_platform(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt as _;

    let mut builder = std::fs::DirBuilder::new();
    builder.mode(0o700).create(path)
}

#[cfg(windows)]
pub(super) fn file_identity_platform(file: &File) -> io::Result<FileIdentity> {
    windows::file_identity(file)
}

#[cfg(windows)]
pub(super) fn file_space_usage_platform(file: &File) -> io::Result<FileSpaceUsage> {
    windows::file_space_usage(file)
}

#[cfg(windows)]
pub(super) fn path_identity_platform(path: &Path) -> io::Result<FileIdentity> {
    windows::identity(path)
}

#[cfg(windows)]
pub(super) fn file_link_count_platform(file: &File) -> io::Result<u64> {
    windows::link_count(file)
}

#[cfg(windows)]
pub(super) fn path_link_count_platform(path: &Path) -> io::Result<u64> {
    windows::path_link_count(path)
}

#[cfg(unix)]
pub(super) fn install_new_file_platform(
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
pub(super) fn replace_file_platform(
    directory: &File,
    source_name: &OsStr,
    target_name: &OsStr,
    expected_source: Option<FileIdentity>,
    expected_target: Option<FileIdentity>,
) -> Result<(), ReplaceFileError> {
    windows::replace_file(
        directory,
        source_name,
        target_name,
        expected_source,
        expected_target,
    )
}

#[cfg(windows)]
pub(super) fn install_new_file_platform(
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
pub(super) fn create_private_directory_platform(path: &Path) -> io::Result<()> {
    windows::create_private_directory(path)
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn replace_file_platform(
    _directory: &File,
    _source_name: &OsStr,
    _target_name: &OsStr,
    _expected_source: Option<FileIdentity>,
    _expected_target: Option<FileIdentity>,
) -> Result<(), ReplaceFileError> {
    Err(ReplaceFileError::NotReplaced(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic replacement is unsupported on this platform",
    )))
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn install_new_file_platform(
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
pub(super) fn create_private_directory_platform(_path: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "private directory creation is unsupported on this platform",
    ))
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn file_identity_platform(_file: &File) -> io::Result<FileIdentity> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "identity unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn file_space_usage_platform(_file: &File) -> io::Result<FileSpaceUsage> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "allocated-byte measurement is unsupported on this platform",
    ))
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn path_identity_platform(_path: &Path) -> io::Result<FileIdentity> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "identity unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn file_link_count_platform(_file: &File) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "link count unsupported",
    ))
}

#[cfg(all(not(unix), not(windows)))]
pub(super) fn path_link_count_platform(_path: &Path) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "link count unsupported",
    ))
}
