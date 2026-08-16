//! Audited native filesystem primitives used by GraphForge's durability
//! protocol.

#![deny(unsafe_code)]

use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::path::Path;

/// Stable filesystem identity suitable for NTFS/ReFS and Unix filesystems.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileIdentity {
    /// Native volume/device identity.
    pub volume_serial: u64,
    /// Full native file identity (128-bit on ReFS; zero-extended inode on Unix).
    pub file_id: [u8; 16],
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

/// Failure classification for an attempted atomic replacement.
#[derive(Debug)]
pub enum ReplaceFileError {
    /// The operating system rejected the operation and both named identities
    /// were verified unchanged. The disposable replacement file may have had
    /// streams or attributes changed by the operating system and must not be
    /// reused after any failed call.
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
/// Both files must already be closed and flushed. The caller remains
/// responsible for flushing the containing directory after this returns.
pub fn replace_file(
    directory: &File,
    source_name: &OsStr,
    target_name: &OsStr,
) -> Result<(), ReplaceFileError> {
    verify_single_component(source_name).map_err(ReplaceFileError::NotReplaced)?;
    verify_single_component(target_name).map_err(ReplaceFileError::NotReplaced)?;
    replace_file_platform(directory, source_name, target_name)
}

/// Atomically install a new regular file without replacing an existing entry.
pub fn install_new_file(
    directory: &File,
    source_name: &OsStr,
    target_name: &OsStr,
) -> io::Result<()> {
    verify_single_component(source_name)?;
    verify_single_component(target_name)?;
    install_new_file_platform(directory, source_name, target_name)
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
) -> Result<(), ReplaceFileError> {
    use rustix::fs::{AtFlags, Mode, OFlags, openat, renameat, statat};

    let source = openat(
        directory,
        source_name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)
    .map_err(ReplaceFileError::NotReplaced)?;
    let target = openat(
        directory,
        target_name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
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
    renameat(directory, source_name, directory, target_name)
        .map_err(io::Error::from)
        .map_err(ReplaceFileError::NotReplaced)?;
    let replaced = openat(
        directory,
        target_name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)
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
) -> io::Result<()> {
    use rustix::fs::{Mode, OFlags, RenameFlags, openat, renameat_with};

    let source = openat(
        directory,
        source_name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)?;
    verify_regular_metadata(&source.metadata()?)?;
    let source_identity = unix_identity(&source)?;
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
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(File::from)
    .map_err(io::Error::from)?;
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
) -> Result<(), ReplaceFileError> {
    windows::replace_file(directory, source_name, target_name)
}

#[cfg(windows)]
fn install_new_file_platform(
    directory: &File,
    source_name: &OsStr,
    target_name: &OsStr,
) -> io::Result<()> {
    // Windows rename does not replace an existing destination. The explicit
    // precheck supplies a stable AlreadyExists class; the OS operation remains
    // the race-free authority.
    windows::install_new_file(directory, source_name, target_name)
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

    use windows_sys::Win32::Foundation::LocalFree;
    #[cfg(test)]
    use windows_sys::Win32::Security::Authorization::{
        ConvertSecurityDescriptorToStringSecurityDescriptorW, GetNamedSecurityInfoW, SE_FILE_OBJECT,
    };
    use windows_sys::Win32::Security::Authorization::{
        ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
    };
    #[cfg(test)]
    use windows_sys::Win32::Security::{DACL_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION};
    use windows_sys::Win32::Security::{PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES};
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, CreateDirectoryW, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_ID_INFO, FILE_NAME_NORMALIZED, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, FileIdInfo, GetFileInformationByHandle,
        GetFileInformationByHandleEx, GetFinalPathNameByHandleW, ReplaceFileW, VOLUME_NAME_DOS,
    };

    #[cfg(test)]
    use super::classify_failed_replacement;
    use super::{FileIdentity, ReplaceFileError, verify_regular_metadata};

    pub(super) fn replace_file(
        directory: &File,
        source_name: &OsStr,
        target_name: &OsStr,
    ) -> Result<(), ReplaceFileError> {
        let directory_path = directory_path(directory).map_err(ReplaceFileError::NotReplaced)?;
        let source_path = directory_path.join(source_name);
        let target_path = directory_path.join(target_name);
        verify_windows_regular(&source_path).map_err(ReplaceFileError::NotReplaced)?;
        verify_windows_regular(&target_path).map_err(ReplaceFileError::NotReplaced)?;
        let source_before = identity(&source_path).map_err(ReplaceFileError::NotReplaced)?;
        let target_before = identity(&target_path).map_err(ReplaceFileError::NotReplaced)?;
        let source = wide(source_path.as_os_str()).map_err(ReplaceFileError::NotReplaced)?;
        let target = wide(target_path.as_os_str()).map_err(ReplaceFileError::NotReplaced)?;
        // SAFETY: both strings are owned, NUL-terminated UTF-16 buffers for
        // the duration of the call. Optional backup/exclusion pointers are
        // null as required when unused. ReplaceFileW has no supported flags.
        let succeeded = unsafe {
            ReplaceFileW(
                target.as_ptr(),
                source.as_ptr(),
                std::ptr::null(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if succeeded != 0 {
            return if identity(&target_path).ok() == Some(source_before) && !source_path.exists() {
                Ok(())
            } else {
                Err(ReplaceFileError::StateUnknown(io::Error::other(
                    "replacement success state did not reconcile",
                )))
            };
        }
        let error = io::Error::last_os_error();
        let source_after = identity(&source_path);
        let target_after = identity(&target_path);
        Err(super::classify_failed_replacement(
            error,
            source_before,
            target_before,
            source_after.ok(),
            target_after.ok(),
        ))
    }

    pub(super) fn install_new_file(
        directory: &File,
        source_name: &OsStr,
        target_name: &OsStr,
    ) -> io::Result<()> {
        let directory_path = directory_path(directory)?;
        let source = directory_path.join(source_name);
        let target = directory_path.join(target_name);
        verify_windows_regular(&source)?;
        match std::fs::symlink_metadata(&target) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "target exists",
                ));
            }
            Err(error) => return Err(error),
        }
        let source_identity = identity(&source)?;
        std::fs::rename(&source, &target)?;
        if identity(&target)? != source_identity || source.exists() {
            return Err(io::Error::other("atomic creation state did not reconcile"));
        }
        Ok(())
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
        if written == 0 || usize::try_from(written).unwrap_or(usize::MAX) >= buffer.len() {
            return Err(io::Error::last_os_error());
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

    fn verify_windows_regular(path: &Path) -> io::Result<()> {
        let metadata = std::fs::symlink_metadata(path)?;
        verify_regular_metadata(&metadata)?;
        let file = open_identity_handle(path)?;
        let information = information(&file)?;
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory_handle(path: &Path) -> File {
        #[cfg(unix)]
        return File::open(path).unwrap();

        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
            return std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .open(path)
                .unwrap();
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
}
