use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::windows::fs::OpenOptionsExt as _;
use std::os::windows::io::AsRawHandle as _;
use std::path::{Path, PathBuf};

use windows_sys::Win32::Foundation::{
    ERROR_INVALID_FUNCTION, ERROR_INVALID_PARAMETER, ERROR_NOT_SUPPORTED, GENERIC_WRITE, LocalFree,
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
    ACL, ACL_SIZE_INFORMATION, AclSizeInformation, DACL_SECURITY_INFORMATION, GetAclInformation,
    GetSecurityDescriptorControl, GetSecurityDescriptorDacl, PROTECTED_DACL_SECURITY_INFORMATION,
    PSECURITY_DESCRIPTOR, SE_DACL_PROTECTED, SECURITY_ATTRIBUTES,
};
use windows_sys::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, CreateDirectoryW, DELETE, FILE_ATTRIBUTE_READONLY, FILE_BASIC_INFO,
    FILE_DISPOSITION_FLAG_DELETE, FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE,
    FILE_DISPOSITION_INFO_EX, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    FILE_FLAG_WRITE_THROUGH, FILE_ID_INFO, FILE_NAME_NORMALIZED, FILE_READ_ATTRIBUTES,
    FILE_RENAME_INFO, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_STANDARD_INFO,
    FILE_WRITE_ATTRIBUTES, FileBasicInfo, FileDispositionInfoEx, FileIdInfo, FileRenameInfo,
    FileRenameInfoEx, FileStandardInfo, GetDriveTypeW, GetFileInformationByHandle,
    GetFileInformationByHandleEx, GetFinalPathNameByHandleW, GetVolumeInformationW,
    GetVolumePathNameW, SetFileInformationByHandle, VOLUME_NAME_DOS,
};

#[cfg(test)]
use super::classify_failed_replacement;
use super::{
    FileIdentity, FileSpaceUsage, ReplaceFileError, WindowsVolumeInformation, is_link_or_reparse,
    verify_regular_metadata, verify_space_usage_metadata,
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
    let got_dacl =
        unsafe { GetSecurityDescriptorDacl(descriptor, &mut present, &mut dacl, &mut defaulted) };
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
    if unsafe { GetSecurityDescriptorControl(actual, &raw mut control, &raw mut revision) } == 0 {
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
    expected_target: Option<FileIdentity>,
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
    if expected_target.is_some() {
        verify_space_usage_metadata(&target.metadata().map_err(ReplaceFileError::NotReplaced)?)
            .map_err(ReplaceFileError::NotReplaced)?;
    } else {
        verify_open_regular(&target).map_err(ReplaceFileError::NotReplaced)?;
    }
    let target_before = file_identity(&target).map_err(ReplaceFileError::NotReplaced)?;
    if expected_target.is_some_and(|expected| expected != target_before)
        || identity(&target_path).map_err(ReplaceFileError::NotReplaced)? != target_before
    {
        return Err(ReplaceFileError::NotReplaced(io::Error::other(
            "rename target identity changed during open",
        )));
    }

    let result = rename_handle(
        &source,
        target_path.as_os_str(),
        true,
        expected_target.is_some(),
    );
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
    if opened_source_after != Some(source_before) || opened_target_after != Some(target_before) {
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
    let result = rename_handle(&source, target_path.as_os_str(), false, false);
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
    authenticated_target: bool,
) -> io::Result<()> {
    let mut rename = RenameInformation::new(target_path, replace_if_exists, authenticated_target)?;
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
    fn new(
        target_name: &OsStr,
        replace_if_exists: bool,
        authenticated_target: bool,
    ) -> io::Result<Self> {
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
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "target name too long"))?;
        let required_bytes = std::mem::size_of::<FILE_RENAME_INFO>()
            .checked_add(usize::try_from(name_bytes).unwrap_or(usize::MAX))
            .and_then(|length| length.checked_add(std::mem::size_of::<u16>()))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "target name too long"))?;
        let word_bytes = std::mem::size_of::<usize>();
        let mut words = vec![0usize; required_bytes.div_ceil(word_bytes)];
        let allocated_bytes = words
            .len()
            .checked_mul(word_bytes)
            .and_then(|length| u32::try_from(length).ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "target name too long"))?;
        let information = words.as_mut_ptr().cast::<FILE_RENAME_INFO>();
        // SAFETY: `words` is zero-initialized, pointer-aligned, and at
        // least sizeof(FILE_RENAME_INFO) plus the UTF-16 name and its NUL.
        // FileNameLength excludes the retained zero terminator.
        unsafe {
            if replace_if_exists {
                (*information).Anonymous.Flags = FILE_RENAME_REPLACE_IF_EXISTS_FLAG | FILE_RENAME_POSIX_SEMANTICS_FLAG
                        // FILE_RENAME_IGNORE_READONLY_ATTRIBUTE replaces the name
                        // without changing attributes on the shared prior inode.
                        | if authenticated_target { 0x40 } else { 0 };
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

pub(super) fn rename_no_replace(source: &Path, destination: &Path) -> io::Result<()> {
    let source = wide(source.as_os_str())?;
    let destination = wide(destination.as_os_str())?;
    // SAFETY: both UTF-16 buffers are NUL-terminated and live for the call.
    // Zero flags deliberately omit REPLACE_EXISTING and COPY_ALLOWED:
    // an existing destination or cross-volume move must fail unchanged.
    let succeeded = unsafe {
        windows_sys::Win32::Storage::FileSystem::MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            0,
        )
    };
    if succeeded == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
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
            u32::try_from(std::mem::size_of::<FILE_ID_INFO>()).expect("FILE_ID_INFO size fits u32"),
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
    let succeeded = unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut information) };
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
        let unsupported =
            readonly_safe_delete_error(io::Error::from_raw_os_error(ERROR_NOT_SUPPORTED as i32));
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

        rename_handle(&source, target_path.as_os_str(), true, false).unwrap();

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
        let mut rename = RenameInformation::new(target_path, false, false).unwrap();
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

        let mut replacement = RenameInformation::new(target_path, true, false).unwrap();
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

        let error = install_new_file(&caller, OsStr::new("source"), OsStr::new("published"), None)
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
