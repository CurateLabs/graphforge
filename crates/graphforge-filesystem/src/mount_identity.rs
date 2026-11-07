//! File-to-directory mount identity, including split-device overlayfs.
use std::{fs::File, io};

/// Compare retained mounts, rather than overlayfs file and directory devices.
/// Older Linux kernels without mount IDs retain the strict device comparison.
///
/// # Errors
/// Returns native metadata or statx errors; only ENOSYS permits the legacy check.
pub fn same_mount(left: &File, right: &File) -> io::Result<bool> {
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{AtFlags, StatxFlags, statx};
        let mount = |file: &File| match statx(file, "", AtFlags::EMPTY_PATH, StatxFlags::MNT_ID) {
            Ok(stat)
                if StatxFlags::from_bits_retain(stat.stx_mask).contains(StatxFlags::MNT_ID) =>
            {
                Ok(Some(stat.stx_mnt_id))
            }
            Ok(_) | Err(rustix::io::Errno::NOSYS) => Ok(None),
            Err(error) => Err(io::Error::from(error)),
        };
        if let (Some(left), Some(right)) = (mount(left)?, mount(right)?) {
            return Ok(left == right);
        }
    }
    Ok(crate::file_identity(left)?.volume_serial == crate::file_identity(right)?.volume_serial)
}

/// Safe Linux filesystem name for qualification evidence, never a mount path.
#[must_use]
pub fn filesystem_type_name(kind: u64) -> String {
    if let Some(class) = crate::admitted_filesystem_class(kind) {
        return class.into();
    }
    match kind {
        0x794c_7630 => "overlayfs".into(),
        0x0102_1994 => "tmpfs".into(),
        0x6969 => "nfs".into(),
        0x6573_5546 => "fuse".into(),
        0xff53_4d42 => "cifs".into(),
        0xfe53_4d42 => "smb2".into(),
        0x0102_1997 => "9p".into(),
        0x2fc1_2fc1 => "zfs".into(),
        0x4d44 => "vfat".into(),
        0x2011_bab0 => "exfat".into(),
        _ => format!("{kind:#x}"),
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    #[test]
    fn compares_mounts_for_files_and_directories() {
        let dir = tempfile::tempdir().unwrap();
        let file = File::create(dir.path().join("file")).unwrap();
        let parent = File::open(dir.path()).unwrap();
        assert!(same_mount(&file, &parent).unwrap());
        assert!(!same_mount(&File::open("/proc/self/status").unwrap(), &parent).unwrap());
    }
}
