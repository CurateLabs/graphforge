//! The generation number of an inode.
//!
//! A filesystem reuses the inode number of a deleted file, and its creation time
//! comes from a coarse clock that repeats for a whole timer tick, so neither
//! tells a file from a deleted file whose number it inherited. The generation
//! number does: ext4, XFS and btrfs assign a new one every time an inode is
//! reused. It is read with `FS_IOC_GETVERSION`.

use std::fs::File;
use std::io;

/// The generation number of the inode behind `file`.
///
/// Together with the volume and inode number it names one file for its whole
/// life. A filesystem that cannot report one, and a platform that cannot ask,
/// is an error: nothing here falls back to a weaker name.
///
/// # Errors
/// `Unsupported` where the filesystem or platform has no generation number,
/// otherwise the error of the call.
pub fn inode_generation(file: &File) -> io::Result<u32> {
    platform(file)
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn platform(file: &File) -> io::Result<u32> {
    use rustix::ioctl::{Getter, ioctl, opcode};

    /// `FS_IOC_GETVERSION`, `_IOR('v', 1, long)`. The request number says
    /// `long`; the kernel stores an `int` (the generation) at the address, so
    /// the output is read as the `u32` it is.
    type GetVersion = Getter<{ opcode::read::<core::ffi::c_long>(b'v', 1) }, u32>;

    // SAFETY: the opcode is `FS_IOC_GETVERSION`, whose kernel implementations
    // (ext4, XFS, btrfs and others) write exactly one `int` to the pointer; a
    // filesystem that does not implement it answers `ENOTTY` and writes
    // nothing, and the output is then never read.
    let generation = unsafe { ioctl(file, GetVersion::new()) };
    generation.map_err(|error| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            format!("the filesystem reports no inode generation (FS_IOC_GETVERSION): {error}"),
        )
    })
}

#[cfg(not(target_os = "linux"))]
fn platform(_file: &File) -> io::Result<u32> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "inode generation numbers are only read on Linux",
    ))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    /// The number is stable for a file and changes when its inode is reused.
    /// Reuse is forced: files are created and removed until a number repeats.
    #[test]
    fn a_reused_inode_number_has_a_new_generation() {
        let directory = tempfile::tempdir().unwrap();
        let mut seen = std::collections::HashMap::new();
        for round in 0..20_000 {
            let path = directory.path().join(format!("file-{round}"));
            let file = File::create(&path).unwrap();
            let identity = crate::file_identity(&file).unwrap();
            let generation = inode_generation(&file).unwrap();
            assert_eq!(generation, inode_generation(&file).unwrap());
            std::fs::remove_file(&path).unwrap();
            drop(file);
            let key = (identity.volume_serial, identity.file_id);
            if let Some(previous) = seen.insert(key, generation) {
                assert_ne!(
                    previous, generation,
                    "an inode number was reused with the same generation"
                );
                return;
            }
        }
        panic!("no inode number was reused in 20,000 files");
    }

    /// A filesystem without generation numbers (procfs answers `ENOTTY`) is an
    /// error, never a default.
    #[test]
    fn a_filesystem_without_generation_numbers_is_an_error() {
        let file = File::open("/proc/self/status").unwrap();
        let error = inode_generation(&file).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported, "{error}");
    }
}
