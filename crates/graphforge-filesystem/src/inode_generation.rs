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

/// The architectures whose ioctl request numbers use the generic layout
/// (direction in the top two bits), the only one this reads with.
macro_rules! generic_ioctl_linux {
    ($($item:item)*) => {
        $(
            #[cfg(all(
                target_os = "linux",
                any(
                    target_arch = "x86",
                    target_arch = "x86_64",
                    target_arch = "arm",
                    target_arch = "aarch64",
                    target_arch = "riscv32",
                    target_arch = "riscv64",
                    target_arch = "loongarch64",
                    target_arch = "s390x"
                )
            ))]
            $item
        )*
    };
}

generic_ioctl_linux! {
    /// `_IOR('v', 1, long)`: the direction, size, type and number fields of a
    /// Linux ioctl request.
    const FS_IOC_GETVERSION: u64 = (2 << 30)
        | ((std::mem::size_of::<libc::c_long>() as u64) << 16)
        | ((b'v' as u64) << 8)
        | 1;

    fn platform(file: &File) -> io::Result<u32> {
        use std::os::fd::AsRawFd as _;

        // The kernel stores an `int` at the address; the buffer is larger and
        // aligned, and read back as the `u32` it was written as.
        let mut generation = [0_u32; 2];
        // SAFETY: `ioctl` with `FS_IOC_GETVERSION` writes at most one `int` to
        // the pointer, which addresses eight writable, aligned bytes that
        // outlive the call; the descriptor is open while `file` is borrowed.
        let status = unsafe {
            libc::ioctl(
                file.as_raw_fd(),
                FS_IOC_GETVERSION as _,
                generation.as_mut_ptr(),
            )
        };
        if status < 0 {
            let error = io::Error::last_os_error();
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("the filesystem reports no inode generation (FS_IOC_GETVERSION): {error}"),
            ));
        }
        Ok(generation[0])
    }
}

#[cfg(not(all(
    target_os = "linux",
    any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "s390x"
    )
)))]
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
