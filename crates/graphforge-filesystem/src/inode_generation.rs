//! The generation number of an inode.
//!
//! A filesystem reuses the inode number of a deleted file, and its creation time
//! comes from a coarse clock that repeats for a whole timer tick, so neither
//! tells a file from a deleted file whose number it inherited. The generation
//! number does: ext4, XFS and btrfs assign a new one every time an inode is
//! reused. It is read with `FS_IOC_GETVERSION`, from the admitted filesystem
//! classes only.

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

/// The class of a Linux local filesystem whose durability protocol is admitted
/// (the `f_type` of `statfs`): ext2/3/4, XFS and btrfs. Their inode generation
/// numbers are the ones this crate relies on.
#[must_use]
pub fn admitted_filesystem_class(f_type: u64) -> Option<&'static str> {
    match f_type {
        0xEF53 => Some("ext"),
        0x5846_5342 => Some("xfs"),
        0x9123_683E => Some("btrfs"),
        _ => None,
    }
}

#[cfg(target_os = "linux")]
fn platform(file: &File) -> io::Result<u32> {
    let unsupported = |reason: String| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            format!("no inode generation for this file: {reason}"),
        )
    };
    // Only an admitted class is asked. Another filesystem (a FUSE mount, say)
    // may answer the request in ways this does not model.
    let stat = rustix::fs::fstatfs(file)
        .map_err(|error| unsupported(format!("the filesystem cannot be classified: {error}")))?;
    let class = u64::try_from(stat.f_type).unwrap_or_default();
    if admitted_filesystem_class(class).is_none() {
        return Err(unsupported(format!(
            "filesystem type {class:#x} is not an admitted class (ext, xfs, btrfs)"
        )));
    }
    get_version(file).map_err(|error| unsupported(format!("FS_IOC_GETVERSION is refused: {error}")))
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn get_version(file: &File) -> rustix::io::Result<u32> {
    use rustix::ioctl::{Ioctl, IoctlOutput, Opcode, ioctl, opcode};

    /// `FS_IOC_GETVERSION`, `_IOR('v', 1, long)`.
    ///
    /// The request number says the kernel may write a `long`, and a filesystem
    /// that sizes its write from the request (FUSE does) writes that many bytes,
    /// so the buffer is a whole zeroed `long`. ext4, XFS and btrfs `put_user` one
    /// `int`, the generation, at the start of it; that is the `u32` read back,
    /// in the machine's byte order, from the first four bytes.
    struct GetVersion {
        buffer: core::ffi::c_long,
    }

    // SAFETY: `opcode` is `FS_IOC_GETVERSION`; `as_ptr` addresses `buffer`, an
    // initialized, aligned `long` that is the size the request names, so the
    // kernel writes within it whatever the filesystem; `output_from_ptr` reads
    // four initialized bytes from the start of it.
    unsafe impl Ioctl for GetVersion {
        type Output = u32;

        const IS_MUTATING: bool = true;

        fn opcode(&self) -> Opcode {
            opcode::read::<core::ffi::c_long>(b'v', 1)
        }

        fn as_ptr(&mut self) -> *mut core::ffi::c_void {
            std::ptr::addr_of_mut!(self.buffer).cast()
        }

        unsafe fn output_from_ptr(
            _: IoctlOutput,
            pointer: *mut core::ffi::c_void,
        ) -> rustix::io::Result<u32> {
            // SAFETY: the caller passes the pointer `as_ptr` returned, to the
            // buffer above, which is at least four bytes and aligned for `u32`.
            Ok(unsafe { pointer.cast::<u32>().read() })
        }
    }

    // SAFETY: see the implementation above; the descriptor is open while
    // `file` is borrowed.
    unsafe { ioctl(file, GetVersion { buffer: 0 }) }
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

    /// The property that makes the number a name: across many create and delete
    /// cycles, no (inode number, generation) pair is shared by two distinct file
    /// lifetimes. That holds whether or not the filesystem reuses inode numbers
    /// (btrfs never does within a mount). Where it did reuse one (ext4 does,
    /// at once), the generation must also have changed; the run says whether the
    /// discriminating case happened.
    #[test]
    fn no_inode_number_and_generation_name_two_file_lifetimes() {
        const CYCLES: usize = 20_000;
        const ENOUGH_REUSE: usize = 100;
        let directory = tempfile::tempdir().unwrap();
        let mut generations: std::collections::HashMap<(u64, [u8; 16]), Vec<u32>> =
            std::collections::HashMap::new();
        let mut reused = 0_usize;
        for cycle in 0..CYCLES {
            let path = directory.path().join(format!("file-{cycle}"));
            let file = File::create(&path).unwrap();
            let identity = crate::file_identity(&file).unwrap();
            let generation = inode_generation(&file).unwrap();
            // Stable for the lifetime.
            assert_eq!(generation, inode_generation(&file).unwrap());
            std::fs::remove_file(&path).unwrap();
            drop(file);
            let earlier = generations
                .entry((identity.volume_serial, identity.file_id))
                .or_default();
            if !earlier.is_empty() {
                reused += 1;
            }
            assert!(
                !earlier.contains(&generation),
                "inode {:?} was named by generation {generation} in two file lifetimes",
                identity.file_id
            );
            earlier.push(generation);
            if reused >= ENOUGH_REUSE {
                break;
            }
        }
        println!("inode numbers reused {reused} times; every reuse had a new generation");
    }

    /// A filesystem that is not an admitted class (procfs here) is refused
    /// before the request is made: an error, never a default.
    #[test]
    fn a_filesystem_that_is_not_admitted_is_an_error() {
        let file = File::open("/proc/self/status").unwrap();
        let error = inode_generation(&file).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported, "{error}");
        assert!(
            error.to_string().contains("not an admitted class"),
            "{error}"
        );
    }

    /// The directory a test runs in is an admitted class, so its inodes have
    /// generations.
    #[test]
    fn an_admitted_filesystem_reports_a_generation() {
        let directory = tempfile::tempdir().unwrap();
        let file = File::create(directory.path().join("file")).unwrap();
        inode_generation(&file).unwrap();
        let directory = File::open(directory.path()).unwrap();
        inode_generation(&directory).unwrap();
    }
}
