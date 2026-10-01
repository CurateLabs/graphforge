use crate::durable_commit::{AtomicHooks, Visibility, fault, publish_atomic_in};
use graphforge_filesystem::StableDirectory;
use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;

fn fresh_read(path: &std::path::Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    File::open(path).unwrap().read_to_end(&mut bytes).unwrap();
    bytes
}
fn refusal(point: fault::Point) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("CURRENT");
    std::fs::write(&path, b"old").unwrap();
    let allocation =
        crate::StorageAllocationOperation::from_paths(&[root.path().to_owned()]).unwrap();
    let baseline = allocation.snapshot().unwrap();
    fault::arm(point);
    let failure = publish_atomic_in(
        StableDirectory::open(root.path()).unwrap(),
        OsStr::new("CURRENT"),
        b"new",
        AtomicHooks {
            after_write: || Ok(()),
            after_seal: || Ok(()),
            before_visible: || Ok(()),
        },
        Some(&allocation),
    )
    .unwrap_err();
    assert_eq!(failure.visibility, Visibility::NotPublished);
    assert!(failure.pending.is_none());
    assert_eq!(fresh_read(&path), b"old");
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    let observed = allocation.snapshot().unwrap();
    assert_eq!(
        observed.current_allocated_bytes(),
        baseline.current_allocated_bytes()
    );
    assert!(observed.peak_allocated_bytes() > baseline.peak_allocated_bytes());
}
#[test]
fn partial_writer_failure_preserves_old_authority_and_owned_cleanup() {
    refusal(fault::Point::PartialWrite);
}
#[test]
fn file_fence_failure_preserves_old_authority_and_owned_cleanup() {
    refusal(fault::Point::FileFence);
}
#[test]
fn final_predicate_failure_preserves_old_authority_and_owned_cleanup() {
    refusal(fault::Point::BeforeVisible);
}
#[test]
fn uncertain_native_visibility_retains_the_same_acknowledgment_capability() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("CURRENT");
    std::fs::write(&path, b"old").unwrap();
    fault::arm(fault::Point::NativeUnknown);
    let failure = publish_atomic_in(
        StableDirectory::open(root.path()).unwrap(),
        OsStr::new("CURRENT"),
        b"new",
        AtomicHooks {
            after_write: || Ok(()),
            after_seal: || Ok(()),
            before_visible: || Ok(()),
        },
        None,
    )
    .unwrap_err();
    assert_eq!(failure.visibility, Visibility::StateUnknown);
    assert_eq!(fresh_read(&path), b"new");
    (*failure.pending.unwrap()).acknowledge(None).unwrap();
    assert_eq!(fresh_read(&path), b"new");
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}
#[test]
fn failed_parent_fence_never_rolls_back_visible_authority() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("CURRENT");
    std::fs::write(&path, b"old").unwrap();
    let pending = publish_atomic_in(
        StableDirectory::open(root.path()).unwrap(),
        OsStr::new("CURRENT"),
        b"new",
        AtomicHooks {
            after_write: || Ok(()),
            after_seal: || Ok(()),
            before_visible: || Ok(()),
        },
        None,
    )
    .unwrap();
    fault::arm(fault::Point::ParentFence);
    let failure = pending.acknowledge(None).unwrap_err();
    assert_eq!(failure.visibility, Visibility::VisibleUnacknowledged);
    assert_eq!(fresh_read(&path), b"new");
    (*failure.pending.unwrap()).acknowledge(None).unwrap();
    assert_eq!(fresh_read(&path), b"new");
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}
