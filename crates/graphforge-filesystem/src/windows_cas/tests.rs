use super::*;
use crate::{StableDirectory, file_identity, file_link_count, path_identity, windows};
use std::ffi::OsStr;

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
