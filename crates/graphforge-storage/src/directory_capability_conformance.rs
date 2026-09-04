// Shared adversarial cases for the filesystem and lifecycle capabilities.
//
// Before consolidation these cases exercise the two independent implementations.
// Lifecycle retains ancestor authority; a standalone StableDirectory validates
// only its own named directory, so that distinction is asserted explicitly.

use std::path::Path;

use graphforge_filesystem::StableDirectory;

use super::LifecycleDirectory;

trait DirectoryContract: Sized {
    fn open(path: &Path) -> Result<Self, String>;
    fn child(&self, name: &str) -> Result<Self, String>;
    fn revalidate(&self) -> Result<(), String>;
}

impl DirectoryContract for StableDirectory {
    fn open(path: &Path) -> Result<Self, String> {
        Self::open(path).map_err(|error| error.to_string())
    }

    fn child(&self, name: &str) -> Result<Self, String> {
        self.open_child_directory(name.as_ref())
            .map_err(|error| error.to_string())
    }

    fn revalidate(&self) -> Result<(), String> {
        self.revalidate_named().map_err(|error| error.to_string())
    }
}

impl DirectoryContract for LifecycleDirectory {
    fn open(path: &Path) -> Result<Self, String> {
        Self::open(path, "IDENTITY", "conformance_open").map_err(|error| error.to_string())
    }

    fn child(&self, name: &str) -> Result<Self, String> {
        Self::open_child(
            self,
            name.as_ref(),
            &self.path.join(name),
            "IDENTITY",
            "conformance_child",
        )
        .map_err(|error| error.to_string())
    }

    fn revalidate(&self) -> Result<(), String> {
        self.revalidate("IDENTITY", "conformance_revalidation")
            .map_err(|error| error.to_string())
    }
}

fn real_directory_and_regular_file<D: DirectoryContract>() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("directory");
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(root.path().join("regular"), b"unchanged").unwrap();
    let parent = D::open(root.path()).unwrap();
    let child = parent.child("directory").unwrap();
    child.revalidate().unwrap();
    assert!(parent.child("regular").is_err());
    assert!(D::open(&root.path().join("regular")).is_err());
    assert_eq!(
        std::fs::read(root.path().join("regular")).unwrap(),
        b"unchanged"
    );
}

#[test]
fn both_capabilities_accept_real_directories_and_reject_files() {
    real_directory_and_regular_file::<StableDirectory>();
    real_directory_and_regular_file::<LifecycleDirectory>();
}

fn named_directory_replacement<D: DirectoryContract>() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("directory");
    let displaced = root.path().join("displaced");
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(directory.join("authority"), b"original").unwrap();
    let parent = D::open(root.path()).unwrap();
    let child = parent.child("directory").unwrap();
    #[cfg(unix)]
    {
        std::fs::rename(&directory, &displaced).unwrap();
        std::fs::create_dir(&directory).unwrap();
        assert!(child.revalidate().is_err());
        assert_eq!(
            std::fs::read(displaced.join("authority")).unwrap(),
            b"original"
        );
        assert!(!directory.join("authority").exists());
    }
    #[cfg(windows)]
    {
        // No FILE_SHARE_DELETE: Windows prevents the replacement rather than
        // allowing a rename and discovering it on the next revalidation.
        let failure = std::fs::rename(&directory, &displaced).unwrap_err();
        // Rust does not map ERROR_SHARING_VIOLATION to PermissionDenied;
        // pin the native refusal caused by the retained no-delete-share handle.
        const ERROR_SHARING_VIOLATION: i32 = 32;
        assert_eq!(failure.raw_os_error(), Some(ERROR_SHARING_VIOLATION), "{failure:?}");
        child.revalidate().unwrap();
        assert_eq!(
            std::fs::read(directory.join("authority")).unwrap(),
            b"original"
        );
        assert!(!displaced.exists());
    }
}

#[test]
fn both_capabilities_fail_closed_on_named_directory_replacement() {
    named_directory_replacement::<StableDirectory>();
    named_directory_replacement::<LifecycleDirectory>();
}

#[cfg(unix)]
fn link_directory(source: &Path, target: &Path) {
    std::os::unix::fs::symlink(source, target).unwrap();
}

#[cfg(windows)]
fn link_directory(source: &Path, target: &Path) {
    // Junctions exercise reparse rejection without requiring symlink privilege.
    let result = std::process::Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(target)
        .arg(source)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "junction creation failed: {result:?}"
    );
}

fn linked_directory<D: DirectoryContract>() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("authority"), b"outside").unwrap();
    let linked = root.path().join("linked");
    link_directory(outside.path(), &linked);
    let parent = D::open(root.path()).unwrap();
    assert!(D::open(&linked).is_err());
    assert!(parent.child("linked").is_err());
    assert_eq!(
        std::fs::read(outside.path().join("authority")).unwrap(),
        b"outside"
    );
}

#[test]
fn both_capabilities_reject_link_or_reparse_directory_opens() {
    linked_directory::<StableDirectory>();
    linked_directory::<LifecycleDirectory>();
}

#[cfg(unix)]
fn ancestor_replacement<D: DirectoryContract>(retains_ancestors: bool) {
    let root = tempfile::tempdir().unwrap();
    let parent_path = root.path().join("parent");
    let displaced = root.path().join("displaced");
    std::fs::create_dir(&parent_path).unwrap();
    std::fs::create_dir(parent_path.join("child")).unwrap();
    let parent = D::open(&parent_path).unwrap();
    let child = parent.child("child").unwrap();
    std::fs::rename(&parent_path, &displaced).unwrap();
    link_directory(&displaced, &parent_path);
    assert!(parent.revalidate().is_err());
    assert_eq!(child.revalidate().is_err(), retains_ancestors);
}

#[cfg(unix)]
#[test]
fn same_ancestor_attack_exposes_the_lifecycle_policy_boundary() {
    ancestor_replacement::<StableDirectory>(false);
    ancestor_replacement::<LifecycleDirectory>(true);
}
