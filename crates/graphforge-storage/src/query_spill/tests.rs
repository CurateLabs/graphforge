use super::*;

fn entries(root: &Path) -> Vec<String> {
    let mut names = std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[test]
fn an_instance_owns_a_locked_subdirectory_until_dropped() {
    let project = tempfile::TempDir::new().unwrap();
    let root = project.path().join(QUERY_SPILL_DIR);
    let scratch = QuerySpillDirectory::acquire(project.path()).unwrap();
    assert!(scratch.path().is_dir());
    assert_eq!(scratch.path().parent(), Some(root.as_path()));
    std::fs::write(scratch.path().join("spill-0"), b"rows").unwrap();
    assert_eq!(entries(&root).len(), 2, "{:?}", entries(&root));
    drop(scratch);
    assert!(entries(&root).is_empty(), "{:?}", entries(&root));
}

#[test]
fn a_live_instance_is_never_reclaimed() {
    let project = tempfile::TempDir::new().unwrap();
    let first = QuerySpillDirectory::acquire(project.path()).unwrap();
    std::fs::write(first.path().join("spill-0"), b"rows").unwrap();
    // A second instance, even in the same process, holds a separate lock and
    // must leave the first one's files in place.
    let second = QuerySpillDirectory::acquire(project.path()).unwrap();
    assert_ne!(first.path(), second.path());
    assert!(first.path().join("spill-0").is_file());
    drop(second);
    assert!(first.path().join("spill-0").is_file());
}

#[cfg(unix)]
#[test]
fn abandoned_scratch_is_reclaimed_and_other_entries_are_left_alone() {
    let project = tempfile::TempDir::new().unwrap();
    let root = project.path().join(QUERY_SPILL_DIR);
    std::fs::create_dir(&root).unwrap();
    // An owner that crashed: its lock file exists but nobody holds it.
    let crashed = "0123456789abcdef0123456789abcdef";
    std::fs::write(root.join(format!("{crashed}.lock")), b"").unwrap();
    std::fs::create_dir(root.join(crashed)).unwrap();
    std::fs::write(root.join(crashed).join("spill-0"), b"rows").unwrap();
    // A subdirectory whose lock file is already gone.
    let orphan = "fedcba9876543210fedcba9876543210";
    std::fs::create_dir(root.join(orphan)).unwrap();
    // Names outside the grammar, and a link shaped like a token.
    std::fs::write(root.join("README"), b"not ours").unwrap();
    std::fs::create_dir(root.join("keep")).unwrap();
    let outside = tempfile::TempDir::new().unwrap();
    std::fs::write(outside.path().join("precious"), b"data").unwrap();
    let link = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    std::os::unix::fs::symlink(outside.path(), root.join(link)).unwrap();

    let scratch = QuerySpillDirectory::acquire(project.path()).unwrap();
    let own = scratch
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let mut expected = vec![
        "README".to_owned(),
        link.to_owned(),
        own.clone(),
        format!("{own}.lock"),
        "keep".to_owned(),
    ];
    expected.sort();
    assert_eq!(entries(&root), expected);
    assert!(
        outside.path().join("precious").is_file(),
        "a link was followed"
    );
}

#[cfg(unix)]
#[test]
fn a_scratch_root_that_is_not_a_directory_is_refused() {
    let project = tempfile::TempDir::new().unwrap();
    std::fs::write(project.path().join(QUERY_SPILL_DIR), b"a file").unwrap();
    let error = QuerySpillDirectory::acquire(project.path()).unwrap_err();
    assert!(error.to_string().contains("not a directory"), "{error}");

    let linked = tempfile::TempDir::new().unwrap();
    let target = tempfile::TempDir::new().unwrap();
    std::os::unix::fs::symlink(target.path(), linked.path().join(QUERY_SPILL_DIR)).unwrap();
    let error = QuerySpillDirectory::acquire(linked.path()).unwrap_err();
    assert!(error.to_string().contains("not a directory"), "{error}");
}
