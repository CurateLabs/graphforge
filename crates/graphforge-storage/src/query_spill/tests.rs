use super::*;
use std::cell::{Cell, RefCell};

thread_local! {
    /// What another process does, in a test, between an owner creating its
    /// temporary lock file and locking it.
    static INTERLEAVE: Cell<Option<Interleave>> = const { Cell::new(None) };
    /// Locks the simulated other process holds.
    static HELD: RefCell<Vec<File>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone, Copy)]
enum Interleave {
    /// Reclaim the unheld temporary, as an opener's reclamation would.
    Reclaim,
    /// Lock the temporary first and keep holding it.
    Hold,
}

pub(super) fn between_create_and_lock(root: &Path, temporary: &Path) {
    match INTERLEAVE.with(Cell::take) {
        Some(Interleave::Reclaim) => reclaim_abandoned(root),
        Some(Interleave::Hold) => {
            let other = File::open(temporary).unwrap();
            assert!(try_lock_exclusive(&other).unwrap());
            HELD.with(|held| held.borrow_mut().push(other));
        }
        None => {}
    }
}

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
    // A lock file that crashed before it was published.
    let unpublished = "abcdefabcdefabcdefabcdefabcdefab";
    std::fs::write(root.join(format!("{unpublished}.lock.new")), b"").unwrap();
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

/// Reclaiming other owners' leftovers is housekeeping: an abandoned entry that
/// cannot be removed is skipped, and this instance still gets its scratch.
#[cfg(unix)]
#[test]
fn an_unremovable_abandoned_entry_does_not_block_acquisition() {
    use std::os::unix::fs::PermissionsExt;
    let project = tempfile::TempDir::new().unwrap();
    let root = project.path().join(QUERY_SPILL_DIR);
    std::fs::create_dir(&root).unwrap();
    let stuck = "0123456789abcdef0123456789abcdef";
    std::fs::write(root.join(format!("{stuck}.lock")), b"").unwrap();
    std::fs::create_dir(root.join(stuck)).unwrap();
    std::fs::create_dir(root.join(stuck).join("inner")).unwrap();
    std::fs::write(root.join(stuck).join("inner").join("spill-0"), b"rows").unwrap();
    // Its contents cannot be unlinked.
    std::fs::set_permissions(
        root.join(stuck).join("inner"),
        std::fs::Permissions::from_mode(0o500),
    )
    .unwrap();
    let scratch = QuerySpillDirectory::acquire(project.path());
    std::fs::set_permissions(
        root.join(stuck).join("inner"),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    let scratch = scratch.expect("a stuck leftover must not block acquisition");
    assert!(scratch.path().is_dir());
}

/// A lock file is only ever visible under its published name once its owner
/// holds it, so a reclaimer never sees an unheld live lock.
#[test]
fn a_published_lock_is_already_held() {
    let project = tempfile::TempDir::new().unwrap();
    let scratch = QuerySpillDirectory::acquire(project.path()).unwrap();
    let root = project.path().join(QUERY_SPILL_DIR);
    let token = scratch
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let lock = File::open(root.join(format!("{token}.lock"))).unwrap();
    assert!(
        !try_lock_exclusive(&lock).unwrap(),
        "the owner does not hold its lock"
    );
    assert!(!root.join(format!("{token}.lock.new")).exists());
}

/// Concurrent acquirers of one project: every acquisition succeeds and no
/// instance's scratch disappears while it is held. A smoke test only: the race
/// windows are too narrow to hit reliably here, so the two interleaving tests
/// above are what prove a raced claim is retried.
#[test]
fn concurrent_acquisitions_never_fail_or_reclaim_a_live_instance() {
    let project = tempfile::TempDir::new().unwrap();
    std::thread::scope(|scope| {
        for _ in 0..16 {
            scope.spawn(|| {
                for _ in 0..50 {
                    let scratch = QuerySpillDirectory::acquire(project.path())
                        .expect("a concurrent acquisition failed");
                    std::fs::write(scratch.path().join("spill-0"), b"rows")
                        .expect("a live instance's scratch was reclaimed");
                    assert!(scratch.path().join("spill-0").is_file());
                }
            });
        }
    });
    assert!(entries(&project.path().join(QUERY_SPILL_DIR)).is_empty());
}

/// Another process reclaims an owner's temporary lock file between its
/// creation and its lock: the owner must notice and claim a fresh one, and
/// end up with a published lock it holds.
#[test]
fn a_claim_reclaimed_before_its_lock_is_retried() {
    let project = tempfile::TempDir::new().unwrap();
    INTERLEAVE.with(|step| step.set(Some(Interleave::Reclaim)));
    let scratch = QuerySpillDirectory::acquire(project.path()).expect("the claim is retried");
    let root = project.path().join(QUERY_SPILL_DIR);
    let token = scratch
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let lock = File::open(root.join(format!("{token}.lock"))).unwrap();
    assert!(
        !try_lock_exclusive(&lock).unwrap(),
        "the published lock is not held"
    );
    let entries = entries(&root);
    assert_eq!(
        entries,
        vec![token.clone(), format!("{token}.lock")],
        "{entries:?}"
    );
}

/// Another process locks an owner's temporary lock file first: the owner
/// must claim a fresh one rather than fail the query.
#[test]
fn a_claim_whose_lock_is_taken_is_retried() {
    let project = tempfile::TempDir::new().unwrap();
    INTERLEAVE.with(|step| step.set(Some(Interleave::Hold)));
    let scratch = QuerySpillDirectory::acquire(project.path()).expect("the claim is retried");
    let root = project.path().join(QUERY_SPILL_DIR);
    let token = scratch
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let lock = File::open(root.join(format!("{token}.lock"))).unwrap();
    assert!(
        !try_lock_exclusive(&lock).unwrap(),
        "the published lock is not held"
    );
    HELD.with(|held| held.borrow_mut().clear());
}
