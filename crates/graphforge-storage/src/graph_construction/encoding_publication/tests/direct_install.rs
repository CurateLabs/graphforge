//! Each published object is written once and takes its content address by a
//! link, not a copy (#1899): link, dedupe and crash behaviour of that install.

use super::*;
use std::collections::BTreeMap;
use std::os::unix::fs::MetadataExt;

const ARTIFACT: &str = "topology/runtime_catalog.parquet";
const OPERATION: u128 = 9_470;
const TARGET: u128 = 9_471;
const TRANSACTION: u128 = 9_472;

struct Staged {
    root: TempDir,
    session: GraphConstructionSession,
    encoded: GraphConstructionEncoding,
    graph: StableDirectory,
}

fn staged(operation: u128) -> Staged {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, operation);
    session
        .append(ConstructionChunkKind::Node, "nodes", &node_batch(1, 4))
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoded = session.encode_canonical(&shape, 1).unwrap();
    let graph = session
        .root
        .open_child_directory(OsStr::new(&encoded.root))
        .unwrap()
        .open_child_directory(OsStr::new("graph"))
        .unwrap();
    Staged {
        root,
        session,
        encoded,
        graph,
    }
}

impl Staged {
    fn inventory(&self) -> CapturedEncodedInventory<'_> {
        CapturedEncodedInventory {
            root: &self.graph,
            artifacts: &self.encoded.artifacts,
            active_identities: &self
                .session
                .checkpoint
                .evidence
                .storage_active_identity_allocated_bytes,
        }
    }

    fn staged_path(&self) -> PathBuf {
        self.graph.path().join(ARTIFACT)
    }

    fn object_path(&self) -> PathBuf {
        let artifact = self
            .encoded
            .artifacts
            .iter()
            .find(|artifact| artifact.path == ARTIFACT)
            .unwrap();
        crate::graph_object_path(self.root.path(), &artifact.sha256).unwrap()
    }

    fn current(&self) -> Vec<u8> {
        std::fs::read(self.root.path().join("CURRENT")).unwrap()
    }
}

fn install(
    lease: &crate::GraphObjectPublicationLease,
    source: &CapturedEncodedArtifact<'_>,
) -> Result<crate::GraphObjectInstallEvidence, GfError> {
    crate::graph_object_store::install_captured_encoded_artifact_with_lease(
        lease,
        source,
        &mut || false,
    )
}

/// Every object-store entry by address, each proven to hash to its own name.
fn object_store(root: &Path) -> BTreeMap<String, u64> {
    let mut objects = BTreeMap::new();
    let sha256 = root.join("graph-objects/sha256");
    let Ok(buckets) = std::fs::read_dir(&sha256) else {
        return objects;
    };
    for bucket in buckets {
        let bucket = bucket.unwrap().path();
        for object in std::fs::read_dir(&bucket).unwrap() {
            let object = object.unwrap().path();
            let name = format!(
                "{}{}",
                bucket.file_name().unwrap().to_str().unwrap(),
                object.file_name().unwrap().to_str().unwrap()
            );
            let bytes = std::fs::read(&object).unwrap();
            assert_eq!(
                sha256_hex(&bytes),
                name,
                "an object-store entry does not hash to its address"
            );
            objects.insert(name, bytes.len() as u64);
        }
    }
    objects
}

fn sha256_hex(bytes: &[u8]) -> String {
    super::super::super::sha256(bytes)
}

#[test]
fn installed_object_is_the_staged_inode_and_nothing_is_written() {
    let staged = staged(15_001);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let before = std::fs::read(staged.staged_path()).unwrap();
    let staged_metadata = std::fs::metadata(staged.staged_path()).unwrap();
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    let capture = graphforge_core::hash_observation::operation::Capture::start();
    let evidence = install(&lease, &source).unwrap();
    let observed = capture.snapshot();

    // Nothing was copied, read back, or hashed again.
    assert!(!evidence.reused_existing);
    assert!(evidence.attempted_install);
    assert_eq!(evidence.write_bytes, 0);
    assert_eq!(evidence.write_calls, 0);
    assert_eq!(evidence.read_calls, 0);
    assert_eq!(evidence.bytes_hashed, 0);
    assert_eq!(evidence.checksum_read_bytes, 0);
    assert_eq!(observed.artifact_payload_sha256_bytes, 0);
    assert_eq!(observed.checksum_bytes, 0);
    assert_eq!(evidence.bytes_installed, source.bytes());
    assert_eq!(evidence.content_xxh64, Some(source.checksum()));
    // One payload barrier and one namespace barrier for the address (ADR 0013).
    assert_eq!(evidence.file_fsync_calls, 1);
    assert_eq!(evidence.directory_fsync_calls, 1);

    // The object is the encoder's inode, on the encoder's filesystem, sealed.
    let object = std::fs::metadata(staged.object_path()).unwrap();
    assert_eq!(object.ino(), staged_metadata.ino());
    assert_eq!(object.dev(), staged_metadata.dev());
    assert_eq!(object.nlink(), 2);
    assert!(object.permissions().readonly());
    assert_eq!(std::fs::read(staged.staged_path()).unwrap(), before);
    assert_eq!(
        crate::read_graph_object(staged.root.path(), source.content_sha256(), source.bytes())
            .unwrap(),
        before
    );
}

#[test]
fn staged_files_and_the_object_store_share_one_filesystem() {
    let staged = staged(15_002);
    let objects = std::fs::metadata(staged.root.path().join("graph-objects")).unwrap();
    let encoded = std::fs::metadata(staged.graph.path()).unwrap();
    assert_eq!(objects.dev(), encoded.dev());
}

#[test]
fn publication_writes_only_the_control_objects_it_authors() {
    let mut staged = staged(15_003);
    assert!(object_store(staged.root.path()).is_empty());
    let published: BTreeMap<_, _> = staged
        .encoded
        .artifacts
        .iter()
        .map(|artifact| (artifact.sha256.clone(), artifact.bytes))
        .collect();
    staged
        .session
        .publish_canonical(
            &staged.encoded,
            Uuid::from_u128(TARGET),
            Uuid::from_u128(TRANSACTION),
        )
        .unwrap();
    let store = object_store(staged.root.path());
    let encoder_bytes: u64 = published.values().sum();
    let store_bytes: u64 = store.values().sum();
    for (digest, bytes) in &published {
        assert_eq!(store.get(digest), Some(bytes), "{digest}");
    }
    // Conservation: the only bytes the object store writes are the ones the
    // encoder did not already write (manifest nodes), not a second copy.
    assert!(store_bytes > encoder_bytes);
    assert_eq!(
        staged.session.checkpoint.evidence.cas_application_write_bytes,
        store_bytes - encoder_bytes
    );
    eprintln!(
        "cas read bytes {} write bytes {} encoder bytes {encoder_bytes}",
        staged.session.checkpoint.evidence.cas_application_read_bytes,
        staged.session.checkpoint.evidence.cas_application_write_bytes,
    );
}

/// Dedupe: an identical object is already installed, so the staged inode is
/// not linked and the existing object is the one that stays.
#[test]
fn identical_existing_object_is_reused_and_the_staged_file_is_not_aliased() {
    let staged = staged(15_004);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let bytes = std::fs::read(staged.staged_path()).unwrap();
    let (digest, _) = crate::install_graph_object_bytes(staged.root.path(), &bytes).unwrap();
    assert_eq!(digest, source.content_sha256());
    let existing = std::fs::metadata(staged.object_path()).unwrap();
    let staged_inode = std::fs::metadata(staged.staged_path()).unwrap().ino();
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    let evidence = install(&lease, &source).unwrap();
    assert!(evidence.reused_existing);
    assert_eq!(evidence.bytes_installed, 0);
    assert_eq!(evidence.write_bytes, 0);
    // The existing object is checked in full against the address: the lease
    // holds no capture for an object it did not install.
    assert_eq!(evidence.bytes_hashed, source.bytes());
    let after = std::fs::metadata(staged.object_path()).unwrap();
    assert_eq!(after.ino(), existing.ino());
    assert_ne!(after.ino(), staged_inode);
    assert_eq!(after.nlink(), 1);
    assert_eq!(std::fs::metadata(staged.staged_path()).unwrap().nlink(), 1);
    assert_eq!(std::fs::read(staged.staged_path()).unwrap(), bytes);
}

/// Dedupe never trusts the address: a same-length corrupt object and a
/// wrong-length object are both refused, left in place, and the staged file
/// is not aliased to either.
#[test]
fn corrupt_existing_object_is_refused_and_the_staged_file_is_left_alone() {
    for same_length in [true, false] {
        let staged = staged(15_005);
        let inventory = staged.inventory();
        let source = inventory.open(Path::new(ARTIFACT)).unwrap();
        let bytes = std::fs::read(staged.staged_path()).unwrap();
        crate::install_graph_object_bytes(staged.root.path(), &bytes).unwrap();
        let mut corrupt = bytes.clone();
        if same_length {
            corrupt[0] ^= 0xff;
        } else {
            corrupt.pop();
        }
        crate::graph_object_store::corrupt_sealed_graph_object_for_test(
            &staged.object_path(),
            &corrupt,
        );
        let current = staged.current();
        let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
        assert!(install(&lease, &source).is_err(), "same_length={same_length}");
        assert_eq!(std::fs::read(staged.object_path()).unwrap(), corrupt);
        assert_eq!(std::fs::read(staged.staged_path()).unwrap(), bytes);
        assert_eq!(std::fs::metadata(staged.staged_path()).unwrap().nlink(), 1);
        assert_eq!(staged.current(), current);
    }
}

/// A concurrent publisher installs the same object between this install's
/// existence check and its link: the winner is authenticated and kept.
#[test]
fn lost_link_race_authenticates_the_winner_and_leaves_the_staged_file_unaliased() {
    let staged = staged(15_006);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let bytes = std::fs::read(staged.staged_path()).unwrap();
    let root = staged.root.path().to_path_buf();
    let winner_bytes = bytes.clone();
    crate::graph_object_store::set_before_object_link_hook(Some(Box::new(move || {
        crate::install_graph_object_bytes(&root, &winner_bytes).unwrap();
    })));
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    let evidence = install(&lease, &source);
    crate::graph_object_store::set_before_object_link_hook(None);
    let evidence = evidence.unwrap();
    assert!(evidence.reused_existing);
    assert_eq!(evidence.bytes_installed, 0);
    let object = std::fs::metadata(staged.object_path()).unwrap();
    assert_ne!(object.ino(), std::fs::metadata(staged.staged_path()).unwrap().ino());
    assert_eq!(object.nlink(), 1);
    assert_eq!(std::fs::read(staged.object_path()).unwrap(), bytes);
}

/// An error returned at any install boundary leaves the staged file intact and
/// `CURRENT` unchanged, and the same install then succeeds with the same bytes.
#[test]
fn returned_error_at_each_install_boundary_leaves_the_staged_file_and_retries() {
    for (boundary, linked) in [
        ("install:temp-sealed", false),
        ("install:final-linked", true),
        ("install:bucket-synced", true),
    ] {
        let staged = staged(15_007);
        let inventory = staged.inventory();
        let source = inventory.open(Path::new(ARTIFACT)).unwrap();
        let bytes = std::fs::read(staged.staged_path()).unwrap();
        let inode = std::fs::metadata(staged.staged_path()).unwrap().ino();
        let current = staged.current();
        let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
        crate::graph_object_store::inject_returned_error_at(Some(boundary));
        let failed = install(&lease, &source);
        crate::graph_object_store::inject_returned_error_at(None);
        let error = failed.unwrap_err();
        assert!(error.to_string().contains(boundary), "{boundary}: {error}");
        assert_eq!(std::fs::read(staged.staged_path()).unwrap(), bytes);
        assert_eq!(std::fs::metadata(staged.staged_path()).unwrap().ino(), inode);
        assert_eq!(staged.current(), current);
        assert_eq!(
            staged.object_path().exists(),
            linked,
            "{boundary}: visibility of the address"
        );
        assert_eq!(
            std::fs::metadata(staged.staged_path()).unwrap().nlink(),
            if linked { 2 } else { 1 },
            "{boundary}"
        );
        // Every visible entry already hashes to its address.
        object_store(staged.root.path());

        install(&lease, &source).unwrap();
        assert_eq!(std::fs::read(staged.object_path()).unwrap(), bytes);
        assert_eq!(
            std::fs::metadata(staged.object_path()).unwrap().ino(),
            inode
        );
        assert_eq!(staged.current(), current);
    }
}

/// A source that is not the admitted regular file is refused before it can
/// take a content address.
#[test]
fn source_that_changed_length_is_refused_before_linking() {
    let staged = staged(15_008);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let current = staged.current();
    let mut permissions = std::fs::metadata(staged.staged_path()).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o600);
    std::fs::set_permissions(staged.staged_path(), permissions).unwrap();
    let file = std::fs::OpenOptions::new()
        .append(true)
        .open(staged.staged_path())
        .unwrap();
    std::io::Write::write_all(&mut &file, b"x").unwrap();
    drop(file);
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    assert!(install(&lease, &source).is_err());
    assert!(!staged.object_path().exists());
    assert_eq!(staged.current(), current);
}

/// The install no longer reads the file back, so a same-length edit of the
/// staged file between the encoder's write and publication is not caught at
/// install. It is caught wherever the address is authenticated: the object
/// never reads as the content its address names.
#[test]
fn edited_staged_file_never_authenticates_as_its_address() {
    let staged = staged(15_010);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let path = staged.staged_path();
    let mut edited = std::fs::read(&path).unwrap();
    edited[0] ^= 0xff;
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o600);
    std::fs::set_permissions(&path, permissions).unwrap();
    std::fs::write(&path, &edited).unwrap();
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    let _ = install(&lease, &source);
    if staged.object_path().exists() {
        assert!(
            crate::read_graph_object(staged.root.path(), source.content_sha256(), source.bytes())
                .is_err()
        );
    }
}

/// A second name on the staged inode that is not the content address is
/// refused: it is not something this install created.
#[test]
fn foreign_alias_of_the_staged_file_is_refused() {
    let staged = staged(15_009);
    let alias = staged.root.path().join("foreign-alias");
    std::fs::hard_link(staged.staged_path(), &alias).unwrap();
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    let error = install(&lease, &source).unwrap_err();
    assert!(error.to_string().contains("alias"), "{error}");
    assert!(!staged.object_path().exists());
    assert_eq!(std::fs::metadata(&alias).unwrap().nlink(), 2);
}

// ---- crash tests: the process exits at the named point, a new one resumes ----

fn crash_publication_child(root: &Path, failpoint: Option<&str>) -> Option<i32> {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg("graph_construction::tests::crash_subprocess_helper")
        .arg("--nocapture")
        .env("GF_CONSTRUCTION_CRASH_ROOT", root)
        .env("GF_CONSTRUCTION_PUBLICATION_CRASH", "1");
    if let Some(failpoint) = failpoint {
        command
            .env(
                "GF_CONSTRUCTION_FAILPOINT_COOKIE",
                "graphforge-construction-test-v1",
            )
            .env("GF_CONSTRUCTION_FAILPOINT", failpoint);
    }
    command.status().unwrap().code()
}

/// A second process that resumes the interrupted publication, optionally
/// crashing again.
#[test]
fn resume_publication_child() {
    let Ok(root) = std::env::var("GF_DIRECT_INSTALL_RESUME_ROOT") else {
        return;
    };
    resume_publication(Path::new(&root)).unwrap();
}

fn resume_publication(root: &Path) -> Result<crate::ProjectPublicationReceipt, GfError> {
    let operation = Uuid::from_u128(OPERATION);
    let encoding: GraphConstructionEncoding = serde_json::from_slice(
        &std::fs::read(
            root.join(PRIVATE_ROOT)
                .join(operation.simple().to_string())
                .join("encoded-v1/inventory.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let mut session =
        GraphConstructionSession::open(root, operation, 0, GraphConstructionBudgets::default())?;
    session.publish_canonical(
        &encoding,
        Uuid::from_u128(TARGET),
        Uuid::from_u128(TRANSACTION),
    )
}

fn resume_child(root: &Path, failpoint: &str) -> Option<i32> {
    std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("graph_construction::encoding_publication::tests::direct_install::resume_publication_child")
        .arg("--nocapture")
        .env("GF_DIRECT_INSTALL_RESUME_ROOT", root)
        .env(
            "GF_CONSTRUCTION_FAILPOINT_COOKIE",
            "graphforge-construction-test-v1",
        )
        .env("GF_CONSTRUCTION_FAILPOINT", failpoint)
        .status()
        .unwrap()
        .code()
}

fn private_encoded_graph(root: &Path) -> PathBuf {
    root.join(PRIVATE_ROOT)
        .join(Uuid::from_u128(OPERATION).simple().to_string())
        .join("encoded-v1/graph")
}

fn artifact_address(root: &Path) -> PathBuf {
    let encoding: GraphConstructionEncoding = serde_json::from_slice(
        &std::fs::read(
            private_encoded_graph(root)
                .parent()
                .unwrap()
                .join("inventory.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let artifact = encoding
        .artifacts
        .iter()
        .find(|artifact| artifact.path == ARTIFACT)
        .unwrap();
    crate::graph_object_path(root, &artifact.sha256).unwrap()
}

fn assert_parent_current_and_no_staging_residue(root: &Path) {
    assert_ne!(
        crate::resolve_project_generation(root)
            .unwrap()
            .generation_uuid(),
        Uuid::from_u128(TARGET),
        "the interrupted publication must not have reached CURRENT"
    );
    let temporaries = root.join("graph-objects/tmp");
    assert!(
        std::fs::read_dir(&temporaries)
            .map(|entries| entries.count() == 0)
            .unwrap_or(true),
        "the link install must leave nothing in the object store's staging directory"
    );
    object_store(root);
}

#[test]
fn publication_crashed_at_each_install_boundary_resumes_to_identical_bytes() {
    let reference = TempDir::new().unwrap();
    assert_eq!(crash_publication_child(reference.path(), None), Some(0));
    let expected = object_store(reference.path());
    assert!(!expected.is_empty());

    for (point, visible) in [
        ("after_object_sync", false),
        ("after_link", true),
        ("after_bucket_sync", true),
    ] {
        let root = TempDir::new().unwrap();
        let failpoint = format!("cas.install.{point}.{ARTIFACT}");
        assert_eq!(
            crash_publication_child(root.path(), Some(&failpoint)),
            Some(86),
            "{failpoint}"
        );
        assert_parent_current_and_no_staging_residue(root.path());
        assert_eq!(artifact_address(root.path()).exists(), visible, "{point}");
        assert!(
            private_encoded_graph(root.path()).join(ARTIFACT).is_file(),
            "{point}: the staged file survives for the rerun"
        );

        let receipt = resume_publication(root.path()).unwrap();
        assert_eq!(receipt.generation_uuid, Uuid::from_u128(TARGET));
        assert_eq!(
            crate::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            Uuid::from_u128(TARGET)
        );
        assert_eq!(object_store(root.path()), expected, "{point}");
    }
}

/// The resumed install finds its object already in place: that is the dedupe
/// path. A second crash there still leaves a clean state and an identical
/// publication on the third attempt.
#[test]
fn crash_in_the_dedupe_of_an_already_installed_object_resumes_to_identical_bytes() {
    let reference = TempDir::new().unwrap();
    assert_eq!(crash_publication_child(reference.path(), None), Some(0));
    let expected = object_store(reference.path());

    let root = TempDir::new().unwrap();
    assert_eq!(
        crash_publication_child(
            root.path(),
            Some(&format!("cas.install.after_bucket_sync.{ARTIFACT}"))
        ),
        Some(86)
    );
    assert_eq!(
        resume_child(
            root.path(),
            &format!("cas.install.after_dedupe.{ARTIFACT}")
        ),
        Some(86),
        "the resumed install must reach the dedupe of the installed object"
    );
    assert_parent_current_and_no_staging_residue(root.path());
    assert!(artifact_address(root.path()).exists());

    let receipt = resume_publication(root.path()).unwrap();
    assert_eq!(receipt.generation_uuid, Uuid::from_u128(TARGET));
    assert_eq!(object_store(root.path()), expected);
}
