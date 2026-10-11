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
    root: std::rc::Rc<TempDir>,
    session: GraphConstructionSession,
    encoded: GraphConstructionEncoding,
    graph: StableDirectory,
}

fn staged(operation: u128) -> Staged {
    staged_in(std::rc::Rc::new(TempDir::new().unwrap()), operation)
}

fn staged_in(root: std::rc::Rc<TempDir>, operation: u128) -> Staged {
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
) -> Result<crate::graph_object_store::GraphObjectInstallEvidence, GfError> {
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

/// The producer's witness lives in its process. An installer that does not find
/// it, as after a restart, runs the object's file barrier itself.
#[test]
fn a_staged_object_without_a_producer_witness_is_sealed_when_installed() {
    let staged = staged(15_006);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    // The encoder recorded its barrier; forget it, as a new process would.
    assert!(crate::durable_commit::producer_seals::take(source.source()).unwrap());
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    let evidence = install(&lease, &source).unwrap();
    assert!(!evidence.reused_existing);
    assert_eq!(evidence.file_fsync_calls, 1);
}

/// A producer's barrier covers the file as it was sealed: a staged file that
/// changed afterwards is sealed again.
#[test]
fn a_staged_object_changed_after_its_producer_sealed_it_is_sealed_when_installed() {
    let staged = staged(15_007);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    // Same length, new modification time.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(staged.staged_path())
        .unwrap();
    file.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(5))
        .unwrap();
    drop(file);
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    let evidence = install(&lease, &source).unwrap();
    assert_eq!(evidence.file_fsync_calls, 1);
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
    // The encoder already ran this object's one payload barrier on the inode it
    // named (ADR 0058 decision 3), so installing runs none. One namespace
    // barrier for the address (ADR 0013), plus the `sha256` barrier that makes
    // the bucket this object created durable.
    assert_eq!(evidence.file_fsync_calls, 0);
    assert_eq!(evidence.bucket_creations, 1);
    assert_eq!(
        evidence.directory_fsync_calls,
        1 + evidence.bucket_creations
    );

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
    let _lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
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
        staged
            .session
            .checkpoint
            .evidence
            .cas_application_write_bytes,
        store_bytes - encoder_bytes
    );
    eprintln!(
        "cas read bytes {} write bytes {} encoder bytes {encoder_bytes}",
        staged
            .session
            .checkpoint
            .evidence
            .cas_application_read_bytes,
        staged
            .session
            .checkpoint
            .evidence
            .cas_application_write_bytes,
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
    // ADR 0013: the reused dirent gets its namespace barrier here, because a
    // crashed earlier attempt may have linked it without one.
    assert_eq!(evidence.file_fsync_calls, 0);
    assert_eq!(evidence.directory_fsync_calls, 1);
    assert_eq!(evidence.fsync_calls, 1);
    // The existing object is checked in full against the address: the lease
    // holds no capture for an object it did not install.
    assert_eq!(evidence.bytes_hashed, source.bytes());
    let capture = graphforge_core::hash_observation::operation::Capture::start();
    let captured = install(&lease, &source).unwrap();
    let observed = capture.snapshot();
    assert!(captured.reused_existing);
    assert_eq!(captured.bytes_installed, 0);
    assert_eq!(captured.bytes_hashed, 0);
    assert_eq!(captured.checksum_read_bytes, source.bytes());
    assert_eq!(
        captured.read_calls,
        source
            .bytes()
            .div_ceil(crate::GRAPH_OBJECT_IO_BUFFER_BYTES as u64)
    );
    assert_eq!(observed.artifact_payload_sha256_bytes, 0);
    assert_eq!(observed.checksum_bytes, source.bytes());
    let after = std::fs::metadata(staged.object_path()).unwrap();
    assert_eq!(after.ino(), existing.ino());
    assert_ne!(after.ino(), staged_inode);
    assert_eq!(after.nlink(), 1);
    assert_eq!(std::fs::metadata(staged.staged_path()).unwrap().nlink(), 1);
    assert_eq!(std::fs::read(staged.staged_path()).unwrap(), bytes);
}

/// Dedupe never trusts the address. An existing object that is not the bytes
/// its address names, whether the same length or not, is replaced by the
/// correct install; it is never permanent, and the replacement keeps `CURRENT`.
#[test]
fn mis_addressed_existing_object_is_replaced_by_the_correct_install() {
    for captured in [false, true] {
        for same_length in [true, false] {
            let staged = staged(15_005);
            let inventory = staged.inventory();
            let source = inventory.open(Path::new(ARTIFACT)).unwrap();
            let bytes = std::fs::read(staged.staged_path()).unwrap();
            crate::install_graph_object_bytes(staged.root.path(), &bytes).unwrap();
            let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
            if captured {
                assert!(install(&lease, &source).unwrap().reused_existing);
            }
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
            let capture = graphforge_core::hash_observation::operation::Capture::start();
            let evidence = install(&lease, &source).unwrap();
            let observed = capture.snapshot();
            assert!(
                !evidence.reused_existing,
                "captured={captured}, same_length={same_length}"
            );
            assert_eq!(evidence.bytes_installed, source.bytes());
            // An uncaptured inode needs one SHA pass. A captured inode whose
            // XXH64 refuses needs SHA before removal, so both passes count.
            // A different length is proved mismatched without reading bytes.
            let sha_bytes = if same_length { source.bytes() } else { 0 };
            let checksum_only_bytes = if same_length && captured {
                source.bytes()
            } else {
                0
            };
            assert_eq!(evidence.bytes_hashed, sha_bytes);
            assert_eq!(evidence.checksum_read_bytes, checksum_only_bytes);
            assert_eq!(
                evidence.read_calls,
                sha_bytes.div_ceil(crate::GRAPH_OBJECT_IO_BUFFER_BYTES as u64)
                    + checksum_only_bytes.div_ceil(crate::GRAPH_OBJECT_IO_BUFFER_BYTES as u64)
            );
            assert_eq!(observed.artifact_payload_sha256_bytes, sha_bytes);
            assert_eq!(observed.checksum_bytes, sha_bytes + checksum_only_bytes);
            assert_eq!(evidence.content_xxh64, Some(source.checksum()));
            // One barrier for the removal of the bad entry, one for the new one.
            // The encoder ran the payload barrier; installing runs no second.
            assert_eq!(evidence.directory_fsync_calls, 2);
            assert_eq!(evidence.fsync_calls, 2);
            assert_eq!(std::fs::read(staged.object_path()).unwrap(), bytes);
            object_store(staged.root.path());
            assert_eq!(staged.current(), current);
        }
    }
}

/// An inventory checksum disagreement cannot authorize removal of an object
/// whose complete SHA-256 proves it already occupies the correct address.
#[test]
fn inventory_checksum_mismatch_preserves_a_correctly_addressed_object() {
    let mut staged = staged(15_013);
    let bytes = std::fs::read(staged.staged_path()).unwrap();
    crate::install_graph_object_bytes(staged.root.path(), &bytes).unwrap();
    let before = std::fs::metadata(staged.object_path()).unwrap().ino();
    staged
        .encoded
        .artifacts
        .iter_mut()
        .find(|artifact| artifact.path == ARTIFACT)
        .unwrap()
        .xxh64 ^= 1;
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let current = staged.current();
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    let capture = graphforge_core::hash_observation::operation::Capture::start();
    let error = install(&lease, &source).unwrap_err();
    let observed = capture.snapshot();
    assert!(
        error
            .to_string()
            .contains("staged inventory checksum differs"),
        "{error}"
    );
    assert_eq!(observed.artifact_payload_sha256_bytes, source.bytes());
    assert_eq!(observed.checksum_bytes, source.bytes());
    assert_eq!(
        std::fs::metadata(staged.object_path()).unwrap().ino(),
        before
    );
    assert_eq!(std::fs::read(staged.object_path()).unwrap(), bytes);
    assert_eq!(std::fs::metadata(staged.staged_path()).unwrap().nlink(), 1);
    assert_eq!(staged.current(), current);
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
    assert_ne!(
        object.ino(),
        std::fs::metadata(staged.staged_path()).unwrap().ino()
    );
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
        assert_eq!(
            std::fs::metadata(staged.staged_path()).unwrap().ino(),
            inode
        );
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
    let mut permissions = std::fs::metadata(staged.staged_path())
        .unwrap()
        .permissions();
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

fn edit_in_place(path: &Path) {
    let mut edited = std::fs::read(path).unwrap();
    let middle = edited.len() / 2;
    edited[middle] ^= 0xff;
    let mut permissions = std::fs::metadata(path).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o600);
    std::fs::set_permissions(path, permissions).unwrap();
    std::fs::write(path, &edited).unwrap();
}

fn write_in_place(path: &Path, bytes: &[u8]) {
    let mut permissions = std::fs::metadata(path).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o600);
    std::fs::set_permissions(path, permissions).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn file_entry(source: &CapturedEncodedArtifact<'_>) -> crate::GraphFileEntry {
    crate::GraphFileEntry {
        content_xxh64: source.checksum(),
        relative_path: ARTIFACT.to_owned(),
        byte_length: source.bytes(),
        content_sha256: source.content_sha256().to_owned(),
        role: crate::graph_files::infer_role(Path::new(ARTIFACT)),
    }
}

/// The install no longer reads the file back, so a same-length edit of the
/// staged file between the encoder's write and publication is linked at its
/// claimed address. The first read refuses it by exact length and XXH64
/// (ADR 0049). The commit boundary refuses it too, and then retires the object
/// this lease linked: no object stays at an address its bytes do not hash to,
/// and a later correct install of the same digest succeeds.
#[test]
fn edited_staged_file_is_refused_at_admission_and_leaves_no_mis_addressed_object() {
    let staged = staged(15_010);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let good = std::fs::read(staged.staged_path()).unwrap();
    edit_in_place(&staged.staged_path());
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    install(&lease, &source).unwrap();
    assert!(staged.object_path().exists());
    let entry = file_entry(&source);
    let error =
        crate::graph_object_store::admit_graph_object(staged.root.path(), &entry).unwrap_err();
    assert!(error.to_string().contains("checksum"), "{error}");

    let error =
        crate::graph_object_store::admit_graph_object_with_lease(&lease, &entry).unwrap_err();
    assert!(error.to_string().contains("checksum"), "{error}");
    assert!(
        !staged.object_path().exists(),
        "a refused object must not stay at its claimed address"
    );
    object_store(staged.root.path());

    // The encoder's file is correct again (a rerun re-encodes it); installing
    // the same digest now succeeds and admits.
    write_in_place(&staged.staged_path(), &good);
    install(&lease, &source).unwrap();
    crate::graph_object_store::admit_graph_object_with_lease(&lease, &entry).unwrap();
    assert_eq!(std::fs::read(staged.object_path()).unwrap(), good);
    object_store(staged.root.path());
}

/// A retry with no capture finds the encoder's own inode at the address,
/// rewritten in place, because the interrupted attempt that linked it never
/// reached admission. Linking it again would reinstall the same bad bytes, so
/// the install refuses; it also retires the entry, so no mis-addressed object
/// stays at the address. Restoring the bytes lets the same install pass.
#[test]
fn uncaptured_object_that_is_the_rewritten_staged_inode_is_retired_and_refused() {
    let staged = staged(15_012);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let good = std::fs::read(staged.staged_path()).unwrap();
    let first = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    install(&first, &source).unwrap();
    // The lease's record of what it linked is memory only, so dropping it is
    // what a crash before admission leaves behind.
    drop(first);
    edit_in_place(&staged.staged_path());
    let current = staged.current();

    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    let capture = graphforge_core::hash_observation::operation::Capture::start();
    let error = install(&lease, &source).unwrap_err();
    let observed = capture.snapshot();
    assert!(
        error.to_string().contains("not the content its address"),
        "{error}"
    );
    assert_eq!(observed.artifact_payload_sha256_bytes, source.bytes());
    assert!(
        !staged.object_path().exists(),
        "a refused install must not leave the mis-addressed entry"
    );
    assert_eq!(std::fs::metadata(staged.staged_path()).unwrap().nlink(), 1);
    object_store(staged.root.path());
    assert_eq!(staged.current(), current);

    write_in_place(&staged.staged_path(), &good);
    let evidence = install(&lease, &source).unwrap();
    assert!(!evidence.reused_existing);
    assert_eq!(std::fs::read(staged.object_path()).unwrap(), good);
}

/// The same, after a real process crash between the link and admission: the
/// retry in a new process retires the entry the dead one left.
#[test]
fn retry_after_a_crash_retires_the_rewritten_staged_inode() {
    let root = TempDir::new().unwrap();
    assert_eq!(
        crash_publication_child(
            root.path(),
            Some(&format!("cas.install.after_link.{ARTIFACT}"))
        ),
        Some(86)
    );
    let address = artifact_address(root.path());
    assert!(address.exists(), "the crash left the linked object");
    let encoding: GraphConstructionEncoding = serde_json::from_slice(
        &std::fs::read(
            root.path()
                .join(PRIVATE_ROOT)
                .join(Uuid::from_u128(OPERATION).simple().to_string())
                .join("encoded-v1/inventory.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let session = GraphConstructionSession::open(
        root.path(),
        Uuid::from_u128(OPERATION),
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    let graph = session
        .root
        .open_child_directory(OsStr::new(&encoding.root))
        .unwrap()
        .open_child_directory(OsStr::new("graph"))
        .unwrap();
    let inventory = CapturedEncodedInventory {
        root: &graph,
        artifacts: &encoding.artifacts,
        active_identities: &session
            .checkpoint
            .evidence
            .storage_active_identity_allocated_bytes,
    };
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let good = std::fs::read(&address).unwrap();
    let staged_path = private_encoded_graph(root.path()).join(ARTIFACT);
    edit_in_place(&staged_path);
    assert_ne!(
        sha256_hex(&std::fs::read(&address).unwrap()),
        source.content_sha256()
    );

    let lease = crate::begin_graph_object_publication(root.path()).unwrap();
    let error = install(&lease, &source).unwrap_err();
    assert!(error.to_string().contains("not the content"), "{error}");
    assert!(!address.exists());
    object_store(root.path());

    write_in_place(&staged_path, &good);
    install(&lease, &source).unwrap();
    assert_eq!(std::fs::read(&address).unwrap(), good);
}

/// Installer B classified a corrupt object, and installer A repaired it before
/// B could take the repair lock. B must see A's correct object and keep it.
#[test]
fn repair_that_lost_the_race_keeps_the_winners_replacement() {
    let staged = staged(15_031);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let bytes = std::fs::read(staged.staged_path()).unwrap();
    crate::install_graph_object_bytes(staged.root.path(), &bytes).unwrap();
    let mut corrupt = bytes.clone();
    corrupt[0] ^= 0xff;
    crate::graph_object_store::corrupt_sealed_graph_object_for_test(
        &staged.object_path(),
        &corrupt,
    );
    let address = staged.object_path();
    let root = staged.root.path().to_path_buf();
    let winner = bytes.clone();
    let rival_inode = std::rc::Rc::new(std::cell::Cell::new(0_u64));
    let recorded = std::rc::Rc::clone(&rival_inode);
    crate::graph_object_store::set_boundary_hook(
        "repair:before-lock",
        Box::new(move || {
            std::fs::remove_file(&address).unwrap();
            crate::install_graph_object_bytes(&root, &winner).unwrap();
            recorded.set(std::fs::metadata(&address).unwrap().ino());
        }),
    );
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    let evidence = install(&lease, &source).unwrap();
    assert_ne!(rival_inode.get(), 0, "the rival repair ran");
    assert!(evidence.reused_existing);
    assert_eq!(evidence.bytes_installed, 0);
    let after = std::fs::metadata(staged.object_path()).unwrap();
    assert_eq!(after.ino(), rival_inode.get(), "the winner's object stayed");
    assert_eq!(std::fs::metadata(staged.staged_path()).unwrap().nlink(), 1);
    assert_eq!(std::fs::read(staged.object_path()).unwrap(), bytes);
    object_store(staged.root.path());
}

/// Retiring and replacing a mis-addressed entry happens under exclusive bucket
/// authority: while the repair unlinks, another repairer cannot take the lock,
/// so it cannot delete the correct object this repair is about to install.
#[test]
fn repair_holds_exclusive_bucket_authority_while_it_retires_and_replaces() {
    let staged = staged(15_032);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let bytes = std::fs::read(staged.staged_path()).unwrap();
    crate::install_graph_object_bytes(staged.root.path(), &bytes).unwrap();
    let mut corrupt = bytes.clone();
    corrupt[0] ^= 0xff;
    crate::graph_object_store::corrupt_sealed_graph_object_for_test(
        &staged.object_path(),
        &corrupt,
    );
    let bucket = staged.object_path().parent().unwrap().to_path_buf();
    let rival_got_lock = std::rc::Rc::new(std::cell::Cell::new(None));
    let recorded = std::rc::Rc::clone(&rival_got_lock);
    let rival_bucket = bucket.clone();
    crate::graph_object_store::set_boundary_hook(
        "repair:retiring",
        Box::new(move || {
            let rival = graphforge_filesystem::StableDirectory::open(&rival_bucket).unwrap();
            let acquired = rival.try_lock_exclusive().unwrap();
            if acquired {
                rival.unlock().unwrap();
            }
            recorded.set(Some(acquired));
        }),
    );
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    install(&lease, &source).unwrap();
    assert_eq!(
        rival_got_lock.get(),
        Some(false),
        "a second repairer must not enter while this one retires"
    );
    let rival = graphforge_filesystem::StableDirectory::open(&bucket).unwrap();
    assert!(
        rival.try_lock_exclusive().unwrap(),
        "the repair lock is released when the install ends"
    );
    rival.unlock().unwrap();
}

/// Retiring an object that commit-boundary admission refused takes the same
/// authority.
#[test]
fn retiring_an_unadmitted_link_holds_exclusive_bucket_authority() {
    let staged = staged(15_033);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    edit_in_place(&staged.staged_path());
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    install(&lease, &source).unwrap();
    let bucket = staged.object_path().parent().unwrap().to_path_buf();
    let rival_got_lock = std::rc::Rc::new(std::cell::Cell::new(None));
    let recorded = std::rc::Rc::clone(&rival_got_lock);
    crate::graph_object_store::set_boundary_hook(
        "repair:retiring",
        Box::new(move || {
            let rival = graphforge_filesystem::StableDirectory::open(&bucket).unwrap();
            let acquired = rival.try_lock_exclusive().unwrap();
            if acquired {
                rival.unlock().unwrap();
            }
            recorded.set(Some(acquired));
        }),
    );
    let entry = file_entry(&source);
    crate::graph_object_store::admit_graph_object_with_lease(&lease, &entry).unwrap_err();
    assert_eq!(rival_got_lock.get(), Some(false));
    assert!(!staged.object_path().exists());
}

/// The same edit through a whole publication: the commit boundary admits every
/// installed object by XXH64 before `CURRENT` can move, so the edit leaves the
/// prior generation intact and no object at a wrong address.
#[test]
fn edited_staged_file_fails_publication_and_preserves_current() {
    let mut staged = staged(15_011);
    let current = staged.current();
    edit_in_place(&staged.staged_path());
    let error = staged
        .session
        .publish_canonical(
            &staged.encoded,
            Uuid::from_u128(TARGET),
            Uuid::from_u128(TRANSACTION),
        )
        .unwrap_err();
    assert!(error.to_string().contains("checksum"), "{error}");
    assert_eq!(staged.current(), current);
    assert_ne!(
        crate::resolve_project_generation(staged.root.path())
            .unwrap()
            .generation_uuid(),
        Uuid::from_u128(TARGET)
    );
    assert!(!staged.object_path().exists());
    object_store(staged.root.path());
}

/// A crash between the link and admission leaves the edited object at its
/// address with nothing to unlink it. A later correct publication of the same
/// digest (a fresh encode of the same input, so another inode) replaces it
/// instead of failing on it forever.
#[test]
fn object_left_mis_addressed_by_a_crash_is_replaced_by_a_later_correct_publication() {
    let a = staged(15_021);
    // The same project encodes the same input again in a second session.
    let b = staged_in(std::rc::Rc::clone(&a.root), 15_022);
    // Two encodes of one input agree on at least one non-trivial artifact.
    let path = a
        .encoded
        .artifacts
        .iter()
        .find(|artifact| {
            artifact.bytes > 64
                && b.encoded
                    .artifacts
                    .iter()
                    .any(|other| other.path == artifact.path && other.sha256 == artifact.sha256)
        })
        .map(|artifact| artifact.path.clone())
        .expect("two encodes of one input share an artifact");
    let digest = a
        .encoded
        .artifacts
        .iter()
        .find(|artifact| artifact.path == path)
        .unwrap()
        .sha256
        .clone();
    let address = crate::graph_object_path(a.root.path(), &digest).unwrap();
    let good = std::fs::read(b.graph.path().join(&path)).unwrap();

    // A's staged file is edited and linked; the process "crashes" before
    // admission, so nothing retires the object.
    let inventory_a = a.inventory();
    let source_a = inventory_a.open(Path::new(&path)).unwrap();
    edit_in_place(&a.graph.path().join(&path));
    let lease = crate::begin_graph_object_publication(a.root.path()).unwrap();
    install(&lease, &source_a).unwrap();
    drop(lease);
    assert_ne!(sha256_hex(&std::fs::read(&address).unwrap()), digest);

    // The correct bytes arrive on another inode (B is on the same filesystem).
    let inventory_b = b.inventory();
    let source_b = inventory_b.open(Path::new(&path)).unwrap();
    let current = a.current();
    let lease = crate::begin_graph_object_publication(a.root.path()).unwrap();
    let evidence = install(&lease, &source_b).unwrap();
    assert!(!evidence.reused_existing);
    assert_eq!(std::fs::read(&address).unwrap(), good);
    assert_eq!(sha256_hex(&good), digest);
    object_store(a.root.path());
    assert_eq!(a.current(), current);
}

/// The object store is not on the encoder's filesystem: the install says so by
/// copying, and the published object is a separate inode with the same bytes.
#[test]
fn cross_filesystem_link_falls_back_to_an_explicit_copy() {
    // The kernel's EXDEV is what reports this to the installer.
    assert_eq!(
        std::io::Error::from_raw_os_error(18).kind(),
        std::io::ErrorKind::CrossesDevices
    );
    let staged = staged(15_012);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let bytes = std::fs::read(staged.staged_path()).unwrap();
    let current = staged.current();
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    crate::graph_object_store::force_cross_device_link(true);
    let installed = install(&lease, &source);
    crate::graph_object_store::force_cross_device_link(false);
    let evidence = installed.unwrap();
    assert!(!evidence.reused_existing);
    assert_eq!(evidence.write_bytes, source.bytes());
    assert_eq!(evidence.bytes_installed, source.bytes());
    let object = std::fs::metadata(staged.object_path()).unwrap();
    assert_ne!(
        object.ino(),
        std::fs::metadata(staged.staged_path()).unwrap().ino()
    );
    assert_eq!(object.nlink(), 1);
    assert_eq!(std::fs::read(staged.object_path()).unwrap(), bytes);
    assert_eq!(std::fs::read(staged.staged_path()).unwrap(), bytes);
    assert_eq!(staged.current(), current);
}

/// After the link the encoded name and the object are one inode. No writer of
/// the encoded tree may rewrite a file in place: every one creates a fresh
/// file exclusively and swaps it in, or unlinks. Pin the two guards that hold
/// that line, then show a reopen and a publication leave installed bytes alone.
#[test]
fn encoded_names_cannot_be_rewritten_in_place_once_installed() {
    let staged = staged(15_013);
    let inventory = staged.inventory();
    let source = inventory.open(Path::new(ARTIFACT)).unwrap();
    let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
    install(&lease, &source).unwrap();
    let bytes = std::fs::read(staged.object_path()).unwrap();
    // Exclusive creation: an encoder writing this name again fails.
    let parent = staged
        .graph
        .open_child_directory(OsStr::new("topology"))
        .unwrap();
    let error = parent
        .create_replaceable_child_file(OsStr::new("runtime_catalog.parquet"))
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    // The shared inode is sealed read-only: a truncating open is refused
    // (a superuser bypasses permission bits, so skip the check for one).
    let superuser = std::fs::metadata("/proc/self").unwrap().uid() == 0;
    if !superuser {
        let error = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(staged.staged_path())
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    }
    assert_eq!(std::fs::read(staged.object_path()).unwrap(), bytes);
}

type ObjectSnapshot = BTreeMap<String, (u64, Vec<u8>)>;

fn object_snapshot(root: &Path) -> ObjectSnapshot {
    object_store(root)
        .into_keys()
        .map(|name| {
            let path = crate::graph_object_path(root, &name).unwrap();
            let inode = std::fs::metadata(&path).unwrap().ino();
            (name, (inode, std::fs::read(path).unwrap()))
        })
        .collect()
}

#[test]
fn reopening_and_publishing_after_install_leaves_installed_objects_untouched() {
    let mut staged = staged(15_014);
    {
        let inventory = staged.inventory();
        let lease = crate::begin_graph_object_publication(staged.root.path()).unwrap();
        for artifact in &staged.encoded.artifacts {
            let source = inventory.open(Path::new(&artifact.path)).unwrap();
            install(&lease, &source).unwrap();
        }
    }
    let before = object_snapshot(staged.root.path());
    assert!(before.len() >= staged.encoded.artifacts.len() / 2);
    let reopened = staged.session.prepare_canonical_encoding(1).unwrap();
    staged
        .session
        .publish_canonical(
            &reopened,
            Uuid::from_u128(TARGET),
            Uuid::from_u128(TRANSACTION),
        )
        .unwrap();
    let after = object_snapshot(staged.root.path());
    for (name, installed) in &before {
        assert_eq!(after.get(name), Some(installed), "{name} changed");
    }
}

/// A second name on the staged inode that is not the content address is
/// refused when the source is captured: it is not something an install made.
#[test]
fn foreign_alias_of_the_staged_file_is_refused() {
    let staged = staged(15_009);
    let alias = staged.root.path().join("foreign-alias");
    std::fs::hard_link(staged.staged_path(), &alias).unwrap();
    let inventory = staged.inventory();
    let error = inventory.open(Path::new(ARTIFACT)).err().unwrap();
    assert!(error.to_string().contains("identity"), "{error}");
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

/// The encoder's bytes for every artifact its inventory lists, by content
/// address, read from the staged files a crash leaves behind.
fn staged_artifacts(root: &Path) -> BTreeMap<String, (String, Vec<u8>)> {
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
    encoding
        .artifacts
        .into_iter()
        .map(|artifact| {
            let bytes = std::fs::read(private_encoded_graph(root).join(&artifact.path)).unwrap();
            assert_eq!(bytes.len() as u64, artifact.bytes, "{}", artifact.path);
            (artifact.sha256, (artifact.path, bytes))
        })
        .collect()
}

/// Every artifact is published at its address with exactly the bytes the
/// encoder wrote before the crash: the rerun published identical bytes.
fn assert_published(root: &Path, staged: &BTreeMap<String, (String, Vec<u8>)>, context: &str) {
    // `object_store` proves each entry hashes to its own address.
    let store = object_store(root);
    for (digest, (path, bytes)) in staged {
        assert_eq!(
            store.get(digest),
            Some(&(bytes.len() as u64)),
            "{context}: {path} {digest}"
        );
        let object = crate::graph_object_path(root, digest).unwrap();
        assert_eq!(&std::fs::read(object).unwrap(), bytes, "{context}: {path}");
    }
}

#[test]
fn publication_crashed_at_each_install_boundary_resumes_to_identical_bytes() {
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
        let staged = staged_artifacts(root.path());
        assert!(!staged.is_empty());

        let receipt = resume_publication(root.path()).unwrap();
        assert_eq!(receipt.generation_uuid, Uuid::from_u128(TARGET));
        assert_eq!(
            crate::resolve_project_generation(root.path())
                .unwrap()
                .generation_uuid(),
            Uuid::from_u128(TARGET)
        );
        assert_published(root.path(), &staged, point);
    }
}

/// The resumed install finds its object already in place: that is the dedupe
/// path. A second crash there still leaves a clean state and an identical
/// publication on the third attempt.
#[test]
fn crash_in_the_dedupe_of_an_already_installed_object_resumes_to_identical_bytes() {
    let root = TempDir::new().unwrap();
    assert_eq!(
        crash_publication_child(
            root.path(),
            Some(&format!("cas.install.after_bucket_sync.{ARTIFACT}"))
        ),
        Some(86)
    );
    let staged = staged_artifacts(root.path());
    assert_eq!(
        resume_child(root.path(), &format!("cas.install.after_dedupe.{ARTIFACT}")),
        Some(86),
        "the resumed install must reach the dedupe of the installed object"
    );
    assert_parent_current_and_no_staging_residue(root.path());
    assert!(artifact_address(root.path()).exists());
    assert_eq!(staged_artifacts(root.path()), staged);

    let receipt = resume_publication(root.path()).unwrap();
    assert_eq!(receipt.generation_uuid, Uuid::from_u128(TARGET));
    assert_published(root.path(), &staged, "after the second crash");
}
