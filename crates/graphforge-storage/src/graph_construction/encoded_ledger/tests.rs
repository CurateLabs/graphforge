//! Tests for the inventory-derived encoded ledger entries (#900).

use super::super::tests::{
    CheckpointLimit, allocation_evidence, construction_session_root, node_batch,
};
use super::super::*;
use super::*;
use std::path::Path;
use tempfile::TempDir;

const TARGET: u128 = 9_990;
const TRANSACTION: u128 = 9_991;

/// Default budgets with two-row batches. The canonical encoder writes one
/// node file per batch window, so an encoding's artifact count follows its
/// row count while every other checkpoint field keeps its shape.
fn two_row_budgets() -> GraphConstructionBudgets {
    GraphConstructionBudgets {
        max_batch_rows: 2,
        ..GraphConstructionBudgets::default()
    }
}

fn open_session(root: &Path, operation: Uuid) -> GraphConstructionSession {
    GraphConstructionSession::open(root, operation, 0, two_row_budgets())
        .unwrap_or_else(|error| panic!("open {operation}: {error}"))
}

/// Stage `chunks` two-row node chunks, seal and shape.
fn stage_and_shape(session: &mut GraphConstructionSession, chunks: u64) -> ConstructionShape {
    for chunk in 0..chunks {
        session
            .append(
                ConstructionChunkKind::Node,
                &format!("n-{chunk}"),
                &node_batch(1 + u128::from(chunk) * 2, 2),
            )
            .unwrap();
    }
    session.seal().unwrap();
    session.shape_canonical_with_cancellation(|| false).unwrap()
}

fn read_encoding(session_root: &Path) -> GraphConstructionEncoding {
    serde_json::from_slice(&std::fs::read(session_root.join("encoded-v1/inventory.json")).unwrap())
        .unwrap()
}

/// The ledger entry of every artifact the installed inventory names, read
/// from the files by path rather than through any session code.
fn encoded_ledger_on_disk(session_root: &Path) -> BTreeMap<String, u64> {
    read_encoding(session_root)
        .artifacts
        .iter()
        .map(|artifact| {
            let file =
                File::open(session_root.join("encoded-v1/graph").join(&artifact.path)).unwrap();
            let identity = file_identity(&file).unwrap();
            let usage = graphforge_filesystem::file_space_usage(&file).unwrap();
            (
                format!("{:016x}:{}", identity.volume_serial, hex(&identity.file_id)),
                usage.allocated_bytes,
            )
        })
        .collect()
}

/// The ledger key of every payload file in the session tree. Controls are
/// left out: rewriting one at open can reuse the inode of a payload the open
/// just retired.
fn live_identities(session_root: &Path) -> std::collections::BTreeSet<String> {
    fn walk(directory: &Path, control_level: bool, keys: &mut std::collections::BTreeSet<String>) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if path.is_dir() {
                walk(&path, name == "encoded-v1", keys);
            } else if !(control_level
                && path
                    .extension()
                    .is_some_and(|extension| extension == "json")
                || name == "session.lock")
            {
                let identity = graphforge_filesystem::path_identity(&path).unwrap();
                keys.insert(format!(
                    "{:016x}:{}",
                    identity.volume_serial,
                    hex(&identity.file_id)
                ));
            }
        }
    }
    let mut keys = std::collections::BTreeSet::new();
    walk(session_root, true, &mut keys);
    keys
}

fn persisted(session_root: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(session_root.join(CHECKPOINT)).unwrap()).unwrap()
}

fn persisted_ledger(control: &serde_json::Value) -> BTreeMap<String, u64> {
    serde_json::from_value(control["evidence"]["storage_active_identity_allocated_bytes"].clone())
        .unwrap()
}

/// The record the writer before #900's encoded omission produced from this
/// state: the whole ledger, without transition history.
fn old_record_bytes(session: &GraphConstructionSession) -> u64 {
    let mut old = session.checkpoint.clone();
    old.evidence.storage_allocation_transitions.clear();
    old.encoded_index = None;
    serde_json::to_vec(&old).unwrap().len() as u64
}

fn publish(session: &mut GraphConstructionSession, encoding: &GraphConstructionEncoding) {
    session
        .publish_canonical(
            encoding,
            Uuid::from_u128(TARGET),
            Uuid::from_u128(TRANSACTION),
        )
        .unwrap();
}

/// The persisted checkpoint omits every encoded entry, records the digest of
/// exactly the entries on disk, and a reopen restores the ledger the writer
/// held.
fn assert_omitted_and_restored(root: &Path, operation: Uuid, label: &str) {
    let session_root = construction_session_root_path(root, operation);
    let control = persisted(&session_root);
    let on_disk = encoded_ledger_on_disk(&session_root);
    let ledger = persisted_ledger(&control);
    assert!(
        on_disk.keys().all(|key| !ledger.contains_key(key)),
        "{label}: an encoded entry was persisted"
    );
    assert_eq!(
        control["encoded_ledger_sha256"],
        serde_json::json!(encoded_ledger_sha256(&on_disk)),
        "{label}"
    );
    let reopened = open_session(root, operation);
    // Opening also finishes interrupted work, such as retiring the shaped
    // outputs once an encoding is pinned; their files are then gone. What
    // must hold is that every persisted or encoded entry for a file that
    // still exists is restored, at its allocation, and nothing else.
    let live = live_identities(&session_root);
    let mut expected = ledger;
    expected.extend(on_disk);
    expected.retain(|key, _| live.contains(key));
    assert_eq!(
        reopened.evidence().storage_active_identity_allocated_bytes,
        expected,
        "{label}: reopened ledger differs from persisted plus encoded files"
    );
    let first = allocation_evidence(reopened.evidence());
    drop(reopened);
    assert_eq!(
        allocation_evidence(open_session(root, operation).evidence()),
        first,
        "{label}: a second reopen differs"
    );
}

fn construction_session_root_path(root: &Path, operation: Uuid) -> std::path::PathBuf {
    root.join(PRIVATE_ROOT).join(operation.simple().to_string())
}

/// Checkpoint sizes after encoding and after publication, the encoded
/// artifact count, and the largest record the old writer would have produced.
struct Measured {
    encoded: u64,
    published: u64,
    artifacts: usize,
    old_record: u64,
    smallest_entry: u64,
}

fn encode_and_publish(chunks: u64, bound: Option<u64>) -> Measured {
    let root = TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let operation = Uuid::from_u128(9_900 + u128::from(chunks));
    let session_root = construction_session_root(&root, operation);
    let limit = bound.map(CheckpointLimit::set);
    let mut session = open_session(root.path(), operation);
    let shape = stage_and_shape(&mut session, chunks);
    let encoding = session.encode_canonical(&shape, 1).unwrap();
    let artifacts = encoding.artifacts.len();
    // Shaped outputs are retired with the encoding, so the ledger now holds
    // the encoded artifacts and nothing else, and the record persists none.
    assert_eq!(
        session.evidence().storage_active_identity_allocated_bytes,
        encoded_ledger_on_disk(&session_root),
        "{chunks} chunks"
    );
    assert_eq!(
        persisted(&session_root)["evidence"]["storage_active_identity_allocated_bytes"],
        serde_json::json!({}),
        "{chunks} chunks"
    );
    let encoded = std::fs::metadata(session_root.join(CHECKPOINT))
        .unwrap()
        .len();
    let mut old_record = old_record_bytes(&session);
    let smallest_entry = session
        .evidence()
        .storage_active_identity_allocated_bytes
        .iter()
        .map(|(key, allocated)| key.len() + allocated.to_string().len() + 4)
        .min()
        .unwrap() as u64;
    drop(session);
    assert_omitted_and_restored(root.path(), operation, "encoded");

    let mut session = open_session(root.path(), operation);
    publish(&mut session, &encoding);
    assert_eq!(
        session.checkpoint.publication_state,
        Some(ConstructionPublicationState::Published)
    );
    let published = std::fs::metadata(session_root.join(CHECKPOINT))
        .unwrap()
        .len();
    old_record = old_record.max(old_record_bytes(&session));
    drop(session);
    assert_omitted_and_restored(root.path(), operation, "published");
    drop(limit);
    Measured {
        encoded,
        published,
        artifacts,
        old_record,
        smallest_entry,
    }
}

/// #900. Every checkpoint written during and after canonical encoding and
/// publication must keep its size independent of the encoded-artifact count.
///
/// S26 failed `import-session validate` after 6,613 s with 21,525 encoded
/// artifacts: the checkpoint that pins the encoded inventory carried one
/// ledger entry per artifact (about 1.2 MB) past the 1 MiB control bound. The
/// invariant is asserted as a slope between a small and a five-times-larger
/// encoding, and the larger run executes under a write bound only 1 KiB above
/// the small run's largest record, which the old record provably crosses.
#[test]
fn encoded_checkpoints_are_independent_of_encoded_artifact_count() {
    let small = encode_and_publish(8, None);
    let bound = small.encoded.max(small.published) + 1024;
    let large = encode_and_publish(40, Some(bound));
    assert!(
        large.artifacts >= small.artifacts + 32,
        "the fixture must scale the encoded artifact count: {} vs {}",
        large.artifacts,
        small.artifacts
    );
    // The positive control: the old writer's record from the larger run's
    // state is over the bound every one of its writes stayed under.
    assert!(
        large.old_record > bound,
        "the old record must cross the lowered bound: {} <= {bound}",
        large.old_record
    );
    eprintln!(
        "encoded artifacts {} -> {}; encoded checkpoint {} -> {} B; published {} -> {} B; \
         bound {bound} B; old record {} B; smallest entry {} B",
        small.artifacts,
        large.artifacts,
        small.encoded,
        large.encoded,
        small.published,
        large.published,
        large.old_record,
        large.smallest_entry
    );
    // The slope: 32 more encoded artifacts, and each record grows by less
    // than one ledger entry (only counter digits change).
    for (phase, small_bytes, large_bytes) in [
        ("encoded", small.encoded, large.encoded),
        ("published", small.published, large.published),
    ] {
        let growth = large_bytes.saturating_sub(small_bytes);
        assert!(
            growth < large.smallest_entry,
            "{phase} checkpoint grew {growth} bytes over {} more encoded artifacts \
             ({small_bytes} -> {large_bytes})",
            large.artifacts - small.artifacts
        );
    }
}

/// The flow a crash child runs: stage three chunks, seal, shape, encode and
/// publish, arming `failpoint` at its `occurrence` just before `stage`.
fn run_flow(root: &Path, chunks: u64, armed: Option<(&str, &str, u32)>) {
    crate::open_or_initialize_project(root).unwrap();
    let operation = Uuid::from_u128(9_980);
    let arm = |stage: &str| {
        if let Some((armed_stage, failpoint, occurrence)) = armed
            && armed_stage == stage
        {
            *ARMED_FAILPOINT.lock().unwrap() = Some((failpoint.to_owned(), occurrence));
        }
    };
    let mut session = open_session(root, operation);
    let shape = stage_and_shape(&mut session, chunks);
    arm("encode");
    let encoding = session.encode_canonical(&shape, 1).unwrap();
    arm("publish");
    publish(&mut session, &encoding);
}

#[test]
fn encoded_ledger_crash_child() {
    let Ok(root) = std::env::var("GF_ENCODED_LEDGER_CRASH_ROOT") else {
        return;
    };
    let stage = std::env::var("GF_ENCODED_LEDGER_CRASH_STAGE").unwrap();
    let failpoint = std::env::var("GF_ENCODED_LEDGER_CRASH_POINT").unwrap();
    let occurrence = std::env::var("GF_ENCODED_LEDGER_CRASH_OCCURRENCE")
        .unwrap()
        .parse()
        .unwrap();
    let chunks = std::env::var("GF_ENCODED_LEDGER_CRASH_CHUNKS")
        .unwrap()
        .parse()
        .unwrap();
    run_flow(
        Path::new(&root),
        chunks,
        Some((&stage, &failpoint, occurrence)),
    );
}

fn crash(root: &Path, chunks: u64, stage: &str, failpoint: &str, occurrence: u32) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("graph_construction::encoded_ledger::tests::encoded_ledger_crash_child")
        .arg("--nocapture")
        .env("GF_ENCODED_LEDGER_CRASH_ROOT", root)
        .env("GF_ENCODED_LEDGER_CRASH_STAGE", stage)
        .env("GF_ENCODED_LEDGER_CRASH_POINT", failpoint)
        .env("GF_ENCODED_LEDGER_CRASH_OCCURRENCE", occurrence.to_string())
        .env("GF_ENCODED_LEDGER_CRASH_CHUNKS", chunks.to_string())
        .env(
            "GF_CONSTRUCTION_FAILPOINT_COOKIE",
            "graphforge-construction-test-v1",
        )
        .status()
        .unwrap();
    // A failpoint that never fires exits 0 and fails here, so every case is
    // a real interruption.
    assert_eq!(status.code(), Some(86), "{stage} {failpoint}#{occurrence}");
}

/// Allocation evidence that does not name native identities: current and
/// authority category totals, recorded and authority peaks, the total peak
/// and the sorted allocations.
type IdentityFreeEvidence = (
    BTreeMap<crate::ArtifactCategory, crate::ArtifactStorageTotals>,
    BTreeMap<crate::ArtifactCategory, crate::ArtifactStorageTotals>,
    BTreeMap<crate::ArtifactCategory, u64>,
    BTreeMap<crate::ArtifactCategory, u64>,
    u64,
    Vec<u64>,
);

fn identity_free(evidence: &GraphConstructionEvidence) -> IdentityFreeEvidence {
    let (ledger, _, current, authorities, peaks, peak_authorities, total) =
        allocation_evidence(evidence);
    let mut allocations = ledger.into_values().collect::<Vec<_>>();
    allocations.sort_unstable();
    (
        current,
        authorities,
        peaks,
        peak_authorities,
        total,
        allocations,
    )
}

/// #900. Crash and resume at each encoding and publication boundary
/// reconstructs the allocation evidence exactly.
///
/// After each crash the reopened ledger must equal the persisted ledger plus
/// the encoded entries read from the files themselves, under the recorded
/// digest; category totals must equal the identity union; a second reopen
/// must agree; and once the flow completes, every category total, peak and
/// allocation must equal those of a run that never crashed.
#[test]
fn encoding_and_publication_crashes_reconstruct_allocation_evidence() {
    const CHUNKS: u64 = 3;
    let reference_root = TempDir::new().unwrap();
    run_flow(reference_root.path(), CHUNKS, None);
    let operation = Uuid::from_u128(9_980);
    let reference = identity_free(open_session(reference_root.path(), operation).evidence());

    // The pre-encoding reclaim writes the checkpoint twice, so the third
    // replace after arming the encode stage is the write that pins the
    // inventory; the case assertions below check that it is.
    let cases: [(&str, &str, u32, Option<&str>); 14] = [
        (
            "encode",
            "encode.control.after_install.inventory.json",
            1,
            None,
        ),
        (
            "encode",
            "control.replace.after_partial.checkpoint.json",
            3,
            None,
        ),
        (
            "encode",
            "control.replace.after_temp_fsync.checkpoint.json",
            3,
            None,
        ),
        (
            "encode",
            "control.replace.after_replace.checkpoint.json",
            3,
            Some("sealed"),
        ),
        (
            "encode",
            "supersession.encoded_authenticated",
            1,
            Some("sealed"),
        ),
        (
            "encode",
            "supersession.before_shape_checkpoint",
            1,
            Some("sealed"),
        ),
        (
            "encode",
            "control.replace.after_replace.checkpoint.json",
            4,
            Some("sealed"),
        ),
        (
            "encode",
            "control.replace.after_replace.checkpoint.json",
            5,
            Some("sealed"),
        ),
        (
            "publish",
            "control.install.after_install.publication-intent.json",
            1,
            Some("sealed"),
        ),
        (
            "publish",
            "control.replace.after_partial.checkpoint.json",
            1,
            Some("sealed"),
        ),
        (
            "publish",
            "control.replace.after_replace.checkpoint.json",
            1,
            Some("publishing"),
        ),
        (
            "publish",
            "publication.after_current_before_receipt",
            1,
            Some("publishing"),
        ),
        (
            "publish",
            "control.install.after_install.publication-receipt.json",
            1,
            Some("publishing"),
        ),
        (
            "publish",
            "control.replace.after_replace.checkpoint.json",
            2,
            Some("published"),
        ),
    ];
    for (stage, failpoint, occurrence, pinned_state) in cases {
        let label = format!("{stage} {failpoint}#{occurrence}");
        let root = TempDir::new().unwrap();
        crash(root.path(), CHUNKS, stage, failpoint, occurrence);
        let session_root = construction_session_root_path(root.path(), operation);
        let control = persisted(&session_root);
        // The positive control for each window: the surviving record pins
        // the inventory (and omits its entries) exactly when it should.
        match pinned_state {
            None => assert!(control["encoding_inventory_sha256"].is_null(), "{label}"),
            Some(state) => {
                assert_eq!(control["publication_state"], state, "{label}");
                assert_omitted_and_restored(root.path(), operation, &label);
            }
        }
        let mut resumed = open_session(root.path(), operation);
        allocation_evidence(resumed.evidence());
        let encoding = resumed.prepare_canonical_encoding(1).unwrap();
        publish(&mut resumed, &encoding);
        drop(resumed);
        assert_omitted_and_restored(root.path(), operation, &label);
        let completed = open_session(root.path(), operation);
        assert_eq!(
            completed.evidence().storage_active_identity_allocated_bytes,
            encoded_ledger_on_disk(&session_root),
            "{label}: the completed ledger is not exactly the encoded files"
        );
        assert_eq!(
            identity_free(completed.evidence()),
            reference,
            "{label}: allocation evidence differs from an uncrashed run"
        );
    }
}

/// #900. The retained S26 session's state, reproduced: sealed, shape
/// complete, inputs retired, and the encoded inventory installed but not yet
/// pinned by a checkpoint, which is where the oversized write failed. Under a
/// write bound the old record crosses, it reopens, re-encodes without
/// reshaping, pins the inventory and publishes.
#[test]
fn unpinned_inventory_resumes_under_the_control_bound_without_reshaping() {
    const CHUNKS: u64 = 40;
    let root = TempDir::new().unwrap();
    crash(
        root.path(),
        CHUNKS,
        "encode",
        "encode.control.after_install.inventory.json",
        1,
    );
    let operation = Uuid::from_u128(9_980);
    let session_root = construction_session_root_path(root.path(), operation);
    let before = persisted(&session_root);
    assert_eq!(before["state"], "sealed");
    assert_eq!(before["publication_state"], "sealed");
    assert_eq!(before["inputs_retired"], true);
    assert!(before["encoding_inventory_sha256"].is_null());
    assert!(session_root.join("encoded-v1/inventory.json").is_file());
    let artifacts = read_encoding(&session_root).artifacts.len();
    assert!(artifacts > 40, "{artifacts} encoded artifacts");

    let bound = std::fs::metadata(session_root.join(CHECKPOINT))
        .unwrap()
        .len()
        + 1024;
    let limit = CheckpointLimit::set(bound);
    let mut resumed = open_session(root.path(), operation);
    let encoding = resumed.prepare_canonical_encoding(1).unwrap();
    assert_eq!(encoding.artifacts.len(), artifacts);
    // Shaping was not redone: its phase counters are exactly as persisted.
    for counter in [
        "shape_application_read_bytes",
        "merge_read_bytes",
        "merge_read_records",
        "merge_groups",
        "parquet_read_bytes",
    ] {
        assert_eq!(
            serde_json::to_value(resumed.evidence()).unwrap()[counter],
            before["evidence"][counter],
            "{counter}"
        );
    }
    let old_record = old_record_bytes(&resumed);
    assert!(
        old_record > bound,
        "the old record must cross the bound: {old_record} <= {bound}"
    );
    publish(&mut resumed, &encoding);
    drop(resumed);
    drop(limit);
    assert_omitted_and_restored(root.path(), operation, "resumed");
}

/// #900. An encoded artifact replaced by a byte-identical file under a new
/// inode cannot be adopted as ledger authority on reopen: the re-derived
/// entries no longer reproduce the recorded digest.
#[test]
fn reopen_refuses_encoded_entries_that_differ_from_the_recorded_digest() {
    let root = TempDir::new().unwrap();
    run_flow(root.path(), 3, None);
    let operation = Uuid::from_u128(9_980);
    let session_root = construction_session_root_path(root.path(), operation);
    let before = std::fs::read(session_root.join(CHECKPOINT)).unwrap();
    let path = session_root.join("encoded-v1/graph/topology/surrogate_tails.parquet");
    let body = std::fs::read(&path).unwrap();
    let replacement = path.with_extension("parquet.replacement");
    std::fs::write(&replacement, &body).unwrap();
    std::fs::rename(&replacement, &path).unwrap();
    let error = GraphConstructionSession::open(root.path(), operation, 0, two_row_budgets())
        .err()
        .expect("reopen must refuse");
    assert!(
        error
            .to_string()
            .contains("encoded artifact identities or allocations differ from checkpoint"),
        "{error}"
    );
    assert_eq!(
        std::fs::read(session_root.join(CHECKPOINT)).unwrap(),
        before
    );
}

/// #900. A record whose omission marker is lost, as a format-12 reader from
/// before this change would see it, is refused at open rather than read as a
/// complete ledger without the encoded entries.
#[test]
fn reopen_refuses_a_record_whose_encoded_marker_is_lost() {
    let root = TempDir::new().unwrap();
    run_flow(root.path(), 3, None);
    let operation = Uuid::from_u128(9_980);
    let session_root = construction_session_root_path(root.path(), operation);
    let mut control = persisted(&session_root);
    assert!(control["encoded_ledger_sha256"].is_string());
    control
        .as_object_mut()
        .unwrap()
        .remove("encoded_ledger_sha256");
    std::fs::write(
        session_root.join(CHECKPOINT),
        serde_json::to_vec(&control).unwrap(),
    )
    .unwrap();
    let error = GraphConstructionSession::open(root.path(), operation, 0, two_row_budgets())
        .err()
        .expect("reopen must refuse");
    assert!(
        error
            .to_string()
            .contains("supersession encoded artifact identity changed"),
        "{error}"
    );
}

#[test]
fn encoded_discard_crash_child() {
    let Ok(root) = std::env::var("GF_ENCODED_DISCARD_CRASH_ROOT") else {
        return;
    };
    open_session(Path::new(&root), Uuid::from_u128(9_981))
        .discard()
        .unwrap();
}

/// #900. A discard of an encoded session interrupted after it unlinked the
/// inventory reopens as aborted, keeps its persisted record, and completes.
#[test]
fn interrupted_discard_of_an_encoded_session_reopens_aborted_and_completes() {
    let root = TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let operation = Uuid::from_u128(9_981);
    let mut session = open_session(root.path(), operation);
    let shape = stage_and_shape(&mut session, 3);
    session.encode_canonical(&shape, 1).unwrap();
    drop(session);
    let session_root = construction_session_root_path(root.path(), operation);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("graph_construction::encoded_ledger::tests::encoded_discard_crash_child")
        .arg("--nocapture")
        .env("GF_ENCODED_DISCARD_CRASH_ROOT", root.path())
        .env(
            "GF_CONSTRUCTION_FAILPOINT_COOKIE",
            "graphforge-construction-test-v1",
        )
        .env(
            "GF_CONSTRUCTION_FAILPOINT",
            "discard.after_unlink.inventory.json",
        )
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(86));
    // The positive control: the inventory the omitted entries depend on is
    // gone, and the surviving record is aborted with its marker.
    assert!(!session_root.join("encoded-v1/inventory.json").exists());
    let control = persisted(&session_root);
    assert_eq!(control["state"], "aborted");
    assert!(control["encoded_ledger_sha256"].is_string());
    let reopened = open_session(root.path(), operation);
    assert_eq!(reopened.state(), GraphConstructionState::Aborted);
    reopened.discard().unwrap();
    assert!(!session_root.exists());
}

fn index(entries: &[(&str, u64)]) -> EncodedIdentityIndex {
    EncodedIdentityIndex::new(
        "ab".repeat(32),
        entries
            .iter()
            .map(|(key, allocated)| ((*key).to_owned(), *allocated))
            .collect(),
    )
}

fn ledger(entries: &[(&str, u64)]) -> BTreeMap<String, u64> {
    entries
        .iter()
        .map(|(key, allocated)| ((*key).to_owned(), *allocated))
        .collect()
}

#[test]
fn elide_omits_every_encoded_entry_and_keeps_the_rest() {
    let encoded = index(&[("a", 4096), ("c", 8192)]);
    let pinned = "ab".repeat(32);
    let (persisted, digest) = encoded
        .elide(
            Some(&pinned),
            &ledger(&[("a", 4096), ("b", 1), ("c", 8192)]),
        )
        .unwrap();
    assert_eq!(persisted, ledger(&[("b", 1)]));
    assert_eq!(
        digest,
        encoded_ledger_sha256(&ledger(&[("a", 4096), ("c", 8192)]))
    );
}

#[test]
fn elide_omits_nothing_unless_the_record_pins_the_inventory() {
    let encoded = index(&[("a", 4096)]);
    let full = ledger(&[("a", 4096)]);
    assert!(encoded.elide(None, &full).is_none());
    assert!(encoded.elide(Some(&"cd".repeat(32)), &full).is_none());
}

#[test]
fn elide_omits_nothing_unless_every_entry_is_held_at_its_allocation() {
    let encoded = index(&[("a", 4096), ("c", 8192)]);
    let pinned = "ab".repeat(32);
    // An entry missing, or held at another allocation, keeps the ledger whole.
    assert!(
        encoded
            .elide(Some(&pinned), &ledger(&[("a", 4096)]))
            .is_none()
    );
    assert!(
        encoded
            .elide(Some(&pinned), &ledger(&[("a", 4096), ("c", 4096)]))
            .is_none()
    );
    assert!(index(&[]).elide(Some(&pinned), &ledger(&[])).is_none());
}
