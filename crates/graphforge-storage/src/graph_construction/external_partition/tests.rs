use super::*;
use crate::graph_construction::partition_shaping::{FixedRangePartitioner, PartitionFamily};
use crate::graph_construction::{
    GraphConstructionBudgets, GraphConstructionEvidence, GraphConstructionSession,
};
use sha2::Digest;
use std::ffi::OsStr;

/// Record width: a 16-byte key and an 8-byte distinguishing suffix.
const WIDTH: usize = 24;
const RECORDS: u64 = 4_096;
const HUB: u64 = 7;
const PARTITIONS: usize = 4;

/// Hub-heavy records: half share one key, as a high-degree node's endpoints
/// do; the rest have distinct keys. Every record is distinct by its suffix.
fn records() -> Vec<[u8; WIDTH]> {
    (0..RECORDS)
        .map(|index| {
            let key = if index % 2 == 0 { HUB } else { index + 100 };
            let mut record = [0_u8; WIDTH];
            record[..16].copy_from_slice(&u128::from(key).to_be_bytes());
            // Reversed suffix, so routing order is not the sorted order.
            record[16..].copy_from_slice(&(RECORDS - index).to_be_bytes());
            record
        })
        .collect()
}

/// Range routing: the hub alone in partition 0, the rest split by key range.
fn partition_of(record: &[u8; WIDTH]) -> usize {
    let key = u64::try_from(u128::from_be_bytes(record[..16].try_into().unwrap())).unwrap();
    if key < 100 {
        0
    } else {
        1 + usize::try_from((key - 100) * (PARTITIONS as u64 - 1) / RECORDS).unwrap()
    }
}

fn scratch_names(session_root: &StableDirectory) -> Vec<String> {
    session_root
        .child_names()
        .unwrap()
        .into_iter()
        .filter_map(|name| name.into_string().ok())
        .filter(|name| name.contains("xrun"))
        .collect()
}

#[derive(Debug)]
struct Finished {
    digest: String,
    evidence: GraphConstructionEvidence,
    scratch_left: Vec<String>,
}

/// Route the fixture through a real partitioner and finish it.
fn finish(
    seed: u128,
    max_partition_bytes: u64,
    max_external_partition_bytes: u64,
) -> Result<Finished, String> {
    let root = tempfile::TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let mut session = super::super::tests::open(&root, seed);
    let GraphConstructionSession {
        root: session_root,
        checkpoint,
        ..
    } = &mut session;
    let mut partitioner = FixedRangePartitioner::<WIDTH>::new(
        session_root,
        PartitionFamily::Endpoints,
        PARTITIONS,
        None,
        false,
    )
    .unwrap()
    .with_materialization_limit(max_partition_bytes)
    .with_external_partitions(max_external_partition_bytes);
    for record in &records() {
        partitioner
            .route_slice(partition_of(record), record, 1, &mut checkpoint.evidence)
            .unwrap();
    }
    let finished = partitioner.finish_optional(
        "staged-endpoints.run",
        0,
        false,
        &mut || false,
        &mut checkpoint.evidence,
    );
    let scratch_left = scratch_names(session_root);
    let output = finished.map_err(|error| error.to_string())?.unwrap();
    let mut bytes = Vec::new();
    session_root
        .open_child_file(OsStr::new(&output))
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    assert_eq!(bytes.len() as u64, RECORDS * WIDTH as u64);
    Ok(Finished {
        digest: super::super::hex(&sha2::Sha256::digest(&bytes)),
        evidence: checkpoint.evidence.clone(),
        scratch_left,
    })
}

/// A partition over its resident budget is sorted into runs and publishes
/// exactly the bytes the resident path publishes with a budget large enough.
#[test]
fn external_runs_publish_the_resident_bytes() {
    let resident = finish(
        0x1585,
        super::super::partition::default_materialization_bytes(),
        0,
    )
    .unwrap();
    assert_eq!(resident.evidence.external_partitions, 0);
    // 64 records per run; the hub partition holds at least 2,048.
    let budget = 64 * WIDTH as u64;
    let external = finish(0x1585, budget, 1 << 30).unwrap();
    assert_eq!(external.digest, resident.digest);
    assert!(
        external.evidence.external_partitions >= 1,
        "{:?}",
        external.evidence
    );
    assert!(
        external.evidence.external_runs >= 32,
        "{:?}",
        external.evidence
    );
    assert!(external.evidence.external_run_bytes >= 2_048 * WIDTH as u64);
    assert!(
        external.evidence.peak_partition_records <= 64,
        "an external partition must not be materialized whole: {:?}",
        external.evidence
    );
    assert!(
        external.scratch_left.is_empty(),
        "{:?}",
        external.scratch_left
    );
}

/// A zero external bound is the pre-ADR-0047 contract: refuse.
#[test]
fn zero_external_bound_keeps_the_refusal() {
    let error = finish(0x1585, 64 * WIDTH as u64, 0).unwrap_err();
    assert!(error.contains("exceeds recorded budget"), "{error}");
}

/// A partition larger than the external bound refuses before writing a run.
#[test]
fn external_bound_refuses_a_larger_partition() {
    let budget = 64 * WIDTH as u64;
    let error = finish(0x1585, budget, budget).unwrap_err();
    assert!(
        error.contains("exceeds recorded external budget"),
        "{error}"
    );
}

/// Sealed segments of the fixture's hub partition, and the session holding
/// them, for tests that drive `sort_into_runs` directly.
fn hub_segments(root: &tempfile::TempDir) -> (GraphConstructionSession, Vec<String>, u64) {
    crate::open_or_initialize_project(root.path()).unwrap();
    let mut session = super::super::tests::open(root, 0x1586);
    let names;
    {
        let GraphConstructionSession {
            root: session_root,
            checkpoint,
            ..
        } = &mut session;
        let mut partitioner = FixedRangePartitioner::<WIDTH>::new(
            session_root,
            PartitionFamily::Endpoints,
            1,
            None,
            false,
        )
        .unwrap();
        for record in &records() {
            partitioner
                .route_slice(0, record, 1, &mut checkpoint.evidence)
                .unwrap();
        }
        partitioner.seal(0, &mut checkpoint.evidence).unwrap();
        names = partitioner
            .sealed_segments()
            .into_iter()
            .map(|receipt| receipt.name)
            .collect::<Vec<_>>();
    }
    (session, names, RECORDS)
}

fn sorted_fixture() -> Vec<[u8; WIDTH]> {
    let mut sorted = records();
    sorted.sort_unstable();
    sorted
}

fn run_path(session: &GraphConstructionSession, run: &Run) -> std::path::PathBuf {
    session.root.path().join(&run.temporary)
}

#[test]
fn merge_emits_the_sorted_records_and_drop_removes_every_run() {
    let root = tempfile::TempDir::new().unwrap();
    let (session, names, records) = hub_segments(&root);
    let stop = AtomicBool::new(false);
    let (partition, counters) = sort_into_runs::<WIDTH>(
        &session.root,
        &names,
        Some(records),
        100 * WIDTH as u64,
        1 << 30,
        &stop,
    )
    .unwrap();
    assert_eq!(counters.records, records);
    assert_eq!(counters.external_runs, records.div_ceil(100));
    assert_eq!(counters.external_peak_run_records, 100);
    let paths = partition
        .runs
        .iter()
        .map(|run| run_path(&session, run))
        .collect::<Vec<_>>();
    assert!(paths.iter().all(|path| path.exists()));
    let mut merged = Vec::new();
    partition
        .for_each_record(|record| {
            merged.push(<[u8; WIDTH]>::try_from(record).unwrap());
            Ok(())
        })
        .unwrap();
    assert_eq!(merged, sorted_fixture());
    assert!(
        paths.iter().all(|path| !path.exists()),
        "runs were not removed"
    );
}

#[test]
fn a_mutated_run_is_refused_and_still_removed() {
    let root = tempfile::TempDir::new().unwrap();
    let (session, names, records) = hub_segments(&root);
    let stop = AtomicBool::new(false);
    let (partition, _) = sort_into_runs::<WIDTH>(
        &session.root,
        &names,
        Some(records),
        100 * WIDTH as u64,
        1 << 30,
        &stop,
    )
    .unwrap();
    let first = run_path(&session, &partition.runs[0]);
    let mut bytes = std::fs::read(&first).unwrap();
    // Flip a suffix byte: the record still sorts, only its bytes changed.
    bytes[WIDTH - 1] ^= 0x5a;
    std::fs::write(&first, &bytes).unwrap();
    let paths = partition
        .runs
        .iter()
        .map(|run| run_path(&session, run))
        .collect::<Vec<_>>();
    let error = partition
        .for_each_record(|_| Ok(()))
        .unwrap_err()
        .to_string();
    assert!(error.contains("checksum differs"), "{error}");
    assert!(paths.iter().all(|path| !path.exists()));
}

#[test]
fn a_truncated_run_is_refused() {
    let root = tempfile::TempDir::new().unwrap();
    let (session, names, records) = hub_segments(&root);
    let stop = AtomicBool::new(false);
    let (partition, _) = sort_into_runs::<WIDTH>(
        &session.root,
        &names,
        Some(records),
        100 * WIDTH as u64,
        1 << 30,
        &stop,
    )
    .unwrap();
    let first = run_path(&session, &partition.runs[0]);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&first)
        .unwrap()
        .set_len((WIDTH / 2) as u64)
        .unwrap();
    let error = partition
        .for_each_record(|_| Ok(()))
        .unwrap_err()
        .to_string();
    assert!(error.contains("length differs"), "{error}");
}

#[test]
fn a_stopped_sort_returns_and_leaves_no_run() {
    let root = tempfile::TempDir::new().unwrap();
    let (session, names, records) = hub_segments(&root);
    let stop = AtomicBool::new(true);
    let error = sort_into_runs::<WIDTH>(
        &session.root,
        &names,
        Some(records),
        100 * WIDTH as u64,
        1 << 30,
        &stop,
    )
    .err()
    .unwrap()
    .to_string();
    assert!(error.contains("abandoned"), "{error}");
    assert!(scratch_names(&session.root).is_empty());
}

#[test]
fn a_record_count_that_differs_from_routing_is_refused() {
    let root = tempfile::TempDir::new().unwrap();
    let (session, names, records) = hub_segments(&root);
    let stop = AtomicBool::new(false);
    let error = sort_into_runs::<WIDTH>(
        &session.root,
        &names,
        Some(records + 1),
        100 * WIDTH as u64,
        1 << 30,
        &stop,
    )
    .err()
    .unwrap()
    .to_string();
    assert!(
        error.contains("differs from admitted record count"),
        "{error}"
    );
    assert!(scratch_names(&session.root).is_empty());
}

/// A checkpoint recorded before external partitions has no bound; it reads as
/// zero, and a default-budget resume keeps that recorded refusal rather than
/// failing as a parameter change. An explicit different bound still fails.
#[test]
fn a_legacy_checkpoint_keeps_its_refusal_on_resume() {
    let mut value = serde_json::to_value(GraphConstructionBudgets::default()).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .remove("max_external_partition_bytes")
        .expect("the field is serialized");
    let legacy: GraphConstructionBudgets = serde_json::from_value(value).unwrap();
    assert_eq!(legacy.max_external_partition_bytes, 0);

    let root = tempfile::TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let operation = uuid::Uuid::from_u128(0x1585_0001);
    drop(GraphConstructionSession::open(root.path(), operation, 0, legacy).unwrap());
    let resumed = GraphConstructionSession::open(
        root.path(),
        operation,
        0,
        GraphConstructionBudgets::default(),
    )
    .unwrap();
    assert_eq!(resumed.checkpoint.budgets.max_external_partition_bytes, 0);
    drop(resumed);
    let changed = GraphConstructionBudgets {
        max_external_partition_bytes: 1 << 30,
        ..GraphConstructionBudgets::default()
    };
    assert!(GraphConstructionSession::open(root.path(), operation, 0, changed).is_err());
}

/// The external bound must admit at least one resident budget, or be zero.
#[test]
fn an_external_bound_below_the_resident_budget_is_invalid() {
    let root = tempfile::TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let invalid = GraphConstructionBudgets {
        max_external_partition_bytes: 1,
        ..GraphConstructionBudgets::default()
    };
    let error = GraphConstructionSession::open(root.path(), uuid::Uuid::from_u128(9), 0, invalid)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("invalid construction budgets"), "{error}");
}

/// Recovery reclaims a run a crash leaves behind, and only that shape of name.
#[test]
fn recovery_recognizes_run_temporaries_only() {
    let random = "0123456789abcdef0123456789abcdef";
    let owned = |target: &str| {
        super::super::recovery::is_owned_artifact_temp(&format!(".artifact-{target}-{random}.tmp"))
    };
    assert!(owned("xrun-p0"));
    assert!(owned("xrun-p17"));
    assert!(!owned("xrun-p"));
    assert!(!owned("xrun-px"));
    assert!(!owned("xrun-p1-extra"));
    assert!(!owned("xrun"));
    assert!(!super::super::recovery::canonical_artifact_target(
        "xrun-p0"
    ));
}
