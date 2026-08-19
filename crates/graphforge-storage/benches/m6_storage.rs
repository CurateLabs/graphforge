//! Deterministic M6 storage kernels for CodSpeed CPU simulation (#782).

use divan::Bencher;
use graphforge_storage::{
    GraphDeltaJournalLimits, GraphDeltaOp, GraphDeltaOpKind, GraphDeltaPayload, decode_delta_run,
    encode_delta_run,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

fn main() {
    divan::main();
}

fn fixture(count: usize) -> Vec<GraphDeltaOp> {
    (0..count)
        .map(|index| GraphDeltaOp {
            operation_uuid: Uuid::from_u128(0x1000 + index as u128),
            kind: GraphDeltaOpKind::UpsertNode,
            payload: GraphDeltaPayload::UpsertNodeV2 {
                node_uuid: Uuid::from_u128(0x2000 + index as u128).to_string(),
                node_id: index as u64 + 1,
                type_ids: vec![1],
                created_at_micros: index as i64,
                updated_at_micros: index as i64,
            },
        })
        .collect()
}

#[divan::bench(args = [1, 100, 10_000])]
fn gfdr_encode(bencher: Bencher, count: usize) {
    let operations = fixture(count);
    bencher.bench(|| {
        encode_delta_run(
            1,
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            divan::black_box(&operations),
            GraphDeltaJournalLimits::default(),
        )
        .unwrap()
    });
}

#[divan::bench(args = [1, 100, 10_000])]
fn gfdr_decode_verify(bencher: Bencher, count: usize) {
    let bytes = encode_delta_run(
        1,
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        &fixture(count),
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    bencher.bench(|| {
        decode_delta_run(
            divan::black_box(&bytes),
            Some(1),
            GraphDeltaJournalLimits::default(),
        )
        .unwrap()
    });
}

#[divan::bench(args = [1, 100, 10_000])]
fn replay_merge_fingerprint(bencher: Bencher, operations: usize) {
    // One bounded run across the operation ladder. Run-count limits are an
    // admission contract, not a benchmark parameter to bypass.
    let encoded = encode_delta_run(
        1,
        Uuid::from_u128(10),
        Uuid::from_u128(20),
        &fixture(operations),
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    bencher.bench(|| {
        let mut fingerprint = Sha256::new();
        let run = decode_delta_run(&encoded, Some(1), GraphDeltaJournalLimits::default()).unwrap();
        fingerprint.update(&run.bytes);
        divan::black_box(fingerprint.finalize())
    });
}

#[divan::bench(args = [1, 100, 10_000])]
fn transaction_classification(bencher: Bencher, count: usize) {
    let operations = fixture(count);
    bencher.bench(|| {
        divan::black_box(
            operations
                .iter()
                .filter(|op| matches!(op.kind, GraphDeltaOpKind::UpsertNode))
                .count(),
        )
    });
}

#[divan::bench(args = [1, 100, 10_000])]
fn manifest_reachability(bencher: Bencher, count: usize) {
    // Version-1 synthetic generation manifest: each generation retains its
    // immediate predecessor. Building it is fixture setup, while the measured
    // closure is the deterministic bounded ancestor walk used by cleanup.
    let parents: BTreeMap<u64, Option<u64>> = (0..count as u64)
        .map(|generation| (generation, generation.checked_sub(1)))
        .collect();
    bencher.bench(|| {
        let mut reachable = BTreeSet::new();
        let mut cursor = (count as u64).checked_sub(1);
        while let Some(generation) = cursor {
            reachable.insert(generation);
            cursor = parents[&generation];
        }
        divan::black_box(reachable)
    });
}

#[divan::bench(args = [1, 100, 10_000])]
fn transaction_stage_and_classify(bencher: Bencher, count: usize) {
    let operations = fixture(count);
    bencher.bench(|| {
        let staged: Vec<_> = operations
            .iter()
            .map(|operation| (operation.operation_uuid, operation.kind))
            .collect();
        divan::black_box(staged)
    });
}
