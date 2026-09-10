//! Deterministic M6 storage kernels for CodSpeed CPU simulation (#782).

use divan::Bencher;
use graphforge_storage::{
    GraphDeltaJournalLimits, GraphDeltaOp, GraphDeltaOpKind, GraphDeltaPayload,
    ReconstructedGraphState, apply_delta_runs, decode_delta_run, encode_delta_run,
};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

fn main() {
    divan::main();
}

fn fixture(count: usize) -> Vec<GraphDeltaOp> {
    (0..count)
        .map(|index| GraphDeltaOp {
            operation_uuid: Uuid::from_u128(0x1000 + index as u128),
            kind: GraphDeltaOpKind::SetNodeProperty,
            payload: GraphDeltaPayload::SetNodeProperty {
                node_uuid: Uuid::from_u128(0x2000 + index as u128).to_string(),
                property_stem: "1".into(),
                key: "rank".into(),
                value: graphforge_storage::encode_graph_delta_value(
                    &graphforge_ir::IrLiteral::Int(index as i64),
                )
                .unwrap(),
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
    let first = fixture(operations);
    let mut second = fixture(operations);
    for (index, operation) in second.iter_mut().enumerate() {
        operation.operation_uuid = Uuid::from_u128(0x1_0000 + index as u128);
        if let GraphDeltaPayload::SetNodeProperty { value, .. } = &mut operation.payload {
            *value = graphforge_storage::encode_graph_delta_value(&graphforge_ir::IrLiteral::Int(
                index as i64 + 1,
            ))
            .unwrap();
        }
    }
    let encoded_first = encode_delta_run(
        1,
        Uuid::from_u128(10),
        Uuid::from_u128(20),
        &first,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    let encoded_second = encode_delta_run(
        2,
        Uuid::from_u128(11),
        Uuid::from_u128(21),
        &second,
        GraphDeltaJournalLimits::default(),
    )
    .unwrap();
    let mut base = ReconstructedGraphState::default();
    for index in 0..operations {
        let node = Uuid::from_u128(0x2000 + index as u128).to_string();
        base.nodes.insert(
            node.clone(),
            vec![graphforge_value::EntityTypeId::decode(1).unwrap()],
        );
        base.node_ids.insert(node, index as u64 + 1);
    }
    bencher.bench(|| {
        let limits = GraphDeltaJournalLimits::default();
        let runs = [
            decode_delta_run(&encoded_first, Some(1), limits).unwrap(),
            decode_delta_run(&encoded_second, Some(2), limits).unwrap(),
        ];
        let mut state = base.clone();
        let evidence = apply_delta_runs(&mut state, &runs, limits).unwrap();
        divan::black_box(evidence.state_fingerprint)
    });
}

#[divan::bench(args = [1, 100, 10_000])]
fn transaction_classification(bencher: Bencher, count: usize) {
    let operations = fixture(count);
    bencher.bench(|| {
        divan::black_box(
            operations
                .iter()
                .filter(|op| matches!(op.kind, GraphDeltaOpKind::SetNodeProperty))
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
