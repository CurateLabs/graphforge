//! Initial imports run on the bulk builder (#1883): every intake refusal still
//! fires, routing is decided at plan time from the footers and the budget, and
//! row groups that straddle task boundaries publish the same bytes on every
//! route. Typed-ontology equivalence is proven in storage
//! (`construction_bulk_tests`).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryArray, StringArray};

use super::test_fixtures::{fixture, nodes};
use super::*;
use crate::{bulk_edge_input_schema, bulk_node_input_schema};

fn v7(value: u128) -> Uuid {
    Uuid::from_u128((value << 64) | (0x7 << 76) | (0x8 << 60) | 1)
}

fn uuids(values: &[Uuid]) -> ArrayRef {
    Arc::new(
        FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.as_bytes().as_slice()))
            .unwrap(),
    )
}

fn node_rows(ids: &[Uuid], label: &str) -> RecordBatch {
    RecordBatch::try_new(
        bulk_node_input_schema(Vec::new()).unwrap(),
        vec![
            uuids(ids),
            Arc::new(StringArray::from(vec![label; ids.len()])),
        ],
    )
    .unwrap()
}

fn edge_rows(ids: &[Uuid], rel: &str, from: &[Uuid], to: &[Uuid]) -> RecordBatch {
    RecordBatch::try_new(
        bulk_edge_input_schema(Vec::new()).unwrap(),
        vec![
            uuids(ids),
            Arc::new(StringArray::from(vec![rel; ids.len()])),
            uuids(from),
            uuids(to),
        ],
    )
    .unwrap()
}

/// The error an initial import of these batches ends in, after proving the
/// project still has no generation of its own.
fn refusal(node_batches: &[RecordBatch], edge_batches: &[RecordBatch]) -> String {
    let (_directory, _project, graph) = fixture();
    let before = *graph.current_generation_uuid.lock().unwrap();
    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    if !node_batches.is_empty() {
        session
            .append_arrow(BulkInputKind::Node, node_batches)
            .unwrap();
    }
    if !edge_batches.is_empty() {
        session
            .append_arrow(BulkInputKind::Edge, edge_batches)
            .unwrap();
    }
    let error = session.validate(&graph).unwrap_err();
    let error = if bulk_source::TEST_BUDGET.with(std::cell::Cell::get) == Some(NODE_SCRATCH_BUDGET)
        && matches!(&error, GfError::Project { code: graphforge_core::ProjectErrorCode::ResourceLimit, message }
            if message.starts_with("graph construction encoding: scratch requires "))
    {
        // Admit the exact fixed source metadata before testing the data's
        // semantic refusal on the natural node-scratch route.
        let required = required_scratch_bytes(&error);
        pin_budget(Budget::Bytes(required));
        let error = session.validate(&graph).unwrap_err();
        pin_budget(Budget::Bytes(NODE_SCRATCH_BUDGET));
        error.to_string()
    } else {
        error.to_string()
    };
    assert_eq!(*graph.current_generation_uuid.lock().unwrap(), before);
    assert_eq!(session.manifest.progress.rows_accepted, 0);
    assert!(
        session
            .commit(&graph, None)
            .unwrap_err()
            .to_string()
            .contains("validated"),
        "a refused import must not commit"
    );
    error
}

/// Every intake refusal fires on every route a bulk build takes:
/// resident, edges on scratch, and node tables on scratch too (#1929).
#[test]
fn every_intake_refusal_fires_on_an_initial_import() {
    for budget in [
        Budget::Host,
        Budget::Bytes(SCRATCH_BUDGET),
        Budget::Bytes(NODE_SCRATCH_BUDGET),
    ] {
        pin_budget(budget);
        every_intake_refusal_fires();
    }
    pin_budget(Budget::Host);
}

fn every_intake_refusal_fires() {
    let (a, b, c) = (v7(1), v7(2), v7(3));
    let e = v7(100);

    let message = refusal(&[node_rows(&[Uuid::new_v4()], "Person")], &[]);
    assert!(message.contains("UUIDv7"), "{message}");

    let message = refusal(&[node_rows(&[a, a], "Person")], &[]);
    assert!(message.contains("duplicate"), "{message}");

    let message = refusal(
        &[node_rows(&[a, b], "Person"), node_rows(&[b, c], "Person")],
        &[],
    );
    assert!(message.contains("duplicate"), "{message}");

    let message = refusal(
        &[node_rows(&[a, b], "Person")],
        &[
            edge_rows(&[e], "KNOWS", &[a], &[b]),
            edge_rows(&[e], "KNOWS", &[b], &[a]),
        ],
    );
    assert!(message.contains("duplicate"), "{message}");

    let message = refusal(
        &[node_rows(&[a, b], "Person")],
        &[edge_rows(&[a], "KNOWS", &[a], &[b])],
    );
    assert!(message.contains("duplicate"), "{message}");

    let message = refusal(
        &[node_rows(&[a], "Person")],
        &[edge_rows(&[e], "KNOWS", &[a], &[c])],
    );
    assert!(
        message.contains("edge endpoint UUID does not exist"),
        "{message}"
    );

    // An edge may not point at an edge, nor at itself.
    let message = refusal(
        &[node_rows(&[a, b], "Person")],
        &[
            edge_rows(&[e], "KNOWS", &[a], &[b]),
            edge_rows(&[v7(101)], "KNOWS", &[a], &[e]),
        ],
    );
    assert!(
        message.contains("edge endpoint is not a node UUID"),
        "{message}"
    );
    let message = refusal(
        &[node_rows(&[a], "Person")],
        &[edge_rows(&[e], "KNOWS", &[a], &[e])],
    );
    assert!(message.contains("duplicate"), "{message}");

    let message = refusal(&[node_rows(&[a], "not an identifier")], &[]);
    assert!(message.contains("invalid identifier"), "{message}");

    let message = refusal(
        &[node_rows(&[a, b], "Person")],
        &[edge_rows(&[e], "bad rel!", &[a], &[b])],
    );
    assert!(message.contains("invalid identifier"), "{message}");

    // Nothing at all names no graph. This has no node table and stays on the
    // ordinary route in the low-budget control.
    let message = refusal(&[], &[]);
    assert!(message.contains("no identities"), "{message}");
}

const CLOCK: i64 = 1_789_000_000_000_000;

/// Pin the recorded session clock of a construction before it stages anything,
/// so two builds of one input are comparable (the bulk builder stamps it into
/// the encoded topology).
fn pin_clock(root: &Path) {
    let path = root.join("checkpoint.json");
    let mut checkpoint: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    checkpoint["session_now_micros"] = serde_json::json!(CLOCK);
    fs::write(path, serde_json::to_vec(&checkpoint).unwrap()).unwrap();
}

fn read_checkpoint_clock(root: &Path) -> i64 {
    serde_json::from_slice::<serde_json::Value>(&fs::read(root.join("checkpoint.json")).unwrap())
        .unwrap()["session_now_micros"]
        .as_i64()
        .unwrap()
}

/// `(path, bytes, sha256)` of every encoded artifact except the ordinal
/// receipt, which carries a random rebuild nonce (ADR 0038).
fn encoded_inventory(root: &Path) -> BTreeMap<String, (u64, String)> {
    let inventory: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("encoded-v1/inventory.json")).unwrap()).unwrap();
    inventory["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|artifact| artifact["path"] != "topology/uuid-membership/ordinal-v4-receipt.json")
        .map(|artifact| {
            (
                artifact["path"].as_str().unwrap().to_owned(),
                (
                    artifact["bytes"].as_u64().unwrap(),
                    artifact["sha256"].as_str().unwrap().to_owned(),
                ),
            )
        })
        .collect()
}

/// Too small for the in-memory estimate, large enough for the node tables.
const SCRATCH_BUDGET: u64 = 800 << 20;
/// The fixed pool plus any positive node table exceeds this budget.
const NODE_SCRATCH_BUDGET: u64 = 512 << 20;

/// How a test pins the plan-time budget.
#[derive(Clone, Copy, Debug)]
enum Budget {
    /// Whatever the host derives.
    Host,
    Bytes(u64),
}

fn pin_budget(budget: Budget) {
    bulk_source::TEST_BUDGET.with(|cell| {
        cell.set(match budget {
            Budget::Bytes(bytes) => Some(bytes),
            Budget::Host => None,
        });
    });
}

/// The node-table route reports the exact pre-decode resident requirement.
fn required_scratch_bytes(error: &GfError) -> u64 {
    let GfError::Project { code, message } = error else {
        panic!("expected a typed resource refusal, got {error:?}");
    };
    assert_eq!(*code, graphforge_core::ProjectErrorCode::ResourceLimit);
    let required = message
        .strip_prefix("graph construction encoding: scratch requires ")
        .and_then(|message| message.split_once(" resident bytes before decoding; budget is "))
        .map(|(required, _)| required)
        .expect("exact pre-decode scratch admission diagnostic");
    required.parse().expect("resident byte requirement is u64")
}

/// Resolve a scratch minimum that also determines the budget-sized digest
/// buffer. Replanning at the first reported minimum increases that buffer, so
/// solve the small monotone fixed point before retrying.
fn stable_required_scratch_bytes(error: &GfError, initial_budget: u64) -> u64 {
    let required = required_scratch_bytes(error);
    let initial_source_level =
        bulk_source::Digests::for_build_budget(initial_budget, true).pending_budget_bytes();
    let fixed = required.saturating_sub(initial_source_level);
    let mut candidate = required;
    for _ in 0..64 {
        let source_level =
            bulk_source::Digests::for_build_budget(candidate, true).pending_budget_bytes();
        let next = fixed.saturating_add(source_level);
        if next <= candidate {
            return candidate;
        }
        candidate = next;
    }
    panic!("budget-sized source reservation did not converge");
}

#[test]
fn routing_is_memory_then_scratch_then_scratch_nodes() {
    let ids = (1..=30).map(v7).collect::<Vec<_>>();
    let edge_ids = (100..=160).map(v7).collect::<Vec<_>>();
    let from = (0..61).map(|i| ids[i % 30]).collect::<Vec<_>>();
    let to = (0..61).map(|i| ids[(i * 7 + 1) % 30]).collect::<Vec<_>>();
    for (budget, scratch, node_scratch) in [
        (Budget::Host, false, false),
        (Budget::Bytes(SCRATCH_BUDGET), true, false),
        (Budget::Bytes(NODE_SCRATCH_BUDGET), true, true),
    ] {
        pin_budget(budget);
        let (_directory, _project, graph) = fixture();
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&ids)])
            .unwrap();
        session
            .append_arrow(
                BulkInputKind::Edge,
                &[edge_rows(&edge_ids, "KNOWS", &from, &to)],
            )
            .unwrap();
        let construction = if node_scratch {
            let error = session.validate(&graph).unwrap_err();
            let required = stable_required_scratch_bytes(&error, NODE_SCRATCH_BUDGET);
            assert_eq!(graph.node_count("Person").unwrap(), 0);
            pin_budget(Budget::Bytes(required));
            let progress = session.validate(&graph);
            pin_budget(Budget::Host);
            progress.unwrap().construction.unwrap()
        } else {
            let progress = session.validate(&graph);
            pin_budget(Budget::Host);
            progress.unwrap().construction.unwrap()
        };
        // The route is decided once, at plan time, from the footers: the same
        // bytes on every route, and no budget stages an initial build.
        let report = construction.bulk_build.as_ref().expect("a bulk build");
        assert_eq!(construction.accepted_chunks, 0, "{budget:?}");
        assert_eq!(report.edge_partitions > 0, scratch, "{report:?}");
        assert_eq!(report.node_partitions > 0, node_scratch, "{report:?}");
        assert_eq!(report.scratch_write_bytes > 0, scratch, "{report:?}");
        assert_eq!(report.scratch_read_bytes, report.scratch_write_bytes);
        assert_eq!(
            report.node_scratch_read_bytes,
            report.node_scratch_write_bytes
        );
        assert_eq!(
            report.endpoint_scratch_read_bytes,
            report.endpoint_scratch_write_bytes
        );
        assert_eq!(report.endpoint_scratch_write_bytes > 0, node_scratch);
        session.commit(&graph, None).unwrap();
        assert_eq!(graph.node_count("Person").unwrap(), 30);
    }
}

#[test]
fn a_budget_below_the_fixed_workspace_refuses_on_the_bulk_route() {
    let ids = (1..=30).map(v7).collect::<Vec<_>>();
    let (_directory, _project, graph) = fixture();
    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    session
        .append_arrow(BulkInputKind::Node, &[nodes(&ids)])
        .unwrap();
    pin_budget(Budget::Bytes(512 << 10));
    let error = session.validate(&graph).unwrap_err();
    pin_budget(Budget::Host);
    assert!(matches!(
        error,
        GfError::Project {
            code: graphforge_core::ProjectErrorCode::ResourceLimit,
            ..
        }
    ));
    // No node count stages for want of memory: the session keeps the bulk
    // route, and a retry with room builds the same graph.
    session.validate(&graph).unwrap();
    session.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 30);
}

#[test]
fn edge_properties_use_scratch_and_reopen_with_their_values() {
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field};

    let ids = (1..=30).map(v7).collect::<Vec<_>>();
    let edge_ids = (100..=129).map(v7).collect::<Vec<_>>();
    let from = ids.clone();
    let to = (0..30).map(|i| ids[(i + 1) % 30]).collect::<Vec<_>>();
    let schema =
        bulk_edge_input_schema(vec![Field::new("weight", DataType::Float64, true)]).unwrap();
    let edges = RecordBatch::try_new(
        schema,
        vec![
            uuids(&edge_ids),
            Arc::new(StringArray::from(vec!["KNOWS"; 30])),
            uuids(&from),
            uuids(&to),
            Arc::new(Float64Array::from(vec![0.5; 30])),
        ],
    )
    .unwrap();
    for budget in [None, Some(920 << 20), Some(944 << 20)] {
        bulk_source::TEST_BUDGET.with(|cell| cell.set(budget));
        let (_directory, project, graph) = fixture();
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&ids)])
            .unwrap();
        session
            .append_arrow(BulkInputKind::Edge, std::slice::from_ref(&edges))
            .unwrap();
        let progress = session.validate(&graph);
        bulk_source::TEST_BUDGET.with(|cell| cell.set(None));
        let report = progress.unwrap().construction.unwrap().bulk_build.unwrap();
        assert_eq!(
            report.property_scratch_write_bytes > 0,
            budget.is_some(),
            "{report:?}"
        );
        session.commit(&graph, None).unwrap();
        assert_eq!(graph.node_count("Person").unwrap(), 30);
        drop(session);
        drop(graph);
        let reopened = GraphForge::new(project.to_str()).unwrap();
        let queried = reopened
            .execute("MATCH ()-[r:KNOWS]->() RETURN r.weight AS weight")
            .unwrap();
        let values = queried
            .batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column_by_name("weight")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..values.len())
                    .map(|row| values.value(row).to_bits())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![0.5_f64.to_bits(); 30]);
    }
}

#[test]
fn an_unallocatable_ipc_footer_refuses_before_any_build() {
    let (_directory, _project, graph) = fixture();
    let before = *graph.current_generation_uuid.lock().unwrap();
    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    session
        .append_arrow(BulkInputKind::Node, &[nodes(&[v7(1)])])
        .unwrap();
    bulk_source::TEST_BUDGET.with(|cell| cell.set(Some(1)));
    let error = session.validate(&graph).unwrap_err();
    bulk_source::TEST_BUDGET.with(|cell| cell.set(None));
    assert!(matches!(
        error,
        GfError::Project {
            code: graphforge_core::ProjectErrorCode::ResourceLimit,
            ..
        }
    ));
    assert!(error.to_string().contains("footer"));
    assert_eq!(*graph.current_generation_uuid.lock().unwrap(), before);
    session.validate(&graph).unwrap();
    session.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 1);
}

/// Rows whose UUID is null get a deterministic UUID derived from the operation,
/// the source sequence and the batch index, so a task boundary that moved a
/// batch boundary would change the published bytes.
fn null_uuid_nodes(count: usize) -> RecordBatch {
    let mut builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(count, 16);
    for index in 0..count {
        if index % 5 == 0 {
            builder
                .append_value(v7(1_000 + index as u128).as_bytes())
                .unwrap();
        } else {
            builder.append_null();
        }
    }
    RecordBatch::try_new(
        bulk_node_input_schema(Vec::new()).unwrap(),
        vec![
            Arc::new(builder.finish()),
            Arc::new(StringArray::from(vec!["Person"; count])),
        ],
    )
    .unwrap()
}

#[test]
fn row_groups_that_straddle_task_boundaries_publish_identical_bytes_on_every_route() {
    // 4-row batches, 16 batches per task (64 rows), 7-row row groups: every
    // task boundary falls inside a row group.
    let batch = null_uuid_nodes(300);
    let mut inventories = Vec::new();
    for budget in [
        Budget::Host,
        Budget::Bytes(SCRATCH_BUDGET),
        Budget::Bytes(NODE_SCRATCH_BUDGET),
    ] {
        pin_budget(budget);
        let source_dir = tempfile::tempdir().unwrap();
        let parquet = source_dir.path().join("nodes.parquet");
        let properties = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(7))
            .build();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            File::create(&parquet).unwrap(),
            batch.schema(),
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let (_directory, _project, graph) = fixture();
        let limits = ImportSessionLimits {
            batch_rows: 4,
            ..ImportSessionLimits::default()
        };
        let mut session = graph
            .begin_import_session(OperationId(v7(5)), limits)
            .unwrap();
        session
            .register_parquet(BulkInputKind::Node, &parquet)
            .unwrap();
        let construction = session.open_construction(&graph).unwrap();
        let root = graph
            .resolved_generation
            .container_root()
            .join(".graphforge-construction")
            .join(construction.session_uuid().simple().to_string());
        drop(construction);
        pin_clock(&root);
        let construction = if matches!(budget, Budget::Bytes(NODE_SCRATCH_BUDGET)) {
            let error = session.validate(&graph).unwrap_err();
            let required = stable_required_scratch_bytes(&error, NODE_SCRATCH_BUDGET);
            assert_eq!(graph.node_count("Person").unwrap(), 0);
            assert!(!root.join("encoded-v1/inventory.json").exists());
            // A failed initial build is restarted by the next validation. Pin
            // the replacement session, rather than the discarded checkpoint,
            // so all three real routes construct the same timestamp bytes.
            let failed = session.open_construction(&graph).unwrap();
            let replacement = session.restart_construction(&graph, failed).unwrap();
            let replacement_root = graph
                .resolved_generation
                .container_root()
                .join(".graphforge-construction")
                .join(replacement.session_uuid().simple().to_string());
            drop(replacement);
            pin_clock(&replacement_root);
            pin_budget(Budget::Bytes(required));
            let progress = session.validate(&graph);
            pin_budget(Budget::Host);
            progress.unwrap().construction.unwrap()
        } else {
            let progress = session.validate(&graph);
            pin_budget(Budget::Host);
            progress.unwrap().construction.unwrap()
        };
        let output_root = graph
            .resolved_generation
            .container_root()
            .join(".graphforge-construction")
            .join(
                session
                    .manifest
                    .construction_session_uuid
                    .expect("construction session was pinned")
                    .simple()
                    .to_string(),
            );
        let report = construction.bulk_build.expect("a bulk build");
        assert_eq!(
            report.node_partitions > 0,
            matches!(budget, Budget::Bytes(NODE_SCRATCH_BUDGET)),
            "{report:?}"
        );
        assert_eq!(read_checkpoint_clock(&output_root), CLOCK);
        inventories.push(encoded_inventory(&output_root));
        session.commit(&graph, None).unwrap();
        assert_eq!(graph.node_count("Person").unwrap(), 300);
    }
    assert_eq!(inventories[0], inventories[1]);
    assert_eq!(inventories[0], inventories[2]);
}

fn two_batch_import_with_a_cross_batch_duplicate(graph: &GraphForge) -> (GraphImportSession, Uuid) {
    let (a, b, c) = (v7(1), v7(2), v7(3));
    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    session
        .append_arrow(
            BulkInputKind::Node,
            &[node_rows(&[a, b], "Person"), node_rows(&[b, c], "Person")],
        )
        .unwrap();
    let id = session.session_uuid();
    (session, id)
}

#[test]
fn a_refused_bulk_attempt_retries_the_same_build_when_memory_drops() {
    let (_directory, _project, graph) = fixture();
    let (mut session, id) = two_batch_import_with_a_cross_batch_duplicate(&graph);
    // The first attempt fits in memory, seals the storage session and is refused.
    let first = session.validate(&graph).unwrap_err().to_string();
    assert!(first.contains("duplicate"), "{first}");
    // The route is durable: the same session, reopened or not, on a host whose
    // memory has since shrunk, refuses the resource shortage before loading
    // data instead of staging a sealed session or ignoring the smaller budget.
    for reopen in [false, true] {
        bulk_source::TEST_BUDGET.with(|cell| cell.set(Some(1)));
        let second = if reopen {
            graph
                .resume_import_session(id)
                .unwrap()
                .validate(&graph)
                .unwrap_err()
        } else {
            session.validate(&graph).unwrap_err()
        };
        bulk_source::TEST_BUDGET.with(|cell| cell.set(None));
        assert!(matches!(
            second,
            GfError::Project {
                code: graphforge_core::ProjectErrorCode::ResourceLimit,
                ..
            }
        ));
        assert!(
            !second.to_string().contains("not accepting chunks"),
            "{second}"
        );
        // Restoring memory retries the original sealed bulk build and proves
        // the original intake refusal is unchanged.
        let restored = graph
            .resume_import_session(id)
            .unwrap()
            .validate(&graph)
            .unwrap_err()
            .to_string();
        assert_eq!(first, restored);
    }
}

#[test]
fn an_append_import_stages_every_attempt() {
    let (_directory, _project, graph) = fixture();
    // An append stages: only an initial build runs on the bulk builder. The
    // first import gives the project its generation.
    let first_ids = (10..=12).map(v7).collect::<Vec<_>>();
    let mut first = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    first
        .append_arrow(BulkInputKind::Node, &[node_rows(&first_ids, "Person")])
        .unwrap();
    first.validate(&graph).unwrap();
    first.commit(&graph, None).unwrap();
    let (mut session, _) = two_batch_import_with_a_cross_batch_duplicate(&graph);
    let first = session.validate(&graph);
    assert!(first.is_err());
    // A retry stages again and refuses the same duplicate.
    let again = session.validate(&graph).unwrap_err().to_string();
    assert!(again.contains("duplicate"), "{again}");
    assert!(
        session
            .manifest
            .progress
            .construction
            .as_ref()
            .is_some_and(|construction| construction.bulk_build.is_none()
                && construction.accepted_chunks > 0)
    );
}

/// An import whose construction session already holds staged chunks of an
/// initial build (from a build that staged them) cannot resume on the bulk
/// builder: it is refused with an instruction to restart, and a fresh import of
/// the same sources builds.
#[test]
fn an_import_holding_staged_initial_chunks_is_refused_and_asks_for_a_restart() {
    let (_directory, _project, graph) = fixture();
    let ids = (1..=3).map(v7).collect::<Vec<_>>();
    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    session
        .append_arrow(BulkInputKind::Node, &[nodes(&ids)])
        .unwrap();
    // The construction session is opened on first use; an earlier build staged
    // the registered rows into it as chunks.
    let mut construction = session.open_construction(&graph).unwrap();
    construction
        .append_nodes(
            "staged-by-an-earlier-build",
            &RecordBatch::try_new(
                graphforge_storage::CONSTRUCTION_NODE_SCHEMA.clone(),
                vec![uuids(&ids), Arc::new(StringArray::from(vec!["Person"; 3]))],
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(construction.progress().accepted_chunks, 1);
    drop(construction);

    for _ in 0..2 {
        let refused = session.validate(&graph).unwrap_err();
        assert!(matches!(refused, GfError::Validation(_)), "{refused:?}");
        assert!(
            refused.to_string().contains("restart the import"),
            "{refused}"
        );
    }
    assert_eq!(graph.node_count("Person").unwrap(), 0);
    session.abort(&graph).unwrap();

    let mut restarted = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    restarted
        .append_arrow(BulkInputKind::Node, &[nodes(&ids)])
        .unwrap();
    let built = restarted.validate(&graph).unwrap();
    assert!(built.construction.unwrap().bulk_build.is_some());
    restarted.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 3);
}

#[test]
fn a_second_validate_of_a_built_session_reports_its_rows() {
    let (_directory, _project, graph) = fixture();
    let (a, b) = (v7(1), v7(2));
    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    session
        .append_arrow(BulkInputKind::Node, &[node_rows(&[a, b], "Person")])
        .unwrap();
    session
        .append_arrow(
            BulkInputKind::Edge,
            &[edge_rows(&[v7(100)], "KNOWS", &[a], &[b])],
        )
        .unwrap();
    let first = session.validate(&graph).unwrap();
    // Reuse of the pinned inventory builds nothing, and still says what it holds.
    let second = session.validate(&graph).unwrap();
    assert_eq!((first.rows_accepted, second.rows_accepted), (3, 3));
    let built = second.construction.unwrap().bulk_build.unwrap();
    assert_eq!((built.nodes, built.edges), (2, 1));
    session.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 2);
}

#[test]
fn a_refused_batch_counts_as_rejected_rows() {
    // Three rows that are not UUIDv7: the batch is refused by name.
    let (_directory, _project, graph) = fixture();
    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    let bad = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    session
        .append_arrow(BulkInputKind::Node, &[node_rows(&bad, "Person")])
        .unwrap();
    session.validate(&graph).unwrap_err();
    assert_eq!(session.manifest.progress.rows_rejected, 3);
    // The count is durable, not just in memory.
    assert_eq!(
        read_manifest(&session.root).unwrap().progress.rows_rejected,
        3
    );

    // A refusal that names no batch (a duplicate across batches) rejects no rows.
    let (mut session, _) = two_batch_import_with_a_cross_batch_duplicate(&graph);
    session.validate(&graph).unwrap_err();
    assert_eq!(session.manifest.progress.rows_rejected, 0);
}

const STRICT_ONTOLOGY: &str = "ontology_id: bulk\nversion: \"1\"\nentity_types:\n  - name: Host\n    abstract: false\nrelation_types:\n  - name: CONNECTS\n    src: Host\n    dst: Host\nproperties: []\n";

/// The bulk reader runs the same owner check as the staged path: under a strict
/// ontology an undeclared entity or relation type is refused before storage
/// sees the row, and the refused batch is the one counted as rejected. (A strict
/// single-ontology project cannot open a construction at all, so the reader is
/// driven directly.)
#[test]
fn the_bulk_reader_refuses_undeclared_types_under_a_strict_ontology() {
    let directory = tempfile::tempdir().unwrap();
    let project = directory.path().join("project");
    fs::create_dir(&project).unwrap();
    let ontology = directory.path().join("strict.yaml");
    fs::write(&ontology, STRICT_ONTOLOGY).unwrap();
    let mut graph = GraphForge::new(project.to_str()).unwrap();
    graph
        .adopt_ontology(crate::AdoptOntologyRequest {
            context: crate::WriteContext {
                operation_uuid: OperationId(v7(7)),
                actor_uuid: None,
            },
            path: ontology,
            mode: crate::OntologyMode::Strict,
        })
        .unwrap();

    let write = |name: &str, batch: &RecordBatch| {
        let path = directory.path().join(name);
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            File::create(&path).unwrap(),
            batch.schema(),
            None,
        )
        .unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        path
    };
    let refuse = |kind: BulkInputKind, path: &Path| {
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        session.register_parquet(kind, path).unwrap();
        let refusals = bulk_source::Refusals::default();
        let digests = bulk_source::Digests::default();
        let plan = session
            .plan_bulk_build(&graph, None, &refusals, &digests)
            .unwrap();
        let source = match kind {
            BulkInputKind::Node => &plan.nodes[0],
            BulkInputKind::Edge => &plan.edges[0],
        };
        let error = source
            .reader
            .read_task(0, &mut |_| Ok(()))
            .unwrap_err()
            .to_string();
        (error, refusals.take_rows())
    };

    let unknown_node = write(
        "unknown-nodes.parquet",
        &node_rows(&[v7(1), v7(2)], "Unknown"),
    );
    let (error, rows) = refuse(BulkInputKind::Node, &unknown_node);
    assert!(
        error.contains("unknown strict ontology entity type"),
        "{error}"
    );
    assert_eq!(rows, Some(2));

    let hosts = write("host-nodes.parquet", &node_rows(&[v7(1), v7(2)], "Host"));
    let (error, rows) = {
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        session
            .register_parquet(BulkInputKind::Node, &hosts)
            .unwrap();
        let refusals = bulk_source::Refusals::default();
        let digests = bulk_source::Digests::default();
        let plan = session
            .plan_bulk_build(&graph, None, &refusals, &digests)
            .unwrap();
        let mut batches = 0;
        let result = plan.nodes[0].reader.read_task(0, &mut |_| {
            batches += 1;
            Ok(())
        });
        (result.map(|()| batches), refusals.take_rows())
    };
    assert_eq!((error.unwrap(), rows), (1, None));

    let unknown_edge = write(
        "unknown-edges.parquet",
        &edge_rows(&[v7(100)], "UNDECLARED", &[v7(1)], &[v7(2)]),
    );
    let (error, rows) = refuse(BulkInputKind::Edge, &unknown_edge);
    assert!(
        error.contains("unknown strict ontology relationship type"),
        "{error}"
    );
    assert_eq!(rows, Some(1));
}

/// Pending digest reads use one build-scoped source reservation, separate from
/// each reader's decoded page workspace (#1918).
#[test]
fn the_digest_s_held_reads_are_counted_and_bounded_by_the_budget() {
    let (_directory, _project, graph) = fixture();
    let sources = tempfile::tempdir().unwrap();
    let path = sources.path().join("nodes.parquet");
    let batch = node_rows(&[v7(1), v7(2), v7(3)], "Person");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(File::create(&path).unwrap(), batch.schema(), None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let mut needs = Vec::new();
    for budget in [256_u64 << 20, 4 << 30, 64 << 30] {
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        session
            .register_parquet(BulkInputKind::Node, &path)
            .unwrap();
        bulk_source::TEST_BUDGET.with(|cell| cell.set(Some(budget)));
        let refusals = bulk_source::Refusals::default();
        let digests = bulk_source::Digests::for_build_budget(budget, true);
        let plan = session
            .plan_bulk_build(&graph, None, &refusals, &digests)
            .unwrap();
        bulk_source::TEST_BUDGET.with(|cell| cell.set(None));
        let needed = plan.nodes[0].reader.decoded_workspace_bytes();
        // Decoded page workspace and pending digest bytes have separate owners.
        assert!(needed <= budget / 64 + (1 << 20), "{budget}: {needed}");
        assert_eq!(
            u64::try_from(digests.pending_budget_bytes()).unwrap(),
            budget.min(1 << 30) / 64,
            "{budget}"
        );
        needs.push(needed);
        session.abort(&graph).unwrap();
    }
    // The page requirement is independent from the build-scoped digest cap.
    assert!(needs.iter().all(|needed| *needed < 1 << 20), "{needs:?}");
}

mod encodings;
