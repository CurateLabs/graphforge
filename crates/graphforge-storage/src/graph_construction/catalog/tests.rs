//! Runtime-catalog derivation for property-free schemas (#1455).
//!
//! A kind whose every chunk carries the bare canonical schema produces no
//! shaped row artifact; its catalog observations come from the details
//! family. The contract is that the catalog bytes are identical to the ones
//! a scan of the (single, property-free) row group would have produced.
//! The comparison here needs no test-only switch: appending an all-null
//! property column to the same rows forces the row path -- the schema is
//! property-bearing -- without adding a single property observation, so the
//! two catalogs must agree byte for byte.

use super::super::intake::write_parquet;
use super::super::tests::fixed;
use super::super::*;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::OntologyMode;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;
use tempfile::TempDir;

/// Fixed session clock for encoded topology metadata; catalog observations
/// independently derive their timestamp from the parent catalog.
const FIXED_NOW_MICROS: i64 = 1_789_000_000_000_000;
const LABELS: [&str; 3] = ["Person", "Company", "Place"];
const ROUTES: [&str; 4] = ["KNOWS", "WORKS_AT", "LIVES_IN", "OWNS"];
const NODE_CHUNKS: u128 = 3;
const EDGE_CHUNKS: u128 = 4;
const CHUNK_ROWS: usize = 256;
const NODE_COUNT: u128 = NODE_CHUNKS * CHUNK_ROWS as u128;
const EDGE_COUNT: u128 = EDGE_CHUNKS * CHUNK_ROWS as u128;
const EDGE_BASE: u128 = 1_000_000;

/// Labels and routes are interleaved across UUID order, so the intern order
/// -- first-seen sequence and per-name observation counts -- is sensitive to
/// the order rows are visited in.
fn label_of(uuid: u128) -> &'static str {
    LABELS[usize::try_from(uuid % LABELS.len() as u128).unwrap()]
}

fn route_of(uuid: u128) -> &'static str {
    ROUTES[usize::try_from((uuid / 3) % ROUTES.len() as u128).unwrap()]
}

/// With `null_property`, an all-null Int64 column is appended: the schema is
/// property-bearing, so shaping takes the row path, but no property is ever
/// observed by the catalog.
fn nodes(first: u128, null_property: bool) -> RecordBatch {
    let uuids = (first..first + CHUNK_ROWS as u128)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let labels = (first..first + CHUNK_ROWS as u128)
        .map(label_of)
        .collect::<Vec<_>>();
    let mut fields = CONSTRUCTION_NODE_SCHEMA
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let mut columns: Vec<ArrayRef> =
        vec![Arc::new(fixed(&uuids)), Arc::new(StringArray::from(labels))];
    if null_property {
        fields.push(Field::new("score", DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(vec![None::<i64>; CHUNK_ROWS])));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// `node_count` is how many nodes `1..=node_count` the session stages: every
/// endpoint must reference a staged node, or the endpoint family's
/// distinct-key balance check refuses the unreachable UUID band.
fn edges(first: u128, node_count: u128, null_property: bool) -> RecordBatch {
    let uuids = (first..first + CHUNK_ROWS as u128)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let routes = (first..first + CHUNK_ROWS as u128)
        .map(route_of)
        .collect::<Vec<_>>();
    let src = (first..first + CHUNK_ROWS as u128)
        .map(|edge| 1 + (edge * 7) % node_count)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let dst = (first..first + CHUNK_ROWS as u128)
        .map(|edge| 1 + (edge * 11 + 5) % node_count)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let mut fields = CONSTRUCTION_EDGE_SCHEMA
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(fixed(&uuids)),
        Arc::new(StringArray::from(routes)),
        Arc::new(fixed(&src)),
        Arc::new(fixed(&dst)),
    ];
    if null_property {
        fields.push(Field::new("weight", DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(vec![None::<i64>; CHUNK_ROWS])));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn pinned_session(root: &TempDir, operation: u128) -> GraphConstructionSession {
    let mut session = GraphConstructionSession::open_with_mode(
        root.path(),
        Uuid::from_u128(operation),
        0,
        OntologyMode::Exploratory,
        GraphConstructionBudgets {
            max_batch_rows: CHUNK_ROWS,
            max_run_records: 4 * CHUNK_ROWS,
            partition_count: 16,
            ..GraphConstructionBudgets::default()
        },
    )
    .unwrap();
    session.checkpoint.session_now_micros = FIXED_NOW_MICROS;
    session
}

/// Everything one ingest exposes about how its catalog was built.
#[derive(Debug)]
struct Run {
    shape: ConstructionShape,
    evidence: GraphConstructionEvidence,
    shaped_catalog_sha256: String,
    encoded_catalog_sha256: String,
    encoded_nodes: usize,
    encoded_edges: usize,
    entity_types: usize,
    relation_types: usize,
}

fn ingest(operation: u128, null_property: bool) -> Run {
    let root = TempDir::new().unwrap();
    let mut session = pinned_session(&root, operation);
    for chunk in 0..NODE_CHUNKS {
        session
            .append(
                ConstructionChunkKind::Node,
                &format!("nodes-{chunk}"),
                &nodes(1 + chunk * CHUNK_ROWS as u128, null_property),
            )
            .unwrap();
    }
    for chunk in 0..EDGE_CHUNKS {
        session
            .append(
                ConstructionChunkKind::Edge,
                &format!("edges-{chunk}"),
                &edges(
                    EDGE_BASE + chunk * CHUNK_ROWS as u128,
                    NODE_COUNT,
                    null_property,
                ),
            )
            .unwrap();
    }
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let evidence = session.evidence().clone();
    let shaped_catalog_sha256 = receipt_for_existing(&session.root, &shape.runtime_catalog)
        .unwrap()
        .sha256;
    let catalog = {
        let file = session
            .root
            .open_child_file(OsStr::new(&shape.runtime_catalog))
            .unwrap();
        let batches = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        graphforge_ir::RuntimeCatalog::from_record_batches(batches.iter()).unwrap()
    };
    let encoding = session.encode_canonical(&shape, 1).unwrap();
    // Ingest byte accounting for this path, so the two paths' I/O can be
    // compared on identical logical input (#1455).
    let after_encode = session.evidence();
    println!(
        "CATALOG_BYTES {}",
        serde_json::json!({
            "null_property": null_property,
            "shape": {
                "parquet_read_bytes": evidence.parquet_read_bytes,
                "parquet_read_operations": evidence.parquet_read_operations,
                "parquet_write_bytes": evidence.parquet_write_bytes,
                "parquet_write_operations": evidence.parquet_write_operations,
                "merge_read_bytes": evidence.merge_read_bytes,
                "merge_written_bytes": evidence.merge_written_bytes,
                "merge_fsync_operations": evidence.merge_fsync_operations,
                "shape_input_validation_read_bytes": evidence.shape_input_validation_read_bytes,
                "shaped_output_authentication_bytes": evidence.shaped_output_authentication_bytes,
                "shape_application_read_bytes": evidence.shape_application_read_bytes,
                "write_bytes": evidence.write_bytes,
                "fsync_operations": evidence.fsync_operations,
                "partition_outputs": evidence.partition_outputs,
                "storage_transient_peak_total_allocated_bytes":
                    evidence.storage_transient_peak_total_allocated_bytes,
            },
            "encode": {
                "input_read_bytes": encoding.evidence.input_read_bytes,
                "output_write_bytes": encoding.evidence.output_write_bytes,
                "source_spool_write_bytes": encoding.evidence.source_spool_write_bytes,
                "source_spool_read_bytes": encoding.evidence.source_spool_read_bytes,
                "source_spool_fsync_operations": encoding.evidence.source_spool_fsync_operations,
                "fsync_operations": encoding.evidence.fsync_operations,
                "application_read_bytes": after_encode.encode_application_read_bytes,
                "application_write_bytes": after_encode.encode_application_write_bytes,
            },
        })
    );
    let encoded_catalog_sha256 = encoding
        .artifacts
        .iter()
        .find(|artifact| artifact.path == "topology/runtime_catalog.parquet")
        .expect("encoding publishes the runtime catalog")
        .sha256
        .clone();
    let graph = root
        .path()
        .join(PRIVATE_ROOT)
        .join(Uuid::from_u128(operation).simple().to_string())
        .join(&encoding.root)
        .join("graph");
    let encoded_nodes = crate::read_nodes(&graph)
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let (files, _) = crate::capture_graph_files(&graph).unwrap();
    let admitted =
        crate::AuthenticatedPropertyInventory::from_inventory_at_root(&graph, files, None).unwrap();
    let encoded_edges = crate::read_edges_from_inventory(&admitted, "*", OntologyMode::Exploratory)
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    Run {
        shape,
        evidence,
        shaped_catalog_sha256,
        encoded_catalog_sha256,
        encoded_nodes,
        encoded_edges,
        entity_types: catalog.entity_types().len(),
        relation_types: catalog.relation_types().len(),
    }
}

/// The load-bearing check: a property-free session skips the row artifacts
/// and its catalog is byte-identical to the one the row scan produces.
#[test]
fn property_free_kinds_derive_the_catalog_from_details_byte_for_byte() {
    let derived = ingest(0x1455_0001, false);
    let scanned = ingest(0x1455_0002, true);

    // The property-free session produced no shaped row artifact for either
    // kind and decoded no Parquet batch for the catalog; the details
    // families that replace them are present.
    assert!(
        derived.shape.node_rows.is_empty(),
        "{:?}",
        derived.shape.node_rows
    );
    assert!(
        derived.shape.edge_rows.is_empty(),
        "{:?}",
        derived.shape.edge_rows
    );
    assert!(derived.shape.node_details.is_some());
    assert!(derived.shape.edge_details.is_some());
    assert_eq!(derived.evidence.peak_catalog_decoded_batch_bytes, 0);

    // The property-bearing session took the full row path.
    assert_eq!(scanned.shape.node_rows.len(), 1);
    assert_eq!(scanned.shape.edge_rows.len(), 1);
    assert!(scanned.shape.node_rows[0].starts_with("shaped-rows-0-"));
    assert!(scanned.shape.edge_rows[0].starts_with("shaped-rows-1-"));
    assert!(scanned.evidence.peak_catalog_decoded_batch_bytes > 0);

    // Same catalog bytes, shaped and published.
    assert_eq!(derived.shaped_catalog_sha256, scanned.shaped_catalog_sha256);
    assert_eq!(
        derived.encoded_catalog_sha256,
        scanned.encoded_catalog_sha256
    );
    assert_eq!(derived.entity_types, LABELS.len());
    assert_eq!(derived.relation_types, ROUTES.len());
    assert_eq!(
        derived.evidence.peak_catalog_entries,
        (LABELS.len() + ROUTES.len()) as u64
    );
    assert_eq!(
        derived.evidence.peak_catalog_entries,
        scanned.evidence.peak_catalog_entries
    );
    assert_eq!(
        derived.evidence.peak_catalog_identifier_bytes,
        scanned.evidence.peak_catalog_identifier_bytes
    );

    // The encoder's "no staged rows" guard is a count, not the row list:
    // every staged node and edge is published on both paths.
    for run in [&derived, &scanned] {
        assert_eq!(
            (run.shape.node_count, run.shape.edge_count),
            (NODE_COUNT as u64, EDGE_COUNT as u64)
        );
        assert_eq!(run.encoded_nodes, NODE_COUNT as usize);
        assert_eq!(run.encoded_edges, EDGE_COUNT as usize);
    }
    println!(
        "CATALOG_DERIVATION {}",
        serde_json::json!({
            "shaped_catalog_sha256": derived.shaped_catalog_sha256,
            "encoded_catalog_sha256": derived.encoded_catalog_sha256,
            "derived_row_artifacts": derived.shape.node_rows.len() + derived.shape.edge_rows.len(),
            "scanned_row_artifacts": scanned.shape.node_rows.len() + scanned.shape.edge_rows.len(),
            "nodes": derived.encoded_nodes,
            "edges": derived.encoded_edges,
        })
    );
}

/// A kind that mixes bare and property-bearing schemas keeps the row path:
/// its rows are grouped by exact schema before UUID, which the details
/// family's plain UUID order cannot reproduce.
#[test]
fn a_kind_mixing_bare_and_property_schemas_keeps_its_row_artifacts() {
    let root = TempDir::new().unwrap();
    let mut session = pinned_session(&root, 0x1455_0003);
    session
        .append(ConstructionChunkKind::Node, "bare", &nodes(1, false))
        .unwrap();
    session
        .append(
            ConstructionChunkKind::Node,
            "property",
            &nodes(1 + CHUNK_ROWS as u128, true),
        )
        .unwrap();
    session
        .append(
            ConstructionChunkKind::Edge,
            "edges",
            &edges(EDGE_BASE, 2 * CHUNK_ROWS as u128, false),
        )
        .unwrap();
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    // Nodes: two exact-schema row groups, scanned. Edges: bare only, derived.
    assert_eq!(shape.node_rows.len(), 2);
    assert!(shape.edge_rows.is_empty());
    assert!(session.evidence().peak_catalog_decoded_batch_bytes > 0);
    assert_eq!(
        (shape.node_count, shape.edge_count),
        (2 * CHUNK_ROWS as u64, CHUNK_ROWS as u64)
    );
}

#[test]
fn bulk_catalog_observations_use_parent_history_or_epoch_without_advancing_time() {
    use arrow::array::{TimestampMicrosecondArray, UInt64Array};

    for (times, expected_time) in [
        (None, 0),
        (Some([11, 13, 17, 19]), 19),
        (Some([11, 29, 17, 19]), 29),
        (Some([11, 13, 29, 19]), 29),
        (Some([-100, -90, -80, -70]), -70),
        (Some([11, 13, 17, i64::MAX]), i64::MAX),
    ] {
        let root = TempDir::new().unwrap();
        let directory = StableDirectory::open(root.path()).unwrap();
        let mut parent = RuntimeCatalog::new();
        if let Some([first, last, relation, property]) = times {
            parent.intern_label_at("Person", first).unwrap();
            parent.intern_label_at("Person", last).unwrap();
            parent
                .intern_relation_type_at("UNOBSERVED", relation)
                .unwrap();
            parent
                .intern_property_at("legacy", Some("Person"), property)
                .unwrap();
        }
        // Encode/decode the parent through the durable carrier before deriving time.
        let parent = RuntimeCatalog::from_record_batch(&parent.to_record_batch()).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("label", DataType::Utf8, false),
                Field::new("score", DataType::Int64, true),
            ])),
            vec![
                Arc::new(fixed(&[
                    1_u128.to_be_bytes(),
                    2_u128.to_be_bytes(),
                    3_u128.to_be_bytes(),
                ])),
                Arc::new(StringArray::from(vec!["Person", "Person", "New"])),
                Arc::new(Int64Array::from(vec![Some(7), None, None])),
            ],
        )
        .unwrap();
        let mut evidence = GraphConstructionEvidence::default();
        for category in crate::ArtifactCategory::ALL {
            evidence
                .storage_current
                .insert(category, Default::default());
            evidence
                .storage_receipt_category_authorities
                .insert(category, Default::default());
            evidence
                .storage_transient_peak_allocated_bytes
                .insert(category, 0);
            evidence
                .storage_receipt_transient_peak_authorities
                .insert(category, 0);
        }
        write_parquet(&directory, "nodes.parquet", &batch, &mut evidence).unwrap();
        let output = build_runtime_catalog(
            parent,
            &directory,
            CatalogSource::Rows(&["nodes.parquet".to_owned()]),
            CatalogSource::Rows(&[]),
            DetailCodec::from_version(FORMAT_VERSION).unwrap(),
            GraphConstructionBudgets::default(),
            &mut || false,
            &mut evidence,
        )
        .unwrap();
        let batches = ParquetRecordBatchReaderBuilder::try_new(
            directory.open_child_file(OsStr::new(&output)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let result = RuntimeCatalog::from_record_batches(batches.iter())
            .unwrap()
            .to_record_batch();
        let names = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = result
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let first = result
            .column(4)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let last = result
            .column(5)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for row in 0..result.num_rows() {
            let expected = match (names.value(row), times) {
                ("Person", Some(times)) => (4, times[0], expected_time),
                ("Person", None) => (2, 0, 0),
                ("UNOBSERVED", Some(times)) => (1, times[2], times[2]),
                ("legacy", Some(times)) => (1, times[3], times[3]),
                ("New" | "score", _) => (1, expected_time, expected_time),
                other => panic!("unexpected catalog entry {other:?}"),
            };
            assert_eq!(
                (counts.value(row), first.value(row), last.value(row)),
                expected
            );
        }
        assert_eq!(result.num_rows(), if times.is_some() { 5 } else { 3 });
    }
}
