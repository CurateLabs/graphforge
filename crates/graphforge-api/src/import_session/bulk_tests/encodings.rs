//! Rows of every encoding whose expansion differs from its stored bytes
//! (dictionary, delta-encoded and plain strings, nullable integers, lists), with
//! derived identities, in row groups that straddle tasks: the resident route, the
//! route through scratch and the route with node tables on scratch publish the
//! same artifacts (path,
//! length and SHA-256), catalog IDs and derived identities included (#1918).

use arrow::array::{Int64Array, Int64Builder, ListBuilder, StringDictionaryBuilder};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::basic::Encoding;
use parquet::file::properties::WriterProperties;

use super::*;

const SCRATCH: u64 = 1_100 << 20;
// Start below the bounded node-scratch minimum, then retry at its stable
// budget-sized digest boundary. Initial builds no longer stage on this route.
const NODE_SCRATCH_PROBE: u64 = 512 << 10;
const ROWS: usize = 120;
const EDGES: usize = 90;

fn identities(values: &[Option<Uuid>]) -> ArrayRef {
    let mut builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        match value {
            Some(value) => builder.append_value(value.as_bytes()).unwrap(),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn dictionary(value: impl Fn(usize) -> String) -> ArrayRef {
    let mut builder = StringDictionaryBuilder::<Int32Type>::new();
    for row in 0..ROWS {
        builder.append_value(value(row));
    }
    Arc::new(builder.finish())
}

fn nodes() -> (RecordBatch, Vec<Uuid>) {
    let ids = (0..ROWS)
        .map(|row| (row % 5 == 0).then(|| v7(100 + row as u128)))
        .collect::<Vec<_>>();
    let mut lists = ListBuilder::new(Int64Builder::new());
    for row in 0..ROWS {
        for child in 0..(row % 4) {
            lists.values().append_value((row * 10 + child) as i64);
        }
        lists.append(row % 11 != 0);
    }
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("a_dict", dictionary(|row| format!("entry-{}", row % 7))),
        (
            "b_delta",
            Arc::new(StringArray::from(
                (0..ROWS)
                    .map(|row| format!("{}{row:08}", "p".repeat(32)))
                    .collect::<Vec<_>>(),
            )),
        ),
        (
            "c_plain",
            Arc::new(StringArray::from(
                (0..ROWS)
                    .map(|row| (row % 6 != 0).then(|| format!("plain-{row:05}")))
                    .collect::<Vec<_>>(),
            )),
        ),
        (
            "d_int",
            Arc::new(Int64Array::from(
                (0..ROWS)
                    .map(|row| (row % 9 != 0).then_some(row as i64 * 3))
                    .collect::<Vec<_>>(),
            )),
        ),
        ("e_list", Arc::new(lists.finish())),
        // Enough decoded bytes that the planner routes this small input through
        // scratch under a 1.1 GiB budget.
        ("zz_pad", dictionary(|_| "z".repeat(400 << 10))),
    ];
    let mut fields = vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), true),
        Field::new("label", DataType::Utf8, false),
    ];
    fields.extend(
        columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true)),
    );
    let mut arrays: Vec<ArrayRef> = vec![
        identities(&ids),
        Arc::new(StringArray::from(vec!["Thing"; ROWS])),
    ];
    arrays.extend(columns.into_iter().map(|(_, array)| array));
    (
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap(),
        ids.into_iter().flatten().collect(),
    )
}

/// Edges between the nodes that carry their own identity, two thirds with one.
fn edges(anchors: &[Uuid]) -> RecordBatch {
    let edge_ids = (0..EDGES)
        .map(|row| (row % 3 != 0).then(|| v7(2_000 + row as u128)))
        .collect::<Vec<_>>();
    let sources = (0..EDGES)
        .map(|row| Some(anchors[row * 7 % anchors.len()]))
        .collect::<Vec<_>>();
    let targets = (0..EDGES)
        .map(|row| Some(anchors[(row * 11 + 3) % anchors.len()]))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("edge_uuid", DataType::FixedSizeBinary(16), true),
            Field::new("rel_type", DataType::Utf8, false),
            Field::new("source_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("target_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("weight", DataType::Int64, true),
        ])),
        vec![
            identities(&edge_ids),
            Arc::new(StringArray::from(vec!["LINKS"; EDGES])),
            identities(&sources),
            identities(&targets),
            Arc::new(Int64Array::from(
                (0..EDGES as i64).map(|row| row * 5).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn write(path: &Path, batch: &RecordBatch, properties: WriterProperties) {
    let mut writer = ArrowWriter::try_new_with_options(
        File::create(path).unwrap(),
        batch.schema(),
        ArrowWriterOptions::new()
            .with_properties(properties)
            .with_skip_arrow_metadata(true),
    )
    .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn every_encoding_publishes_identical_bytes_across_bulk_routes() {
    let (node_batch, anchors) = nodes();
    let sources = tempfile::tempdir().unwrap();
    let nodes_path = sources.path().join("nodes.parquet");
    write(
        &nodes_path,
        &node_batch,
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(7))
            .set_dictionary_page_size_limit(64 << 20)
            .set_column_dictionary_enabled("b_delta".into(), false)
            .set_column_encoding("b_delta".into(), Encoding::DELTA_BYTE_ARRAY)
            .set_column_dictionary_enabled("c_plain".into(), false)
            .build(),
    );
    let edges_path = sources.path().join("edges.parquet");
    write(
        &edges_path,
        &edges(&anchors),
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(7))
            .build(),
    );

    let mut inventories = Vec::new();
    for budget in [None, Some(SCRATCH), Some(NODE_SCRATCH_PROBE)] {
        bulk_source::TEST_BUDGET.with(|cell| cell.set(budget));
        let (_directory, _project, graph) = fixture();
        let limits = ImportSessionLimits {
            batch_rows: 4,
            ..ImportSessionLimits::default()
        };
        let mut session = graph
            .begin_import_session(OperationId(v7(5)), limits)
            .unwrap();
        session
            .register_parquet(BulkInputKind::Node, &nodes_path)
            .unwrap();
        session
            .register_parquet(BulkInputKind::Edge, &edges_path)
            .unwrap();
        let construction = session.open_construction(&graph).unwrap();
        let mut root = graph
            .resolved_generation
            .container_root()
            .join(".graphforge-construction")
            .join(construction.session_uuid().simple().to_string());
        drop(construction);
        pin_clock(&root);
        let progress = if budget == Some(NODE_SCRATCH_PROBE) {
            let error = session.validate(&graph).unwrap_err();
            let required = stable_required_scratch_bytes(&error, NODE_SCRATCH_PROBE);
            let failed = session.open_construction(&graph).unwrap();
            let replacement = session.restart_construction(&graph, failed).unwrap();
            root = graph
                .resolved_generation
                .container_root()
                .join(".graphforge-construction")
                .join(replacement.session_uuid().simple().to_string());
            drop(replacement);
            pin_clock(&root);
            pin_budget(Budget::Bytes(required));
            let progress = session.validate(&graph);
            pin_budget(Budget::Host);
            progress
        } else {
            let progress = session.validate(&graph);
            bulk_source::TEST_BUDGET.with(|cell| cell.set(None));
            progress
        };
        let construction = progress.unwrap().construction.unwrap();
        let report = construction.bulk_build.as_ref().expect("a bulk build");
        if budget == Some(SCRATCH) {
            assert!(report.property_scratch_write_bytes > 0, "{report:?}");
        }
        if budget == Some(NODE_SCRATCH_PROBE) {
            assert!(report.node_partitions > 0, "{report:?}");
        }
        session.commit(&graph, None).unwrap();
        assert_eq!(graph.node_count("Thing").unwrap(), ROWS as u64);
        inventories.push(encoded_inventory(&root));
    }
    assert!(inventories[0].len() > 10, "{:?}", inventories[0].keys());
    assert_eq!(inventories[0], inventories[1]);
    assert_eq!(inventories[0], inventories[2]);
}
