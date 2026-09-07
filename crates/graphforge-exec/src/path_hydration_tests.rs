//! Moved hydration semantic tests exercise the execution-owned implementation.
use super::*;
use datafusion::logical_expr::ScalarUDFImpl;
#[derive(Clone)]
struct TestHydration {
    dir: Option<std::path::PathBuf>,
    labels_by_type: Vec<(graphforge_value::EntityTypeId, String)>,
    prop_stems: Vec<String>,
    fields: arrow::datatypes::Fields,
}
thread_local! {
    static LAST_RESOURCE: std::cell::RefCell<Option<Arc<HydrationResource>>> = const { std::cell::RefCell::new(None) };
    static TEST_CANCELLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static TEST_BUDGET: std::cell::Cell<usize> = const { std::cell::Cell::new(usize::MAX) };
}
struct PathHydrationTestGuard;
impl PathHydrationTestGuard {
    fn arm() -> Self {
        path_hydration_stats::reset();
        Self
    }
}
impl Drop for PathHydrationTestGuard {
    fn drop(&mut self) {
        path_hydration_stats::reset();
    }
}
fn test_udf(hydrate: TestHydration) -> HydratedPathNodes {
    let dir = hydrate.dir.unwrap();
    let catalog = Arc::new(
        graphforge_storage::GraphCatalog::open(&dir, None, &graphforge_ir::RuntimeCatalog::new())
            .unwrap(),
    );
    let tables = hydrate
        .prop_stems
        .iter()
        .map(|stem| catalog.property_table(&dir, stem))
        .collect();
    let graph = Arc::new(crate::read_resource::GraphReadContext {
        dir,
        catalog,
        mode: graphforge_core::OntologyMode::Exploratory,
        ontology: None,
    });
    let pool: Arc<dyn MemoryPool> = Arc::new(
        datafusion::execution::memory_pool::GreedyMemoryPool::new(TEST_BUDGET.get()),
    );
    let resource = HydrationResource::new(graph, pool);
    if TEST_CANCELLED.get() {
        resource.cancel();
    }
    LAST_RESOURCE.set(Some(Arc::clone(&resource)));
    HydratedPathNodes {
        signature: Signature::any(2, Volatility::Volatile),
        descriptor: PathNodeHydration {
            labels_by_type: hydrate.labels_by_type,
            prop_stems: hydrate.prop_stems,
            fields: hydrate.fields,
        },
        tables,
        resource,
    }
}
mod path_hydration_stats {
    use super::*;
    pub(super) fn reset() {
        LAST_RESOURCE.set(None);
        TEST_CANCELLED.set(false);
        TEST_BUDGET.set(usize::MAX);
    }
    pub(super) fn set_cancelled(value: bool) {
        TEST_CANCELLED.set(value);
    }
    pub(super) fn set_memory_budget(value: usize) {
        TEST_BUDGET.set(value);
    }
    pub(super) struct Snapshot {
        pub unique_uuids_requested: u64,
        pub unique_uuids_resolved_labels: u64,
        pub node_batches_read: u64,
        pub node_rows_examined: u64,
        pub node_rows_gathered: u64,
        pub property_stems_opened: u64,
        pub property_batches_read: u64,
        pub property_rows_examined: u64,
        pub property_rows_gathered: u64,
        pub peak_gathered_entries: u64,
    }
    pub(super) fn snapshot() -> Snapshot {
        LAST_RESOURCE.with_borrow(|r| {
            let r = r.as_ref().unwrap();
            let n = |name| r.counters[name].value() as u64;
            Snapshot {
                unique_uuids_requested: n("requested"),
                unique_uuids_resolved_labels: n("resolved"),
                node_batches_read: n("node_batches"),
                node_rows_examined: n("node_rows"),
                node_rows_gathered: n("node_gathered"),
                property_stems_opened: n("stems"),
                property_batches_read: n("property_batches"),
                property_rows_examined: n("property_rows"),
                property_rows_gathered: n("property_gathered"),
                peak_gathered_entries: n("gathered_entries"),
            }
        })
    }
}
/// A 16-byte uuid stand-in: byte `b` repeated.
fn uuid16(b: u8) -> Vec<u8> {
    vec![b; 16]
}

/// Build a `List<Struct{src_uuid, dst_uuid}>` edge-list column. Each edge
/// is `(src_byte, dst_byte)` in **storage** orientation; `None` rows are
/// null lists.
fn edge_list(rows: &[Option<&[(u8, u8)]>]) -> datafusion::arrow::array::ArrayRef {
    use datafusion::arrow::array::{FixedSizeBinaryBuilder, ListBuilder, StructBuilder};
    use datafusion::arrow::datatypes::Field;

    let fields: datafusion::arrow::datatypes::Fields = vec![
        Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
    ]
    .into();
    let mut b = ListBuilder::new(StructBuilder::new(
        fields,
        vec![
            Box::new(FixedSizeBinaryBuilder::new(16)),
            Box::new(FixedSizeBinaryBuilder::new(16)),
        ],
    ));
    for row in rows {
        let Some(edges) = row else {
            b.append_null();
            continue;
        };
        for (src, dst) in *edges {
            b.values()
                .field_builder::<FixedSizeBinaryBuilder>(0)
                .unwrap()
                .append_value(uuid16(*src))
                .unwrap();
            b.values()
                .field_builder::<FixedSizeBinaryBuilder>(1)
                .unwrap()
                .append_value(uuid16(*dst))
                .unwrap();
            b.values().append(true);
        }
        b.append(true);
    }
    std::sync::Arc::new(b.finish())
}

/// Build the seed-uuid column; `None` entries are null seeds.
fn seed_uuids(vals: &[Option<u8>]) -> datafusion::arrow::array::ArrayRef {
    use datafusion::arrow::array::FixedSizeBinaryBuilder;
    let mut b = FixedSizeBinaryBuilder::new(16);
    for v in vals {
        match v {
            Some(x) => b.append_value(uuid16(*x)).unwrap(),
            None => b.append_null(),
        }
    }
    std::sync::Arc::new(b.finish())
}

fn path_node_bytes(out: &datafusion::arrow::array::ArrayRef, i: usize) -> Option<Vec<u8>> {
    use datafusion::arrow::array::{Array, FixedSizeBinaryArray, ListArray, StructArray};
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    if list.is_null(i) {
        return None;
    }
    let items = list.value(i);
    let items = items.as_any().downcast_ref::<StructArray>().unwrap();
    let uuids = items.column_by_name("node_uuid").unwrap();
    let uuids = uuids
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    Some((0..uuids.len()).map(|j| uuids.value(j)[0]).collect())
}

/// Hydrated `cypher_path_nodes` invoke over a real topology directory (#705).
fn invoke_hydrated_path_nodes(
    hydrate: TestHydration,
    seed: datafusion::arrow::array::ArrayRef,
    rels: datafusion::arrow::array::ArrayRef,
) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef> {
    invoke_hydrated_path_nodes_with_batch_size(hydrate, seed, rels, 8_192)
}

fn invoke_hydrated_path_nodes_with_batch_size(
    hydrate: TestHydration,
    seed: datafusion::arrow::array::ArrayRef,
    rels: datafusion::arrow::array::ArrayRef,
    batch_size: usize,
) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef> {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    let udf = test_udf(hydrate);
    let n = seed.len();
    let mut config = ConfigOptions::default();
    config.execution.batch_size = batch_size.max(1);
    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::clone(&seed)),
            ColumnarValue::Array(Arc::clone(&rels)),
        ],
        arg_fields: vec![
            Arc::new(Field::new("seed", seed.data_type().clone(), true)),
            Arc::new(Field::new("rels", rels.data_type().clone(), true)),
        ],
        number_rows: n,
        return_field: Arc::new(Field::new("nodes", udf.return_type(&[])?, true)),
        config_options: Arc::new(config),
    };
    udf.invoke_with_args(args).map(|v| match v {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => s.to_array_of_size(n).unwrap(),
    })
}

fn path_node_label_lists(
    out: &datafusion::arrow::array::ArrayRef,
    row: usize,
) -> Option<Vec<Vec<Option<String>>>> {
    use datafusion::arrow::array::{Array, ListArray, StringArray, StructArray};
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    if list.is_null(row) {
        return None;
    }
    let items = list.value(row);
    let items = items.as_any().downcast_ref::<StructArray>().unwrap();
    let labels = items
        .column_by_name("labels")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    Some(
        (0..labels.len())
            .map(|i| {
                let values = labels.value(i);
                let strings = values.as_any().downcast_ref::<StringArray>().unwrap();
                (0..strings.len())
                    .map(|j| (!strings.is_null(j)).then(|| strings.value(j).to_owned()))
                    .collect()
            })
            .collect(),
    )
}

#[test]
fn hydrated_path_nodes_preserve_full_type_ids_labels() {
    // #705: multi-label nodes keep every catalog-resolved label from
    // authoritative `type_ids` (not the legacy primary `type_id` alone).
    let _guard = PathHydrationTestGuard::arm();
    use datafusion::arrow::array::FixedSizeBinaryArray;
    use datafusion::arrow::datatypes::Field;
    use graphforge_core::OntologyMode;
    use graphforge_core::uuid::{new_v7, to_bytes};
    use graphforge_storage::GraphWriter;

    let dir = tempfile::TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 0).unwrap();
    let multi = new_v7();
    let single = new_v7();
    let unknown_only = new_v7();
    w.create_node_with_labels(
        multi,
        &[
            graphforge_value::EntityTypeId::decode(1).unwrap(),
            graphforge_value::EntityTypeId::decode(3).unwrap(),
        ],
    )
    .unwrap();
    w.create_node_with_labels(
        single,
        &[graphforge_value::EntityTypeId::decode(2).unwrap()],
    )
    .unwrap();
    // type_ids present but absent from the baked catalog → empty label list.
    w.create_node_with_labels(
        unknown_only,
        &[graphforge_value::EntityTypeId::decode(99).unwrap()],
    )
    .unwrap();
    w.flush().unwrap();

    let multi_bytes = to_bytes(&multi);
    let single_bytes = to_bytes(&single);
    let unknown_bytes = to_bytes(&unknown_only);
    let missing_bytes = [0xABu8; 16];

    let hydrate = TestHydration {
        dir: Some(dir.path().to_path_buf()),
        labels_by_type: vec![
            (
                graphforge_value::EntityTypeId::decode(1).unwrap(),
                "Person".to_owned(),
            ),
            (
                graphforge_value::EntityTypeId::decode(2).unwrap(),
                "Company".to_owned(),
            ),
            (
                graphforge_value::EntityTypeId::decode(3).unwrap(),
                "Employee".to_owned(),
            ),
        ],
        prop_stems: vec![],
        fields: vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
        ]
        .into(),
    };

    // Zero-hop walk over four seeds: multi-label, single-label, missing
    // catalog id, and uuid absent from topology.
    let seed = std::sync::Arc::new(
        FixedSizeBinaryArray::try_from_iter(
            [multi_bytes, single_bytes, unknown_bytes, missing_bytes]
                .iter()
                .copied(),
        )
        .unwrap(),
    ) as datafusion::arrow::array::ArrayRef;
    let rels = edge_list(&[Some(&[]), Some(&[]), Some(&[]), Some(&[])]);
    let out = invoke_hydrated_path_nodes(hydrate, seed, rels).unwrap();
    let labels = path_node_label_lists(&out, 0).unwrap();
    assert_eq!(
        labels[0],
        vec![Some("Person".into()), Some("Employee".into())],
        "multi-label node keeps full type_ids set in catalog order"
    );
    // Remaining seeds are separate rows (one zero-hop path each).
    let labels1 = path_node_label_lists(&out, 1).unwrap();
    assert_eq!(labels1[0], vec![Some("Company".into())]);
    let labels2 = path_node_label_lists(&out, 2).unwrap();
    assert_eq!(
        labels2[0],
        Vec::<Option<String>>::new(),
        "unknown catalog ids skipped"
    );
    let labels3 = path_node_label_lists(&out, 3).unwrap();
    assert_eq!(
        labels3[0],
        vec![None],
        "missing topology row keeps a single-null labels element"
    );

    // Repeated node on a self-loop walk must repeat the full label set.
    let seed_loop = std::sync::Arc::new(
        FixedSizeBinaryArray::try_from_iter([multi_bytes].iter().copied()).unwrap(),
    ) as datafusion::arrow::array::ArrayRef;
    // Build a one-edge list with real uuids (not the byte-tag helper).
    use datafusion::arrow::array::{FixedSizeBinaryBuilder, ListBuilder, StructBuilder};
    let fields: datafusion::arrow::datatypes::Fields = vec![
        Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
    ]
    .into();
    let mut b = ListBuilder::new(StructBuilder::new(
        fields,
        vec![
            Box::new(FixedSizeBinaryBuilder::new(16)),
            Box::new(FixedSizeBinaryBuilder::new(16)),
        ],
    ));
    b.values()
        .field_builder::<FixedSizeBinaryBuilder>(0)
        .unwrap()
        .append_value(multi_bytes)
        .unwrap();
    b.values()
        .field_builder::<FixedSizeBinaryBuilder>(1)
        .unwrap()
        .append_value(multi_bytes)
        .unwrap();
    b.values().append(true);
    b.append(true);
    let loop_rels = std::sync::Arc::new(b.finish()) as datafusion::arrow::array::ArrayRef;

    let hydrate2 = TestHydration {
        dir: Some(dir.path().to_path_buf()),
        labels_by_type: vec![
            (
                graphforge_value::EntityTypeId::decode(1).unwrap(),
                "Person".to_owned(),
            ),
            (
                graphforge_value::EntityTypeId::decode(2).unwrap(),
                "Company".to_owned(),
            ),
            (
                graphforge_value::EntityTypeId::decode(3).unwrap(),
                "Employee".to_owned(),
            ),
        ],
        prop_stems: vec![],
        fields: vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
        ]
        .into(),
    };
    let looped = invoke_hydrated_path_nodes(hydrate2, seed_loop, loop_rels).unwrap();
    let loop_labels = path_node_label_lists(&looped, 0).unwrap();
    assert_eq!(loop_labels.len(), 2);
    assert_eq!(loop_labels[0], loop_labels[1]);
    assert_eq!(
        loop_labels[0],
        vec![Some("Person".into()), Some("Employee".into())]
    );
}

fn path_node_prop_strings(
    out: &datafusion::arrow::array::ArrayRef,
    row: usize,
    field: &str,
) -> Option<Vec<Option<String>>> {
    use datafusion::arrow::array::{Array, ListArray, StringArray, StructArray};
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    if list.is_null(row) {
        return None;
    }
    let items = list.value(row);
    let items = items.as_any().downcast_ref::<StructArray>().unwrap();
    let col = items
        .column_by_name(field)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    Some(
        (0..col.len())
            .map(|i| (!col.is_null(i)).then(|| col.value(i).to_owned()))
            .collect(),
    )
}

#[test]
fn hydrated_path_nodes_sparse_selection_bounds_gather_work() {
    // #706: a small path in a large property table gathers only requested
    // UUIDs, stops reading once they are found, and never materializes the
    // full stem via concat.
    let _guard = PathHydrationTestGuard::arm();
    use std::collections::HashMap;

    use datafusion::arrow::array::FixedSizeBinaryArray;
    use datafusion::arrow::datatypes::Field;
    use graphforge_core::OntologyMode;
    use graphforge_core::uuid::{new_v7, to_bytes};
    use graphforge_ir::IrLiteral;
    use graphforge_storage::GraphWriter;

    let dir = tempfile::TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 0).unwrap();
    let keep_a = new_v7();
    let keep_b = new_v7();
    w.create_node_with_labels(
        keep_a,
        &[graphforge_value::EntityTypeId::decode(1).unwrap()],
    )
    .unwrap();
    w.create_node_with_labels(
        keep_b,
        &[graphforge_value::EntityTypeId::decode(1).unwrap()],
    )
    .unwrap();
    w.set_properties(
        &keep_a,
        None,
        HashMap::from([("name".into(), IrLiteral::Str("Ada".into()))]),
    )
    .unwrap();
    w.set_properties(
        &keep_b,
        None,
        HashMap::from([("name".into(), IrLiteral::Str("Bob".into()))]),
    )
    .unwrap();
    // Many irrelevant property rows after the selected pair.
    for i in 0..200 {
        let u = new_v7();
        w.create_node_with_labels(u, &[graphforge_value::EntityTypeId::decode(1).unwrap()])
            .unwrap();
        w.set_properties(
            &u,
            None,
            HashMap::from([("name".into(), IrLiteral::Str(format!("filler-{i}")))]),
        )
        .unwrap();
    }
    w.flush().unwrap();

    let a = to_bytes(&keep_a);
    let b = to_bytes(&keep_b);
    let hydrate = TestHydration {
        dir: Some(dir.path().to_path_buf()),
        labels_by_type: vec![(
            graphforge_value::EntityTypeId::decode(1).unwrap(),
            "Person".to_owned(),
        )],
        prop_stems: vec!["_untyped".to_owned()],
        fields: vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
            Field::new("name", DataType::Utf8, true),
        ]
        .into(),
    };
    // Repeated UUID in the walk: hydrate once, emit twice publicly.
    let seed =
        std::sync::Arc::new(FixedSizeBinaryArray::try_from_iter([a].iter().copied()).unwrap())
            as datafusion::arrow::array::ArrayRef;
    use datafusion::arrow::array::{FixedSizeBinaryBuilder, ListBuilder, StructBuilder};
    let edge_fields: datafusion::arrow::datatypes::Fields = vec![
        Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
    ]
    .into();
    let mut edge_b = ListBuilder::new(StructBuilder::new(
        edge_fields,
        vec![
            Box::new(FixedSizeBinaryBuilder::new(16)),
            Box::new(FixedSizeBinaryBuilder::new(16)),
        ],
    ));
    edge_b
        .values()
        .field_builder::<FixedSizeBinaryBuilder>(0)
        .unwrap()
        .append_value(a)
        .unwrap();
    edge_b
        .values()
        .field_builder::<FixedSizeBinaryBuilder>(1)
        .unwrap()
        .append_value(b)
        .unwrap();
    edge_b.values().append(true);
    edge_b
        .values()
        .field_builder::<FixedSizeBinaryBuilder>(0)
        .unwrap()
        .append_value(b)
        .unwrap();
    edge_b
        .values()
        .field_builder::<FixedSizeBinaryBuilder>(1)
        .unwrap()
        .append_value(a)
        .unwrap();
    edge_b.values().append(true);
    edge_b.append(true);
    let rels = std::sync::Arc::new(edge_b.finish()) as datafusion::arrow::array::ArrayRef;

    path_hydration_stats::reset();
    let out =
        invoke_hydrated_path_nodes_with_batch_size(hydrate.clone(), seed.clone(), rels.clone(), 8)
            .unwrap();
    let snap = path_hydration_stats::snapshot();
    assert_eq!(snap.unique_uuids_requested, 2);
    assert_eq!(snap.unique_uuids_resolved_labels, 2);
    assert!(snap.node_batches_read > 0);
    assert!(snap.property_batches_read > 0);
    assert_eq!(snap.node_rows_gathered, 2);
    assert_eq!(snap.property_rows_gathered, 2);
    assert!(
        snap.property_rows_examined < 50,
        "early-exit must avoid examining all filler rows: examined {}",
        snap.property_rows_examined
    );
    assert!(
        snap.node_rows_examined < 50,
        "early-exit must avoid examining all filler node rows: examined {}",
        snap.node_rows_examined
    );
    assert!(
        snap.peak_gathered_entries <= 2,
        "gather map peak must stay within unique requested UUIDs"
    );
    assert_eq!(
        path_node_prop_strings(&out, 0, "name").unwrap(),
        vec![Some("Ada".into()), Some("Bob".into()), Some("Ada".into())]
    );
    let labels = path_node_label_lists(&out, 0).unwrap();
    assert_eq!(labels.len(), 3);
    assert_eq!(labels[0], vec![Some("Person".into())]);
    assert_eq!(labels[2], labels[0]);

    // Batch-size / reopen parity: same public values under a larger batch.
    path_hydration_stats::reset();
    let out_large = invoke_hydrated_path_nodes_with_batch_size(hydrate, seed, rels, 8_192).unwrap();
    assert_eq!(
        path_node_prop_strings(&out, 0, "name"),
        path_node_prop_strings(&out_large, 0, "name")
    );
    assert_eq!(
        path_node_label_lists(&out, 0),
        path_node_label_lists(&out_large, 0)
    );
}

#[test]
fn hydrated_path_nodes_coalesce_properties_across_stems() {
    // #807: complementary properties in two stems must both hydrate.
    // `Company` sorts before `Person`, so first-stem-only gather would keep
    // `title` and leave later-stem `name` null.
    let _guard = PathHydrationTestGuard::arm();
    use std::collections::HashMap;

    use datafusion::arrow::array::{
        FixedSizeBinaryArray, FixedSizeBinaryBuilder, ListBuilder, StructBuilder,
    };
    use datafusion::arrow::datatypes::Field;
    use graphforge_core::OntologyMode;
    use graphforge_core::uuid::{new_v7, to_bytes};
    use graphforge_ir::IrLiteral;
    use graphforge_storage::GraphWriter;

    let dir = tempfile::TempDir::new().unwrap();
    let keep = new_v7();
    {
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Advisory, 0).unwrap();
        w.create_node_with_labels(
            keep,
            &[
                graphforge_value::EntityTypeId::decode(1).unwrap(),
                graphforge_value::EntityTypeId::decode(2).unwrap(),
            ],
        )
        .unwrap();
        w.set_properties(
            &keep,
            Some("Company"),
            HashMap::from([("title".into(), IrLiteral::Str("CEO".into()))]),
        )
        .unwrap();
        for i in 0..200 {
            let filler = new_v7();
            w.create_node_with_labels(
                filler,
                &[graphforge_value::EntityTypeId::decode(2).unwrap()],
            )
            .unwrap();
            w.set_properties(
                &filler,
                Some("Company"),
                HashMap::from([("title".into(), IrLiteral::Str(format!("filler-title-{i}")))]),
            )
            .unwrap();
        }
        w.set_properties(
            &keep,
            Some("Person"),
            HashMap::from([("name".into(), IrLiteral::Str("Ada".into()))]),
        )
        .unwrap();
        for i in 0..200 {
            let filler = new_v7();
            w.create_node_with_labels(
                filler,
                &[graphforge_value::EntityTypeId::decode(1).unwrap()],
            )
            .unwrap();
            w.set_properties(
                &filler,
                Some("Person"),
                HashMap::from([("name".into(), IrLiteral::Str(format!("filler-name-{i}")))]),
            )
            .unwrap();
        }
        w.flush().unwrap();
    }

    let bytes = to_bytes(&keep);
    let hydrate = TestHydration {
        dir: Some(dir.path().to_path_buf()),
        labels_by_type: vec![
            (
                graphforge_value::EntityTypeId::decode(1).unwrap(),
                "Person".to_owned(),
            ),
            (
                graphforge_value::EntityTypeId::decode(2).unwrap(),
                "Company".to_owned(),
            ),
        ],
        prop_stems: vec!["Company".to_owned(), "Person".to_owned()],
        fields: vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
            Field::new("name", DataType::Utf8, true),
            Field::new("title", DataType::Utf8, true),
        ]
        .into(),
    };
    let seed =
        std::sync::Arc::new(FixedSizeBinaryArray::try_from_iter([bytes].iter().copied()).unwrap())
            as datafusion::arrow::array::ArrayRef;
    let edge_fields: datafusion::arrow::datatypes::Fields = vec![
        Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
    ]
    .into();
    let mut edge_b = ListBuilder::new(StructBuilder::new(
        edge_fields,
        vec![
            Box::new(FixedSizeBinaryBuilder::new(16)),
            Box::new(FixedSizeBinaryBuilder::new(16)),
        ],
    ));
    edge_b
        .values()
        .field_builder::<FixedSizeBinaryBuilder>(0)
        .unwrap()
        .append_value(bytes)
        .unwrap();
    edge_b
        .values()
        .field_builder::<FixedSizeBinaryBuilder>(1)
        .unwrap()
        .append_value(bytes)
        .unwrap();
    edge_b.values().append(true);
    edge_b.append(true);
    let rels = std::sync::Arc::new(edge_b.finish()) as datafusion::arrow::array::ArrayRef;

    path_hydration_stats::reset();
    let out =
        invoke_hydrated_path_nodes_with_batch_size(hydrate.clone(), seed.clone(), rels.clone(), 8)
            .unwrap();
    let snap = path_hydration_stats::snapshot();
    assert_eq!(snap.unique_uuids_requested, 1);
    assert_eq!(snap.property_stems_opened, 2);
    assert_eq!(snap.property_rows_gathered, 2);
    assert!(
        snap.peak_gathered_entries <= 1,
        "gather map peak must stay within unique requested UUIDs, got {}",
        snap.peak_gathered_entries
    );
    assert!(
        snap.property_rows_examined < 50,
        "per-stem early-exit must avoid examining filler rows: examined {}",
        snap.property_rows_examined
    );
    let names = path_node_prop_strings(&out, 0, "name").unwrap();
    let titles = path_node_prop_strings(&out, 0, "title").unwrap();
    assert_eq!(names, vec![Some("Ada".into()), Some("Ada".into())]);
    assert_eq!(titles, vec![Some("CEO".into()), Some("CEO".into())]);

    path_hydration_stats::reset();
    let out_large = invoke_hydrated_path_nodes_with_batch_size(
        hydrate.clone(),
        seed.clone(),
        rels.clone(),
        8_192,
    )
    .unwrap();
    assert_eq!(
        path_node_prop_strings(&out, 0, "name"),
        path_node_prop_strings(&out_large, 0, "name")
    );
    assert_eq!(
        path_node_prop_strings(&out, 0, "title"),
        path_node_prop_strings(&out_large, 0, "title")
    );

    drop(GraphWriter::open_at(dir.path(), OntologyMode::Advisory, 0).unwrap());
    path_hydration_stats::reset();
    let out_reopen = invoke_hydrated_path_nodes_with_batch_size(hydrate, seed, rels, 8).unwrap();
    assert_eq!(
        path_node_prop_strings(&out, 0, "name"),
        path_node_prop_strings(&out_reopen, 0, "name")
    );
    assert_eq!(
        path_node_prop_strings(&out, 0, "title"),
        path_node_prop_strings(&out_reopen, 0, "title")
    );
}

#[test]
fn hydrated_path_nodes_cancel_and_resource_limit_are_structured() {
    let _guard = PathHydrationTestGuard::arm();
    use datafusion::arrow::array::FixedSizeBinaryArray;
    use datafusion::arrow::datatypes::Field;
    use graphforge_core::OntologyMode;
    use graphforge_core::uuid::{new_v7, to_bytes};
    use graphforge_storage::GraphWriter;

    let dir = tempfile::TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 0).unwrap();
    let u = new_v7();
    w.create_node_with_labels(u, &[graphforge_value::EntityTypeId::decode(1).unwrap()])
        .unwrap();
    for _ in 0..32 {
        w.create_node_with_labels(
            new_v7(),
            &[graphforge_value::EntityTypeId::decode(1).unwrap()],
        )
        .unwrap();
    }
    w.flush().unwrap();
    let bytes = to_bytes(&u);
    let hydrate = TestHydration {
        dir: Some(dir.path().to_path_buf()),
        labels_by_type: vec![(
            graphforge_value::EntityTypeId::decode(1).unwrap(),
            "Person".to_owned(),
        )],
        prop_stems: vec![],
        fields: vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
        ]
        .into(),
    };
    let seed =
        std::sync::Arc::new(FixedSizeBinaryArray::try_from_iter([bytes].iter().copied()).unwrap())
            as datafusion::arrow::array::ArrayRef;
    let rels = edge_list(&[Some(&[])]);

    path_hydration_stats::set_cancelled(true);
    let err =
        invoke_hydrated_path_nodes_with_batch_size(hydrate.clone(), seed.clone(), rels.clone(), 4)
            .expect_err("cancelled hydration must fail closed");
    assert!(
        err.to_string().contains("cancelled"),
        "expected structured cancel, got {err}"
    );
    path_hydration_stats::set_cancelled(false);

    path_hydration_stats::reset();
    path_hydration_stats::set_memory_budget(1);
    let err = invoke_hydrated_path_nodes_with_batch_size(hydrate, seed, rels, 4)
        .expect_err("resource limit must fail closed");
    assert!(
        matches!(
            err,
            datafusion::common::DataFusionError::ResourcesExhausted(_)
        ),
        "expected structured resource error, got {err}"
    );
}

fn minimal_fixture() -> (tempfile::TempDir, TestHydration) {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = graphforge_storage::GraphWriter::open_at(
        dir.path(),
        graphforge_core::OntologyMode::Exploratory,
        0,
    )
    .unwrap();
    writer
        .create_node_with_labels(
            graphforge_core::uuid::Uuid::from_bytes([1; 16]),
            &[graphforge_value::EntityTypeId::decode(1).unwrap()],
        )
        .unwrap();
    writer.flush().unwrap();
    let descriptor = TestHydration {
        dir: Some(dir.path().to_path_buf()),
        labels_by_type: vec![(
            graphforge_value::EntityTypeId::decode(1).unwrap(),
            "Person".into(),
        )],
        prop_stems: vec![],
        fields: vec![
            arrow::datatypes::Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            arrow::datatypes::Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
        ]
        .into(),
    };
    (dir, descriptor)
}

#[test]
fn resource_pool_refusal_releases_reservations_and_queries_are_isolated() {
    let (_dir, descriptor) = minimal_fixture();
    path_hydration_stats::reset();
    let first = test_udf(descriptor.clone());
    let second = test_udf(descriptor.clone());
    assert!(!Arc::ptr_eq(&first.resource, &second.resource));
    first.resource.cancel();
    assert!(first.resource.check().is_err());
    second.resource.check().unwrap();
    let output = invoke_hydrated_path_nodes(
        descriptor.clone(),
        seed_uuids(&[Some(1)]),
        edge_list(&[Some(&[])]),
    )
    .unwrap();
    assert_eq!(path_node_bytes(&output, 0), Some(vec![1]));
    LAST_RESOURCE.with_borrow(|r| assert_eq!(r.as_ref().unwrap().pool.reserved(), 0));
    path_hydration_stats::set_memory_budget(1);
    let error =
        invoke_hydrated_path_nodes(descriptor, seed_uuids(&[Some(1)]), edge_list(&[Some(&[])]))
            .unwrap_err();
    assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
    LAST_RESOURCE.with_borrow(|r| assert_eq!(r.as_ref().unwrap().pool.reserved(), 0));
    path_hydration_stats::reset();
}

#[test]
fn nested_quantifier_propagates_actual_hydration_reservation_error() {
    use datafusion::common::{DFSchema, ScalarValue};
    use datafusion::logical_expr::{Expr as DfExpr, execution_props::ExecutionProps};
    use graphforge_ir::{ExprArena, IrExpr, IrLiteral, VarId};
    let (_dir, descriptor) = minimal_fixture();
    path_hydration_stats::reset();
    path_hydration_stats::set_memory_budget(1);
    let udf = Arc::new(ScalarUDF::new_from_impl(test_udf(descriptor)));
    let seed = ScalarValue::try_from_array(&seed_uuids(&[Some(1)]), 0).unwrap();
    let rels = ScalarValue::try_from_array(&edge_list(&[Some(&[])]), 0).unwrap();
    for nonempty in [false, true] {
        let mut arena = ExprArena::new();
        let value = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let list = arena.push(IrExpr::ListLiteral(if nonempty {
            vec![value]
        } else {
            vec![]
        }));
        let var = arena.push(IrExpr::VarRef(VarId(0)));
        let predicate = arena.push(IrExpr::BinaryOp {
            op: graphforge_ir::BinaryOpKind::Gt,
            left: var,
            right: value,
        });
        let quantifier = arena.push(IrExpr::Quantifier {
            kind: graphforge_ir::QuantifierKind::All,
            loop_var: VarId(0),
            list,
            predicate,
        });
        let vars = graphforge_rel::VarMap::new();
        let lowered = graphforge_rel::ExprLowerer::new(&arena, None, &vars)
            .lower(quantifier)
            .unwrap();
        let rewritten = graphforge_rel::expr::rewrite_embedded_expressions(lowered, &mut |expr| {
            if matches!(&expr, DfExpr::ScalarFunction(call) if call.func.name() == "cypher_cmp_pred") {
                return Ok(udf
                    .call(vec![
                        datafusion::prelude::lit(seed.clone()),
                        datafusion::prelude::lit(rels.clone()),
                    ])
                    .is_not_null());
            }
            Ok(expr)
        })
        .unwrap();
        let physical = datafusion::physical_expr::create_physical_expr(
            &rewritten,
            &DFSchema::empty(),
            &ExecutionProps::new(),
        )
        .unwrap();
        let batch = arrow::record_batch::RecordBatch::try_new_with_options(
            Arc::new(arrow::datatypes::Schema::empty()),
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();
        let result = physical.evaluate(&batch);
        if nonempty {
            assert!(
                matches!(result, Err(DataFusionError::ResourcesExhausted(_))),
                "actual predicate result: {result:?}"
            );
        } else {
            assert!(
                result.is_ok(),
                "empty list must not evaluate predicate: {result:?}"
            );
        }
    }
    LAST_RESOURCE.with_borrow(|r| assert_eq!(r.as_ref().unwrap().pool.reserved(), 0));
    path_hydration_stats::reset();
}

#[tokio::test]
async fn wrapper_preserves_sibling_partitions_and_requires_the_planned_pool() {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::TryStreamExt;
    let _guard = PathHydrationTestGuard::arm();
    let (_dir, descriptor) = minimal_fixture();
    let original = test_udf(descriptor);
    let session = datafusion::prelude::SessionContext::new();
    let resource = HydrationResource::new(
        Arc::clone(&original.resource.graph),
        Arc::clone(session.task_ctx().memory_pool()),
    );
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(arrow::array::Int64Array::from(vec![7]))],
    )
    .unwrap();
    let input: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[vec![], vec![batch.clone()]], schema, None).unwrap();
    let wrapper = HydrationExec {
        input: Arc::clone(&input),
        resource: Arc::clone(&resource),
    };
    assert!(Arc::ptr_eq(wrapper.properties(), input.properties()));
    let empty = wrapper
        .execute(0, session.task_ctx())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(empty.is_empty());
    resource.check().unwrap();
    let nonempty = wrapper
        .execute(1, session.task_ctx())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(nonempty, vec![batch]);
    assert!(
        matches!(wrapper.execute(1, datafusion::prelude::SessionContext::new().task_ctx()), Err(DataFusionError::Execution(message)) if message.contains("memory pool differs"))
    );
    let plan: Arc<dyn ExecutionPlan> = Arc::new(wrapper);
    drop(QueryGuard(plan));
    assert!(matches!(
        resource.check(),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(resource.pool.reserved(), 0);
}

#[test]
fn many_unique_nodes_reserve_hash_capacity_before_reading() {
    let _guard = PathHydrationTestGuard::arm();
    let (_dir, descriptor) = minimal_fixture();
    // The old occupied-entry estimate would admit this budget, despite missing
    // spare buckets/control bytes. Rejection must precede topology reads.
    let count = 200;
    let old_entry_bytes = std::mem::size_of::<([u8; 16], Vec<graphforge_value::EntityTypeId>)>()
        + std::mem::size_of::<([u8; 16], Vec<PropRowLoc>)>()
        + 32;
    path_hydration_stats::set_memory_budget(count * old_entry_bytes);
    let seeds: Vec<_> = (1..=200).map(Some).collect();
    let edges: Vec<Option<&[(u8, u8)]>> = vec![Some(&[]); count];
    let error =
        invoke_hydrated_path_nodes(descriptor, seed_uuids(&seeds), edge_list(&edges)).unwrap_err();
    assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
    LAST_RESOURCE.with_borrow(|resource| {
        let resource = resource.as_ref().unwrap();
        assert_eq!(resource.counters["node_batches"].value(), 0);
        assert_eq!(resource.pool.reserved(), 0);
    });
}

thread_local! {
    static PAUSE_COLLECT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static PAUSED_RESOURCE: std::cell::RefCell<Option<Arc<HydrationResource>>> = const { std::cell::RefCell::new(None) };
}
pub(super) async fn pause_collect(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    fn resource(
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> Option<Arc<HydrationResource>> {
        if let Some(wrapper) = plan.downcast_ref::<HydrationExec>() {
            return Some(Arc::clone(&wrapper.resource));
        }
        plan.children().into_iter().find_map(resource)
    }
    if PAUSE_COLLECT.get() {
        if let Some(resource) = resource(plan) {
            PAUSED_RESOURCE.set(Some(resource));
            std::future::pending::<()>().await;
        }
    }
}

#[tokio::test]
async fn public_write_prefix_and_suffix_drop_cancel_their_planned_hydration() {
    use graphforge_ir::{Binder, OntologyMode, RuntimeCatalog};
    use std::sync::Mutex;
    let root = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let bind = |query: &str| {
        Binder::new(None, Arc::clone(&catalog), OntologyMode::Exploratory)
            .bind(&graphforge_cypher::parse(query).unwrap())
            .unwrap()
    };
    let session = || {
        crate::ExecutionSession::new_with_target(
            graphforge_storage::GraphCatalog::open(root.path(), None, &catalog.lock().unwrap())
                .unwrap(),
            None,
            root.path().to_path_buf(),
            OntologyMode::Exploratory,
        )
        .unwrap()
    };
    let create = bind(
        "CREATE (a:Person {name:'a', mark:0}), (b:Person {name:'b', mark:0}), (a)-[:KNOWS]->(b)",
    );
    session().execute_create(&create).await.unwrap();
    for query in [
        "MATCH p=(a:Person)-[:KNOWS*1..2]->(b:Person) SET a.mark=1 RETURN nodes(p) AS ns",
        "MATCH p=(a:Person)-[:KNOWS*1..2]->(b:Person) WITH a,nodes(p) AS ns SET a.mark=size(ns)",
    ] {
        let plan = bind(query);
        let session = session();
        PAUSE_COLLECT.set(true);
        let mut future = Box::pin(session.execute_write_statement(&plan));
        std::future::poll_fn(|cx| {
            use std::future::Future;
            let result = future.as_mut().poll(cx);
            assert!(
                result.is_pending(),
                "write {query} must pause before collection: {result:?}"
            );
            if PAUSED_RESOURCE.with_borrow(Option::is_some) {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await;
        let resource = PAUSED_RESOURCE
            .take()
            .expect("actual write plan reached hydration collect");
        resource.check().unwrap();
        drop(future);
        PAUSE_COLLECT.set(false);
        assert!(matches!(
            resource.check(),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(resource.pool.reserved(), 0);
        // The unpaused public path still produces the query's real result.
        session.execute_write_statement(&plan).await.unwrap();
    }
}

#[test]
fn selected_rows_across_many_stems_release_unretained_batch_reservations() {
    use graphforge_core::uuid::Uuid;
    let _guard = PathHydrationTestGuard::arm();
    let dir = tempfile::tempdir().unwrap();
    let mut writer = graphforge_storage::GraphWriter::open_at(
        dir.path(),
        graphforge_core::OntologyMode::Advisory,
        0,
    )
    .unwrap();
    let selected = Uuid::from_bytes([1; 16]);
    let labels: Vec<_> = (1..=16)
        .map(|id| graphforge_value::EntityTypeId::decode(id).unwrap())
        .collect();
    writer.create_node_with_labels(selected, &labels).unwrap();
    let stems: Vec<_> = (0..16).map(|index| format!("Stem{index:02}")).collect();
    for stem in &stems {
        writer
            .set_properties(
                &selected,
                Some(stem),
                HashMap::from([(
                    "value".into(),
                    graphforge_ir::IrLiteral::Str("selected".into()),
                )]),
            )
            .unwrap();
        for row in 2..=65u8 {
            let uuid = graphforge_core::uuid::new_v7();
            writer.create_node_with_labels(uuid, &labels[..1]).unwrap();
            writer
                .set_properties(
                    &uuid,
                    Some(stem),
                    HashMap::from([(
                        "value".into(),
                        graphforge_ir::IrLiteral::Str(format!("{row}{}", "x".repeat(512))),
                    )]),
                )
                .unwrap();
        }
    }
    writer.flush().unwrap();
    let descriptor = TestHydration {
        dir: Some(dir.path().to_path_buf()),
        labels_by_type: labels.into_iter().zip(stems.clone()).collect(),
        prop_stems: stems,
        fields: vec![
            arrow::datatypes::Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            arrow::datatypes::Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
            arrow::datatypes::Field::new("value", DataType::Utf8, true),
        ]
        .into(),
    };
    let providers = test_udf(descriptor.clone());
    let mut largest_batch = 0;
    let mut all_batches = 0;
    for table in &providers.tables {
        table
            .visit_authenticated_batches(64, |batch| {
                largest_batch = largest_batch.max(batch.get_array_memory_size());
                all_batches += batch.get_array_memory_size();
                Ok(true)
            })
            .unwrap();
    }
    // Permit two complete scan batches, but not retaining all sixteen source
    // batches when only one small row from each stem is needed.
    let budget = 2 * largest_batch;
    assert!(all_batches > budget);
    path_hydration_stats::set_memory_budget(budget);
    let output = invoke_hydrated_path_nodes_with_batch_size(
        descriptor,
        seed_uuids(&[Some(1)]),
        edge_list(&[Some(&[])]),
        64,
    )
    .unwrap();
    assert_eq!(path_node_bytes(&output, 0), Some(vec![1]));
    let list = output
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    let row = list.value(0);
    let node = row
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .unwrap();
    let value = node
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(value.value(0), "selected");
    LAST_RESOURCE.with_borrow(|resource| assert_eq!(resource.as_ref().unwrap().pool.reserved(), 0));
}
