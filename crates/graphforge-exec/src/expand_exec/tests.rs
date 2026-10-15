use super::hop_read::batches_bytes;
use super::*;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use std::sync::Arc;

#[test]
fn unused_list_output_preserves_exact_non_nullable_child_schema() {
    let item = Arc::new(Field::new("item", DataType::UInt32, false));
    let field = Field::new("type_ids", DataType::List(Arc::clone(&item)), false);
    let column = unused_expand_column(&field, 3).unwrap();

    assert_eq!(column.data_type(), field.data_type());
    assert_eq!(column.len(), 3);
    assert_eq!(column.null_count(), 0);
}

#[test]
fn required_v4_destination_identity_fails_closed_without_admitted_session() {
    let error = require_admitted_ordinal_identity(true, false)
        .expect_err("required v4 identity must not fall back without its admitted session");
    assert!(
        error
            .to_string()
            .contains("requires admitted v4 ordinal identity")
    );
    require_admitted_ordinal_identity(true, true).expect("admitted v4 session");
    require_admitted_ordinal_identity(false, false).expect("legacy generation fallback");
}

/// A real `ExpandExec` over a real multi-partition input (#1985, #1984).
///
/// The traversal expands each input partition on its own, so it has as many
/// output partitions as its input. The whole plan is estimated from the whole
/// input; a partition is its even share of that, rounded up, so the shares
/// never sum to less than the whole. The estimate is a byte size, never a row
/// count (see `expand_statistics`).
#[test]
fn a_partitioned_single_hop_expansion_estimates_its_even_share_of_the_whole_plan() {
    use datafusion::common::stats::Precision;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::logical_plan::LogicalTableSource;
    use graphforge_core::uuid::{Uuid, new_v7};

    const TS: i64 = 1_700_000_000_000_000;
    const RING: usize = 100;
    const DEGREE: usize = 3;
    const SEEDS: usize = 50;

    let dir = tempfile::TempDir::new().unwrap();
    let person = graphforge_value::EntityTypeId::decode(0).unwrap();
    let mut writer =
        graphforge_storage::GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    let uuids: Vec<Uuid> = (0..RING).map(|_| new_v7()).collect();
    let ids: Vec<u64> = uuids
        .iter()
        .map(|uuid| writer.create_node(*uuid, person).unwrap())
        .collect();
    for (index, source) in uuids.iter().enumerate() {
        for offset in 1..=DEGREE {
            writer
                .create_edge(new_v7(), "KNOWS", source, &uuids[(index + offset) % RING])
                .unwrap();
        }
    }
    writer.flush().unwrap();
    graphforge_storage::adjacency::build_adjacency_index(dir.path(), TS).unwrap();

    let catalog = Arc::new(
        graphforge_storage::GraphCatalog::open(
            dir.path(),
            None,
            &graphforge_ir::RuntimeCatalog::new(),
        )
        .unwrap(),
    );
    let provider: Arc<dyn AdjacencyProvider> = Arc::new(
        adjacency::PersistentAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict)
            .with_inventory(catalog.admitted_inventory().unwrap()),
    );
    let resource = read_resource::GraphReadContext {
        health: crate::mutation::MutationHealth::default(),
        dir: dir.path().to_path_buf(),
        mode: OntologyMode::Strict,
        catalog,
        ontology: None,
    };

    let frontier = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
        "node_id",
        DataType::UInt64,
        false,
    )]));
    let logical = LogicalPlanBuilder::scan(
        "var_0",
        Arc::new(LogicalTableSource::new(Arc::clone(&frontier))),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let node = graphforge_plan::ExpandNode::new(
        Arc::new(logical),
        "KNOWS",
        0,
        2,
        1,
        Direction::Out,
        Some(graphforge_value::RelationTypeId::decode(0).unwrap()),
        vec![],
        vec![],
        vec![],
    );

    for partitions in [1_usize, 2, 3, 4] {
        let chunks: Vec<Vec<RecordBatch>> = ids[..SEEDS]
            .chunks(SEEDS.div_ceil(partitions))
            .map(|chunk| {
                vec![
                    RecordBatch::try_new(
                        Arc::clone(&frontier),
                        vec![Arc::new(UInt64Array::from(chunk.to_vec()))],
                    )
                    .unwrap(),
                ]
            })
            .collect();
        let input = MemorySourceConfig::try_new_exec(&chunks, Arc::clone(&frontier), None).unwrap();
        assert_eq!(
            input.properties().output_partitioning().partition_count(),
            partitions
        );

        let exec = ExpandExec::new(&node, input, Arc::clone(&provider), None, false, &resource);
        assert_eq!(
            exec.properties().output_partitioning().partition_count(),
            partitions
        );
        // A row is charged the width of each fixed-width column and 64 bytes for
        // every other, as the join memory rule charges it.
        let row_width: usize = exec
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().primitive_width().unwrap_or(64))
            .sum();
        let whole = SEEDS * DEGREE * row_width;
        let statistics = exec.partition_statistics(None).unwrap();
        assert_eq!(statistics.num_rows, Precision::Absent);
        assert_eq!(
            statistics.total_byte_size,
            Precision::Inexact(whole),
            "{partitions} partitions"
        );
        let mut shares = 0;
        for partition in 0..partitions {
            let statistics = exec.partition_statistics(Some(partition)).unwrap();
            assert_eq!(statistics.num_rows, Precision::Absent);
            let share = statistics.total_byte_size;
            assert_eq!(
                share,
                Precision::Inexact(whole.div_ceil(partitions)),
                "{partitions} partitions, partition {partition}"
            );
            if let Precision::Inexact(bytes) = share {
                shares += bytes;
            }
        }
        assert!(
            shares >= whole,
            "{partitions} partition estimates sum to {shares}, below the whole plan {whole}"
        );
    }
}

/// A legacy project of `count` nodes whose label sets cycle through
/// `{0, 1}`, `{1}` and `{0}`, each node knowing the next two around the ring.
///
/// Returns the ordinals in creation order.
fn labelled_ring(dir: &std::path::Path, count: usize) -> Vec<u64> {
    use graphforge_core::uuid::new_v7;

    const TS: i64 = 1_700_000_000_000_000;
    let first = graphforge_value::EntityTypeId::decode(0).unwrap();
    let second = graphforge_value::EntityTypeId::decode(1).unwrap();
    let mut writer =
        graphforge_storage::GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let uuids: Vec<_> = (0..count).map(|_| new_v7()).collect();
    let ids = uuids
        .iter()
        .enumerate()
        .map(|(index, uuid)| {
            let labels = match index % 3 {
                0 => vec![first, second],
                1 => vec![second],
                _ => vec![first],
            };
            writer.create_node_with_labels(*uuid, &labels).unwrap()
        })
        .collect::<Vec<_>>();
    for (index, source) in uuids.iter().enumerate() {
        for offset in 1..=2 {
            writer
                .create_edge(new_v7(), "KNOWS", source, &uuids[(index + offset) % count])
                .unwrap();
        }
    }
    writer.flush().unwrap();
    graphforge_storage::adjacency::build_adjacency_index(dir, TS).unwrap();
    ids
}

/// Delegates to a real provider but answers `node_labels` from a fixed index
/// and counts how often the traversal consulted it.
struct LabelsOverride {
    inner: Arc<dyn AdjacencyProvider>,
    labels: Option<Arc<graphforge_storage::NodeLabelIndex>>,
    consulted: std::sync::atomic::AtomicUsize,
}

impl AdjacencyProvider for LabelsOverride {
    fn admitted_inventory(
        &self,
    ) -> Option<Arc<graphforge_storage::AuthenticatedPropertyInventory>> {
        self.inner.admitted_inventory()
    }

    fn adjacency(
        &self,
        relation: &str,
        direction: Direction,
    ) -> Result<Arc<adjacency::Adjacency>, GfError> {
        self.inner.adjacency(relation, direction)
    }

    fn status(&self, relation: &str, direction: Direction) -> adjacency::AdjacencyStatus {
        self.inner.status(relation, direction)
    }

    fn edge_cardinality(&self, relation: &str, direction: Direction) -> Result<u64, GfError> {
        self.inner.edge_cardinality(relation, direction)
    }

    fn node_labels(&self) -> Result<Option<Arc<graphforge_storage::NodeLabelIndex>>, GfError> {
        self.consulted
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(self.labels.clone())
    }
}

/// A one-hop `KNOWS` expansion of `seeds` that demands every output column.
fn ring_expand_exec(
    provider: &Arc<dyn AdjacencyProvider>,
    resource: &read_resource::GraphReadContext,
    seeds: &[u64],
) -> ExpandExec {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::logical_plan::LogicalTableSource;

    let frontier = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
        "node_id",
        DataType::UInt64,
        false,
    )]));
    let logical = LogicalPlanBuilder::scan(
        "var_0",
        Arc::new(LogicalTableSource::new(Arc::clone(&frontier))),
        None,
    )
    .unwrap()
    .build()
    .unwrap();
    let node = graphforge_plan::ExpandNode::new(
        Arc::new(logical),
        "KNOWS",
        0,
        2,
        1,
        Direction::Out,
        Some(graphforge_value::RelationTypeId::decode(0).unwrap()),
        vec![
            Arc::new(Field::new(
                "edge_uuid",
                DataType::FixedSizeBinary(16),
                false,
            )),
            Arc::new(Field::new("edge_id", DataType::UInt64, false)),
            Arc::new(Field::new("src_id", DataType::UInt64, false)),
            Arc::new(Field::new("dst_id", DataType::UInt64, false)),
        ],
        vec![],
        graphforge_storage::TOPOLOGY_NODES_SCHEMA
            .fields()
            .iter()
            .cloned()
            .collect(),
    );
    let batch = RecordBatch::try_new(
        Arc::clone(&frontier),
        vec![Arc::new(UInt64Array::from(seeds.to_vec()))],
    )
    .unwrap();
    let input = MemorySourceConfig::try_new_exec(&[vec![batch]], frontier, None).unwrap();
    ExpandExec::new(&node, input, Arc::clone(provider), None, false, resource)
}

/// Expand `seeds` one hop over `KNOWS`, demanding only the destination's
/// `node_id` and `type_ids`, and return each reached row as
/// `(node_id, type_ids)`, sorted.
async fn expand_destination_labels(
    provider: &Arc<dyn AdjacencyProvider>,
    resource: &read_resource::GraphReadContext,
    seeds: &[u64],
) -> Result<Vec<(u64, Vec<u32>)>, GfError> {
    use arrow::array::{Array, ListArray};
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::collect;

    let exec = ring_expand_exec(provider, resource, seeds);
    let width = exec.schema().fields().len();
    let node_id = width - 6 + 1;
    let type_ids = width - 6 + 3;
    assert_eq!(exec.schema().field(node_id).name(), "node_id");
    assert_eq!(exec.schema().field(type_ids).name(), "type_ids");
    let mut required = vec![false; width];
    required[0] = true;
    required[node_id] = true;
    required[type_ids] = true;
    let exec = exec.with_required_output(required);
    let batches = collect(exec, SessionContext::new().task_ctx())
        .await
        .map_err(|error| GfError::Execution(error.to_string()))?;
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(node_id)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let lists = batch
            .column(type_ids)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let labels = lists.value(row);
            let labels = labels
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .unwrap();
            rows.push((ids.value(row), labels.values().to_vec()));
        }
    }
    rows.sort();
    Ok(rows)
}

fn open_ring_provider(
    dir: &std::path::Path,
) -> (
    Arc<dyn AdjacencyProvider>,
    read_resource::GraphReadContext,
    Arc<graphforge_storage::AuthenticatedPropertyInventory>,
) {
    let catalog = Arc::new(
        graphforge_storage::GraphCatalog::open(dir, None, &graphforge_ir::RuntimeCatalog::new())
            .unwrap(),
    );
    let inventory = catalog.admitted_inventory().unwrap();
    let provider: Arc<dyn AdjacencyProvider> = Arc::new(
        adjacency::PersistentAdjacencyProvider::new(dir.to_path_buf(), OntologyMode::Strict)
            .with_inventory(Arc::clone(&inventory)),
    );
    let resource = read_resource::GraphReadContext {
        health: crate::mutation::MutationHealth::default(),
        dir: dir.to_path_buf(),
        mode: OntologyMode::Strict,
        catalog,
        ontology: None,
    };
    (provider, resource, inventory)
}

/// A destination demanded for identity and labels is answered from the label
/// index, and the rows are those the node files give: the same ordinals with
/// the same complete label sets, multi-label nodes included (#1984).
#[tokio::test]
async fn a_label_only_destination_matches_the_node_file_read() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::TempDir::new().unwrap();
    let ids = labelled_ring(dir.path(), 60);
    let (provider, resource, inventory) = open_ring_provider(dir.path());
    let index = graphforge_storage::NodeLabelIndex::build(
        &graphforge_storage::TopologyFiles::from_inventory(&inventory).unwrap(),
        u64::MAX,
    )
    .unwrap()
    .expect("an index within an unbounded budget");

    let indexed = Arc::new(LabelsOverride {
        inner: Arc::clone(&provider),
        labels: Some(Arc::new(index)),
        consulted: std::sync::atomic::AtomicUsize::new(0),
    });
    let from_files = Arc::new(LabelsOverride {
        inner: Arc::clone(&provider),
        labels: None,
        consulted: std::sync::atomic::AtomicUsize::new(0),
    });
    let seeds = &ids[..30];
    let by_index = expand_destination_labels(
        &(Arc::clone(&indexed) as Arc<dyn AdjacencyProvider>),
        &resource,
        seeds,
    )
    .await
    .unwrap();
    let by_files = expand_destination_labels(
        &(Arc::clone(&from_files) as Arc<dyn AdjacencyProvider>),
        &resource,
        seeds,
    )
    .await
    .unwrap();

    assert!(indexed.consulted.load(Ordering::SeqCst) > 0);
    assert!(from_files.consulted.load(Ordering::SeqCst) > 0);
    assert_eq!(by_index, by_files);
    // Thirty seeds with two edges each give sixty rows. Every third node carries
    // two labels, so a primary-label-only answer cannot pass unseen.
    assert_eq!(by_index.len(), 60);
    for (node, labels) in &by_index {
        let position = ids.iter().position(|id| id == node).unwrap();
        assert_eq!(labels.len(), if position % 3 == 0 { 2 } else { 1 });
    }
}

/// An ordinal the label index lacks (the index was built before the node
/// existed, or the node was removed) fails with the file read's error rather
/// than yielding a row with no labels (#1984).
#[tokio::test]
async fn a_destination_absent_from_the_label_index_is_reported_unknown() {
    let full = tempfile::TempDir::new().unwrap();
    let ids = labelled_ring(full.path(), 60);
    let partial = tempfile::TempDir::new().unwrap();
    labelled_ring(partial.path(), 50);
    let (provider, resource, _) = open_ring_provider(full.path());

    // The ring node 48 knows nodes 49 and 50, and the partial project stops at 49.
    let short = graphforge_storage::NodeLabelIndex::build(
        &graphforge_storage::TopologyFiles::discover_legacy(partial.path()).unwrap(),
        u64::MAX,
    )
    .unwrap()
    .expect("an index within an unbounded budget");
    let indexed: Arc<dyn AdjacencyProvider> = Arc::new(LabelsOverride {
        inner: Arc::clone(&provider),
        labels: Some(Arc::new(short)),
        consulted: std::sync::atomic::AtomicUsize::new(0),
    });
    let seeds = &ids[48..49];

    let rows = expand_destination_labels(&provider, &resource, seeds)
        .await
        .expect("the node files hold every destination");
    assert_eq!(
        rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![ids[49], ids[50]]
    );
    let error = expand_destination_labels(&indexed, &resource, seeds)
        .await
        .expect_err("ordinal 50 is not in the index");
    assert!(
        error.to_string().contains(&format!(
            "Expand reached unknown destination node_id {}",
            ids[50]
        )),
        "{error}"
    );
}

/// An identity authority that owns every ordinal but those in `disowned`, with
/// a UUID derived from the ordinal, recording the size of each request.
fn authority_disowning<'a>(
    disowned: &'a [u64],
    requests: &'a std::cell::RefCell<Vec<usize>>,
) -> impl FnMut(&[u64]) -> Result<graphforge_storage::V4OrdinalLookup, GfError> + 'a {
    use graphforge_core::uuid::Uuid;
    move |ids| {
        requests.borrow_mut().push(ids.len());
        let values = ids
            .iter()
            .map(|id| (!disowned.contains(id)).then(|| Uuid::from_u128(u128::from(*id) + 1)))
            .collect::<Vec<_>>();
        let found = values.iter().flatten().count() as u64;
        Ok(graphforge_storage::V4OrdinalLookup {
            values,
            metrics: graphforge_storage::V4OrdinalLookupMetrics {
                requested: ids.len() as u64,
                unique_requested: ids.len() as u64,
                found,
                ..Default::default()
            },
        })
    }
}

fn destination_columns(ordinals: &[u64]) -> Vec<(&'static str, arrow::array::ArrayRef)> {
    vec![
        (
            "node_id",
            Arc::new(arrow::array::UInt64Array::from(ordinals.to_vec())),
        ),
        (
            "type_id",
            Arc::new(arrow::array::UInt32Array::from(
                ordinals
                    .iter()
                    .map(|id| *id as u32 + 100)
                    .collect::<Vec<_>>(),
            )),
        ),
    ]
}

/// A frontier larger than one identity request is looked up in requests of at
/// most the authority's bound, each reported, and the UUIDs line up with the
/// ordinals in request order (#1984).
#[test]
fn a_large_frontier_is_looked_up_in_bounded_requests() {
    use arrow::array::{Array, FixedSizeBinaryArray, UInt64Array};

    let ordinals = (10..17).collect::<Vec<u64>>();
    let requests = std::cell::RefCell::new(Vec::new());
    let resolved = destination_rows::resolve_identities(
        &ordinals,
        3,
        true,
        authority_disowning(&[], &requests),
    )
    .unwrap();
    assert_eq!(*requests.borrow(), vec![3, 3, 1]);

    let (batch, lookups) =
        destination_rows::assemble_batch(destination_columns(&ordinals), Some(resolved)).unwrap();
    assert_eq!(lookups.len(), 3);
    assert_eq!(lookups.iter().map(|l| l.found).sum::<u64>(), 7);
    assert_eq!(batch.num_rows(), 7);
    let node_ids = batch
        .column_by_name("node_id")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let uuids = batch
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    for row in 0..batch.num_rows() {
        assert_eq!(
            uuids.value(row),
            (u128::from(node_ids.value(row)) + 1).to_be_bytes()
        );
    }
}

/// An ordinal the identity authority does not own is dropped from every
/// column, so the hop's missing-destination check reports it; no UUID is
/// invented and no other row shifts (#1984).
#[test]
fn a_destination_the_identity_authority_disowns_is_dropped_from_every_column() {
    use arrow::array::{Array, FixedSizeBinaryArray, UInt32Array, UInt64Array};

    let ordinals = vec![3, 4, 5, 6];
    let requests = std::cell::RefCell::new(Vec::new());
    let resolved = destination_rows::resolve_identities(
        &ordinals,
        2,
        true,
        authority_disowning(&[4, 6], &requests),
    )
    .unwrap();
    let (batch, lookups) =
        destination_rows::assemble_batch(destination_columns(&ordinals), Some(resolved)).unwrap();

    assert_eq!(lookups.iter().map(|l| l.found).sum::<u64>(), 2);
    let ids = |name: &str| -> Vec<u64> {
        let column = batch.column_by_name(name).unwrap();
        column
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.values().to_vec())
            .or_else(|| {
                column
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .map(|a| a.values().iter().map(|v| u64::from(*v)).collect())
            })
            .unwrap()
    };
    assert_eq!(ids("node_id"), vec![3, 5]);
    assert_eq!(ids("type_id"), vec![103, 105]);
    let uuids = batch
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(uuids.len(), 2);
    assert_eq!(uuids.value(0), 4u128.to_be_bytes());
    assert_eq!(uuids.value(1), 6u128.to_be_bytes());
}

/// A hop that demands no UUID still asks the authority which nodes exist, as
/// the node-file read it replaces did, and adds no `node_uuid` column
/// (#1984).
#[test]
fn an_identity_only_destination_without_uuids_still_checks_existence() {
    let ordinals = vec![3, 4, 5];
    let requests = std::cell::RefCell::new(Vec::new());
    let resolved = destination_rows::resolve_identities(
        &ordinals,
        8,
        false,
        authority_disowning(&[4], &requests),
    )
    .unwrap();
    let (batch, lookups) = destination_rows::assemble_batch(
        destination_columns(&ordinals)[..1].to_vec(),
        Some(resolved),
    )
    .unwrap();
    assert_eq!(lookups.len(), 1);
    assert!(batch.column_by_name("node_uuid").is_none());
    assert_eq!(batch.num_rows(), 2);
}

/// An authority that answers a different number of ordinals than it was asked
/// would misalign UUIDs with rows, so it is an error rather than a guess.
#[test]
fn an_identity_answer_of_the_wrong_length_is_an_error() {
    let error = destination_rows::resolve_identities(&[1, 2, 3], 8, true, |_| {
        Ok(graphforge_storage::V4OrdinalLookup {
            values: vec![None],
            metrics: graphforge_storage::V4OrdinalLookupMetrics::default(),
        })
    })
    .err()
    .expect("a short answer must fail");
    assert!(error.to_string().contains("1 of 3"), "{error}");
}

/// A variable-length expansion over four input columns: the seed `var_0.node_id`
/// (0), a nullable `var_0.name` (1), a nullable `var_0.age` (2), and a bound
/// destination `var_9.node_id` (3). Its output appends the edge list as column 4
/// (a reachability search appends the hop count there instead).
fn variable_length_over_wide_input(reachability: bool) -> VarLenExpandExec {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::logical_plan::LogicalTableSource;

    let seed_side = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("node_id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
    ]));
    let end_side = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
        "node_id",
        DataType::UInt64,
        false,
    )]));
    let scan = |name: &str, schema: &Arc<arrow::datatypes::Schema>| {
        LogicalPlanBuilder::scan(
            name,
            Arc::new(LogicalTableSource::new(Arc::clone(schema))),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    };
    let logical = LogicalPlanBuilder::from(scan("var_0", &seed_side))
        .cross_join(scan("var_9", &end_side))
        .unwrap()
        .build()
        .unwrap();
    let physical_schema = Arc::new(logical.schema().as_arrow().clone());
    let input =
        MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&physical_schema), None).unwrap();
    let mut node = VarLenExpandNode::new(
        Arc::new(logical),
        "KNOWS",
        1,
        Some(3),
        0,
        1,
        2,
        Direction::Out,
        None,
        vec![],
        graphforge_plan::var_len_edge_list_field(&[]),
    )
    .with_bound_destination(Some("var_9".to_string()));
    if reachability {
        node = node.into_reachability();
    }
    let dir = tempfile::TempDir::new().unwrap();
    let provider: Arc<dyn AdjacencyProvider> = Arc::new(
        adjacency::PersistentAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict),
    );
    VarLenExpandExec::new(
        &node,
        input,
        provider,
        dir.path().to_path_buf(),
        OntologyMode::Strict,
    )
}

fn narrowed(exec: &VarLenExpandExec, required: &[usize]) -> Arc<dyn ExecutionPlan> {
    exec.with_required_output(&required.iter().copied().collect())
}

fn variable_length_of(plan: &Arc<dyn ExecutionPlan>) -> &VarLenExpandExec {
    plan.downcast_ref::<VarLenExpandExec>()
        .expect("a variable-length expansion")
}

fn copied_mask(plan: &Arc<dyn ExecutionPlan>) -> Option<Vec<bool>> {
    variable_length_of(plan)
        .copy_input
        .as_ref()
        .map(|mask| mask.to_vec())
}

fn displayed(plan: &Arc<dyn ExecutionPlan>) -> String {
    datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string()
}

/// The expansion asks its input for the seed, the bound destination and the
/// input columns its parent reads, and nothing else. Its own edge list is the
/// last output column, not an input column, so demanding it asks for no input.
/// (A per-relationship predicate reads one edge's struct, never an input column,
/// so it adds no demand.) (#1984)
#[test]
fn a_variable_length_expansion_demands_its_seed_destination_and_what_the_parent_reads() {
    use std::collections::BTreeSet;

    let exec = variable_length_over_wide_input(false);
    let demand = |required: &[usize]| exec.input_demand(&required.iter().copied().collect());

    assert_eq!(demand(&[]), BTreeSet::from([0, 3]));
    assert_eq!(demand(&[4]), BTreeSet::from([0, 3]));
    assert_eq!(demand(&[1, 4]), BTreeSet::from([0, 1, 3]));
    assert_eq!(demand(&[2]), BTreeSet::from([0, 2, 3]));
    assert_eq!(demand(&[0, 1, 2, 3, 4]), BTreeSet::from([0, 1, 2, 3]));

    // Without a bound destination only the seed is needed.
    let mut unbound = variable_length_over_wide_input(false);
    unbound.bound_dst_col_idx = None;
    assert_eq!(
        unbound.input_demand(&BTreeSet::from([4])),
        BTreeSet::from([0])
    );
}

/// An input column no operator above reads is not copied into the emitted
/// rows; the seed, the bound destination and the columns the parent reads are.
#[test]
fn a_variable_length_expansion_copies_only_the_columns_its_parent_reads() {
    let exec = variable_length_over_wide_input(false);

    let nothing_read = narrowed(&exec, &[4]);
    assert_eq!(
        copied_mask(&nothing_read),
        Some(vec![true, false, false, true]),
        "the identity columns are not nullable, so they are copied regardless"
    );
    assert!(
        displayed(&nothing_read).contains(", copied=2"),
        "{}",
        displayed(&nothing_read)
    );

    let name_read = narrowed(&exec, &[1, 4]);
    assert_eq!(copied_mask(&name_read), Some(vec![true, true, false, true]));
    assert!(
        displayed(&name_read).contains(", copied=3"),
        "{}",
        displayed(&name_read)
    );

    let age_read = narrowed(&exec, &[2]);
    assert_eq!(copied_mask(&age_read), Some(vec![true, false, true, true]));

    let all_read = narrowed(&exec, &[0, 1, 2, 3, 4]);
    assert_eq!(copied_mask(&all_read), None, "nothing to skip");
    assert!(
        !displayed(&all_read).contains("copied="),
        "{}",
        displayed(&all_read)
    );

    assert_eq!(
        nothing_read.schema(),
        exec.schema(),
        "a narrowed expansion keeps every column position"
    );
}

/// The trails of an expansion whose parent never reads them are produced from
/// the adjacency alone; a reachability search always needs its hop count.
#[test]
fn the_edge_list_is_read_only_when_a_parent_reads_it() {
    let trails = variable_length_over_wide_input(false);
    assert!(!variable_length_of(&narrowed(&trails, &[1])).edge_list_demanded);
    assert!(variable_length_of(&narrowed(&trails, &[1, 4])).edge_list_demanded);
    assert!(
        trails.edge_list_demanded,
        "an expansion nobody narrowed reads it"
    );

    let reachability = variable_length_over_wide_input(true);
    let hops_unread = narrowed(&reachability, &[1]);
    assert!(variable_length_of(&hops_unread).edge_list_demanded);
    assert!(displayed(&hops_unread).contains("mode=reachability"));
    assert_eq!(
        copied_mask(&hops_unread),
        Some(vec![true, true, false, true])
    );
}

/// The columns an emitted row copies from its input row: the selected rows'
/// values for a demanded column, nulls of the output's own type for a skipped
/// one.
#[test]
fn a_skipped_input_column_is_emitted_as_nulls_and_a_demanded_one_is_taken() {
    use arrow::array::Int64Array;

    let input_schema = Arc::new(arrow::datatypes::Schema::new(vec![
        Field::new("node_id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
    ]));
    let input = RecordBatch::try_new(
        Arc::clone(&input_schema),
        vec![
            Arc::new(UInt64Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    let config = |copy_input: Option<Arc<[bool]>>| ExpandConfig {
        capture_epoch: 0,
        rel_type_name: "KNOWS".into(),
        direction: Direction::Out,
        min_hops: 1,
        max_hops: Some(2),
        dir: dir.path().to_path_buf(),
        mode: OntologyMode::Strict,
        src_col_idx: 0,
        bound_dst_col_idx: None,
        reverse_edge_list: false,
        expansion: VarLenMode::Trails,
        edge_filter: None,
        edge_var: 0,
        edge_list_demanded: true,
        copy_input,
        out_schema: Arc::clone(&input_schema),
        provider: Arc::new(adjacency::PersistentAdjacencyProvider::new(
            dir.path().to_path_buf(),
            OntologyMode::Strict,
        )),
        lifecycle: Default::default(),
        metrics: ExpandMetrics::default().partition(0),
    };
    let take = UInt32Array::from(vec![2, 0, 0]);

    let mut copied_all = Vec::new();
    push_input_columns(&config(None), &input, &take, &mut copied_all).unwrap();
    assert_eq!(copied_all.len(), 3);
    let names = copied_all[1]
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        names.iter().collect::<Vec<_>>(),
        [Some("c"), Some("a"), Some("a")]
    );

    let mut columns = Vec::new();
    push_input_columns(
        &config(Some(Arc::from(vec![true, false, true]))),
        &input,
        &take,
        &mut columns,
    )
    .unwrap();
    assert_eq!(columns.len(), 3, "every column keeps its position");
    let ids = columns[0].as_any().downcast_ref::<UInt64Array>().unwrap();
    assert_eq!(ids.values(), &[30, 10, 10]);
    assert_eq!(columns[1].data_type(), &DataType::Utf8);
    assert_eq!(columns[1].len(), 3);
    assert_eq!(
        columns[1].null_count(),
        3,
        "the skipped column is all nulls"
    );
    let ages = columns[2].as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ages.values(), &[3, 1, 1]);
}

/// A pool that tells its consumers apart: what each named consumer holds now,
/// the most it ever held, and what every other consumer held at that moment.
#[derive(Debug)]
struct ConsumerPool {
    inner: datafusion::execution::memory_pool::GreedyMemoryPool,
    held: std::sync::Mutex<std::collections::BTreeMap<String, Held>>,
}

#[derive(Debug, Default)]
struct Held {
    now: usize,
    peak: usize,
    /// What the rest of the pool held when this consumer was at its peak.
    others_at_peak: usize,
}

impl ConsumerPool {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: datafusion::execution::memory_pool::GreedyMemoryPool::new(limit),
            held: std::sync::Mutex::default(),
        })
    }

    /// Bytes the pool still has reserved, by every consumer.
    fn leaked(&self) -> usize {
        datafusion::execution::memory_pool::MemoryPool::reserved(&self.inner)
    }

    /// Records that `consumer` now holds `now`; the inner pool already agrees.
    fn record(&self, consumer: &str, now: usize) {
        let total = self.leaked();
        let mut held = self.held.lock().unwrap();
        let entry = held.entry(consumer.to_owned()).or_default();
        entry.now = now;
        if now > entry.peak {
            entry.peak = now;
            entry.others_at_peak = total.saturating_sub(now);
        }
    }

    fn now(&self, consumer: &str) -> usize {
        self.held.lock().unwrap().get(consumer).map_or(0, |e| e.now)
    }

    fn peak(&self, consumer: &str) -> usize {
        self.held
            .lock()
            .unwrap()
            .get(consumer)
            .map_or(0, |e| e.peak)
    }

    /// What the other consumers held while `consumer` was at its peak.
    fn others_at_peak(&self, consumer: &str) -> usize {
        self.held
            .lock()
            .unwrap()
            .get(consumer)
            .map_or(0, |e| e.others_at_peak)
    }
}

impl std::fmt::Display for ConsumerPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConsumerPool({})", self.inner)
    }
}

impl datafusion::execution::memory_pool::MemoryPool for ConsumerPool {
    fn grow(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        additional: usize,
    ) {
        self.inner.grow(reservation, additional);
        // The reservation's own size is updated after the pool is told.
        self.record(
            reservation.consumer().name(),
            reservation.size() + additional,
        );
    }

    fn shrink(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        shrink: usize,
    ) {
        self.inner.shrink(reservation, shrink);
        // MemoryReservation::shrink updates its size before this callback;
        // subtracting again would hide a still-live output reservation.
        self.record(reservation.consumer().name(), reservation.size());
    }

    fn try_grow(
        &self,
        reservation: &datafusion::execution::memory_pool::MemoryReservation,
        additional: usize,
    ) -> datafusion::error::Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.record(
            reservation.consumer().name(),
            reservation.size() + additional,
        );
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.leaked()
    }

    fn name(&self) -> &str {
        "ConsumerPool"
    }

    fn memory_limit(&self) -> datafusion::execution::memory_pool::MemoryLimit {
        self.inner.memory_limit()
    }
}

/// `rows` rows of one `UInt64` column.
fn counting_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
        "v",
        DataType::UInt64,
        false,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from_iter_values(0..rows as u64))],
    )
    .unwrap()
}

fn hop_read_reservation(
    pool: &Arc<ConsumerPool>,
) -> Arc<std::sync::Mutex<crate::chunked_reservation::ChunkedReservation>> {
    let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = pool.clone();
    Arc::new(std::sync::Mutex::new(
        crate::chunked_reservation::ChunkedReservation::register(HOP_READ_CONSUMER, &pool),
    ))
}

/// The pool's chunk for a budget of 8 MiB: `8 MiB / 512`, the 16 KiB floor.
const SMALL_POOL: usize = 8 * 1024 * 1024;
const SMALL_POOL_CHUNK: usize = 16 * 1024;

/// A hop read holds the decoded batches (`D` bytes). Joined from several
/// batches it holds the copy as well while it is built, `2D` at the peak, and
/// `D` after; a lone batch is shared, so it holds `D` throughout. The pool
/// sees those bytes rounded up to its chunk, and gets them back when the chunk
/// ends (#1987).
#[test]
fn a_hop_read_is_charged_its_decoded_bytes_and_the_copy_it_joins() {
    let pool = ConsumerPool::new(SMALL_POOL);
    let reservation = hop_read_reservation(&pool);

    let batches: Vec<_> = (0..3).map(|_| counting_batch(4096)).collect();
    let decoded = batches_bytes(&batches);
    assert!(decoded >= 3 * 4096 * 8, "{decoded}");
    let mut charge = HopReadCharge::new(&reservation);
    let joined = hop_read::concat_charged(&mut charge, batches).unwrap();
    assert_eq!(joined.num_rows(), 3 * 4096);
    assert_eq!(
        charge.held_bytes(),
        decoded,
        "the joined copy replaces the batches"
    );
    assert!(
        joined.get_array_memory_size() <= decoded,
        "the copy is no larger than the batches it joins"
    );
    let peak = pool.peak(HOP_READ_CONSUMER);
    assert!(peak >= 2 * decoded, "{peak} < 2 * {decoded}");
    assert!(peak <= 2 * decoded + SMALL_POOL_CHUNK, "{peak}");
    drop(charge);
    assert!(pool.now(HOP_READ_CONSUMER) <= 2 * SMALL_POOL_CHUNK);

    let lone = vec![counting_batch(4096)];
    let decoded = batches_bytes(&lone);
    let before = pool.peak(HOP_READ_CONSUMER);
    let mut charge = HopReadCharge::new(&reservation);
    let shared = hop_read::concat_charged(&mut charge, lone).unwrap();
    assert_eq!(shared.num_rows(), 4096);
    assert_eq!(charge.held_bytes(), decoded);
    assert!(
        pool.peak(HOP_READ_CONSUMER) <= before.max(decoded + 2 * SMALL_POOL_CHUNK),
        "a lone batch is not charged twice"
    );
    drop(charge);

    drop(reservation);
    assert_eq!(pool.now(HOP_READ_CONSUMER), 0);
    assert_eq!(pool.leaked(), 0);
}

/// A read the budget cannot hold fails with the pool's typed error naming the
/// hop read, before the copy is allocated, and leaves nothing charged.
#[test]
fn a_hop_read_over_the_budget_fails_before_the_copy() {
    let pool = ConsumerPool::new(64 * 1024);
    let reservation = hop_read_reservation(&pool);
    let batches: Vec<_> = (0..3).map(|_| counting_batch(2048)).collect();
    let decoded = batches_bytes(&batches);
    // One copy fits; two do not.
    assert!(decoded > 32 * 1024 && decoded <= 64 * 1024, "{decoded}");
    let mut charge = HopReadCharge::new(&reservation);
    let error = hop_read::concat_charged(&mut charge, batches)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(HOP_READ_CONSUMER) && error.contains("Resources exhausted"),
        "{error}"
    );
    assert_eq!(
        charge.held_bytes(),
        decoded,
        "the failed copy charge is not recorded"
    );
    drop(charge);
    drop(reservation);
    assert_eq!(pool.leaked(), 0);
}

/// One expansion over a ring, run in a pool of `pool`.
async fn expand_ring_in(
    pool: Arc<ConsumerPool>,
    provider: &Arc<dyn AdjacencyProvider>,
    resource: &read_resource::GraphReadContext,
    seeds: &[u64],
) -> Result<usize, GfError> {
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::{SessionConfig, SessionContext};

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .build_arc()
        .unwrap();
    let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    let exec: Arc<dyn ExecutionPlan> = Arc::new(ring_expand_exec(provider, resource, seeds));
    collect(exec, context.task_ctx())
        .await
        .map(|batches| batches.iter().map(RecordBatch::num_rows).sum())
        .map_err(|error| GfError::Execution(error.to_string()))
}

/// The edge and destination-node reads of a real expansion are charged to the
/// pool under the hop-read name and released when the chunk ends, and a budget
/// that holds everything but those reads fails the query naming them (#1987).
#[tokio::test]
async fn a_single_hop_expansion_charges_its_reads_and_fails_typed_over_budget() {
    const NODES: usize = 4_000;
    let dir = tempfile::TempDir::new().unwrap();
    let ids = labelled_ring(dir.path(), NODES);
    let (provider, resource, _inventory) = open_ring_provider(dir.path());

    let pool = ConsumerPool::new(SMALL_POOL);
    let rows = expand_ring_in(Arc::clone(&pool), &provider, &resource, &ids)
        .await
        .unwrap();
    assert_eq!(rows, 2 * NODES);
    let read = pool.peak(HOP_READ_CONSUMER);
    let others = pool.others_at_peak(HOP_READ_CONSUMER);
    eprintln!("hop read peak {read}, other consumers {others}");
    assert!(
        read >= 4 * SMALL_POOL_CHUNK,
        "the reads are too small to tell from the pool's chunk: {read}"
    );
    assert!(
        pool.now(HOP_READ_CONSUMER) <= 2 * SMALL_POOL_CHUNK,
        "the chunk's reads were not released"
    );

    // What else the query holds while it reads fits; half of the reads does not.
    let limit = others + read / 2;
    let pool = ConsumerPool::new(limit);
    let error = expand_ring_in(Arc::clone(&pool), &provider, &resource, &ids)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(HOP_READ_CONSUMER) && error.contains("Resources exhausted"),
        "{error}"
    );
    assert_eq!(pool.leaked(), 0, "a reservation leaked");
}

type DestinationFileRow = (u64, u64, i64, Vec<u32>);

fn destination_file_rows(batches: &[RecordBatch]) -> Vec<DestinationFileRow> {
    use arrow::array::{ListArray, TimestampMicrosecondArray, UInt32Array};
    let mut actual = Vec::new();
    for batch in batches {
        let destination = batch.num_columns() - 6;
        let source = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let reached = batch
            .column(destination + 1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let labels = batch
            .column(destination + 3)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let created = batch
            .column(destination + 4)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let values = labels.value(row);
            let values = values.as_any().downcast_ref::<UInt32Array>().unwrap();
            actual.push((
                source.value(row),
                reached.value(row),
                created.value(row),
                values.values().to_vec(),
            ));
        }
    }
    actual.sort();
    actual
}

#[test]
fn destination_decoder_refusal_precedes_the_real_node_read() {
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    const NODES: usize = 16;
    const TS: i64 = 1_700_000_000_000_000;
    let dir = tempfile::TempDir::new().unwrap();
    let ids = labelled_ring(dir.path(), NODES);
    let (provider, resource, _inventory) = open_ring_provider(dir.path());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for (limit, fits) in [(16 * 1024 * 1024, true), (64 * 1024, false)] {
        let pool = ConsumerPool::new(limit);
        let (result, captured) = demand::capture(|| {
            let execution = RuntimeEnvBuilder::new()
                .with_memory_pool(pool.clone())
                .build_arc()
                .unwrap();
            let context = SessionContext::new_with_config_rt(
                SessionConfig::new().with_batch_size(64),
                execution,
            );
            let exec = ring_expand_exec(&provider, &resource, &ids);
            let width = exec.schema().fields().len();
            let destination = width - graphforge_storage::TOPOLOGY_NODES_SCHEMA.fields().len();
            let mut required = vec![false; width];
            required[0] = true;
            required[destination + 1] = true;
            required[destination + 3] = true;
            // Timestamps force the authenticated node-file path, rather than
            // the label/identity synthesis path. Edges need no materialization.
            required[destination + 4] = true;
            runtime.block_on(collect(
                exec.with_required_output(required),
                context.task_ctx(),
            ))
        });
        let node_reads: u64 = captured
            .hops
            .values()
            .map(|hop| hop.node_reads_started)
            .sum();
        let edge_reads: u64 = captured
            .hops
            .values()
            .map(|hop| hop.edge_reads_started)
            .sum();
        assert_eq!(edge_reads, 0, "this control isolates destination decoding");
        if fits {
            let batches = result.unwrap();
            assert!(
                node_reads > 0,
                "roomy control must execute real node Parquet reads"
            );
            let actual = destination_file_rows(&batches);
            let mut expected = Vec::new();
            for (source, &id) in ids.iter().enumerate() {
                for offset in 1..=2 {
                    let reached = (source + offset) % NODES;
                    let labels = match reached % 3 {
                        0 => vec![0, 1],
                        1 => vec![1],
                        _ => vec![0],
                    };
                    expected.push((id, ids[reached], TS, labels));
                }
            }
            expected.sort();
            assert_eq!(actual, expected);
            drop(batches);
        } else {
            assert_eq!(
                node_reads, 0,
                "an insufficient decoder budget must refuse before storage starts the node read; actual result: {result:?}"
            );
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains(HOP_READ_CONSUMER) && error.contains("Resources exhausted"),
                "{error}"
            );
        }
        assert_eq!(pool.leaked(), 0);
    }
}

#[test]
fn decoder_admission_transfers_to_retained_batches_without_a_duplicate_charge() {
    let pool = ConsumerPool::new(160 * 1024);
    let reservation = hop_read_reservation(&pool);
    let batches: Vec<_> = (0..2).map(|_| counting_batch(4096)).collect();
    let decoded = batches_bytes(&batches);
    assert!(2 * decoded < 160 * 1024);
    let mut charge = HopReadCharge::new(&reservation);
    let envelope = 128 * 1024;
    charge.charge(envelope).unwrap();
    charge.replace(envelope, decoded).unwrap();
    assert_eq!(charge.held_bytes(), decoded);
    let joined = concat_precharged(&mut charge, batches).unwrap();
    assert_eq!(joined.num_rows(), 8192);
    assert_eq!(charge.held_bytes(), decoded);
    assert!(pool.peak(HOP_READ_CONSUMER) <= 160 * 1024);
    drop(joined);
    drop(charge);
    drop(reservation);
    assert_eq!(pool.now(HOP_READ_CONSUMER), 0);
    assert_eq!(pool.leaked(), 0);
}

#[test]
fn a_failed_decoder_transfer_preserves_its_original_reservation() {
    let pool = ConsumerPool::new(64 * 1024);
    let reservation = hop_read_reservation(&pool);
    let mut charge = HopReadCharge::new(&reservation);
    charge.charge(32 * 1024).unwrap();
    let error = charge.replace(32 * 1024, 128 * 1024).unwrap_err();
    assert!(error.to_string().contains(HOP_READ_CONSUMER));
    assert_eq!(charge.held_bytes(), 32 * 1024);
    drop(charge);
    drop(reservation);
    assert_eq!(pool.now(HOP_READ_CONSUMER), 0);
}

#[test]
fn hop_row_tables_are_admitted_before_their_real_constructor() {
    use std::cell::Cell;
    use std::collections::HashMap;
    let entered = Cell::new(false);
    let pool = ConsumerPool::new(64 * 1024);
    let reservation = hop_read_reservation(&pool);
    let mut charge = HopReadCharge::new(&reservation);
    let refused = hop_read::admitted_table::<(u64, usize), _>(&mut charge, 8192, |capacity| {
        entered.set(true);
        HashMap::<u64, usize>::with_capacity(capacity)
    });
    assert!(refused.is_err());
    assert!(
        !entered.get(),
        "the actual table allocator ran before admission"
    );
    assert_eq!(charge.held_bytes(), 0);
    let mut table = hop_read::admitted_table::<(u64, usize), _>(&mut charge, 37, |capacity| {
        entered.set(true);
        HashMap::<u64, usize>::with_capacity(capacity)
    })
    .unwrap();
    for id in 0..37 {
        table.insert(id, id as usize);
    }
    assert!(entered.get());
    assert_eq!(table.len(), 37);
    assert!(pool.now(HOP_READ_CONSUMER) > 0);
    drop(table);
    drop(charge);
    drop(reservation);
    assert_eq!(pool.leaked(), 0);
}

#[test]
fn undirected_seen_table_owns_its_capacity_across_chunks_and_clear() {
    let pool = ConsumerPool::new(64 * 1024);
    let reservation = hop_read_reservation(&pool);
    let mut position = SingleHopPosition::default();
    position.seen_charge = Some(HopReadCharge::new(&reservation));
    for id in 0..300 {
        assert!(
            hop_read::set_insert(
                position.seen_charge.as_mut().unwrap(),
                &mut position.seen_edges,
                id,
            )
            .unwrap()
        );
    }
    let held = position.seen_charge.as_ref().unwrap().held_bytes();
    assert!(held > 0);
    position.seen_edges.clear();
    assert_eq!(position.seen_charge.as_ref().unwrap().held_bytes(), held);
    assert!(position.seen_edges.capacity() >= 300);
    // A completed chunk must not release the resumable position's table.
    let chunk = HopReadCharge::new(&reservation);
    drop(chunk);
    assert_eq!(position.seen_charge.as_ref().unwrap().held_bytes(), held);
    // Refused growth keeps the old table, contents and authority intact.
    let error = position.seen_charge.as_mut().unwrap().charge(128 * 1024);
    assert!(error.is_err());
    assert_eq!(position.seen_charge.as_ref().unwrap().held_bytes(), held);
    drop(reservation);
    assert!(pool.now(HOP_READ_CONSUMER) > 0);
    drop(position);
    assert_eq!(pool.leaked(), 0);
}

#[test]
fn real_undirected_hop_keeps_position_and_output_admitted_through_late_refusal() {
    use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    let dir = tempfile::TempDir::new().unwrap();
    let ids = labelled_ring(dir.path(), 16);
    let (provider, resource, _inventory) = open_ring_provider(dir.path());
    let pool = ConsumerPool::new(16 * 1024 * 1024);
    let execution = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()
        .unwrap();
    let context = SessionContext::new_with_config_rt(SessionConfig::new(), execution);
    let exec = ring_expand_exec(&provider, &resource, &ids[..1]);
    let mut cfg = exec.single_hop_config(&context.task_ctx(), 0);
    cfg.direction = Direction::Undirected;
    let width = cfg.out_schema.fields().len();
    let mut required = vec![false; width];
    for index in [0, width - 5, width - 3, width - 2] {
        required[index] = true;
    }
    cfg.required_output = Some(Arc::from(required));
    let input = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "node_id",
            DataType::UInt64,
            false,
        )])),
        vec![Arc::new(UInt64Array::from(vec![ids[0]]))],
    )
    .unwrap();
    let mut position = SingleHopPosition::default();
    let first = expand_single_hop_chunk(&cfg, &input, &mut position, 1).unwrap();
    assert_eq!(
        position.row, 0,
        "the same source row resumes in the next chunk"
    );
    assert_eq!(position.seen_edges.len(), 1);
    let seen = position.seen_charge.as_ref().unwrap().held_bytes();
    assert!(seen > 0);
    let rows = destination_file_rows(std::slice::from_ref(&first.batch));
    assert_eq!(rows.len(), 1);
    let destination = ids.iter().position(|id| *id == rows[0].1).unwrap();
    assert!([1, 2, 14, 15].contains(&destination));
    let labels = match destination % 3 {
        0 => vec![0, 1],
        1 => vec![1],
        _ => vec![0],
    };
    assert_eq!(
        rows[0],
        (ids[0], ids[destination], 1_700_000_000_000_000, labels)
    );
    let dynamic_pool: Arc<dyn MemoryPool> = pool.clone();
    let blocker = MemoryConsumer::new("late refusal control").register(&dynamic_pool);
    blocker
        .try_grow(16 * 1024 * 1024 - pool.reserved() - 256 * 1024)
        .unwrap();
    let error = match expand_single_hop_chunk(&cfg, &input, &mut position, 1) {
        Ok(_) => panic!("a second decoder envelope cannot fit"),
        Err(error) => error,
    };
    assert!(error.to_string().contains(HOP_READ_CONSUMER), "{error}");
    assert_eq!(
        first.batch.num_rows(),
        1,
        "prior output remains usable after refusal"
    );
    assert!(position.seen_charge.as_ref().unwrap().held_bytes() >= seen);
    drop(blocker);
    drop(cfg);
    drop(position);
    assert!(pool.now(HOP_READ_CONSUMER) >= first.charge.held_bytes());
    drop(first);
    assert_eq!(
        pool.leaked(),
        0,
        "abandoning the partial traversal releases all owners"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn public_hop_output_slice_keeps_its_full_backing_charge_after_next_poll_and_drop() {
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    let dir = tempfile::TempDir::new().unwrap();
    let ids = labelled_ring(dir.path(), 16);
    let (provider, resource, _inventory) = open_ring_provider(dir.path());
    let pool = ConsumerPool::new(16 * 1024 * 1024);
    let execution = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()
        .unwrap();
    let context =
        SessionContext::new_with_config_rt(SessionConfig::new().with_batch_size(2), execution);
    let exec = ring_expand_exec(&provider, &resource, &ids);
    let width = exec.schema().fields().len();
    let created = width - 2;
    let mut required = vec![false; width];
    required[0] = true;
    required[created] = true;
    let exec = exec.with_required_output(required);
    let mut stream = exec.execute(0, context.task_ctx()).unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 2);
    // Keep only one column/row. Its Arrow buffer still owns the complete
    // two-row backing allocation, independently of the batch and stream.
    let retained = first.project(&[created]).unwrap().slice(0, 1);
    drop(first);
    let next = stream.next().await.unwrap().unwrap();
    assert_eq!(next.num_rows(), 2);
    drop(next);
    drop(stream);
    drop(exec);
    drop(context);
    assert!(
        // Exact split reservations share a consumer name; its callback's
        // last reservation size is not their sum. This pool is dedicated.
        pool.leaked() >= retained.column(0).get_buffer_memory_size(),
        "the escaped timestamp slice lost its charge before its backing dropped"
    );
    let timestamp = retained
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(timestamp.value(0), 1_700_000_000_000_000);
    drop(retained);
    assert_eq!(pool.leaked(), 0);
}

#[test]
fn nested_hop_output_keeps_struct_child_vectors_until_the_last_buffer_alias() {
    use arrow::array::{Array, AsArray, StructArray};
    let fields = (0..33)
        .map(|index| {
            Arc::new(Field::new(
                format!("value_{index}"),
                DataType::UInt64,
                false,
            ))
        })
        .collect::<Vec<_>>();
    let children = (0..33)
        .map(|index| {
            Arc::new(UInt64Array::from(vec![
                index,
                index + 100,
                index + 200,
                index + 300,
            ])) as ArrayRef
        })
        .collect::<Vec<_>>();
    let inner = StructArray::new(fields.into(), children, None);
    let outer = StructArray::new(
        vec![Arc::new(Field::new(
            "inner",
            inner.data_type().clone(),
            false,
        ))]
        .into(),
        vec![Arc::new(inner)],
        None,
    );
    let pool = ConsumerPool::new(1024 * 1024);
    let reservation = hop_read_reservation(&pool);
    let mut charge = HopReadCharge::new(&reservation);
    let outer: ArrayRef = Arc::new(outer);
    let selected = hop_read::take(&mut charge, &outer, &UInt32Array::from(vec![3, 1])).unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "record",
            selected.data_type().clone(),
            false,
        )])),
        vec![selected],
    )
    .unwrap();
    let visible = batch.get_array_memory_size();
    // take's exact-sized unzip reserves a four-slot minimum for the outer
    // one-child Struct; the inner Struct retains 33 slots. Every
    // nonnullable primitive leaf also has a BackingLease (exact reservation
    // plus logical size), independently of the constructor guard. The getter omits
    // these child-vector heaps; lease-token storage cannot cover both owners.
    let minimum = visible
        + 37 * std::mem::size_of::<ArrayRef>()
        + 33 * (std::mem::size_of::<datafusion::execution::memory_pool::MemoryReservation>()
            + std::mem::size_of::<usize>())
        + std::mem::size_of::<HopReadCharge>()
        + 2 * std::mem::size_of::<usize>();
    let output = hop_read::lease_output(batch, charge).unwrap();
    assert!(
        pool.leaked() >= minimum,
        "complete construction remains admitted through handoff"
    );
    assert!(output.charge.held_bytes() >= 37 * std::mem::size_of::<ArrayRef>());
    let complete_reserved = pool.leaked();
    let inner = output.batch.column(0).as_struct().column(0).as_struct();
    for (index, child) in inner.columns().iter().enumerate() {
        let child = child.as_primitive::<arrow::datatypes::UInt64Type>();
        assert_eq!(child.value(0), index as u64 + 300);
        assert_eq!(child.value(1), index as u64 + 100);
    }
    let escaped = inner.column(32).slice(0, 1);
    assert_eq!(
        escaped
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        332
    );
    drop(output);
    drop(reservation);
    let escaped_minimum = escaped.get_buffer_memory_size()
        + std::mem::size_of::<datafusion::execution::memory_pool::MemoryReservation>()
        + std::mem::size_of::<usize>();
    assert!(
        pool.leaked() >= escaped_minimum,
        "full backing and its own token remain admitted"
    );
    assert!(
        pool.leaked() < complete_reserved,
        "dropped sibling buffers and construction slack must release"
    );
    assert!(
        pool.leaked() <= escaped.get_array_memory_size(),
        "the escaped buffer must not retain a chunk or dead siblings"
    );
    assert_eq!(
        escaped
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        332
    );
    drop(escaped);
    assert_eq!(pool.leaked(), 0);
}

#[test]
fn destination_footer_is_admitted_before_the_real_metadata_reader_allocates() {
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    use parquet::file::metadata::KeyValue;
    const LARGE_VALUE: usize = 128 * 1024;
    let dir = tempfile::TempDir::new().unwrap();
    let ids = labelled_ring(dir.path(), 16);
    let files = graphforge_storage::TopologyFiles::discover_legacy(dir.path()).unwrap();
    for (path, _) in files.node_fragments() {
        let batches = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let properties = parquet::file::properties::WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "fixture_note".to_owned(),
                Some("v".repeat(LARGE_VALUE)),
            )]))
            .build();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(path).unwrap(),
            batches[0].schema(),
            Some(properties),
        )
        .unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
    }
    // Capture/authenticate the rewritten genuine files before query execution.
    let (provider, resource, _inventory) = open_ring_provider(dir.path());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for (limit, fits) in [(16 * 1024 * 1024, true), (64 * 1024, false)] {
        let pool = ConsumerPool::new(limit);
        let _io = graphforge_storage::lifecycle_io::CaptureScope::install();
        let before = graphforge_storage::lifecycle_io_snapshot().unwrap();
        let (result, captured) = demand::capture(|| {
            let execution = RuntimeEnvBuilder::new()
                .with_memory_pool(pool.clone())
                .build_arc()
                .unwrap();
            let context = SessionContext::new_with_config_rt(SessionConfig::new(), execution);
            let exec = ring_expand_exec(&provider, &resource, &ids);
            let width = exec.schema().fields().len();
            let mut required = vec![false; width];
            required[0] = true;
            required[width - 2] = true;
            runtime.block_on(collect(
                exec.with_required_output(required),
                context.task_ctx(),
            ))
        });
        let io = graphforge_storage::lifecycle_io_snapshot()
            .unwrap()
            .since(&before)
            .unwrap();
        let read = io.phases[&graphforge_storage::StorageIoPhase::ReadPathScan].read_bytes;
        if fits {
            let batches = result.unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 32);
            assert!(
                read >= LARGE_VALUE as u64,
                "the real metadata reader must run"
            );
            assert!(captured.hops.values().any(|hop| hop.node_reads_started > 0));
        } else {
            assert!(result.is_err());
            assert_eq!(
                read, 0,
                "large footer was read/materialized before query admission"
            );
            assert!(
                captured
                    .hops
                    .values()
                    .all(|hop| hop.node_reads_started == 0)
            );
        }
        assert_eq!(pool.leaked(), 0);
    }
}

#[test]
fn hop_view_take_retains_unused_and_zero_capacity_backings_until_the_last_alias() {
    use arrow::array::{Array, AsArray, BinaryViewArray, StringViewArray};
    use arrow::buffer::Buffer;
    for binary in [false, true] {
        // Fixed 4 KiB comfortably holds inline descriptors/wrappers, but not
        // the two overlapping 96-slot Buffer containers. No payload bytes can
        // explain the refusal in this companion control.
        let inline = StringViewArray::from_iter_values(["inline"]);
        let (views, original_buffers, nulls) = inline.into_parts();
        assert!(original_buffers.is_empty());
        drop(original_buffers);
        let plain: ArrayRef = if binary {
            Arc::new(BinaryViewArray::new(
                views.clone(),
                Vec::<Buffer>::new(),
                nulls.clone(),
            ))
        } else {
            Arc::new(StringViewArray::new(
                views.clone(),
                Vec::<Buffer>::new(),
                nulls.clone(),
            ))
        };
        let metadata_only: ArrayRef = if binary {
            Arc::new(BinaryViewArray::new(
                views,
                (0..96)
                    .map(|_| Buffer::from(Vec::<u8>::new()))
                    .collect::<Vec<_>>(),
                nulls,
            ))
        } else {
            Arc::new(StringViewArray::new(
                views,
                (0..96)
                    .map(|_| Buffer::from(Vec::<u8>::new()))
                    .collect::<Vec<_>>(),
                nulls,
            ))
        };
        let indices = UInt32Array::from(vec![0]);
        let pool = ConsumerPool::new(4 * 1024);
        let reservation = hop_read_reservation(&pool);
        let mut charge = HopReadCharge::new(&reservation);
        let selected = hop_read::take(&mut charge, &plain, &indices).unwrap();
        if binary {
            assert_eq!(selected.as_binary_view().value(0), b"inline");
        } else {
            assert_eq!(selected.as_string_view().value(0), "inline");
        }
        drop(selected);
        drop(charge);
        drop(reservation);
        assert_eq!(pool.leaked(), 0);
        let reservation = hop_read_reservation(&pool);
        let mut charge = HopReadCharge::new(&reservation);
        let error = hop_read::take(&mut charge, &metadata_only, &indices).unwrap_err();
        assert!(error.to_string().contains(HOP_READ_CONSUMER), "{error}");
        assert_eq!(charge.held_bytes(), 0);
        drop(charge);
        drop(reservation);
        assert_eq!(pool.leaked(), 0);
        drop(metadata_only);
        drop(plain);
        for source_null in [false, true] {
            let wide = "a distinguishable external payload ".repeat(40);
            let values = [
                Some("inline"),
                Some(wide.as_str()),
                if source_null { None } else { Some("third") },
            ];
            let base = StringViewArray::from_iter(values);
            let (views, source_buffers, nulls) = base.into_parts();
            let mut buffers = source_buffers.to_vec();
            // The fixture must relinquish this Arc too: its original payload
            // Bytes acquire the output token when take aliases their buffers.
            drop(source_buffers);
            // take clones every buffer, even an unused wide payload and many
            // distinct empty Bytes whose dangling data pointers may coincide.
            buffers.push(Buffer::from(vec![7u8; 4096]));
            buffers.extend((0..64).map(|_| Buffer::from(Vec::<u8>::new())));
            let shared_empty = Buffer::from(Vec::<u8>::new());
            buffers.extend(std::iter::repeat_n(shared_empty, 32));
            let input: ArrayRef = if binary {
                Arc::new(BinaryViewArray::new(views, buffers, nulls))
            } else {
                Arc::new(StringViewArray::new(views, buffers, nulls))
            };
            let count = if binary {
                input.as_binary_view().data_buffers().len()
            } else {
                input.as_string_view().data_buffers().len()
            };
            let indices = UInt32Array::from(vec![Some(0), None, Some(2), Some(1)]);
            let tight = ConsumerPool::new(1024);
            let tight_reservation = hop_read_reservation(&tight);
            let mut refused = HopReadCharge::new(&tight_reservation);
            let error = hop_read::take(&mut refused, &input, &indices).unwrap_err();
            assert!(error.to_string().contains(HOP_READ_CONSUMER), "{error}");
            assert_eq!(refused.held_bytes(), 0);
            drop(refused);
            drop(tight_reservation);
            assert_eq!(tight.leaked(), 0);

            let pool = ConsumerPool::new(1024 * 1024);
            let reservation = hop_read_reservation(&pool);
            let mut charge = HopReadCharge::new(&reservation);
            let selected = hop_read::take(&mut charge, &input, &indices).unwrap();
            let cell = |array: &ArrayRef, row| -> Option<Vec<u8>> {
                if array.is_null(row) {
                    None
                } else if binary {
                    Some(array.as_binary_view().value(row).to_vec())
                } else {
                    Some(array.as_string_view().value(row).as_bytes().to_vec())
                }
            };
            assert_eq!(cell(&selected, 0), Some(b"inline".to_vec()));
            assert_eq!(cell(&selected, 1), None);
            assert_eq!(
                cell(&selected, 2),
                if source_null {
                    None
                } else {
                    Some(b"third".to_vec())
                }
            );
            assert_eq!(cell(&selected, 3), Some(wide.as_bytes().to_vec()));
            let batch = RecordBatch::try_new(
                Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                    "carried",
                    selected.data_type().clone(),
                    true,
                )])),
                vec![selected],
            )
            .unwrap();
            let output = hop_read::lease_output(batch, charge).unwrap();
            let escaped = output.batch.column(0).slice(0, 1);
            drop(output);
            drop(reservation);
            drop(indices);
            drop(input);
            let variable_metadata = count * std::mem::size_of::<Buffer>()
                + 2 * std::mem::size_of::<usize>()
                + std::mem::size_of::<datafusion::execution::memory_pool::MemoryReservation>()
                + 2 * std::mem::size_of::<usize>();
            assert!(
                pool.leaked() >= escaped.get_buffer_memory_size() + variable_metadata,
                "full unused payloads and the variable buffer-list owner must survive an inline-only slice"
            );
            assert_eq!(cell(&escaped, 0), Some(b"inline".to_vec()));
            drop(escaped);
            assert_eq!(
                pool.leaked(),
                0,
                "empty backing tokens must not immortalize the metadata lease"
            );
        }
    }
}

#[test]
fn hop_unused_views_preserve_nullability_and_release_empty_payload_metadata() {
    use arrow::array::{Array, AsArray};
    for kind in [DataType::Utf8View, DataType::BinaryView] {
        for nullable in [false, true] {
            let pool = ConsumerPool::new(64 * 1024);
            let reservation = hop_read_reservation(&pool);
            let mut charge = HopReadCharge::new(&reservation);
            let field = Field::new("unused", kind.clone(), nullable);
            let array = hop_read::unused(&mut charge, &field, 4).unwrap();
            assert_eq!(array.data_type(), &kind);
            assert_eq!(array.null_count(), if nullable { 4 } else { 0 });
            match kind {
                DataType::Utf8View => {
                    assert!(array.as_string_view().data_buffers().is_empty());
                }
                DataType::BinaryView => {
                    assert!(array.as_binary_view().data_buffers().is_empty());
                }
                _ => unreachable!(),
            }
            let batch = RecordBatch::try_new(
                Arc::new(arrow::datatypes::Schema::new(vec![field])),
                vec![array],
            )
            .unwrap();
            let output = hop_read::lease_output(batch, charge).unwrap();
            let escaped = output.batch.column(0).slice(0, 1);
            drop(output);
            drop(reservation);
            assert!(pool.leaked() >= escaped.get_buffer_memory_size());
            assert_eq!(escaped.null_count(), usize::from(nullable));
            drop(escaped);
            assert_eq!(pool.leaked(), 0);
        }
    }
}

#[test]
fn unsupported_hop_arrow_shape_reports_a_bound_refusal_before_construction() {
    use arrow::array::{DictionaryArray, Int32Array};
    use arrow::datatypes::Int32Type;
    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0, 1]),
        Arc::new(StringArray::from(vec!["first", "second"])),
    )
    .unwrap();
    let field = Field::new("carried", dictionary.data_type().clone(), false);
    let pool = ConsumerPool::new(64 * 1024);
    let reservation = hop_read_reservation(&pool);
    let mut charge = HopReadCharge::new(&reservation);
    let error = hop_read::unused(&mut charge, &field, 2).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported Arrow allocation bound for Dictionary"),
        "{error}"
    );
    assert!(!error.to_string().contains("overflow"), "{error}");
    assert_eq!(
        charge.held_bytes(),
        0,
        "unsupported shape refuses before output admission or construction"
    );
    assert_eq!(pool.leaked(), 0);
    let batch = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![field])),
        vec![Arc::new(dictionary)],
    )
    .unwrap();
    let error = match hop_read::lease_output(batch, charge) {
        Ok(_) => panic!("dictionary output has no admitted claim traversal"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("unsupported Arrow allocation bound for Dictionary"),
        "{error}"
    );
    assert!(!error.to_string().contains("overflow"), "{error}");
    drop(reservation);
    assert_eq!(pool.leaked(), 0);
}

#[test]
fn hop_property_target_nodes_release_before_the_next_workspace_and_on_error() {
    let pool = ConsumerPool::new(1024 * 1024);
    let reservation = hop_read_reservation(&pool);
    let mut charge = HopReadCharge::new(&reservation);
    charge.charge(4096).unwrap();
    let targets = (0..1024).collect();
    let ordered = hop_read::with_property_targets(&mut charge, &targets, |ordered| {
        assert!(
            pool.leaked() > 512 * 1024,
            "nodes must be prepaid during lookup"
        );
        assert_eq!(
            ordered.iter().copied().collect::<Vec<_>>(),
            (0..1024).collect::<Vec<_>>()
        );
        Ok(ordered.len())
    })
    .unwrap();
    assert_eq!(ordered, 1024);
    assert_eq!(charge.held_bytes(), 4096);
    // This next phase cannot coexist with the target-tree envelope in 1 MiB.
    charge.charge(768 * 1024).unwrap();
    charge.release(768 * 1024);
    let failed = hop_read::with_property_targets(&mut charge, &targets, |_| {
        Err::<(), _>(GfError::Execution("late property lookup failure".into()))
    });
    assert!(
        failed
            .unwrap_err()
            .to_string()
            .contains("late property lookup failure")
    );
    assert_eq!(charge.held_bytes(), 4096);
    let too_many = (0..4096).collect();
    let called = std::cell::Cell::new(false);
    let refused = hop_read::with_property_targets(&mut charge, &too_many, |_| {
        called.set(true);
        Ok(())
    });
    assert!(refused.unwrap_err().to_string().contains(HOP_READ_CONSUMER));
    assert!(
        !called.get(),
        "lookup must not run before target-node admission"
    );
    assert_eq!(charge.held_bytes(), 4096);
    drop(charge);
    drop(reservation);
    assert_eq!(pool.leaked(), 0);
}

#[test]
fn hop_struct_take_uses_real_indices_including_children_of_null_parents() {
    use arrow::array::{AsArray, StructArray};
    use arrow::buffer::NullBuffer;
    let wide = "w".repeat(1024 * 1024);
    let field = Arc::new(Field::new("payload", DataType::Utf8, false));
    let source: ArrayRef = Arc::new(StructArray::new(
        vec![field].into(),
        vec![Arc::new(StringArray::from(vec![
            wide.as_str(),
            "small",
            "other",
        ]))],
        Some(NullBuffer::from(vec![false, true, true])),
    ));
    // The selected payload is 1 MiB plus small strings; max*rows exceeds i32.
    // Arrow still takes the wide child beneath its NULL Struct parent.
    let indices = UInt32Array::from_iter_values(std::iter::repeat_n(1, 4096).chain([0]));
    let pool = ConsumerPool::new(4 * 1024 * 1024);
    let reservation = hop_read_reservation(&pool);
    let mut charge = HopReadCharge::new(&reservation);
    let selected = hop_read::take(&mut charge, &source, &indices).unwrap();
    assert_eq!(selected.len(), 4097);
    assert_eq!(selected.data_type(), source.data_type());
    let result = selected.as_struct();
    let strings = result.column(0).as_string::<i32>();
    assert!((0..4096).all(|row| result.is_valid(row) && strings.value(row) == "small"));
    assert!(result.is_null(4096));
    assert_eq!(strings.value(4096), wide);
    let batch = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "carried",
            selected.data_type().clone(),
            true,
        )])),
        vec![selected],
    )
    .unwrap();
    let output = hop_read::lease_output(batch, charge).unwrap();
    let escaped = output.batch.column(0).as_struct().column(0).slice(4096, 1);
    drop(output);
    drop(reservation);
    drop(source);
    assert!(pool.leaked() >= 1024 * 1024);
    assert_eq!(escaped.as_string::<i32>().value(0), wide);
    drop(escaped);
    assert_eq!(pool.leaked(), 0);

    let source: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("payload", DataType::Utf8, false))].into(),
        vec![Arc::new(StringArray::from(vec![wide.as_str()]))],
        None,
    ));
    let reservation = hop_read_reservation(&pool);
    let mut charge = HopReadCharge::new(&reservation);
    let indices = UInt32Array::from(vec![0; 8]);
    let error = hop_read::take(&mut charge, &source, &indices).unwrap_err();
    assert!(error.to_string().contains(HOP_READ_CONSUMER), "{error}");
    assert_eq!(
        charge.held_bytes(),
        0,
        "refused construction must not change ownership"
    );
    drop(charge);
    drop(reservation);
    assert_eq!(pool.leaked(), 0);
}

#[test]
fn hop_zip_bound_includes_large_nullable_empty_struct_validity() {
    use arrow::array::StructArray;
    use arrow::buffer::NullBuffer;
    let rows = 262_144;
    let truthy: ArrayRef = Arc::new(StructArray::new_empty_fields(
        rows,
        Some(NullBuffer::from_iter((0..rows).map(|row| row % 2 == 0))),
    ));
    let falsy: ArrayRef = Arc::new(StructArray::new_empty_fields(
        rows,
        Some(NullBuffer::from_iter((0..rows).map(|row| row % 3 == 0))),
    ));
    let mask = arrow::array::BooleanArray::from_iter((0..rows).map(|row| Some(row % 5 == 0)));
    let bound = hop_read::zip_copy_bytes(truthy.as_ref()).unwrap();
    let pool = ConsumerPool::new(256 * 1024);
    let reservation = hop_read_reservation(&pool);
    let mut charge = HopReadCharge::new(&reservation);
    charge.charge(bound).unwrap();
    let output = arrow::compute::kernels::zip::zip(&mask, &truthy, &falsy).unwrap();
    assert_eq!(output.len(), rows);
    assert_eq!(output.data_type(), truthy.data_type());
    assert!((0..rows).all(|row| output.is_valid(row)
        == if row % 5 == 0 {
            row % 2 == 0
        } else {
            row % 3 == 0
        }));
    assert!(
        bound >= output.get_array_memory_size(),
        "the actual parent bitmap must fit without any children hiding its size"
    );
    drop(output);
    drop(charge);
    drop(reservation);
    assert_eq!(pool.leaked(), 0);
}

#[test]
fn hop_completed_constructors_release_workspace_but_retain_earlier_columns() {
    use arrow::array::AsArray;
    use arrow::datatypes::TimeUnit;
    let rows = 32_768;
    let field = Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    );
    let pool = ConsumerPool::new(2 * 1024 * 1024);
    let reservation = hop_read_reservation(&pool);
    let mut charge = HopReadCharge::new(&reservation);
    let first = hop_read::construct_array(
        &mut charge,
        hop_read::unused_bytes(field.data_type(), rows).unwrap(),
        || {
            assert!(
                pool.leaked() >= 1024 * 1024,
                "the complete workspace must be reserved before the real constructor"
            );
            unused_expand_column(&field, rows)
        },
    )
    .unwrap();
    let backing = first.get_buffer_memory_size();
    assert!(backing >= rows * 8);
    let second = hop_read::unused(&mut charge, &field, rows).unwrap();
    assert_eq!(
        first
            .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
            .value(rows - 1),
        0
    );
    let third = hop_read::unused(&mut charge, &field, rows).unwrap();
    assert_eq!(
        second
            .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
            .value(rows - 1),
        0
    );
    assert!(
        charge.held_bytes() >= 3 * backing,
        "earlier columns remain fully admitted"
    );
    // Three simultaneous future workspaces exceed 2 MiB; three completed
    // columns and one current constructor fit without changing its envelope.
    assert!(charge.held_bytes() < 1024 * 1024);
    let batch = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            field.clone().with_name("first"),
            field.clone().with_name("second"),
            field.with_name("third"),
        ])),
        vec![first, second, third],
    )
    .unwrap();
    let output = hop_read::lease_output(batch, charge).unwrap();
    let escaped = output.batch.column(0).slice(rows - 1, 1);
    drop(output);
    drop(reservation);
    assert!(
        pool.leaked() >= backing,
        "a one-row slice retains its full backing"
    );
    assert!(
        pool.leaked() < 2 * backing,
        "dropped sibling columns release independently"
    );
    assert_eq!(
        escaped
            .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
            .value(0),
        0
    );
    drop(escaped);
    assert_eq!(pool.leaked(), 0);
}

#[test]
fn hop_property_filter_bounds_real_spans_and_retains_a_wide_selected_value() {
    use arrow::array::AsArray;
    let wide = "a".repeat(1024 * 1024);
    let source: ArrayRef = Arc::new(StringArray::from_iter(
        [Some(wide.as_str())]
            .into_iter()
            .chain(std::iter::repeat_n(None, 4096)),
    ));
    let mask = arrow::compute::is_not_null(&source).unwrap();
    assert_eq!(mask.true_count(), 1);

    // The fixture is already produced; this isolated constructor control has
    // no room for its selected payload, irrespective of source ownership.
    let tight = ConsumerPool::new(64 * 1024);
    let reservation = hop_read_reservation(&tight);
    let mut charge = HopReadCharge::new(&reservation);
    let error = hop_read::filter(&mut charge, &source, &mask).unwrap_err();
    assert!(error.to_string().contains(HOP_READ_CONSUMER), "{error}");
    assert_eq!(
        charge.held_bytes(),
        0,
        "admission must refuse before entering the Arrow constructor"
    );
    drop(charge);
    drop(reservation);
    assert_eq!(tight.leaked(), 0);

    let pool = ConsumerPool::new(8 * 1024 * 1024);
    let reservation = hop_read_reservation(&pool);
    // This control includes retained producer input and the real predicate
    // mask in the same pool, in addition to the unchanged4x copy envelope.
    let mut input_charge = HopReadCharge::new(&reservation);
    input_charge
        .charge(source.get_array_memory_size() + mask.get_array_memory_size())
        .unwrap();
    let mut charge = HopReadCharge::new(&reservation);
    let filtered = hop_read::filter(&mut charge, &source, &mask).unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered.data_type(), source.data_type());
    assert_eq!(filtered.as_string::<i32>().value(0), wide);
    let batch = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "note",
            DataType::Utf8,
            true,
        )])),
        vec![filtered],
    )
    .unwrap();
    let output = hop_read::lease_output(batch, charge).unwrap();
    let escaped = output.batch.column(0).slice(0, 1);
    drop(output);
    drop(mask);
    drop(source);
    drop(input_charge);
    drop(reservation);
    assert!(pool.leaked() >= escaped.get_buffer_memory_size());
    assert_eq!(escaped.as_string::<i32>().value(0), wide);
    drop(escaped);
    assert_eq!(pool.leaked(), 0);
}

#[test]
fn hop_property_filter_preserves_populated_null_spans_and_full_alias_backing() {
    use arrow::array::AsArray;
    use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
    let wide = "n".repeat(1024 * 1024);
    for selected_null in [true, false] {
        // Arrow permits a populated byte span below NULL, and filter_bytes
        // explicitly copies it. The other case slices down to three visible
        // bytes, then takes the all-selected alias fast path.
        let raw = StringArray::new(
            OffsetBuffer::new(ScalarBuffer::from(vec![
                0_i32,
                i32::try_from(wide.len()).unwrap(),
                i32::try_from(wide.len() + 3).unwrap(),
            ])),
            Buffer::from(format!("{wide}yes").into_bytes()),
            Some(NullBuffer::from(vec![false, true])),
        );
        let source: ArrayRef = Arc::new(if selected_null {
            raw.clone()
        } else {
            raw.slice(1, 1)
        });
        let mask = arrow::array::BooleanArray::from(if selected_null {
            vec![true, false]
        } else {
            vec![true]
        });
        let tight = ConsumerPool::new(64 * 1024);
        let reservation = hop_read_reservation(&tight);
        let mut charge = HopReadCharge::new(&reservation);
        let error = hop_read::filter(&mut charge, &source, &mask).unwrap_err();
        assert!(error.to_string().contains(HOP_READ_CONSUMER), "{error}");
        assert_eq!(charge.held_bytes(), 0);
        drop(charge);
        drop(reservation);
        assert_eq!(tight.leaked(), 0);

        let pool = ConsumerPool::new(8 * 1024 * 1024);
        let reservation = hop_read_reservation(&pool);
        let mut input_charge = HopReadCharge::new(&reservation);
        input_charge
            .charge(source.get_array_memory_size() + mask.get_array_memory_size())
            .unwrap();
        let mut charge = HopReadCharge::new(&reservation);
        let selected = hop_read::filter(&mut charge, &source, &mask).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected.is_null(0), selected_null);
        let strings = selected.as_string::<i32>();
        assert_eq!(
            strings.value(0),
            if selected_null { wide.as_str() } else { "yes" }
        );
        assert!(selected.get_buffer_memory_size() >= wide.len());
        let batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "note",
                DataType::Utf8,
                true,
            )])),
            vec![selected],
        )
        .unwrap();
        let output = hop_read::lease_output(batch, charge).unwrap();
        let escaped = output.batch.column(0).slice(0, 1);
        drop(output);
        drop(raw);
        drop(source);
        drop(mask);
        drop(input_charge);
        drop(reservation);
        assert!(pool.leaked() >= wide.len());
        assert_eq!(escaped.is_null(0), selected_null);
        assert_eq!(
            escaped.as_string::<i32>().value(0),
            if selected_null { wide.as_str() } else { "yes" }
        );
        drop(escaped);
        assert_eq!(pool.leaked(), 0);
    }
}
