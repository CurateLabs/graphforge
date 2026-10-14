use super::*;
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
        self.record(
            reservation.consumer().name(),
            reservation.size().saturating_sub(shrink),
        );
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
) -> std::sync::Mutex<crate::chunked_reservation::ChunkedReservation> {
    let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = pool.clone();
    std::sync::Mutex::new(crate::chunked_reservation::ChunkedReservation::register(
        HOP_READ_CONSUMER,
        &pool,
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
    let joined = concat_charged(&mut charge, batches).unwrap();
    assert_eq!(joined.num_rows(), 3 * 4096);
    assert_eq!(charge.held, decoded, "the joined copy replaces the batches");
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
    let shared = concat_charged(&mut charge, lone).unwrap();
    assert_eq!(shared.num_rows(), 4096);
    assert_eq!(charge.held, decoded);
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
    let error = concat_charged(&mut charge, batches)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(HOP_READ_CONSUMER) && error.contains("Resources exhausted"),
        "{error}"
    );
    assert_eq!(
        charge.held, decoded,
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
