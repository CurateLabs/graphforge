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

/// Expand `seeds` one hop over `KNOWS`, demanding only the destination's
/// `node_id` and `type_ids`, and return each reached row as
/// `(node_id, type_ids)`, sorted.
async fn expand_destination_labels(
    provider: &Arc<dyn AdjacencyProvider>,
    resource: &read_resource::GraphReadContext,
    seeds: &[u64],
) -> Result<Vec<(u64, Vec<u32>)>, GfError> {
    use arrow::array::{Array, ListArray};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::logical_plan::LogicalTableSource;
    use datafusion::physical_plan::collect;

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
    let exec = ExpandExec::new(&node, input, Arc::clone(provider), None, false, resource);
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
