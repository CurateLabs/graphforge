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
