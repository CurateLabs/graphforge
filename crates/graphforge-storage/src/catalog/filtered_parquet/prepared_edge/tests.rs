use super::*;
use arrow::array::{
    Array, AsArray, FixedSizeBinaryArray, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

struct Budget {
    pool: Arc<GreedyMemoryPool>,
    reservation: Mutex<MemoryReservation>,
    cancelled: AtomicBool,
}
impl Budget {
    fn new(bytes: usize) -> Self {
        let pool = Arc::new(GreedyMemoryPool::new(bytes));
        let authority: Arc<dyn MemoryPool> = pool.clone();
        let reservation = MemoryConsumer::new("prepared edge read control").register(&authority);
        Self {
            pool,
            reservation: Mutex::new(reservation),
            cancelled: AtomicBool::new(false),
        }
    }
}
impl ReadAdmission for Budget {
    fn reserve(&self, bytes: usize) -> Result<ReadGrant> {
        self.check_cancelled()?;
        let reservation = self.reservation.lock().unwrap();
        reservation.try_grow(bytes)?;
        Ok(ReadGrant::from_reserved(reservation.split(bytes)))
    }
    fn check_cancelled(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(DataFusionError::Execution("prepared read cancelled".into()));
        }
        Ok(())
    }
}
fn edge_batch(start: u64, rows: usize, exploratory: bool) -> RecordBatch {
    let uuid = || {
        Arc::new(
            FixedSizeBinaryArray::try_from_iter((start..start + rows as u64).map(|id| {
                let mut uuid = [0; 16];
                uuid[8..].copy_from_slice(&id.to_be_bytes());
                uuid
            }))
            .unwrap(),
        ) as arrow::array::ArrayRef
    };
    let mut columns = vec![uuid(), uuid(), uuid()];
    for offset in [0, 1, 2] {
        columns.push(Arc::new(UInt64Array::from_iter_values(
            start + offset..start + offset + rows as u64,
        )));
    }
    columns.push(Arc::new(
        TimestampMicrosecondArray::from_iter_values(
            (start..start + rows as u64).map(|id| id as i64),
        )
        .with_timezone("UTC"),
    ));
    let schema = if exploratory {
        columns.push(Arc::new(StringArray::from_iter_values(
            std::iter::repeat_n("AUTHENTICATED_RELATION", rows),
        )));
        crate::EXPLORATORY_EDGE_SCHEMA.clone()
    } else {
        crate::TYPED_EDGE_SCHEMA.clone()
    };
    RecordBatch::try_new(schema, columns).unwrap()
}
fn with_schema_metadata(batch: RecordBatch, value: &str) -> RecordBatch {
    let schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
        batch.schema().fields().clone(),
        std::collections::HashMap::from([("source_file".to_owned(), value.to_owned())]),
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap()
}

fn file(path: &Path, batch: &RecordBatch, indexes: bool) {
    use parquet::file::properties::EnabledStatistics;
    let properties = crate::permanent_parquet::point_access_writer_properties()
        .set_max_row_group_row_count(Some(2048))
        .set_statistics_enabled(if indexes {
            EnabledStatistics::Page
        } else {
            EnabledStatistics::Chunk
        })
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        batch.schema(),
        Some(properties),
    )
    .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}
fn read(
    path: &Path,
    source: &RecordBatch,
    ids: &HashSet<u64>,
    budget: &Budget,
    projection: &[usize],
) -> Result<PrepaidBatch> {
    let _request_credit = budget.reserve(ids.len() * 64 + 4096)?;
    let request = EdgeIdRequest::new(ids);
    let prepared = PreparedEdgeRead::prepare(path, &source.schema(), &request, projection, budget)?;
    prepared.decode(path, None, budget)
}

#[test]
fn prepared_edge_read_preserves_dense_predicate_and_majority_results() {
    let dir = tempfile::TempDir::new().unwrap();
    for exploratory in [false, true] {
        let source = with_schema_metadata(edge_batch(1, 4096, exploratory), "read-strategy");
        for indexes in [false, true] {
            let path = dir
                .path()
                .join(format!("edge-{exploratory}-{indexes}.parquet"));
            file(&path, &source, indexes);
            assert_eq!(
                crate::catalog::admitted_parquet(&path)
                    .unwrap()
                    .schema()
                    .metadata()
                    .get("source_file")
                    .map(String::as_str),
                Some("read-strategy")
            );
            for wanted in [vec![2, 2050], (1..=3000).collect()] {
                let budget = Budget::new(16 * 1024 * 1024);
                let ids = wanted.iter().copied().collect();
                let projection = if exploratory {
                    vec![3, 6, 7]
                } else {
                    vec![3, 6]
                };
                let result = read(&path, &source, &ids, &budget, &projection).unwrap();
                let canonical = if exploratory {
                    crate::EXPLORATORY_EDGE_SCHEMA.clone()
                } else {
                    crate::TYPED_EDGE_SCHEMA.clone()
                };
                let request = EdgeIdRequest::new(&ids);
                let ordinary = super::super::read_required_edge_filtered(
                    &path,
                    canonical,
                    &request,
                    None,
                    Some(&projection),
                )
                .unwrap();
                let ordinary =
                    arrow::compute::concat_batches(&ordinary[0].schema(), &ordinary).unwrap();
                assert_eq!(result.batch(), &ordinary);
                let takes = arrow::array::UInt32Array::from_iter_values(
                    wanted.iter().map(|id| (*id - 1) as u32),
                );
                let expected = source.project(&projection).unwrap();
                let expected = RecordBatch::try_new(
                    ordinary.schema(),
                    expected
                        .columns()
                        .iter()
                        .map(|array| arrow::compute::take(array, &takes, None).unwrap())
                        .collect(),
                )
                .unwrap();
                assert_eq!(result.batch(), &expected);
                assert!(budget.pool.reserved() >= result.batch().get_array_memory_size());
                drop(result);
                assert_eq!(budget.pool.reserved(), 0);
            }
        }
    }
}

#[test]
fn prepared_edge_metadata_refuses_before_ordinary_reader_and_cleans_up() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("edge.parquet");
    let source = edge_batch(1, 32, false);
    file(&path, &source, true);
    let budget = Budget::new(64 * 1024);
    let _capture = crate::lifecycle_io::CaptureScope::install();
    let error = read(&path, &source, &HashSet::from([2]), &budget, &[3, 6]).unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    let stats = crate::lifecycle_io::snapshot().unwrap();
    assert_eq!(
        stats
            .phases
            .get(&crate::StorageIoPhase::ReadPathScan)
            .map_or(0, |phase| phase.read_bytes),
        0
    );
    assert_eq!(budget.pool.reserved(), 0);
}

#[test]
fn prepared_edge_releases_finished_workspace_while_prior_backing_stays_live() {
    let dir = tempfile::TempDir::new().unwrap();
    let budget = Budget::new(16 * 1024 * 1024);
    let source = edge_batch(1, 4096, false);
    let path = dir.path().join("first.parquet");
    file(&path, &source, true);
    let ids = HashSet::from([2, 2050]);
    let first = read(&path, &source, &ids, &budget, &[3, 6]).unwrap();
    let retained = budget.pool.reserved();
    assert!(
        retained > 0 && retained < 1024 * 1024,
        "finished read retained {retained}"
    );
    let alias = first.batch().column_by_name("edge_id").unwrap().slice(0, 1);
    let backing = alias.get_buffer_memory_size();
    assert!(
        first.funds_buffer(
            alias
                .as_primitive::<arrow::datatypes::UInt64Type>()
                .values()
                .inner()
        )
    );
    let later = dir.path().join("later.parquet");
    file(&later, &source, true);
    let second = read(&later, &source, &ids, &budget, &[3, 6]).unwrap();
    assert_eq!(first.batch(), second.batch());
    assert_eq!(budget.pool.reserved(), retained * 2);
    drop(second);
    drop(first);
    assert_eq!(
        budget.pool.reserved(),
        backing + token_bytes(),
        "only escaped backing and its token survive"
    );
    assert_eq!(
        alias
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .value(0),
        2
    );
    drop(alias);
    assert_eq!(budget.pool.reserved(), 0);
    budget.cancelled.store(true, Ordering::Release);
    assert!(
        read(&later, &source, &ids, &budget, &[3])
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert_eq!(budget.pool.reserved(), 0);
}

#[test]
fn prepared_edge_public_inventory_multi_and_zero_file_results_keep_internal_schema_credit() {
    let dir = tempfile::TempDir::new().unwrap();
    let route = dir.path().join("topology/edges/KNOWS");
    std::fs::create_dir_all(&route).unwrap();
    let first = with_schema_metadata(edge_batch(1, 32, false), "first");
    let second = with_schema_metadata(edge_batch(33, 32, false), "second");
    file(&route.join("00000000000000000001.parquet"), &first, true);
    file(&route.join("00000000000000000033.parquet"), &second, true);
    let inventory = crate::AuthenticatedPropertyInventory::capture(dir.path()).unwrap();
    assert_eq!(inventory.edge_files(Some("KNOWS")).len(), 2);
    let budget = Budget::new(16 * 1024 * 1024);
    // Both files take the majority fallback. Its projected reader strips
    // top-level metadata just as dense/predicate reads do; the ordinary
    // authenticated route supplies the complete schema oracle.
    let wanted = (1..=50).collect();
    let result = read_edges_filtered_prepaid_from_inventory(
        &inventory,
        "KNOWS",
        OntologyMode::Strict,
        &wanted,
        &[3, 6],
        None,
        &budget,
    )
    .unwrap();
    let ordinary = super::super::read_edges_filtered_projected_from_inventory(
        &inventory,
        "KNOWS",
        OntologyMode::Strict,
        &wanted,
        &[3, 6],
        None,
    )
    .unwrap();
    let expected = arrow::compute::concat_batches(&ordinary[0].schema(), &ordinary).unwrap();
    assert_eq!(result.batch(), &expected);
    assert!(expected.schema().metadata().is_empty());
    assert_eq!(
        result
            .batch()
            .column_by_name("edge_id")
            .unwrap()
            .as_primitive::<arrow::datatypes::UInt64Type>()
            .values()
            .as_ref(),
        &(1..=50).collect::<Vec<u64>>()
    );
    assert_eq!(
        result
            .batch()
            .column_by_name("created_at")
            .unwrap()
            .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
            .values()
            .as_ref(),
        &(1..=50).collect::<Vec<i64>>()
    );
    assert_eq!(
        result
            .batch()
            .column_by_name("created_at")
            .unwrap()
            .data_type(),
        &arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into())
        )
    );
    let schema = result.batch().schema();
    let owner = result.into_schema_owner();
    assert!(
        budget.pool.reserved() > 0,
        "the borrowed internal schema still has its transferred owner after data drops"
    );
    assert_eq!(schema.fields(), expected.schema().fields());
    drop(schema);
    drop(owner);
    assert_eq!(budget.pool.reserved(), 0);
    let empty = read_edges_filtered_prepaid_from_inventory(
        &inventory,
        "ABSENT_ROUTE",
        OntologyMode::Strict,
        &wanted,
        &[3, 6],
        None,
        &budget,
    )
    .unwrap();
    assert_eq!(empty.batch().num_rows(), 0);
    assert!(empty.batch().schema().metadata().is_empty());
    let schema = empty.batch().schema();
    assert_eq!(schema.fields(), expected.schema().fields());
    let owner = empty.into_schema_owner();
    assert!(
        budget.pool.reserved() > 0,
        "a bufferless internal result must not lose its schema credit"
    );
    drop(schema);
    drop(owner);
    assert_eq!(budget.pool.reserved(), 0);
}

#[test]
fn prepared_edge_absent_route_admits_projection_normalization_before_its_copy() {
    let dir = tempfile::TempDir::new().unwrap();
    let route = dir.path().join("topology/edges/KNOWS");
    std::fs::create_dir_all(&route).unwrap();
    file(
        &route.join("00000000000000000001.parquet"),
        &edge_batch(1, 32, false),
        true,
    );
    let inventory = crate::AuthenticatedPropertyInventory::capture(dir.path()).unwrap();
    let ids = HashSet::from([2]);
    let projection = vec![3; 131_072]; // a valid1MiB repeated projection
    let tight = Budget::new(64 * 1024);
    let error = read_edges_filtered_prepaid_from_inventory(
        &inventory,
        "ABSENT_ROUTE",
        OntologyMode::Strict,
        &ids,
        &projection,
        None,
        &tight,
    )
    .unwrap_err();
    assert!(
        matches!(error, DataFusionError::ResourcesExhausted(_)),
        "{error}"
    );
    assert_eq!(tight.pool.reserved(), 0);
    let roomy = Budget::new(16 * 1024 * 1024);
    let result = read_edges_filtered_prepaid_from_inventory(
        &inventory,
        "ABSENT_ROUTE",
        OntologyMode::Strict,
        &ids,
        &projection,
        None,
        &roomy,
    )
    .unwrap();
    assert_eq!(result.batch().num_rows(), 0);
    assert_eq!(result.batch().num_columns(), 1);
    assert_eq!(
        result.batch().schema().field(0),
        crate::TYPED_EDGE_SCHEMA.field(3)
    );
    drop(result);
    assert_eq!(roomy.pool.reserved(), 0);
}

#[test]
fn prepared_edge_missing_index_pruned_read_copies_only_live_predicate_ids() {
    let dir = tempfile::TempDir::new().unwrap();
    let route = dir.path().join("topology/edges/KNOWS");
    std::fs::create_dir_all(&route).unwrap();
    let path = route.join("00000000000000000001.parquet");
    let source = edge_batch(1, 32, false);
    file(&path, &source, false);
    let inventory = crate::AuthenticatedPropertyInventory::capture(dir.path()).unwrap();
    let options = parquet::arrow::arrow_reader::ArrowReaderOptions::new()
        .with_page_index_policy(PageIndexPolicy::Optional);
    let ordinary = crate::catalog::admitted_parquet_with_options(&path, options).unwrap();
    assert!(
        ordinary.metadata().row_groups().iter().all(|group| {
            group
                .columns()
                .iter()
                .all(|column| column.column_index_offset().is_none())
        }),
        "the genuine file has no serialized column indexes"
    );
    assert!(
        super::super::dense_node_layout(ordinary.metadata(), 3).is_none(),
        "the actual read must use the predicate path"
    );
    drop(ordinary);
    // The caller's existing input table is deliberately sparse. The measured
    // reader budget funds its new predicate copy, not the caller's fixture.
    let mut ids = HashSet::with_capacity(262_144);
    ids.insert(99_999);
    assert!(ids.capacity() > 100_000);
    let expected = RecordBatch::new_empty(source.project(&[3, 6]).unwrap().schema());
    // The unchanged seven-column metadata envelope alone exceeds 1 MiB.
    // Two MiB funds that legitimate workspace while remaining smaller than
    // the sparse input table whose retained capacity must not be cloned.
    for limit in [2 * 1024 * 1024, 16 * 1024 * 1024] {
        let budget = Budget::new(limit);
        let result = read_edges_filtered_prepaid_from_inventory(
            &inventory,
            "KNOWS",
            OntologyMode::Strict,
            &ids,
            &[3, 6],
            None,
            &budget,
        )
        .unwrap();
        assert_eq!(result.batch(), &expected);
        drop(result);
        assert_eq!(budget.pool.reserved(), 0);
    }
}
