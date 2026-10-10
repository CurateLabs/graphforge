use std::collections::HashMap;
use std::fs::{self, File};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{BooleanArray, FixedSizeBinaryArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{JoinType, NullEquality};
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use sha2::Digest;

use crate::property_overlay::{
    PROPERTY_GENERATION_KEY, PROPERTY_KIND_KEY, PROPERTY_ORDINAL_KEY, PROPERTY_OVERLAY_FORMAT,
    PROPERTY_OVERLAY_FORMAT_KEY, PROPERTY_ROUTE_KEY, PROPERTY_TOMBSTONE_FIELD,
};
use crate::{AuthenticatedPropertyInventory, PropertyFragmentId, PropertyTable};

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = sha2::Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn write_property_route(
    root: &std::path::Path,
    rows: usize,
    rows_per_fragment: usize,
    value: &str,
) -> Arc<AuthenticatedPropertyInventory> {
    let route_dir = root.join("properties/Person");
    fs::create_dir_all(&route_dir).unwrap();
    let mut entries = Vec::new();
    let mut start = 0_usize;
    let mut ordinal = 0_u64;
    while start < rows {
        let count = rows_per_fragment.min(rows - start);
        let metadata = HashMap::from([
            (
                PROPERTY_OVERLAY_FORMAT_KEY.into(),
                PROPERTY_OVERLAY_FORMAT.into(),
            ),
            (PROPERTY_ROUTE_KEY.into(), "Person".into()),
            (PROPERTY_KIND_KEY.into(), "node".into()),
            (PROPERTY_GENERATION_KEY.into(), "1".into()),
            (PROPERTY_ORDINAL_KEY.into(), ordinal.to_string()),
        ]);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                Field::new("value", DataType::Utf8, false),
            ],
            metadata,
        ));
        let ids = (start..start + count)
            .map(|row| (row as u128).to_be_bytes())
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(ids.iter().map(|id| id.as_slice()))
                        .unwrap(),
                ),
                Arc::new(BooleanArray::from(vec![false; count])),
                Arc::new(StringArray::from(vec![value; count])),
            ],
        )
        .unwrap();
        let id = PropertyFragmentId {
            generation: 1,
            ordinal,
        };
        let path = route_dir.join(id.file_name());
        let properties = WriterProperties::builder().build();
        let mut writer =
            ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let bytes = fs::read(&path).unwrap();
        entries.push(crate::GraphFileEntry {
            relative_path: format!("properties/Person/{}", id.file_name()),
            byte_length: u64::try_from(bytes.len()).unwrap(),
            content_sha256: sha256_hex(&bytes),
            content_xxh64: crate::corruption_checksum::checksum(&bytes),
            role: crate::GraphFileRole::Properties,
        });
        start += count;
        ordinal += 1;
    }
    Arc::new(AuthenticatedPropertyInventory::from_entries_at_root(root, entries).unwrap())
}

fn property_session(
    inventory: Arc<AuthenticatedPropertyInventory>,
    root: &std::path::Path,
    target_partitions: usize,
) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(target_partitions);
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_physical_optimizer_rule(Arc::new(crate::PropertyFilterApprovalRule))
        .build();
    let context = SessionContext::new_with_state(state);
    let table = PropertyTable::open_authenticated(root, "Person", inventory).unwrap();
    context.register_table("props", Arc::new(table)).unwrap();
    context
}

#[tokio::test]
async fn topk_and_uuid_extrema_do_not_wait_for_their_own_scan() {
    let root = tempfile::tempdir().unwrap();
    let inventory = write_property_route(root.path(), 64, 16, "ok");
    for partitions in [1, 2, 4] {
        let context = property_session(Arc::clone(&inventory), root.path(), partitions);
        let ordered = tokio::time::timeout(Duration::from_secs(10), async {
            context
                .sql("SELECT node_uuid FROM props ORDER BY node_uuid LIMIT 1")
                .await?
                .collect()
                .await
        })
        .await
        .expect("top-k property scan must not wait for its own dynamic filter")
        .unwrap();
        assert_eq!(ordered.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        let extrema = tokio::time::timeout(Duration::from_secs(10), async {
            context
                .sql("SELECT MIN(node_uuid), MAX(node_uuid) FROM props")
                .await?
                .collect()
                .await
        })
        .await
        .expect("UUID extrema must not wait for their own dynamic filter")
        .unwrap();
        assert_eq!(extrema.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        let wanted_ids = [0_u128, 7, 63]
            .map(u128::to_be_bytes)
            .into_iter()
            .collect::<Vec<_>>();
        let wanted = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "node_uuid",
                DataType::FixedSizeBinary(16),
                false,
            )])),
            vec![Arc::new(
                FixedSizeBinaryArray::try_from_iter(wanted_ids.iter().map(|id| id.as_slice()))
                    .unwrap(),
            )],
        )
        .unwrap();
        let wanted_table = MemTable::try_new(wanted.schema(), vec![vec![wanted]]).unwrap();
        context
            .register_table("wanted", Arc::new(wanted_table))
            .unwrap();
        let joined = context
            .sql("SELECT p.node_uuid FROM props p JOIN wanted w USING (node_uuid)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(joined.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }
}

#[tokio::test]
async fn oversized_limited_scan_emits_nothing_before_a_late_decode_failure() {
    const ROWS: usize = 66_000;
    const VALUE_BYTES: usize = 1024;
    assert!(
        ROWS * VALUE_BYTES > super::MAX_HELD_BYTES,
        "selected value bytes alone exceed the bounded holdback"
    );
    let root = tempfile::tempdir().unwrap();
    let inventory = write_property_route(root.path(), ROWS, 2_000, &"x".repeat(VALUE_BYTES));
    inventory.fail_decoder_on_row(65_900);
    let context = property_session(Arc::clone(&inventory), root.path(), 1);
    let mut stream = context
        .sql(&format!("SELECT value FROM props LIMIT {ROWS}"))
        .await
        .unwrap()
        .execute_stream()
        .await
        .unwrap();
    let mut emitted_rows = 0_usize;
    let mut failure = None;
    while let Some(item) = stream.next().await {
        match item {
            Ok(batch) => emitted_rows += batch.num_rows(),
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    let failure = failure.expect("late authenticated decoder failure must be returned");
    assert!(
        failure
            .to_string()
            .contains("injected late authenticated property decoder failure")
    );
    assert_eq!(
        emitted_rows, 0,
        "LIMIT must not observe rows before validation"
    );

    let replayed = context
        .sql(&format!("SELECT value FROM props LIMIT {ROWS}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        replayed.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ROWS,
        "a successful over-cap validation pass must replay the limited prefix"
    );
}

fn property_uuid_join(
    root: &std::path::Path,
    inventory: Arc<AuthenticatedPropertyInventory>,
    limit: Option<usize>,
    join_type: JoinType,
) -> Arc<dyn ExecutionPlan> {
    let table = PropertyTable::open_authenticated(root, "Person", Arc::clone(&inventory)).unwrap();
    let schema = table.schema();
    let build_schema = Arc::new(Schema::new(vec![Field::new(
        "node_uuid",
        DataType::FixedSizeBinary(16),
        false,
    )]));
    let build_key_bytes = 1_u128.to_be_bytes();
    let build_batch = RecordBatch::try_new(
        Arc::clone(&build_schema),
        vec![Arc::new(
            FixedSizeBinaryArray::try_from_iter([build_key_bytes.as_slice()].into_iter()).unwrap(),
        )],
    )
    .unwrap();
    let build: Arc<dyn ExecutionPlan> =
        TestMemoryExec::try_new_exec(&[vec![build_batch]], build_schema, None).unwrap();

    let probe_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("node_uuid", 0));
    let scan = super::PropertyOverlayExec::try_new(
        root.to_path_buf(),
        Some(inventory),
        "Person".into(),
        false,
        schema,
        super::PropertyScanOptions {
            projection: None,
            limit,
            batch_size: 16,
            footer_statistics: true,
            equality: None,
        },
    )
    .unwrap();

    Arc::new(
        HashJoinExec::try_new(
            build,
            Arc::new(scan),
            vec![(
                Arc::new(Column::new("node_uuid", 0)) as Arc<dyn PhysicalExpr>,
                probe_key,
            )],
            None,
            &join_type,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    )
}

fn post_pushdown(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    FilterPushdown::new_post_optimization()
        .optimize(plan, &datafusion::common::config::ConfigOptions::default())
        .unwrap()
}

async fn collect_rows(plan: Arc<dyn ExecutionPlan>) -> usize {
    let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    let mut emitted_rows = 0_usize;
    while let Some(batch) = stream.next().await {
        emitted_rows += batch.unwrap().num_rows();
    }
    emitted_rows
}

#[tokio::test]
async fn limited_inner_and_right_semi_scans_preserve_their_prefix_under_uuid_hints() {
    let root = tempfile::tempdir().unwrap();
    let inventory = write_property_route(root.path(), 3, 3, "ok");

    for join_type in [JoinType::Inner, JoinType::RightSemi] {
        // Rows have UUIDs u=0, v=1, w=2. The build owns only v, while the
        // original scan LIMIT 1 exposes only u. The real physical pushdown
        // rule must attach a dynamic UUID hint to the limited scan, but final
        // approval must decline to move it ahead of that LIMIT.
        let pushed = post_pushdown(property_uuid_join(
            root.path(),
            Arc::clone(&inventory),
            Some(1),
            join_type,
        ));
        let pushed_join = pushed.downcast_ref::<HashJoinExec>().unwrap();
        assert!(pushed_join.dynamic_filter_expr().is_some());
        let pushed_scan = pushed_join
            .right()
            .downcast_ref::<super::PropertyOverlayExec>()
            .expect("pushdown keeps the direct limited probe scan");
        assert!(
            !pushed_scan.uuid_filters.is_empty(),
            "DataFusion post-optimization pushdown should retain its dynamic hint"
        );
        assert_eq!(pushed_scan.limit, Some(1));
        assert_eq!(collect_rows(pushed).await, 0);

        let approved = post_pushdown(property_uuid_join(
            root.path(),
            Arc::clone(&inventory),
            Some(1),
            join_type,
        ));
        let approved = crate::PropertyFilterApprovalRule
            .optimize(
                approved,
                &datafusion::common::config::ConfigOptions::default(),
            )
            .unwrap();
        assert_eq!(
            collect_rows(approved).await,
            0,
            "UUID nominations must not change a limited {join_type:?} result"
        );
    }

    // An unlimited probe remains eligible and still matches UUID v.
    let unlimited = post_pushdown(property_uuid_join(
        root.path(),
        inventory,
        None,
        JoinType::Inner,
    ));
    let unlimited = crate::PropertyFilterApprovalRule
        .optimize(
            unlimited,
            &datafusion::common::config::ConfigOptions::default(),
        )
        .unwrap();
    assert_eq!(collect_rows(unlimited).await, 1);
}
