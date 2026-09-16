use super::super::MAX_ADMITTED_COLUMN_BYTES;
use super::super::MAX_ADMITTED_PARQUET_COLUMNS;
use super::super::PropertyTable;
use super::read_properties;
use super::read_properties_batched;
use super::visit_property_fragments_admitted;
use super::visit_property_overlay_batched_projected;
use arrow::array::Array;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion_catalog::Session;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn property_overlay_batches_honor_bound_and_canonical_schema() {
    let dir = TempDir::new().unwrap();
    let mut writer =
        crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1).unwrap();
    for (index, name) in ["Ada", "Grace", "Katherine"].into_iter().enumerate() {
        let uuid = graphforge_core::uuid::new_v7();
        writer
            .create_node(
                uuid,
                graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
            )
            .unwrap();
        let mut values = HashMap::from([(
            "name".to_owned(),
            graphforge_ir::IrLiteral::Str(name.to_owned()),
        )]);
        if index == 0 {
            values.insert("year".into(), graphforge_ir::IrLiteral::Int(1815));
        }
        writer
            .set_properties(&uuid, Some("Person"), values)
            .unwrap();
    }
    writer.flush().unwrap();

    let batches = read_properties_batched(dir.path(), "Person", 1).unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert!(batches.iter().all(|batch| batch.num_rows() <= 1));
    assert!(
        batches
            .iter()
            .all(|batch| batch.schema() == batches[0].schema())
    );
    assert!(batches[0].column_by_name("year").is_some());
}

#[test]
fn property_overlay_adapter_preserves_error_sources() {
    use graphforge_core::{GfError, ProjectErrorCode};
    let dir = TempDir::new().unwrap();
    let mut writer =
        crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1).unwrap();
    let uuid = graphforge_core::uuid::new_v7();
    writer
        .create_node(
            uuid,
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
        )
        .unwrap();
    writer
        .set_properties(
            &uuid,
            Some("Person"),
            HashMap::from([(
                "name".to_owned(),
                graphforge_ir::IrLiteral::Str("Ada".to_owned()),
            )]),
        )
        .unwrap();
    writer.flush().unwrap();
    let inventory = crate::property_overlay::authenticated_property_inventory_for_route(
        dir.path(),
        crate::PropertyRouteKind::Node,
        "Person",
    )
    .unwrap();
    let original = GfError::Project {
        code: ProjectErrorCode::ProjectCorrupt,
        message: "typed visitor failure".into(),
    };
    // Exercise both callbacks inside the reader and the trailing batch.
    for batch_size in [1, 1024] {
        let error = visit_property_overlay_batched_projected(
            dir.path(),
            Some(&inventory),
            "Person",
            false,
            batch_size,
            None,
            |_| Err(DataFusionError::External(Box::new(original.clone()))),
        )
        .unwrap_err();
        let recovered = GfError::from_execution_error(error);
        assert_eq!(recovered.code(), original.code());
        assert_eq!(recovered.to_string(), original.to_string());
        let error = visit_property_overlay_batched_projected(
            dir.path(),
            Some(&inventory),
            "Person",
            false,
            batch_size,
            None,
            |_| {
                Err(DataFusionError::Execution(
                    "GF_PROJECT_CORRUPT: diagnostic only".into(),
                ))
            },
        )
        .unwrap_err();
        assert_eq!(GfError::from_execution_error(error).code(), "GF_EXECUTION");
    }
    // Inventory acquisition and consumption of retained inventory must both
    // preserve the reader's structured refusal, before delivering any rows.
    let fragment = crate::property_overlay::enumerate_property_fragments(
        dir.path(),
        crate::PropertyRouteKind::Node,
        &crate::route_component::component("Person"),
    )
    .unwrap()
    .pop()
    .unwrap()
    .path;
    std::fs::write(fragment, b"corrupt").unwrap();
    for retained in [None, Some(&inventory)] {
        let error = visit_property_overlay_batched_projected(
            dir.path(),
            retained,
            "Person",
            false,
            1,
            None,
            |_| panic!("corrupt property data must not reach the visitor"),
        )
        .unwrap_err();
        assert!(matches!(
            GfError::from_execution_error(error),
            GfError::Project {
                code: ProjectErrorCode::ProjectCorrupt,
                ..
            }
        ));
    }
}

#[tokio::test]
async fn property_sql_and_direct_reads_share_newest_overlay_authority() {
    let dir = TempDir::new().unwrap();
    let uuid = graphforge_core::uuid::new_v7();
    let mut writer =
        crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1).unwrap();
    writer
        .create_node(
            uuid,
            graphforge_value::EntityTypeId::ontology(graphforge_core::TypeId(1)).unwrap(),
        )
        .unwrap();
    writer
        .set_properties(
            &uuid,
            Some("Person"),
            HashMap::from([(
                "name".to_owned(),
                graphforge_ir::IrLiteral::Str("old".into()),
            )]),
        )
        .unwrap();
    writer.flush().unwrap();
    writer
        .set_properties(
            &uuid,
            Some("Person"),
            HashMap::from([(
                "name".to_owned(),
                graphforge_ir::IrLiteral::Str("new".into()),
            )]),
        )
        .unwrap();
    writer.flush().unwrap();

    let direct = read_properties(dir.path(), "Person").unwrap();
    let direct_names = direct[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(direct_names.len(), 1);
    assert_eq!(direct_names.value(0), "new");

    let ctx = SessionContext::new();
    let table = PropertyTable::open_discovered(dir.path(), "Person");
    let state = ctx.state();
    let full_plan = table
        .scan(&state as &dyn Session, None, &[], None)
        .await
        .unwrap();
    assert_eq!(
        full_plan.partition_statistics(None).unwrap().num_rows,
        datafusion::common::stats::Precision::Inexact(2),
        "all immutable generations contribute a physical upper bound"
    );
    let plan = table
        .scan(&state as &dyn Session, None, &[], Some(1))
        .await
        .unwrap();
    assert_eq!(
        plan.properties().scheduling_type,
        datafusion::physical_plan::execution_plan::SchedulingType::Cooperative
    );
    assert_eq!(
        plan.partition_statistics(None).unwrap().num_rows,
        datafusion::common::stats::Precision::Inexact(1),
        "two physical snapshots are a non-exact upper bound capped by LIMIT"
    );
    ctx.register_table("props", Arc::new(table)).unwrap();
    let sql = ctx
        .sql("SELECT name FROM props LIMIT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let sql_names = sql[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(sql_names.len(), 1);
    assert_eq!(sql_names.value(0), "new");
}

#[test]
fn admitted_property_projection_retains_one_handle_across_replacement() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("route.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("wanted", DataType::Utf8, true),
        Field::new("ignored", DataType::Utf8, true),
    ]));
    let file = File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(
        file,
        schema.clone(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(1))
                .build(),
        ),
    )
    .unwrap();
    for value in ["one", "two"] {
        writer
            .write(
                &RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(
                            FixedSizeBinaryArray::try_from_iter(std::iter::once(vec![1; 16]))
                                .unwrap(),
                        ),
                        Arc::new(StringArray::from(vec![value])),
                        Arc::new(StringArray::from(vec!["unused"])),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
    }
    writer.close().unwrap();
    let fragments = vec![("route".to_owned(), path.clone())];
    let columns = BTreeSet::from(["node_uuid".to_owned(), "wanted".to_owned()]);
    let mut rows = 0;
    let mut evidence = Vec::new();
    visit_property_fragments_admitted(
        &fragments,
        1,
        u64::MAX,
        Some(&columns),
        &mut evidence,
        |_, batch| {
            assert_eq!(batch.num_columns(), 2);
            rows += batch.num_rows();
            if rows == 1 {
                std::fs::rename(&path, path.with_extension("original")).unwrap();
                std::fs::write(&path, b"replacement is not parquet").unwrap();
            } else if rows == 2 {
                std::fs::remove_file(&path).unwrap();
                std::fs::rename(path.with_extension("original"), &path).unwrap();
            }
            Ok(true)
        },
    )
    .unwrap();
    assert_eq!(rows, 2);
    assert_eq!(evidence.len(), 1, "one admitted handle emits one identity");
    let mut reopened = Vec::new();
    visit_property_fragments_admitted(
        &fragments,
        1,
        u64::MAX,
        Some(&columns),
        &mut reopened,
        |_, _| Ok(true),
    )
    .unwrap();
    assert_eq!(
        reopened, evidence,
        "A-B-A cannot redirect the decoded handle"
    );
}

#[test]
fn admitted_property_rejects_wide_schema_before_decode() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wide.parquet");
    let fields = (0..=MAX_ADMITTED_PARQUET_COLUMNS)
        .map(|index| Field::new(format!("c{index}"), DataType::Utf8, true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let arrays = (0..schema.fields().len())
        .map(|_| Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as Arc<dyn Array>)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let error = visit_property_fragments_admitted(
        &[("wide".to_owned(), path)],
        1,
        u64::MAX,
        None,
        &mut Vec::new(),
        |_, _| Ok(true),
    )
    .unwrap_err();
    assert!(error.to_string().contains("column admission limit"));
}

#[test]
fn admitted_property_rejects_compressed_decode_expansion() {
    use parquet::basic::Compression;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("compressed.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Utf8,
        false,
    )]));
    let payload = "x".repeat(usize::try_from(MAX_ADMITTED_COLUMN_BYTES).unwrap() + 1);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![payload]))],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::default()))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let error = visit_property_fragments_admitted(
        &[("compressed".to_owned(), path)],
        1,
        u64::MAX,
        None,
        &mut Vec::new(),
        |_, _| Ok(true),
    )
    .unwrap_err();
    assert!(error.to_string().contains("decoded-byte admission limit"));
}
