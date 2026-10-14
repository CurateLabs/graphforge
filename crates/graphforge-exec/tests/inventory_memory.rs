//! Inventory admission through the public resource-configured Rust session.

use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_core::OntologyMode;
use graphforge_exec::{ExecutionSession, PersistentAdjacencyProvider, SessionResourceConfig};
use graphforge_ir::RuntimeCatalog;
use graphforge_storage::{AuthenticatedPropertyInventory, GraphCatalog};
use parquet::arrow::ArrowWriter;

#[test]
fn public_session_refuses_inventory_metadata_that_exceeds_its_configured_share() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(directory.path().join("properties")).unwrap();
    let mut fields = vec![Field::new(
        "node_uuid",
        DataType::FixedSizeBinary(16),
        false,
    )];
    let mut columns = vec![Arc::new(
        FixedSizeBinaryArray::try_from_iter(
            [graphforge_core::uuid::Uuid::from_u128(1)
                .as_bytes()
                .as_slice()]
            .into_iter(),
        )
        .unwrap(),
    ) as ArrayRef];
    // One row has tiny result data but substantial legitimate schema/footer
    // storage. Every field is under the existing 4,096-column admission cap.
    for index in 0..3_072 {
        fields.push(Field::new(format!("value_{index}"), DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(vec![index])));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
    let path = directory.path().join("properties/Wide.parquet");
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let inventory = Arc::new(AuthenticatedPropertyInventory::capture(directory.path()).unwrap());
    let budget = 16 * 1024 * 1024;
    assert!(
        inventory.retained_metadata_bytes() as u64 > budget / 8,
        "the fixture must actually exceed the requested inventory share"
    );
    let provider = Arc::new(
        PersistentAdjacencyProvider::new(directory.path().to_path_buf(), OntologyMode::Exploratory)
            .with_memory_budget(budget)
            .with_inventory(Arc::clone(&inventory)),
    );
    let catalog = || {
        GraphCatalog::open_authenticated(
            directory.path(),
            None,
            &RuntimeCatalog::new(),
            Arc::clone(&inventory),
        )
        .unwrap()
    };
    let resources = SessionResourceConfig {
        memory_budget_bytes: budget,
        ..SessionResourceConfig::default()
    };
    let error = ExecutionSession::new_with_target_provider_and_resources(
        catalog(),
        None,
        directory.path().to_path_buf(),
        OntologyMode::Exploratory,
        Arc::clone(&provider),
        &resources,
    )
    .err()
    .expect("the inventory cannot fit the explicit small share");
    assert_eq!(error.code(), "GF_RESOURCE_LIMIT");
    assert!(
        error.to_string().contains("property inventory metadata"),
        "{error}"
    );

    // The failure did not reset an existing authority or corrupt its payload.
    // A fitting first explicit configuration can still attach it.
    ExecutionSession::new_with_target_provider_and_resources(
        catalog(),
        None,
        directory.path().to_path_buf(),
        OntologyMode::Exploratory,
        provider,
        &SessionResourceConfig {
            memory_budget_bytes: 512 * 1024 * 1024,
            ..SessionResourceConfig::default()
        },
    )
    .expect("the same valid inventory fits a roomy session");
}
