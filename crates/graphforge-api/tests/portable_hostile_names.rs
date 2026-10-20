//! User-supplied names never become unportable package path components (#1770).
//!
//! Labels, relationship types and property names reach a package only through
//! hashed semantic routes, so a project full of Windows-hostile, mixed-case and
//! non-ASCII names must still export a package that the portable path rule
//! accepts, imports, and reopens with every name intact.

use std::sync::Arc;

use arrow::array::{FixedSizeBinaryBuilder, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    BulkInputKind, GraphForge, ImportSessionLimits, OperationId, PortableExportRequest,
    PortableImportRequest, PortableLimits, PortableOutput, PortableSelection,
    PortableSelectionProfile, bulk_edge_input_schema, bulk_node_input_schema,
};
use graphforge_storage::{PortableMode, verify_portable};

/// The names a graph identifier admits (Unicode identifiers of at most 255
/// bytes) that are hostile to some platform's file system. Punctuation such as
/// `:` or `*`, trailing dots or spaces, non-NFC spellings and over-long names cannot be labels,
/// types or property names at all.
fn hostile_names() -> Vec<String> {
    let mut names = [
        "CON",
        "aux",
        "Prn",
        "NUL",
        "COM1",
        "lpt9",
        "Stra\u{df}e",
        "STRASSE",
        "\u{3c3}",
        "\u{3c2}",
        "\u{131}",
        "I",
        "caf\u{e9}",
        "MixedCase",
        "mixedcase",
        "\u{65e5}\u{672c}\u{8a9e}",
    ]
    .map(str::to_owned)
    .to_vec();
    names.push("\u{e9}".repeat(127));
    names
}

fn uuid(counter: u128) -> [u8; 16] {
    (uuid::Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0000 | counter)).into_bytes()
}

fn uuids(values: impl Iterator<Item = u128>) -> arrow::array::ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::new(16);
    for value in values {
        builder.append_value(uuid(value)).unwrap();
    }
    Arc::new(builder.finish())
}

#[test]
fn hostile_label_type_and_property_names_export_a_portable_package() {
    let names = hostile_names();
    let count = names.len() as u128;
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    let graph = GraphForge::new(source.to_str()).unwrap();

    // Property names must be identifiers; reserved and non-ASCII ones are valid.
    let properties = vec![
        Field::new("CON", DataType::Utf8, true),
        Field::new("Stra\u{df}e", DataType::Utf8, true),
    ];
    let nodes = RecordBatch::try_new(
        bulk_node_input_schema(properties).unwrap(),
        vec![
            uuids(1..=count),
            Arc::new(StringArray::from(names.clone())),
            Arc::new(StringArray::from(names.clone())),
            Arc::new(StringArray::from(names.clone())),
        ],
    )
    .unwrap();
    let edges = RecordBatch::try_new(
        bulk_edge_input_schema(Vec::new()).unwrap(),
        vec![
            uuids(1_000_000..1_000_000 + count),
            Arc::new(StringArray::from(names.clone())),
            uuids(1..=count),
            uuids((1..=count).map(|i| i % count + 1)),
        ],
    )
    .unwrap();
    let mut session = graph
        .begin_import_session(
            OperationId(uuid::Uuid::now_v7()),
            ImportSessionLimits::default(),
        )
        .unwrap();
    session.append_arrow(BulkInputKind::Node, &[nodes]).unwrap();
    session.append_arrow(BulkInputKind::Edge, &[edges]).unwrap();
    session.validate(&graph).unwrap();
    session.commit(&graph, None).unwrap();

    let limits = PortableLimits::default();
    let package = root.path().join("package.gfpb");
    graph
        .export_portable(
            &PortableExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableOutput::Bundle,
                profile: PortableSelectionProfile::Complete,
                subset: None,
                limits,
            },
            None,
            |_| {},
        )
        .unwrap();
    drop(graph);

    // The exporter's own check already ran the path rule; run it again, as a
    // consumer would, on the bytes that were written.
    verify_portable(&package, PortableMode::Full, limits, None).unwrap();

    let target = root.path().join("target");
    GraphForge::import_portable(
        &target,
        &PortableImportRequest {
            input: package,
            operation_id: OperationId(uuid::Uuid::now_v7()),
            limits,
        },
        None,
    )
    .unwrap();
    let imported = GraphForge::new(target.to_str()).unwrap();
    for name in &names {
        assert_eq!(imported.node_count(name).unwrap(), 1, "label {name:?}");
    }
}
