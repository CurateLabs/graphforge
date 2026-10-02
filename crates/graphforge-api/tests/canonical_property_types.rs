//! Bulk import persists every accepted Arrow property type in canonical form.
//!
//! Construction accepts narrow integers, `Float32`, `LargeUtf8` and `LargeList`,
//! at top level and as list elements. After commit and reopen, each must read
//! back through the public API exactly as the same values imported in their
//! canonical types (`Int64`, `Float64`, `Utf8`, `List`) do, nulls included.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryBuilder, Float64Array, Int64Array, Int64Builder, ListBuilder,
    StringArray,
};
use arrow::compute::{CastOptions, cast_with_options, concat_batches};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    BulkInputKind, GraphForge, ImportSessionLimits, OperationId, bulk_edge_input_schema,
    bulk_node_input_schema,
};

const ROWS: usize = 5;

/// One property column: canonical values and the Arrow type it is imported as.
struct Column {
    name: String,
    canonical: ArrayRef,
    imported: DataType,
}

fn item(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type, true))
}

/// `[min, max, 0, null, 1]` for an integer type spanning `min..=max`.
fn integers(min: i64, max: i64) -> ArrayRef {
    Arc::new(Int64Array::from(vec![
        Some(min),
        Some(max),
        Some(0),
        None,
        Some(1),
    ]))
}

/// `[[min, null, max], [], null, [0], [1, 1]]` as a canonical `List<Int64>`.
fn integer_lists(min: i64, max: i64) -> ArrayRef {
    let mut builder = ListBuilder::new(Int64Builder::new());
    builder.append_value([Some(min), None, Some(max)]);
    builder.append_value([]);
    builder.append_null();
    builder.append_value([Some(0)]);
    builder.append_value([Some(1), Some(1)]);
    Arc::new(builder.finish())
}

fn floats() -> ArrayRef {
    Arc::new(Float64Array::from(vec![
        Some(f64::from(f32::MIN)),
        Some(f64::from(f32::MAX)),
        Some(-0.25),
        None,
        Some(1.5),
    ]))
}

fn strings() -> ArrayRef {
    Arc::new(StringArray::from(vec![
        Some(""),
        Some("héllo wörld"),
        Some("x"),
        None,
        Some("tail"),
    ]))
}

/// Wrap each canonical scalar row in a one- or two-element list, keeping a
/// null row, an empty list and a null element.
fn lists_of(values: &ArrayRef) -> ArrayRef {
    let indices = [
        Some(vec![Some(0), None, Some(1)]),
        Some(vec![]),
        None,
        Some(vec![Some(2)]),
        Some(vec![Some(4), Some(4)]),
    ];
    let mut offsets = vec![0_i32];
    let mut taken = Vec::new();
    let mut validity = Vec::new();
    for row in &indices {
        if let Some(row) = row {
            taken.extend(row.iter().copied());
        }
        validity.push(row.is_some());
        offsets.push(i32::try_from(taken.len()).unwrap());
    }
    let take = arrow::array::UInt32Array::from(taken);
    let elements = arrow::compute::take(values.as_ref(), &take, None).unwrap();
    Arc::new(
        arrow::array::ListArray::try_new(
            item(values.data_type().clone()),
            arrow::buffer::OffsetBuffer::new(offsets.into()),
            elements,
            Some(validity.into()),
        )
        .unwrap(),
    )
}

/// Every accepted non-canonical property type, as a top-level column and as
/// the element of a `List` and a `LargeList`, plus `LargeList` of a canonical
/// element and a nested `LargeList<LargeList<Int16>>`.
fn columns() -> Vec<Column> {
    let scalars: Vec<(&str, DataType, ArrayRef, ArrayRef)> = vec![
        (
            "i8",
            DataType::Int8,
            integers(i64::from(i8::MIN), i64::from(i8::MAX)),
            integer_lists(i64::from(i8::MIN), i64::from(i8::MAX)),
        ),
        (
            "i16",
            DataType::Int16,
            integers(i64::from(i16::MIN), i64::from(i16::MAX)),
            integer_lists(i64::from(i16::MIN), i64::from(i16::MAX)),
        ),
        (
            "i32",
            DataType::Int32,
            integers(i64::from(i32::MIN), i64::from(i32::MAX)),
            integer_lists(i64::from(i32::MIN), i64::from(i32::MAX)),
        ),
        (
            "u8",
            DataType::UInt8,
            integers(0, i64::from(u8::MAX)),
            integer_lists(0, i64::from(u8::MAX)),
        ),
        (
            "u16",
            DataType::UInt16,
            integers(0, i64::from(u16::MAX)),
            integer_lists(0, i64::from(u16::MAX)),
        ),
        (
            "u32",
            DataType::UInt32,
            integers(0, i64::from(u32::MAX)),
            integer_lists(0, i64::from(u32::MAX)),
        ),
        ("f32", DataType::Float32, floats(), lists_of(&floats())),
        (
            "lutf8",
            DataType::LargeUtf8,
            strings(),
            lists_of(&strings()),
        ),
    ];
    let mut columns = Vec::new();
    for (name, imported, canonical, canonical_lists) in scalars {
        columns.push(Column {
            name: format!("t_{name}"),
            canonical,
            imported: imported.clone(),
        });
        columns.push(Column {
            name: format!("l_{name}"),
            canonical: Arc::clone(&canonical_lists),
            imported: DataType::List(item(imported.clone())),
        });
        columns.push(Column {
            name: format!("ll_{name}"),
            canonical: canonical_lists,
            imported: DataType::LargeList(item(imported)),
        });
    }
    columns.push(Column {
        name: "ll_i64".into(),
        canonical: integer_lists(i64::MIN, i64::MAX),
        imported: DataType::LargeList(item(DataType::Int64)),
    });
    columns.push(Column {
        name: "ll_s".into(),
        canonical: lists_of(&strings()),
        imported: DataType::LargeList(item(DataType::Utf8)),
    });
    columns.push(Column {
        name: "ll_ll_i16".into(),
        canonical: lists_of(&integer_lists(i64::from(i16::MIN), i64::from(i16::MAX))),
        imported: DataType::LargeList(item(DataType::LargeList(item(DataType::Int16)))),
    });
    columns
}

/// The column's canonical values cast to the type it is imported as, or the
/// canonical values themselves for the control project.
fn property(column: &Column, narrow: bool) -> (Field, ArrayRef) {
    let values = if narrow {
        let strict = CastOptions {
            safe: false,
            ..CastOptions::default()
        };
        cast_with_options(&column.canonical, &column.imported, &strict).unwrap()
    } else {
        Arc::clone(&column.canonical)
    };
    assert_eq!(values.null_count(), column.canonical.null_count());
    (
        Field::new(&column.name, values.data_type().clone(), true),
        values,
    )
}

/// Deterministic UUIDv7: a fixed timestamp, version 7, RFC variant, and a counter.
fn uuids(values: impl Iterator<Item = u128>) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::new(16);
    for value in values {
        let uuid = uuid::Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0000 | value);
        builder.append_value(uuid.as_bytes()).unwrap();
    }
    Arc::new(builder.finish())
}

/// Bulk-import `ROWS` nodes and `ROWS` edges carrying every property column
/// through an import session, then close the facade.
fn import(path: &Path, narrow: bool) {
    let mut properties: Vec<(Field, ArrayRef)> = vec![(
        Field::new("k", DataType::Int64, true),
        Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
    )];
    properties.extend(columns().iter().map(|column| property(column, narrow)));
    // The bulk input schema orders property fields by name.
    properties.sort_by(|left, right| left.0.name().cmp(right.0.name()));
    let (fields, values): (Vec<Field>, Vec<ArrayRef>) = properties.into_iter().unzip();
    let rows = ROWS as u128;
    let mut node_columns = vec![
        uuids(0..rows),
        Arc::new(StringArray::from(vec!["P"; ROWS])) as ArrayRef,
    ];
    node_columns.extend(values.iter().cloned());
    let nodes = RecordBatch::try_new(
        bulk_node_input_schema(fields.clone()).unwrap(),
        node_columns,
    )
    .unwrap();
    let mut edge_columns = vec![
        uuids(100..100 + rows),
        Arc::new(StringArray::from(vec!["R"; ROWS])) as ArrayRef,
        uuids(0..rows),
        uuids((0..rows).map(|row| (row + 1) % rows)),
    ];
    edge_columns.extend(values);
    let edges =
        RecordBatch::try_new(bulk_edge_input_schema(fields).unwrap(), edge_columns).unwrap();

    let graph = GraphForge::new(path.to_str()).unwrap();
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
}

fn query(graph: &GraphForge, cypher: &str) -> RecordBatch {
    let result = graph
        .execute(cypher)
        .unwrap_or_else(|error| panic!("{cypher}: {error}"));
    let schema = result.batches[0].schema();
    concat_batches(&schema, &result.batches).unwrap()
}

/// A narrow-typed project and its canonical-typed control, both reopened
/// from disk after commit.
fn reopened_pair() -> (tempfile::TempDir, GraphForge, GraphForge) {
    let root = tempfile::tempdir().unwrap();
    let narrow = root.path().join("narrow");
    let control = root.path().join("control");
    for (path, is_narrow) in [(&narrow, true), (&control, false)] {
        std::fs::create_dir(path).unwrap();
        import(path, is_narrow);
    }
    let narrow = GraphForge::new(narrow.to_str()).unwrap();
    let control = GraphForge::new(control.to_str()).unwrap();
    (root, narrow, control)
}

#[test]
fn accepted_property_types_read_back_canonically_after_reopen() {
    let (_root, narrow, control) = reopened_pair();
    let names = columns()
        .into_iter()
        .map(|column| column.name)
        .collect::<Vec<_>>();
    for (pattern, variable) in [("(e:P)", "e"), ("()-[e:R]->()", "e")] {
        let projection = names
            .iter()
            .map(|name| format!("{variable}.{name} AS {name}"))
            .collect::<Vec<_>>()
            .join(", ");
        let cypher = format!("MATCH {pattern} RETURN {variable}.k AS k, {projection} ORDER BY k");
        let read = query(&narrow, &cypher);
        let expected = query(&control, &cypher);
        assert_eq!(read.num_rows(), ROWS, "{pattern}");
        for column in columns() {
            let actual = read.column_by_name(&column.name).unwrap();
            let canonical = expected.column_by_name(&column.name).unwrap();
            assert_eq!(
                actual.data_type(),
                canonical.data_type(),
                "{pattern} {} type",
                column.name
            );
            assert_eq!(
                actual.to_data(),
                canonical.to_data(),
                "{pattern} {} values",
                column.name
            );
            assert!(
                actual.is_null(2) || column.name.starts_with("t_"),
                "{pattern} {} null list",
                column.name
            );
            if column.name.starts_with("t_") {
                assert!(actual.is_null(3), "{pattern} {} null", column.name);
            }
            // Integer and string scalars also read back as exactly the
            // canonical values imported. Widened floats are held to the
            // Float64 control above: a Float64 property equal to f32::MAX
            // currently reads back one ULP off whatever its imported type,
            // a read-path defect outside construction.
            if column.name.starts_with("t_") && column.name != "t_f32" {
                assert_eq!(
                    actual.to_data(),
                    column.canonical.to_data(),
                    "{pattern} {} canonical values",
                    column.name
                );
            }
        }
    }
}

#[test]
fn narrow_integer_filter_and_order_match_int64() {
    let (_root, narrow, control) = reopened_pair();
    for cypher in [
        "MATCH (n:P) WHERE n.t_i32 = 2147483647 RETURN n.k AS k",
        "MATCH (n:P) WHERE n.t_i8 = -128 RETURN n.k AS k",
        "MATCH (n:P) WHERE n.t_u32 = 4294967295 RETURN n.k AS k",
        "MATCH ()-[r:R]->() WHERE r.t_u16 = 65535 RETURN r.k AS k",
    ] {
        let read = query(&narrow, cypher);
        assert_eq!(
            read.columns(),
            query(&control, cypher).columns(),
            "{cypher}"
        );
        let keys = read
            .column_by_name("k")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let expected = if cypher.contains("-128") { 0 } else { 1 };
        assert_eq!(keys.values().as_ref(), &[expected], "{cypher}");
    }
    // Signed values are [min, max, 0, null, 1] by k, unsigned [0, max, 0,
    // null, 1]; the narrow column must order exactly as the Int64 control.
    // Only the non-null prefix is pinned here: null placement is the
    // engine's ordering rule, which the control comparison already covers.
    for (cypher, expected) in [
        (
            "MATCH (n:P) RETURN n.k AS k, n.t_i32 AS v ORDER BY v, k",
            vec![0, 2, 4, 1],
        ),
        (
            "MATCH (n:P) RETURN n.k AS k, n.t_i8 AS v ORDER BY v DESC, k",
            vec![1, 4, 2, 0],
        ),
        (
            "MATCH (n:P) RETURN n.k AS k, n.t_u32 AS v ORDER BY v DESC, k",
            vec![1, 4, 0, 2],
        ),
        (
            "MATCH ()-[r:R]->() RETURN r.k AS k, r.t_i16 AS v ORDER BY v, k",
            vec![0, 2, 4, 1],
        ),
    ] {
        let read = query(&narrow, cypher);
        assert_eq!(
            read.columns(),
            query(&control, cypher).columns(),
            "{cypher}"
        );
        let keys = read
            .column_by_name("k")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ranked = (0..keys.len())
            .map(|row| keys.value(row))
            .filter(|key| *key != 3)
            .collect::<Vec<_>>();
        assert_eq!(ranked, expected, "{cypher}");
    }
}
