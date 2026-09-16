use super::*;
use crate::writer::Arc;
use crate::writer::BTreeMap;
use crate::writer::DataType;
use crate::writer::EDGE_PROPERTY_UUID_FIELD;
use crate::writer::EntityTypeId;
use crate::writer::Field;
use crate::writer::FixedSizeBinaryArray;
use crate::writer::GraphWriter;
use crate::writer::HashMap;
use crate::writer::IrLiteral;
use crate::writer::NODE_PROPERTY_UUID_FIELD;
use crate::writer::OntologyMode;
use crate::writer::Path;
use crate::writer::PathBuf;
use crate::writer::RecordBatch;
use crate::writer::Schema;
use crate::writer::SpatialCoordinates;
use crate::writer::SpatialCrs;
use crate::writer::SpatialGeometryType;
use crate::writer::SpatialType;
use crate::writer::SpatialValue;
use crate::writer::TimeUnit;
use crate::writer::Uuid;
use crate::writer::tests::TS;
use crate::writer::tests::read_edge_props;
use crate::writer::tests::read_node_props;
use crate::writer::to_bytes;
use crate::writer::uuid_field;
use graphforge_core::uuid::new_v7;
use std::fs::File;
use tempfile::TempDir;

#[test]
fn explicit_null_and_later_scalar_share_a_lossless_tagged_column() {
    use arrow::datatypes::DataType;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let a = new_v7();
    let b = new_v7();
    let c = new_v7();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    w.create_node(b, EntityTypeId::decode(0).unwrap()).unwrap();
    w.create_node(c, EntityTypeId::decode(0).unwrap()).unwrap();
    // First row: `score` is Null. Later rows: consistently Int.
    w.set_properties(
        &a,
        None,
        HashMap::from([("score".to_owned(), IrLiteral::Null)]),
    )
    .unwrap();
    w.set_properties(
        &b,
        None,
        HashMap::from([("score".to_owned(), IrLiteral::Int(10))]),
    )
    .unwrap();
    w.set_properties(
        &c,
        None,
        HashMap::from([("score".to_owned(), IrLiteral::Int(20))]),
    )
    .unwrap();
    w.flush().unwrap();

    let path = newest_property_fragment(
        dir.path(),
        crate::property_overlay::PropertyRouteKind::Node,
        "_untyped",
    );
    let file = File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = builder.schema().clone();
    // Exactly one tagged `score` column distinguishes explicit Null from a
    // missing property while retaining the later integer values.
    let score_fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|f| f.name() == "score")
        .collect();
    assert_eq!(score_fields.len(), 1, "expected a single score column");
    assert_eq!(
        score_fields[0].data_type(),
        &DataType::Struct(heterogeneous_scalar_fields()),
        "explicit null plus Int must use the lossless scalar union"
    );

    // Round-trip: 3 rows, first null then 10, 20.
    let mut reader = builder.build().unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 3);
    let mut decoded = Vec::new();
    decode_property_batch(&batch, NODE_PROPERTY_UUID_FIELD, |_, values| {
        decoded.push(values["score"].clone());
    })
    .unwrap();
    assert_eq!(
        decoded,
        vec![IrLiteral::Null, IrLiteral::Int(10), IrLiteral::Int(20)]
    );
}

#[test]
fn mixed_type_property_column_uses_tagged_scalars() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let a = new_v7();
    let b = new_v7();
    w.create_node(a, EntityTypeId::decode(0).unwrap()).unwrap();
    w.create_node(b, EntityTypeId::decode(0).unwrap()).unwrap();
    w.set_properties(
        &a,
        None,
        HashMap::from([("x".to_owned(), IrLiteral::Int(1))]),
    )
    .unwrap();
    w.set_properties(
        &b,
        None,
        HashMap::from([("x".to_owned(), IrLiteral::Str("two".to_owned()))]),
    )
    .unwrap();
    w.flush().unwrap();

    let path = newest_property_fragment(
        dir.path(),
        crate::property_overlay::PropertyRouteKind::Node,
        "_untyped",
    );
    let file = File::open(&path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = builder.schema().clone();
    let x = schema.field_with_name("x").unwrap();
    assert_eq!(
        x.data_type(),
        &DataType::Struct(heterogeneous_scalar_fields())
    );

    // The targeted edge reader uses the same canonical encoder in memory.
    // Verify its tagged values and temporal fields survive that boundary,
    // including explicit Null and negative timestamp/duration components.
    let rows = [
        IrLiteral::Null,
        IrLiteral::Int(1),
        IrLiteral::Str("two".into()),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, value)| {
        let uuid = Uuid::from_u128(index as u128 + 1).into_bytes();
        (
            uuid,
            crate::PropertySnapshotRow {
                uuid,
                tombstone: false,
                values: BTreeMap::from([
                    ("x".into(), value),
                    ("at".into(), IrLiteral::DateTime(-1_234_567 + index as i64)),
                    (
                        "elapsed".into(),
                        IrLiteral::Duration {
                            months: -2,
                            days: 3,
                            seconds: -4,
                            nanos: 5,
                        },
                    ),
                ]),
            },
        )
    })
    .collect::<BTreeMap<_, _>>();
    let selected = crate::PropertyTargetSnapshots {
        present: rows.keys().copied().collect(),
        rows,
        metrics: Default::default(),
    };
    let edge_schema = Schema::new_with_metadata(
        vec![
            uuid_field(EDGE_PROPERTY_UUID_FIELD),
            Field::new("x", DataType::Struct(heterogeneous_scalar_fields()), true),
            crate::schemas::ts_field("at"),
            Field::new(
                "elapsed",
                DataType::Struct(crate::schemas::duration_struct_fields()),
                true,
            ),
        ],
        HashMap::from([("fixture".into(), "targeted-edge-types".into())]),
    );
    let encoded = selected.edge_batch(&edge_schema).unwrap();
    assert_eq!(encoded.schema().as_ref(), &edge_schema);
    let mut decoded = BTreeMap::new();
    decode_property_batch(&encoded, EDGE_PROPERTY_UUID_FIELD, |uuid, values| {
        assert!(decoded.insert(uuid, values).is_none());
    })
    .unwrap();
    let expected = selected
        .rows
        .iter()
        .map(|(uuid, row)| {
            (
                *uuid,
                row.values.clone().into_iter().collect::<HashMap<_, _>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(decoded, expected);
}

#[test]
fn malformed_persisted_heterogeneous_batch_emits_no_partial_rows() {
    use arrow::array::{Int8Array, StructArray};
    use graphforge_value::heterogeneous::{self as het, Scalar};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    for expected_error in ["GF_VALUE_TAG", "GF_VALUE_SCHEMA"] {
        let dir = TempDir::new().unwrap();
        let values = het::encode_scalar([Some(Scalar::Int(7)), Some(Scalar::Str("two"))]);
        let mut columns = values.columns().to_vec();
        let mut fields = het::scalar_fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        if expected_error == "GF_VALUE_TAG" {
            columns[0] = Arc::new(Int8Array::from(vec![0, 99]));
        } else {
            fields[1] = Field::new(het::INT, DataType::UInt64, true);
            columns[1] = Arc::new(arrow::array::UInt64Array::from(vec![Some(7), None]));
        }
        let values = StructArray::new(fields.into(), columns, None);
        let ids = FixedSizeBinaryArray::try_from_iter(
            [new_v7(), new_v7()]
                .iter()
                .map(|id| id.as_bytes().as_slice()),
        )
        .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                NODE_PROPERTY_UUID_FIELD,
                DataType::FixedSizeBinary(16),
                false,
            ),
            Field::new("mixed", values.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(values)]).unwrap();
        let path = dir.path().join("malformed.parquet");
        crate::graph_projection::write_parquet(&path, &batch).unwrap();
        let before = std::fs::read(&path).unwrap();
        let persisted = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut emitted = 0;
        let error =
            decode_property_batch(&persisted, NODE_PROPERTY_UUID_FIELD, |_, _| emitted += 1)
                .unwrap_err();
        assert_eq!(error.code(), "GF_PROJECT_CORRUPT");
        assert!(error.to_string().contains(expected_error), "{error}");
        assert_eq!(
            emitted, 0,
            "a later malformed row must prevent partial exposure"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
}

#[test]
fn every_heterogeneous_scalar_tag_round_trips_exactly() {
    let dir = TempDir::new().unwrap();
    let cases = [
        IrLiteral::Int(-1),
        IrLiteral::Float(2.25),
        IrLiteral::Str("three".into()),
        IrLiteral::Bool(true),
    ];
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let mut expected = HashMap::new();
    for value in cases {
        let node = new_v7();
        writer
            .create_node(node, EntityTypeId::decode(0).unwrap())
            .unwrap();
        writer
            .set_properties(
                &node,
                None,
                HashMap::from([("mixed".into(), value.clone())]),
            )
            .unwrap();
        expected.insert(to_bytes(&node), value);
    }
    writer.flush().unwrap();

    let reopened = read_node_props(dir.path(), "_untyped");
    assert_eq!(reopened.len(), expected.len());
    for (node, value) in expected {
        assert_eq!(reopened[&node].get("mixed"), Some(&value));
    }
}

fn newest_property_fragment(
    dir: &Path,
    kind: crate::property_overlay::PropertyRouteKind,
    route: &str,
) -> PathBuf {
    crate::property_overlay::enumerate_property_fragments(
        dir,
        kind,
        &crate::route_component::component(route),
    )
    .unwrap()
    .pop()
    .expect("property route has a committed fragment")
    .path
}

#[test]
fn every_persisted_property_family_round_trips_through_parquet_reopen() {
    let dir = TempDir::new().unwrap();
    let node = new_v7();
    let propertyless = new_v7();
    let values = HashMap::from([
        ("int".into(), IrLiteral::Int(-7)),
        ("float".into(), IrLiteral::Float(2.5)),
        ("bool".into(), IrLiteral::Bool(true)),
        ("str".into(), IrLiteral::Str("value".into())),
        (
            "duration".into(),
            IrLiteral::Duration {
                months: 1,
                days: -2,
                seconds: 3,
                nanos: 4,
            },
        ),
        ("datetime".into(), IrLiteral::DateTime(TS)),
        ("date".into(), IrLiteral::Date(19_000)),
        (
            "local_datetime".into(),
            IrLiteral::LocalDateTime {
                days: 19_001,
                nanos: 123,
            },
        ),
        ("time".into(), IrLiteral::Time(456)),
        (
            "zoned_time".into(),
            IrLiteral::ZonedTime {
                nanos: 789,
                offset: -21_600,
            },
        ),
        (
            "zoned_datetime".into(),
            IrLiteral::ZonedDateTime {
                days: 19_002,
                nanos: 987,
                offset: 3_600,
                zone: Some("Europe/Paris".into()),
            },
        ),
        (
            "offset_datetime".into(),
            IrLiteral::ZonedDateTime {
                days: 19_003,
                nanos: 654,
                offset: 0,
                zone: None,
            },
        ),
        (
            "ints".into(),
            IrLiteral::List(vec![IrLiteral::Int(1), IrLiteral::Null, IrLiteral::Int(3)]),
        ),
        (
            "dates".into(),
            IrLiteral::List(vec![IrLiteral::Date(19_004), IrLiteral::Date(19_005)]),
        ),
        ("empty".into(), IrLiteral::List(Vec::new())),
        ("null".into(), IrLiteral::Null),
    ]);
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    writer
        .create_node(node, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer
        .create_node(propertyless, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer.set_properties(&node, None, values.clone()).unwrap();
    writer
        .set_properties(
            &propertyless,
            None,
            values
                .keys()
                .map(|name| (name.clone(), IrLiteral::Null))
                .collect(),
        )
        .unwrap();
    writer.flush().unwrap();

    let reopened = read_node_props(dir.path(), "_untyped");
    let actual = reopened.get(&to_bytes(&node)).unwrap();
    for (name, expected) in &values {
        if matches!(expected, IrLiteral::Null) {
            assert_eq!(actual.get(name), Some(&IrLiteral::Null));
        } else if name == "empty" {
            assert_eq!(actual.get(name), Some(&IrLiteral::Str("[]".into())));
        } else {
            assert_eq!(actual.get(name), Some(expected), "property {name}");
        }
    }
    let propertyless_values = reopened.get(&to_bytes(&propertyless)).unwrap();
    for (name, value) in &values {
        let retains_explicit_null = matches!(value, IrLiteral::Null)
            || ColType::of(value).is_some_and(|column| column.is_scalar());
        assert_eq!(
            propertyless_values.get(name),
            retains_explicit_null.then_some(&IrLiteral::Null),
            "explicit-null presence for {name}"
        );
    }
}

#[test]
fn canonical_spatial_properties_round_trip_with_exact_geoarrow_metadata() {
    let dir = TempDir::new().unwrap();
    let node = new_v7();
    let other = new_v7();
    let edge = new_v7();
    let spatial = |geometry, coordinates| {
        IrLiteral::Spatial(SpatialValue {
            spatial_type: SpatialType {
                geometry,
                crs: SpatialCrs::Epsg4326,
            },
            coordinates,
            extension_name: None,
            extension_metadata: None,
        })
    };
    let values = HashMap::from([
            (
                "point".into(),
                spatial(
                    SpatialGeometryType::Point,
                    SpatialCoordinates::Point([-104.9903, 39.7392]),
                ),
            ),
            (
                "line".into(),
                spatial(
                    SpatialGeometryType::LineString,
                    SpatialCoordinates::LineString(vec![[0.0, 1.0], [2.0, 3.0]]),
                ),
            ),
            (
                "polygon".into(),
                spatial(
                    SpatialGeometryType::Polygon,
                    SpatialCoordinates::Polygon(vec![vec![[0.0, 0.0], [1.0, 0.0], [0.0, 0.0]]]),
                ),
            ),
            (
                "multipoint".into(),
                spatial(
                    SpatialGeometryType::MultiPoint,
                    SpatialCoordinates::MultiPoint(vec![[4.0, 5.0], [6.0, 7.0]]),
                ),
            ),
            (
                "multiline".into(),
                spatial(
                    SpatialGeometryType::MultiLineString,
                    SpatialCoordinates::MultiLineString(vec![vec![[8.0, 9.0], [10.0, 11.0]]]),
                ),
            ),
            (
                "multipolygon".into(),
                spatial(
                    SpatialGeometryType::MultiPolygon,
                    SpatialCoordinates::MultiPolygon(vec![vec![vec![
                        [0.0, 0.0],
                        [2.0, 0.0],
                        [0.0, 0.0],
                    ]]]),
                ),
            ),
            (
                "preserved".into(),
                IrLiteral::Spatial(SpatialValue {
                    spatial_type: SpatialType {
                        geometry: SpatialGeometryType::Point,
                        crs: SpatialCrs::Preserved("OGC:CRS84".into()),
                    },
                    coordinates: SpatialCoordinates::Point([-104.9903, 39.7392]),
                    extension_name: Some("geoarrow.vendor_point".into()),
                    extension_metadata: Some(
                        "{\"crs\":\"OGC:CRS84\",\"crs_type\":\"authority_code\",\"edges\":\"spherical\"}"
                            .into(),
                    ),
                }),
            ),
        ]);

    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    writer
        .create_node(node, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer
        .create_node(other, EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer.create_edge(edge, "ROUTE", &node, &other).unwrap();
    writer.set_properties(&node, None, values.clone()).unwrap();
    writer
        .set_edge_properties(
            &edge,
            Some("ROUTE"),
            HashMap::from([("location".into(), values["point"].clone())]),
        )
        .unwrap();
    writer.flush().unwrap();

    let node_schema = crate::catalog::discover_parquet_schema(&newest_property_fragment(
        dir.path(),
        crate::property_overlay::PropertyRouteKind::Node,
        "_untyped",
    ))
    .unwrap();
    for (name, extension_name) in [
        ("point", "geoarrow.point"),
        ("line", "geoarrow.linestring"),
        ("polygon", "geoarrow.polygon"),
        ("multipoint", "geoarrow.multipoint"),
        ("multiline", "geoarrow.multilinestring"),
        ("multipolygon", "geoarrow.multipolygon"),
    ] {
        let field = node_schema.field_with_name(name).unwrap();
        assert_eq!(field.metadata()["ARROW:extension:name"], extension_name);
        assert_eq!(
            field.metadata()["ARROW:extension:metadata"],
            "{\"crs\":\"EPSG:4326\",\"crs_type\":\"authority_code\"}"
        );
    }
    let preserved = node_schema.field_with_name("preserved").unwrap();
    assert_eq!(
        preserved.metadata()["ARROW:extension:name"],
        "geoarrow.vendor_point"
    );
    assert_eq!(
        preserved.metadata()["ARROW:extension:metadata"],
        "{\"crs\":\"OGC:CRS84\",\"crs_type\":\"authority_code\",\"edges\":\"spherical\"}"
    );
    assert_eq!(
        read_node_props(dir.path(), "_untyped")[&to_bytes(&node)],
        values
    );
    assert_eq!(
        read_edge_props(dir.path(), "ROUTE")[&to_bytes(&edge)]["location"],
        values["point"]
    );

    // Opening and flushing again exercises the persisted decode/re-encode path.
    let mut reopened = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS + 1).unwrap();
    reopened.flush().unwrap();
    assert_eq!(
        read_node_props(dir.path(), "_untyped")[&to_bytes(&node)],
        values
    );
}

#[test]
fn canonical_temporal_schema_dispatch_preserves_distinct_struct_shapes() {
    assert!(matches!(
        col_type_from_data_type(&DataType::Struct(crate::schemas::date_struct_fields())),
        Some(ColType::Date)
    ));
    assert!(matches!(
        col_type_from_data_type(&DataType::Struct(
            crate::schemas::localdatetime_struct_fields()
        )),
        Some(ColType::LocalDateTime)
    ));
    assert!(matches!(
        col_type_from_data_type(&DataType::Struct(crate::schemas::time_struct_fields())),
        Some(ColType::ZonedTime)
    ));
    assert!(matches!(
        col_type_from_data_type(&DataType::Struct(crate::schemas::datetime_struct_fields())),
        Some(ColType::ZonedDateTime)
    ));
    assert!(matches!(
        col_type_from_data_type(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(crate::schemas::datetime_struct_fields()),
            true,
        )))),
        Some(ColType::List(inner)) if matches!(*inner, ColType::ZonedDateTime)
    ));

    let noncanonical = DataType::Struct(arrow::datatypes::Fields::from(vec![
        Field::new("date", DataType::Int64, true),
        Field::new("time", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("unexpected", DataType::Utf8, true),
    ]));
    assert!(col_type_from_data_type(&noncanonical).is_none());
}

#[test]
fn property_literal_rendering_and_nested_invalid_values_are_deterministic() {
    let uuid = [0xabu8; 16];
    let cases = [
        (IrLiteral::Null, "".into()),
        (IrLiteral::Bool(true), "true".into()),
        (IrLiteral::Int(-2), "-2".into()),
        (IrLiteral::Float(1.25), "1.25".into()),
        (IrLiteral::Str("s".into()), "s".into()),
        (IrLiteral::Uuid(uuid), "ab".repeat(16)),
        (
            IrLiteral::Duration {
                months: 1,
                days: 2,
                seconds: 3,
                nanos: 4,
            },
            "1mo2d3s4ns".into(),
        ),
        (IrLiteral::DateTime(5), "5".into()),
        (IrLiteral::Date(6), "6".into()),
        (
            IrLiteral::LocalDateTime { days: 7, nanos: 8 },
            "7d8ns".into(),
        ),
        (IrLiteral::Time(9), "9ns".into()),
        (
            IrLiteral::ZonedTime {
                nanos: 10,
                offset: -1,
            },
            "10ns-1s".into(),
        ),
        (
            IrLiteral::ZonedDateTime {
                days: 11,
                nanos: 12,
                offset: 13,
                zone: Some("UTC".into()),
            },
            "11d12ns+13sUTC".into(),
        ),
        (
            IrLiteral::List(vec![IrLiteral::Int(1), IrLiteral::Str("x".into())]),
            "[1,x]".into(),
        ),
        (
            IrLiteral::Map(vec![("a".into(), IrLiteral::Bool(false))]),
            "{a:false}".into(),
        ),
    ];
    for (literal, expected) in cases {
        assert_eq!(literal_to_string(&literal), expected);
    }

    for invalid in [
        IrLiteral::Uuid(uuid),
        IrLiteral::List(vec![IrLiteral::Uuid(uuid)]),
        IrLiteral::Map(vec![("nested".into(), IrLiteral::Uuid(uuid))]),
    ] {
        assert_eq!(
            reject_map_property_value("p", &invalid).unwrap_err().code(),
            "GF_VALIDATION"
        );
    }
    for invalid in [
        IrLiteral::Map(vec![]),
        IrLiteral::List(vec![IrLiteral::Map(vec![])]),
    ] {
        assert_eq!(
            reject_map_property_value("p", &invalid).unwrap_err().code(),
            "GF_IO"
        );
    }
}

#[test]
fn persisted_property_decoder_rejects_unsupported_shape_type_and_dynamic_array() {
    use arrow::array::{Int32Array, Int64Array, StructArray, UInt8Array};
    use arrow::datatypes::{DataType, Field, Fields};

    let unsupported: arrow::array::ArrayRef = Arc::new(UInt8Array::from(vec![1]));
    let unsupported_field = Field::new("unsupported", DataType::UInt8, false);
    assert!(decode_value(&unsupported, &unsupported_field, 0).is_err());

    let fields: Fields = vec![Field::new("other", DataType::Int32, false)].into();
    let structure: arrow::array::ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(vec![1]))],
        None,
    ));
    let structure_field = Field::new("structure", DataType::Struct(fields), false);
    assert!(decode_value(&structure, &structure_field, 0).is_err());

    let wrong_dynamic: arrow::array::ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let declared = Field::new("declared", DataType::UInt64, false);
    assert!(decode_value(&wrong_dynamic, &declared, 0).is_err());
}
