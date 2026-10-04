//! UUID and GeoArrow preservation in native CLI result output.

use super::*;

#[test]
fn canonical_json_uuid_conversion_is_exact_and_rejects_malformed_hex() {
    let ty = DataType::FixedSizeBinary(16);
    assert_eq!(
        canonical_json_value(
            serde_json::Value::String("000102030405060708090a0b0c0d0e0f".into()),
            &ty,
        )
        .unwrap(),
        serde_json::Value::String("00010203-0405-0607-0809-0a0b0c0d0e0f".into())
    );
    for value in ["00", "zz0102030405060708090a0b0c0d0e0f"] {
        assert!(matches!(
            canonical_json_value(serde_json::Value::String(value.into()), &ty),
            Err(graphforge_api::GfError::Execution(_))
        ));
    }
    let ordinary = serde_json::json!({"nested": [true, 1, null]});
    assert_eq!(
        canonical_json_value(ordinary.clone(), &DataType::Utf8).unwrap(),
        ordinary
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn arrow_result_export_preserves_geoarrow_fields_values_and_nulls() {
    use std::collections::HashMap;
    use std::io::Cursor;

    use arrow::array::{
        Array, FixedSizeListArray, Float64Array, LargeListArray, ListArray, StructArray,
    };
    use arrow::ipc::reader::StreamReader;
    use graphforge_api::{PropValue, SpatialValue};

    fn flatten_value(array: &dyn Array, row: usize, output: &mut Vec<f64>) {
        if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
            output.push(values.value(row));
        } else if let Some(values) = array.as_any().downcast_ref::<StructArray>() {
            for column in values.columns() {
                flatten_value(column.as_ref(), row, output);
            }
        } else if let Some(values) = array.as_any().downcast_ref::<ListArray>() {
            let values = values.value(row);
            for index in 0..values.len() {
                flatten_value(values.as_ref(), index, output);
            }
        } else if let Some(values) = array.as_any().downcast_ref::<LargeListArray>() {
            let values = values.value(row);
            for index in 0..values.len() {
                flatten_value(values.as_ref(), index, output);
            }
        } else if let Some(values) = array.as_any().downcast_ref::<FixedSizeListArray>() {
            let values = values.value(row);
            for index in 0..values.len() {
                flatten_value(values.as_ref(), index, output);
            }
        } else {
            panic!(
                "unexpected GeoArrow coordinate array: {:?}",
                array.data_type()
            );
        }
    }

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/contracts/geoarrow-interchange-v1.json"
    ))
    .unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    let properties = cases
        .iter()
        .map(|case| {
            let name = case["name"].as_str().unwrap().to_owned();
            let spatial: SpatialValue = serde_json::from_value(serde_json::json!({
                "spatial_type": {
                    "geometry": case["geometry"],
                    "crs": case["crs"],
                },
                "coordinates": case["coordinates"],
                    "extension_name": case.get("preservedOnly").and_then(serde_json::Value::as_bool).unwrap_or(false).then(|| case["extensionName"].clone()),
                    "extension_metadata": case.get("preservedOnly").and_then(serde_json::Value::as_bool).unwrap_or(false).then(|| case["extensionMetadata"].clone()),
            }))
            .unwrap();
            (name, PropValue::Spatial(spatial))
        })
        .collect::<HashMap<_, _>>();
    let graph = GraphForge::new(None).unwrap();
    graph.add_node("Geometry", &properties).unwrap();
    graph.add_node("Geometry", &HashMap::new()).unwrap();
    let projection = cases
        .iter()
        .map(|case| {
            let name = case["name"].as_str().unwrap();
            format!("n.{name} AS {name}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let result = graph
        .execute(&format!(
            "MATCH (n:Geometry) RETURN {projection} ORDER BY n.node_uuid"
        ))
        .unwrap();
    let mut ipc = Vec::new();
    write_result(&result, &mut ipc).unwrap();
    let batches = StreamReader::try_new(Cursor::new(ipc), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected_batches = fixture["rows"]["batchSizes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| usize::try_from(value.as_u64().unwrap()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        expected_batches
    );
    assert_eq!(
        batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let field = batches[0].schema().field_with_name(name).unwrap().clone();
        assert_eq!(
            field.metadata()["ARROW:extension:name"],
            case["extensionName"]
        );
        assert_eq!(
            field.metadata()["ARROW:extension:metadata"],
            case["extensionMetadata"]
        );
        let column = batches[0].column_by_name(name).unwrap();
        let mut coordinates = Vec::new();
        flatten_value(
            column.as_ref(),
            usize::try_from(fixture["rows"]["populated"].as_u64().unwrap()).unwrap(),
            &mut coordinates,
        );
        let expected = case["flat"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_f64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(coordinates, expected);
        assert!(
            column.is_null(usize::try_from(fixture["rows"]["null"].as_u64().unwrap()).unwrap())
        );
    }
}
