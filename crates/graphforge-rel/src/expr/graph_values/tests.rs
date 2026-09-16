use super::super::{ExprArena, VarMap, tests::make_lowerer};
use super::*;
use datafusion::arrow::datatypes::Field;
use std::sync::Arc;

#[test]
fn hydrated_path_nodes_return_type_carries_labels_and_props() {
    use datafusion::arrow::datatypes::Field;

    // Without hydration: the original node_uuid-only element (#754).
    let bare = CypherPathNodes::new().return_type(&[]).unwrap();
    let DataType::List(item) = &bare else {
        panic!("list return, got {bare:?}")
    };
    let DataType::Struct(fields) = item.data_type() else {
        panic!("struct element")
    };
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name(), "node_uuid");

    // With hydration: node_uuid, labels, then the baked property union
    // (#1024) — the shape `render_node_struct` and `x.<prop>` need.
    let hydrated = CypherPathNodes::with_hydration(PathNodeHydration {
        labels_by_type: vec![(
            graphforge_value::EntityTypeId::decode(0).unwrap(),
            "A".to_owned(),
        )],
        prop_stems: vec!["_untyped".to_owned()],
        fields: vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
            Field::new("name", DataType::Utf8, true),
        ]
        .into(),
    })
    .return_type(&[])
    .unwrap();
    let DataType::List(item) = &hydrated else {
        panic!("list return, got {hydrated:?}")
    };
    let DataType::Struct(fields) = item.data_type() else {
        panic!("struct element")
    };
    let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["node_uuid", "labels", "name"]);
}

fn invoke_graph_metadata(
    kind: GraphMetadataKind,
    value: ScalarValue,
) -> datafusion::error::Result<ScalarValue> {
    use datafusion::config::ConfigOptions;

    let udf = CypherGraphMetadata::new(kind);
    let return_type = udf.return_type(&[])?;
    let result = udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Scalar(value.clone())],
        arg_fields: vec![Arc::new(Field::new("value", value.data_type(), true))],
        number_rows: 1,
        return_field: Arc::new(Field::new("metadata", return_type, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })?;
    match result {
        ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0),
        ColumnarValue::Scalar(value) => Ok(value),
    }
}

#[test]
fn graph_metadata_runtime_dispatch_validates_entity_kind() {
    use datafusion::arrow::array::{Int64Array, StringArray, StructArray};
    use datafusion::arrow::datatypes::Fields;

    let labels = ScalarValue::List(ScalarValue::new_list(
        &[ScalarValue::Utf8(Some("Person".into()))],
        &DataType::Utf8,
        true,
    ));
    let node = ScalarValue::Struct(Arc::new(StructArray::new(
        Fields::from(vec![
            Field::new("node_uuid", DataType::Int64, false),
            Field::new("labels", labels.data_type(), true),
            Field::new("rel_type", DataType::Utf8, true),
        ]),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            match &labels {
                ScalarValue::List(array) => Arc::clone(array) as _,
                _ => unreachable!(),
            },
            Arc::new(StringArray::from(vec![Some("property, not metadata")])),
        ],
        None,
    )));
    let relationship = ScalarValue::Struct(Arc::new(StructArray::new(
        Fields::from(vec![
            Field::new("edge_uuid", DataType::Int64, false),
            Field::new("rel_type", DataType::Utf8, false),
        ]),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["KNOWS"])),
        ],
        None,
    )));

    let actual_labels =
        invoke_graph_metadata(GraphMetadataKind::Labels, node.clone()).expect("labels(node)");
    assert_eq!(actual_labels, labels);
    assert_eq!(
        invoke_graph_metadata(GraphMetadataKind::RelationshipType, relationship.clone())
            .expect("type(relationship)"),
        ScalarValue::Utf8(Some("KNOWS".into()))
    );
    assert!(invoke_graph_metadata(GraphMetadataKind::RelationshipType, node).is_err());
    assert!(invoke_graph_metadata(GraphMetadataKind::Labels, relationship).is_err());
    assert!(
        invoke_graph_metadata(GraphMetadataKind::Labels, ScalarValue::Null)
            .expect("labels(null)")
            .is_null()
    );

    let colliding_map = ScalarValue::Struct(Arc::new(StructArray::new(
        Fields::from(vec![Field::new("labels", labels.data_type(), true)]),
        vec![match labels {
            ScalarValue::List(array) => array as _,
            _ => unreachable!(),
        }],
        None,
    )));
    assert!(invoke_graph_metadata(GraphMetadataKind::Labels, colliding_map).is_err());
}

// -----------------------------------------------------------------------
// cypher_path_nodes UDF — traversal node sequence (#754)
// -----------------------------------------------------------------------

#[test]
fn path_nodes_lowers_to_udf_over_node_uuid() {
    let mut arena = ExprArena::new();
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "var_0"); // start node — bare scan qualifier
    vm.insert(VarId(1), "var_1.rels"); // var-length edge list
    let start = arena.push(IrExpr::VarRef(VarId(0)));
    let rels = arena.push(IrExpr::VarRef(VarId(1)));
    let id = arena.push(IrExpr::FunctionCall {
        name: "_path_nodes".into(),
        args: vec![start, rels],
    });
    let expr = make_lowerer(&arena, &vm).lower(id).expect("lower");
    let s = format!("{expr}");
    assert!(s.contains("cypher_path_nodes"), "got {s}");
    assert!(
        s.contains("var_0.node_uuid"),
        "seed is the uuid column: {s}"
    );
}

#[test]
fn path_nodes_rejects_snapshot_missing_discovered_schema() {
    for name in ["_path_nodes", "_PATH_NODES"] {
        let mut arena = ExprArena::new();
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "var_0");
        vm.insert(VarId(1), "var_1.rels");
        let start = arena.push(IrExpr::VarRef(VarId(0)));
        let rels = arena.push(IrExpr::VarRef(VarId(1)));
        let id = arena.push(IrExpr::FunctionCall {
            name: name.into(),
            args: vec![start, rels],
        });
        let snapshot = graphforge_ir::LoweringSnapshot {
            node_property_stems: vec!["missing".into()],
            ..Default::default()
        };
        let error = make_lowerer(&arena, &vm)
            .with_read_target(snapshot)
            .lower(id)
            .unwrap_err();
        assert!(matches!(error, LoweringError::UnsupportedExpr(ref message)
        if message == "lowering snapshot missing node property schema for stem missing"));
    }
}

/// A 16-byte uuid stand-in: byte `b` repeated.
fn uuid16(b: u8) -> Vec<u8> {
    vec![b; 16]
}

/// Build a `List<Struct{src_uuid, dst_uuid}>` edge-list column. Each edge
/// is `(src_byte, dst_byte)` in **storage** orientation; `None` rows are
/// null lists.
fn edge_list(rows: &[Option<&[(u8, u8)]>]) -> datafusion::arrow::array::ArrayRef {
    use datafusion::arrow::array::{FixedSizeBinaryBuilder, ListBuilder, StructBuilder};
    use datafusion::arrow::datatypes::Field;

    let fields: datafusion::arrow::datatypes::Fields = vec![
        Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
    ]
    .into();
    let mut b = ListBuilder::new(StructBuilder::new(
        fields,
        vec![
            Box::new(FixedSizeBinaryBuilder::new(16)),
            Box::new(FixedSizeBinaryBuilder::new(16)),
        ],
    ));
    for row in rows {
        let Some(edges) = row else {
            b.append_null();
            continue;
        };
        for (src, dst) in *edges {
            b.values()
                .field_builder::<FixedSizeBinaryBuilder>(0)
                .unwrap()
                .append_value(uuid16(*src))
                .unwrap();
            b.values()
                .field_builder::<FixedSizeBinaryBuilder>(1)
                .unwrap()
                .append_value(uuid16(*dst))
                .unwrap();
            b.values().append(true);
        }
        b.append(true);
    }
    std::sync::Arc::new(b.finish())
}

/// Build the seed-uuid column; `None` entries are null seeds.
fn seed_uuids(vals: &[Option<u8>]) -> datafusion::arrow::array::ArrayRef {
    use datafusion::arrow::array::FixedSizeBinaryBuilder;
    let mut b = FixedSizeBinaryBuilder::new(16);
    for v in vals {
        match v {
            Some(x) => b.append_value(uuid16(*x)).unwrap(),
            None => b.append_null(),
        }
    }
    std::sync::Arc::new(b.finish())
}

fn invoke_path_nodes(
    seed: datafusion::arrow::array::ArrayRef,
    rels: datafusion::arrow::array::ArrayRef,
) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef> {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    let udf = CypherPathNodes::new();
    let n = seed.len();
    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::clone(&seed)),
            ColumnarValue::Array(Arc::clone(&rels)),
        ],
        arg_fields: vec![
            Arc::new(Field::new("seed", seed.data_type().clone(), true)),
            Arc::new(Field::new("rels", rels.data_type().clone(), true)),
        ],
        number_rows: n,
        return_field: Arc::new(Field::new("nodes", udf.return_type(&[])?, true)),
        config_options: Arc::new(ConfigOptions::default()),
    };
    udf.invoke_with_args(args).map(|v| match v {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => s.to_array_of_size(n).unwrap(),
    })
}

/// Row `i` of the result as the node uuids' first bytes, or `None` for a
/// null path.
fn path_node_bytes(out: &datafusion::arrow::array::ArrayRef, i: usize) -> Option<Vec<u8>> {
    use datafusion::arrow::array::{Array, FixedSizeBinaryArray, ListArray, StructArray};
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    if list.is_null(i) {
        return None;
    }
    let items = list.value(i);
    let items = items.as_any().downcast_ref::<StructArray>().unwrap();
    let uuids = items.column_by_name("node_uuid").unwrap();
    let uuids = uuids
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    Some((0..uuids.len()).map(|j| uuids.value(j)[0]).collect())
}

#[test]
fn path_nodes_walks_forward_chain() {
    let out = invoke_path_nodes(
        seed_uuids(&[Some(1)]),
        edge_list(&[Some(&[(1, 2), (2, 3)])]),
    )
    .unwrap();
    assert_eq!(path_node_bytes(&out, 0), Some(vec![1, 2, 3]));
}

#[test]
fn path_nodes_flips_reversed_storage_orientation() {
    // Edge stored 2→1 but traversed from 1 (an `In`/`Undirected` hop):
    // the next node is the *other* endpoint, not blindly dst_uuid.
    let out = invoke_path_nodes(seed_uuids(&[Some(1)]), edge_list(&[Some(&[(2, 1)])])).unwrap();
    assert_eq!(path_node_bytes(&out, 0), Some(vec![1, 2]));
}

#[test]
fn path_nodes_mixed_orientation_walk() {
    // 1 →(stored 1→2)→ 2 →(stored 3→2, traversed against storage)→ 3.
    let out = invoke_path_nodes(
        seed_uuids(&[Some(1)]),
        edge_list(&[Some(&[(1, 2), (3, 2)])]),
    )
    .unwrap();
    assert_eq!(path_node_bytes(&out, 0), Some(vec![1, 2, 3]));
}

#[test]
fn path_nodes_self_loop_stays_put() {
    let out = invoke_path_nodes(seed_uuids(&[Some(1)]), edge_list(&[Some(&[(1, 1)])])).unwrap();
    assert_eq!(path_node_bytes(&out, 0), Some(vec![1, 1]));
}

#[test]
fn path_nodes_zero_hop_is_seed_only() {
    let out = invoke_path_nodes(seed_uuids(&[Some(7)]), edge_list(&[Some(&[])])).unwrap();
    assert_eq!(path_node_bytes(&out, 0), Some(vec![7]));
}

#[test]
fn path_nodes_null_seed_or_list_is_null() {
    // Unmatched OPTIONAL MATCH rows: null seed (row 0) or null list (row 1).
    let out = invoke_path_nodes(
        seed_uuids(&[None, Some(1)]),
        edge_list(&[Some(&[(1, 2)]), None]),
    )
    .unwrap();
    assert_eq!(path_node_bytes(&out, 0), None);
    assert_eq!(path_node_bytes(&out, 1), None);
}

#[test]
fn path_nodes_disconnected_edge_errors() {
    let err = invoke_path_nodes(seed_uuids(&[Some(1)]), edge_list(&[Some(&[(5, 6)])]))
        .expect_err("an edge touching neither endpoint is a corrupt emission");
    assert!(err.to_string().contains("disconnected"), "got {err}");
}

#[test]
fn path_nodes_output_matches_declared_return_type() {
    // DataFusion verifies the produced array against `return_type` at
    // execution; catch any list-field/nullability drift here first.
    let out = invoke_path_nodes(seed_uuids(&[Some(1)]), edge_list(&[Some(&[(1, 2)])])).unwrap();
    assert_eq!(
        out.data_type(),
        &CypherPathNodes::new().return_type(&[]).unwrap()
    );
}
