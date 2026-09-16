use super::*;
use graphforge_ir::expr::{BinaryOpKind, IrExpr, IrLiteral, UnaryOpKind};
use graphforge_ir::{ExprArena, VarId};
use graphforge_value::PropertyId;
use std::sync::atomic::{AtomicUsize, Ordering};

static VOLATILE_CALLS: AtomicUsize = AtomicUsize::new(0);
static VOLATILE_ROWS: AtomicUsize = AtomicUsize::new(0);
/// Path-hydration structural counters are process-global (#706); serialize
/// tests that arm cancel/resource hooks or assert snapshots.

pub(super) fn invoke_test_udf<U: ScalarUDFImpl>(
    udf: &U,
    values: Vec<ScalarValue>,
) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef> {
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    let types = values
        .iter()
        .map(ScalarValue::data_type)
        .collect::<Vec<_>>();
    let return_type = udf.return_type(&types)?;
    let result = udf.invoke_with_args(ScalarFunctionArgs {
        args: values.into_iter().map(ColumnarValue::Scalar).collect(),
        arg_fields: types
            .iter()
            .enumerate()
            .map(|(index, data_type)| {
                Arc::new(Field::new(format!("arg_{index}"), data_type.clone(), true))
            })
            .collect(),
        number_rows: 1,
        return_field: Arc::new(Field::new("result", return_type.clone(), true)),
        config_options: Arc::new(ConfigOptions::default()),
    })?;
    let array = match result {
        ColumnarValue::Array(array) => array,
        ColumnarValue::Scalar(value) => value.to_array_of_size(1)?,
    };
    assert_eq!(array.data_type(), &return_type);
    Ok(array)
}

pub(super) fn invoke_test_udf_with_return_type<U: ScalarUDFImpl>(
    udf: &U,
    values: Vec<ScalarValue>,
    return_type: DataType,
) -> datafusion::error::Result<ColumnarValue> {
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    let types = values
        .iter()
        .map(ScalarValue::data_type)
        .collect::<Vec<_>>();
    udf.invoke_with_args(ScalarFunctionArgs {
        args: values.into_iter().map(ColumnarValue::Scalar).collect(),
        arg_fields: types
            .iter()
            .enumerate()
            .map(|(index, data_type)| {
                Arc::new(Field::new(format!("arg_{index}"), data_type.clone(), true))
            })
            .collect(),
        number_rows: 1,
        return_field: Arc::new(Field::new("result", return_type, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CountingVolatilePredicate {
    signature: Signature,
}

impl CountingVolatilePredicate {
    fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for CountingVolatilePredicate {
    fn name(&self) -> &'static str {
        "counting_volatile_predicate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::BooleanArray;
        validate_heterogeneous_arguments(&args.args)?;
        VOLATILE_CALLS.fetch_add(1, Ordering::SeqCst);
        VOLATILE_ROWS.fetch_add(args.number_rows, Ordering::SeqCst);
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            BooleanArray::from(vec![true; args.number_rows]),
        )))
    }
}

#[test]
fn quantifier_three_valued_reduce() {
    use datafusion::arrow::array::BooleanArray;
    use graphforge_ir::QuantifierKind::{All, Any, None, Single};
    let b = |v: Vec<Option<bool>>| BooleanArray::from(v);
    let r = |k, v: Vec<Option<bool>>| {
        let arr = b(v.clone());
        reduce_quantifier(k, &arr, v.len())
    };
    // Empty list: all/none → true, any/single → false.
    assert_eq!(r(All, vec![]), Some(true));
    assert_eq!(r(None, vec![]), Some(true));
    assert_eq!(r(Any, vec![]), Some(false));
    assert_eq!(r(Single, vec![]), Some(false));
    // Definitive results.
    assert_eq!(r(All, vec![Some(true), Some(true)]), Some(true));
    assert_eq!(r(All, vec![Some(true), Some(false)]), Some(false));
    assert_eq!(r(Any, vec![Some(false), Some(true)]), Some(true));
    assert_eq!(r(None, vec![Some(false), Some(false)]), Some(true));
    assert_eq!(r(Single, vec![Some(true), Some(false)]), Some(true));
    assert_eq!(r(Single, vec![Some(true), Some(true)]), Some(false));
    // Three-valued: a null only matters when nothing definitive settles it.
    assert_eq!(r(All, vec![Some(true), Option::None]), Option::None); // unknown
    assert_eq!(r(All, vec![Some(false), Option::None]), Some(false)); // false wins
    assert_eq!(r(Any, vec![Some(false), Option::None]), Option::None);
    assert_eq!(r(Any, vec![Some(true), Option::None]), Some(true)); // true wins
    assert_eq!(
        r(Single, vec![Some(true), Some(true), Option::None]),
        Some(false)
    ); // >1 wins
    assert_eq!(r(Single, vec![Some(true), Option::None]), Option::None);
}

#[test]
fn invariant_quantifier_truth_matrix_preserves_cardinality_and_nulls() {
    use graphforge_ir::QuantifierKind::{All, Any, None as NoneQ, Single};

    for kind in [All, Any, NoneQ, Single] {
        for predicate in [Some(true), Some(false), Option::None] {
            for length in [0, 1, 4] {
                assert_eq!(
                    reduce_invariant_quantifier(kind, predicate, length),
                    match predicate {
                        Some(value) => {
                            let values =
                                datafusion::arrow::array::BooleanArray::from(vec![value; length]);
                            reduce_quantifier(kind, &values, length)
                        }
                        Option::None => {
                            let values = datafusion::arrow::array::BooleanArray::new_null(length);
                            reduce_quantifier(kind, &values, length)
                        }
                    },
                    "{kind:?}, predicate={predicate:?}, length={length}"
                );
            }
        }
    }
}

#[test]
fn invariant_quantifier_scaling_counts_rows_not_heterogeneous_elements() {
    use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, ListArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::scalar::ScalarValue as S;
    use graphforge_ir::QuantifierKind::{All, Any, None as NoneQ, Single};
    use std::sync::Arc;

    let pattern = [
        S::Int64(Some(1)),
        S::Null,
        S::Boolean(Some(true)),
        S::Utf8(Some("x".to_owned())),
    ];
    let flat = pattern
        .iter()
        .cloned()
        .chain(pattern.iter().cloned().cycle().take(40))
        .collect::<Vec<_>>();
    let values: ArrayRef = Arc::new(build_het_struct(&flat, 0).unwrap());
    let item = Arc::new(Field::new("item", values.data_type().clone(), true));
    let list: ArrayRef = Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 4, 44, 44])),
        values,
        Some(NullBuffer::from(vec![true, true, false])),
    ));
    INVARIANT_QUANTIFIER_ROWS.store(0, Ordering::SeqCst);

    for kind in [All, Any, NoneQ, Single] {
        for predicate in [Some(true), Some(false)] {
            let udf = CypherInvariantQuantifier::new(kind, predicate);
            let output = udf
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(Arc::clone(&list))],
                    arg_fields: vec![Arc::new(Field::new("list", list.data_type().clone(), true))],
                    number_rows: 3,
                    return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
                    config_options: Arc::new(ConfigOptions::default()),
                })
                .unwrap()
                .into_array(3)
                .unwrap();
            let output = output.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(
                output.value(0),
                reduce_invariant_quantifier(kind, predicate, 4).unwrap()
            );
            assert_eq!(
                output.value(1),
                reduce_invariant_quantifier(kind, predicate, 40).unwrap()
            );
            assert!(output.is_null(2));
        }
    }
    assert_eq!(
        INVARIANT_QUANTIFIER_ROWS.load(Ordering::SeqCst),
        4 * 2 * 3,
        "1x and 10x element counts must keep invariant work at one fold per list row"
    );
}

#[test]
fn invariant_quantifier_lowering_uses_cardinality_only_udf() {
    use graphforge_ir::QuantifierKind::None as NoneQ;

    for (predicate, expected) in [
        (IrLiteral::Bool(true), Some(true)),
        (IrLiteral::Bool(false), Some(false)),
        (IrLiteral::Null, Option::None),
    ] {
        let mut arena = ExprArena::new();
        let list = arena.push(IrExpr::VarRef(VarId(0)));
        let predicate = arena.push(IrExpr::Literal(predicate));
        let quantifier = arena.push(IrExpr::Quantifier {
            kind: NoneQ,
            loop_var: VarId(1),
            list,
            predicate,
        });
        let mut vars = VarMap::new();
        vars.insert(VarId(0), "list");
        let lowered = make_lowerer(&arena, &vars).lower(quantifier).unwrap();
        let DfExpr::ScalarFunction(function) = lowered else {
            panic!("invariant quantifier must lower to a scalar UDF")
        };
        let invariant = function
            .func
            .inner()
            .downcast_ref::<CypherInvariantQuantifier>()
            .expect("cardinality-only quantifier UDF");
        assert_eq!(invariant.predicate, expected);
    }
}

#[test]
fn uncorrelated_list_comprehension_batches_volatile_predicate_once() {
    use datafusion::arrow::array::{Array, Int64Builder, ListArray, ListBuilder};
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use std::sync::Arc;

    VOLATILE_CALLS.store(0, Ordering::SeqCst);
    VOLATILE_ROWS.store(0, Ordering::SeqCst);
    let mut builder = ListBuilder::new(Int64Builder::new());
    builder.append_null();
    builder.append(true);
    builder.values().append_value(1);
    builder.values().append_value(2);
    builder.append(true);
    builder.values().append_value(3);
    builder.append(true);
    let input = Arc::new(builder.finish()) as datafusion::arrow::array::ArrayRef;
    let predicate = ScalarUDF::new_from_impl(CountingVolatilePredicate::new()).call(vec![]);
    let udf = CypherListComp::new(Some(predicate), None, "__gf_elem".to_owned(), vec![]);
    let return_type = udf.return_type(&[input.data_type().clone()]).unwrap();
    let output = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input)],
            arg_fields: vec![Arc::new(Field::new(
                "list",
                DataType::new_list(DataType::Int64, true),
                true,
            ))],
            number_rows: 4,
            return_field: Arc::new(Field::new("out", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
        .into_array(4)
        .unwrap();
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();

    assert!(output.is_null(0));
    assert_eq!(output.value(1).len(), 0);
    assert_eq!(output.value(2).len(), 2);
    assert_eq!(output.value(3).len(), 1);
    assert_eq!(VOLATILE_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(VOLATILE_ROWS.load(Ordering::SeqCst), 3);
}

#[test]
fn correlated_list_comprehension_filters_and_projects_with_outer_value() {
    use datafusion::arrow::array::{Array, ListArray};
    use datafusion::logical_expr::{col, lit};

    let input = ScalarValue::List(ScalarValue::new_list(
        &[
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(3)),
            ScalarValue::Int64(Some(5)),
        ],
        &DataType::Int64,
        true,
    ));
    let udf = CypherListComp::new(
        Some(col("__gf_elem").gt(col("threshold"))),
        Some(col("__gf_elem") + col("threshold") + lit(0_i64)),
        "__gf_elem".into(),
        vec!["threshold".into()],
    );
    let output = invoke_test_udf(&udf, vec![input, ScalarValue::Int64(Some(2))]).unwrap();
    let output = output.as_any().downcast_ref::<ListArray>().expect("List");
    assert!(!output.is_null(0));
    let values = output.value(0);
    assert_eq!(
        (0..values.len())
            .map(|row| ScalarValue::try_from_array(&values, row).unwrap())
            .collect::<Vec<_>>(),
        vec![ScalarValue::Int64(Some(5)), ScalarValue::Int64(Some(7))]
    );
}

#[test]
fn list_comprehension_rejects_non_list_input_through_public_udf_contract() {
    let udf = CypherListComp::new(None, None, "__gf_elem".into(), vec![]);
    let error = invoke_test_udf(&udf, vec![ScalarValue::Int64(Some(1))]).unwrap_err();
    let datafusion::error::DataFusionError::Internal(message) = error else {
        panic!("expected DataFusion internal contract error")
    };
    assert_eq!(
        message,
        "cypher_list_comprehension: first argument is not a list"
    );
}

/// Invoke a `cypher_quantifier` UDF (no outer columns) over a single list
/// column and return the per-row boolean verdicts.
fn invoke_cypher_quantifier(
    kind: graphforge_ir::QuantifierKind,
    predicate: DfExpr,
    list: datafusion::arrow::array::ArrayRef,
) -> datafusion::error::Result<datafusion::arrow::array::BooleanArray> {
    use datafusion::arrow::array::BooleanArray;
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use std::sync::Arc;

    let n = list.len();
    let field = Arc::new(Field::new("l", list.data_type().clone(), true));
    let ret = Arc::new(Field::new("q", DataType::Boolean, true));
    let args = ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(list)],
        arg_fields: vec![field],
        number_rows: n,
        return_field: ret,
        config_options: Arc::new(ConfigOptions::default()),
    };
    let udf = CypherQuantifier::new(kind, predicate, "__gf_elem".to_owned(), vec![]);
    let arr = match udf.invoke_with_args(args)? {
        ColumnarValue::Array(a) => a,
        ColumnarValue::Scalar(s) => s.to_array_of_size(n)?,
    };
    Ok(arr
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("boolean verdicts")
        .clone())
}

#[test]
fn cypher_quantifier_empty_list_yields_identity_without_predicate() {
    use datafusion::arrow::array::{Array, ListArray};
    use datafusion::arrow::datatypes::Int64Type;
    use graphforge_ir::QuantifierKind::{All, Any, None as NoneQ, Single};

    // One empty Int64 list row — the shape a statically-empty `[]` takes.
    let empty = || {
        std::sync::Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(Vec::<Option<i64>>::new()),
        ])) as datafusion::arrow::array::ArrayRef
    };

    // `x.a = 2` cannot plan over Int64 elements; `x` alone evaluates to
    // Int64, not Boolean. Neither may disturb the empty-list identity
    // (#1020) — the predicate must not run at all.
    let field_pred = datafusion::functions::core::expr_fn::get_field(col_literal("__gf_elem"), "a")
        .eq(DfExpr::Literal(ScalarValue::Int64(Some(2)), Option::None));
    let elem_pred = col_literal("__gf_elem");

    for (kind, expected) in [(All, true), (NoneQ, true), (Any, false), (Single, false)] {
        for pred in [field_pred.clone(), elem_pred.clone()] {
            let out = invoke_cypher_quantifier(kind, pred, empty()).expect("no error");
            assert!(!out.is_null(0), "{kind:?}: empty list must be definitive");
            assert_eq!(out.value(0), expected, "{kind:?} identity");
        }
    }
}

#[test]
fn cypher_quantifier_mixed_batch_and_unplannable_predicate() {
    use datafusion::arrow::array::{Array, BooleanBuilder, ListBuilder};
    use graphforge_ir::QuantifierKind::Any;

    // Rows: null list, empty list, [true, false].
    let mut b = ListBuilder::new(BooleanBuilder::new());
    b.append_null();
    b.append(true);
    b.values().append_value(true);
    b.values().append_value(false);
    b.append(true);
    let list = std::sync::Arc::new(b.finish()) as datafusion::arrow::array::ArrayRef;

    // `any(x IN ... WHERE x)` over Boolean elements: per-row verdicts.
    let out =
        invoke_cypher_quantifier(Any, col_literal("__gf_elem"), list.clone()).expect("no error");
    assert!(out.is_null(0), "null list → null");
    assert!(!out.is_null(1) && !out.value(1), "any over [] is false");
    assert!(out.value(2), "any over [true, false] is true");

    // An unbuildable predicate still errors once a non-empty row needs it.
    let bad = datafusion::functions::core::expr_fn::get_field(col_literal("__gf_elem"), "a");
    assert!(
        invoke_cypher_quantifier(Any, bad, list).is_err(),
        "non-empty row against an unbuildable predicate must error"
    );
}

/// Build a minimal lowerer with no ontology.
pub(super) fn make_lowerer<'a>(arena: &'a ExprArena, var_map: &'a VarMap) -> ExprLowerer<'a> {
    ExprLowerer::new(arena, None, var_map)
}

#[test]
fn distance_rejects_preserved_only_spatial_literals_explicitly() {
    use graphforge_core::{
        SpatialCoordinates, SpatialCrs, SpatialGeometryType, SpatialType, SpatialValue,
    };

    let mut arena = ExprArena::new();
    let preserved = arena.push(IrExpr::Literal(IrLiteral::Spatial(SpatialValue {
        spatial_type: SpatialType {
            geometry: SpatialGeometryType::Point,
            crs: SpatialCrs::Preserved("OGC:CRS84".into()),
        },
        coordinates: SpatialCoordinates::Point([-104.9903, 39.7392]),
        extension_name: Some("geoarrow.vendor_point".into()),
        extension_metadata: Some("{\"crs\":\"OGC:CRS84\",\"edges\":\"spherical\"}".into()),
    })));
    let distance = arena.push(IrExpr::FunctionCall {
        name: "distance".into(),
        args: vec![preserved, preserved],
    });
    let vars = VarMap::new();
    let error = make_lowerer(&arena, &vars).lower(distance).unwrap_err();
    assert_eq!(
        error.to_string(),
        "invalid argument type: distance() does not compute preserved-only CRS values"
    );
}

// -----------------------------------------------------------------------
// Conservative operand type-check (#956, InvalidArgumentType)
// -----------------------------------------------------------------------

#[test]
fn boolean_and_numeric_operators_reject_known_bad_operands() {
    use graphforge_ir::expr::{BinaryOpKind as B, IrExpr, IrLiteral, UnaryOpKind as U};
    let vm = VarMap::new();

    // A statically-known incompatible operand is rejected as InvalidType.
    let reject = |build: &dyn Fn(&mut ExprArena) -> ExprId| {
        let mut a = ExprArena::new();
        let id = build(&mut a);
        let err = make_lowerer(&a, &vm).lower(id).expect_err("should reject");
        assert!(
            matches!(err, LoweringError::InvalidType(_)),
            "expected InvalidType, got {err:?}"
        );
    };
    // `1 AND true`
    reject(&|a| {
        let l = a.push(IrExpr::Literal(IrLiteral::Int(1)));
        let r = a.push(IrExpr::Literal(IrLiteral::Bool(true)));
        a.push(IrExpr::BinaryOp {
            op: B::And,
            left: l,
            right: r,
        })
    });
    // `1 XOR true`
    reject(&|a| {
        let l = a.push(IrExpr::Literal(IrLiteral::Int(1)));
        let r = a.push(IrExpr::Literal(IrLiteral::Bool(true)));
        a.push(IrExpr::BinaryOp {
            op: B::Xor,
            left: l,
            right: r,
        })
    });
    // `NOT 'x'`
    reject(&|a| {
        let e = a.push(IrExpr::Literal(IrLiteral::Str("x".into())));
        a.push(IrExpr::UnaryOp {
            op: U::Not,
            expr: e,
        })
    });
    // `-true`
    reject(&|a| {
        let e = a.push(IrExpr::Literal(IrLiteral::Bool(true)));
        a.push(IrExpr::UnaryOp {
            op: U::Neg,
            expr: e,
        })
    });
    // `'a' % 2`
    reject(&|a| {
        let l = a.push(IrExpr::Literal(IrLiteral::Str("a".into())));
        let r = a.push(IrExpr::Literal(IrLiteral::Int(2)));
        a.push(IrExpr::BinaryOp {
            op: B::Mod,
            left: l,
            right: r,
        })
    });
    // `NOT {k: 1}` — a map literal is a known non-boolean.
    reject(&|a| {
        let v = a.push(IrExpr::Literal(IrLiteral::Int(1)));
        let m = a.push(IrExpr::MapLiteral(vec![("k".into(), v)]));
        a.push(IrExpr::UnaryOp {
            op: U::Not,
            expr: m,
        })
    });
}

#[test]
fn operator_type_check_accepts_valid_and_unknown_operands() {
    use graphforge_ir::VarId;
    use graphforge_ir::expr::{BinaryOpKind as B, IrExpr, IrLiteral, UnaryOpKind as U};
    let accept = |vm: &VarMap, build: &dyn Fn(&mut ExprArena) -> ExprId| {
        let mut a = ExprArena::new();
        let id = build(&mut a);
        make_lowerer(&a, vm)
            .lower(id)
            .expect("valid/unknown operands must lower cleanly");
    };
    // `true AND false`
    accept(&VarMap::new(), &|a| {
        let l = a.push(IrExpr::Literal(IrLiteral::Bool(true)));
        let r = a.push(IrExpr::Literal(IrLiteral::Bool(false)));
        a.push(IrExpr::BinaryOp {
            op: B::And,
            left: l,
            right: r,
        })
    });
    // `null AND true` — null is valid in three-valued logic.
    accept(&VarMap::new(), &|a| {
        let l = a.push(IrExpr::Literal(IrLiteral::Null));
        let r = a.push(IrExpr::Literal(IrLiteral::Bool(true)));
        a.push(IrExpr::BinaryOp {
            op: B::And,
            left: l,
            right: r,
        })
    });
    // `NOT x` where x is an untyped variable (unknown type — do not reject).
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "x");
    accept(&vm, &|a| {
        let e = a.push(IrExpr::VarRef(VarId(0)));
        a.push(IrExpr::UnaryOp {
            op: U::Not,
            expr: e,
        })
    });
}

#[test]
fn nested_quantifiers_get_per_depth_elem_columns() {
    use graphforge_ir::QuantifierKind::{None as NoneQ, Single};

    // none(x IN list WHERE single(y IN list WHERE x + y = 15)): the inner
    // predicate references BOTH loop elements, so the bindings must stay
    // distinct across depths (#1021).
    let mut arena = ExprArena::new();
    let list_outer = arena.push(IrExpr::VarRef(VarId(0)));
    let list_inner = arena.push(IrExpr::VarRef(VarId(0)));
    let x = arena.push(IrExpr::VarRef(VarId(1)));
    let y = arena.push(IrExpr::VarRef(VarId(2)));
    let sum = arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::Add,
        left: x,
        right: y,
    });
    let fifteen = arena.push(IrExpr::Literal(IrLiteral::Int(15)));
    let eq = arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::Eq,
        left: sum,
        right: fifteen,
    });
    let inner = arena.push(IrExpr::Quantifier {
        kind: Single,
        loop_var: VarId(2),
        list: list_inner,
        predicate: eq,
    });
    let outer = arena.push(IrExpr::Quantifier {
        kind: NoneQ,
        loop_var: VarId(1),
        list: list_outer,
        predicate: inner,
    });

    let mut vm = VarMap::new();
    vm.insert(VarId(0), "list");
    let lowered = make_lowerer(&arena, &vm).lower(outer).expect("lower");

    let as_quant = |e: &DfExpr| -> Option<(String, Vec<String>, Vec<DfExpr>)> {
        let DfExpr::ScalarFunction(f) = e else {
            return Option::None;
        };
        let q = f.func.inner().downcast_ref::<CypherQuantifier>()?;
        Some((q.elem_name.clone(), q.outer_names.clone(), f.args.clone()))
    };

    // Outer loop keeps the historical name; its only outer column is the
    // real `list` (its own element must NOT leak into its args).
    let (outer_elem, outer_outers, _) = as_quant(&lowered).expect("outer quantifier UDF");
    assert_eq!(outer_elem, "__gf_elem");
    assert_eq!(outer_outers, vec!["list".to_owned()]);

    // The inner loop gets the depth-1 name, and the OUTER element flows in
    // as one of its outer columns (broadcast per outer element at invoke).
    let DfExpr::ScalarFunction(outer_fn) = &lowered else {
        panic!("outer is a scalar function")
    };
    let outer_q = outer_fn
        .func
        .inner()
        .downcast_ref::<CypherQuantifier>()
        .unwrap();
    let (inner_elem, inner_outers, inner_args) =
        as_quant(&outer_q.predicate).expect("inner quantifier UDF");
    assert_eq!(inner_elem, "__gf_elem_1");
    assert_eq!(
        inner_outers,
        vec!["__gf_elem".to_owned()],
        "the outer element is an outer column of the inner UDF (the list \
         argument resolves in the enclosing batch, not via outer_names)"
    );
    // Call args: the list, then one arg per outer_names entry.
    let arg_names: Vec<String> = inner_args.iter().map(|a| a.to_string()).collect();
    assert_eq!(arg_names, vec!["list", "__gf_elem"]);
}

#[test]
fn elem_struct_col_routes_property_to_get_field() {
    // #1004: a property access on the synthetic quantifier/comprehension
    // element column (`__gf_elem`) is a STRUCT-FIELD access, so it must lower
    // via `get_field(__gf_elem, "a")` — not the dotted property-column
    // `__gf_elem.a` used for node properties (which DataFusion reads as a
    // qualified column that does not exist on a single struct column, giving
    // "No field named a"). Any OTHER base still uses the dotted form.
    let mut arena = ExprArena::new();
    let base = arena.push(IrExpr::VarRef(VarId(0)));
    let access = arena.push(IrExpr::PropertyAccess {
        base,
        prop: PropertyId::runtime(graphforge_value::RuntimePropId::new(0).unwrap()),
    });
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "__gf_elem");
    let mut prop_names = HashMap::new();
    prop_names.insert(
        PropertyId::runtime(graphforge_value::RuntimePropId::new(0).unwrap()),
        "a".to_owned(),
    );

    // Without the marker: node-property dotted-column form — a QUALIFIED
    // column `__gf_elem.a` (relation `__gf_elem`, column `a`), which is
    // exactly what DataFusion cannot resolve against a single struct column.
    let dotted = ExprLowerer::with_prop_names(&arena, &vm, prop_names.clone())
        .lower(access)
        .unwrap();
    assert!(
        matches!(&dotted, DfExpr::Column(_)) && dotted.to_string() == "__gf_elem.a",
        "expected dotted column, got {dotted:?}"
    );

    // With the marker: struct-aware `get_field` (a scalar-function call, not a
    // Column), so plan-time validation resolves against the element's Struct.
    let via_get_field = ExprLowerer::with_prop_names(&arena, &vm, prop_names.clone())
        .with_elem_struct_col("__gf_elem".to_owned())
        .lower(access)
        .unwrap();
    assert!(
        !matches!(&via_get_field, DfExpr::Column(_)),
        "element field access must not be a dotted column: {via_get_field:?}"
    );
    assert!(
        via_get_field.to_string().contains("get_field"),
        "expected a get_field call, got {via_get_field}"
    );
}

#[test]
fn map_column_field_access_uses_get_field_via_schema() {
    // #1017: a `PropertyAccess` on a plain-map-typed column (e.g. `input.list`
    // where `input` was bound by `UNWIND [{list: …}] AS input`) resolves via
    // struct-aware `get_field` — NOT a dotted qualified column `input.list`,
    // which fails "No field named input.list" against a single struct column.
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::common::DFSchema;

    let map_ty = DataType::Struct(Fields::from(vec![
        Field::new("list", DataType::new_list(DataType::Int64, true), true),
        Field::new("fixed", DataType::Boolean, true),
    ]));
    let schema = Schema::new(vec![Field::new("input", map_ty, true)]);
    let df_schema = std::sync::Arc::new(DFSchema::try_from(schema).unwrap());

    let mut arena = ExprArena::new();
    let base = arena.push(IrExpr::VarRef(VarId(0)));
    let access = arena.push(IrExpr::PropertyAccess {
        base,
        prop: PropertyId::runtime(graphforge_value::RuntimePropId::new(0).unwrap()),
    });
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "input");
    let mut prop_names = HashMap::new();
    prop_names.insert(
        PropertyId::runtime(graphforge_value::RuntimePropId::new(0).unwrap()),
        "list".to_owned(),
    );

    let out = ExprLowerer::with_prop_names(&arena, &vm, prop_names)
        .with_input_schema(df_schema)
        .lower(access)
        .unwrap();
    assert!(
        !matches!(&out, DfExpr::Column(_)),
        "map field must not be a dotted column: {out:?}"
    );
    assert!(
        out.to_string().contains("get_field"),
        "expected a get_field call, got {out}"
    );
}

#[test]
fn list_plus_uses_native_ops_for_homogeneous_schema_types() {
    // #1017: with the input schema attached, `is_list_typed` types a list-valued
    // COLUMN operand. Homogeneous list ops stay in native Arrow list functions
    // so downstream quantifiers keep concrete element types; heterogeneous
    // list ops route to Cypher list-plus instead of numeric `+`.
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;
    use graphforge_ir::expr::BinaryOpKind;

    let schema = Schema::new(vec![
        Field::new("xs", DataType::new_list(DataType::Int64, true), true),
        Field::new("y", DataType::Int64, true),
        Field::new("ys", DataType::new_list(DataType::Int64, true), true),
        Field::new("s", DataType::Utf8, true),
        Field::new("ss", DataType::new_list(DataType::Utf8, true), true),
    ]);
    let df_schema = std::sync::Arc::new(DFSchema::try_from(schema).unwrap());

    let mut arena = ExprArena::new();
    let xs = arena.push(IrExpr::VarRef(VarId(0)));
    let y = arena.push(IrExpr::VarRef(VarId(1)));
    let ys = arena.push(IrExpr::VarRef(VarId(2)));
    let s = arena.push(IrExpr::VarRef(VarId(3)));
    let ss = arena.push(IrExpr::VarRef(VarId(4)));
    let append = arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::Add,
        left: xs,
        right: y,
    });
    let concat = arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::Add,
        left: xs,
        right: ys,
    });
    let hetero_append = arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::Add,
        left: xs,
        right: s,
    });
    let hetero_concat = arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::Add,
        left: xs,
        right: ss,
    });
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "xs");
    vm.insert(VarId(1), "y");
    vm.insert(VarId(2), "ys");
    vm.insert(VarId(3), "s");
    vm.insert(VarId(4), "ss");

    let lowerer =
        ExprLowerer::with_prop_names(&arena, &vm, HashMap::new()).with_input_schema(df_schema);
    assert!(
        lowerer
            .lower(append)
            .unwrap()
            .to_string()
            .contains("array_append"),
        "list + element should append"
    );
    assert!(
        lowerer
            .lower(concat)
            .unwrap()
            .to_string()
            .contains("array_concat"),
        "list + list should concat"
    );
    assert!(
        lowerer
            .lower(hetero_append)
            .unwrap()
            .to_string()
            .contains("cypher_list_plus"),
        "list + heterogeneous element should use tagged list-plus"
    );
    assert!(
        lowerer
            .lower(hetero_concat)
            .unwrap()
            .to_string()
            .contains("cypher_list_plus"),
        "list + heterogeneous list should use tagged list-plus"
    );
}

// -----------------------------------------------------------------------
// Literal tests
// -----------------------------------------------------------------------

#[test]
fn literal_null() {
    let mut arena = ExprArena::new();
    let id = arena.push(IrExpr::Literal(IrLiteral::Null));
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    assert!(matches!(result, DfExpr::Literal(ScalarValue::Null, _)));
}

#[test]
fn literal_bool() {
    let mut arena = ExprArena::new();
    let id = arena.push(IrExpr::Literal(IrLiteral::Bool(true)));
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    assert!(matches!(
        result,
        DfExpr::Literal(ScalarValue::Boolean(Some(true)), _)
    ));
}

#[test]
fn literal_int() {
    let mut arena = ExprArena::new();
    let id = arena.push(IrExpr::Literal(IrLiteral::Int(42)));
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    assert!(matches!(
        result,
        DfExpr::Literal(ScalarValue::Int64(Some(42)), _)
    ));
}

#[test]
fn literal_float() {
    let mut arena = ExprArena::new();
    let id = arena.push(IrExpr::Literal(IrLiteral::Float(2.71)));
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    assert!(matches!(
        result,
        DfExpr::Literal(ScalarValue::Float64(Some(_)), _)
    ));
}

#[test]
fn literal_str() {
    let mut arena = ExprArena::new();
    let id = arena.push(IrExpr::Literal(IrLiteral::Str("hello".into())));
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    assert!(matches!(
        result,
        DfExpr::Literal(ScalarValue::Utf8(Some(_)), _)
    ));
}

#[test]
fn literal_duration() {
    let mut arena = ExprArena::new();
    let id = arena.push(IrExpr::Literal(IrLiteral::Duration {
        months: 0,
        days: 0,
        seconds: 1,
        nanos: 0,
    }));
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    assert!(matches!(result, DfExpr::Literal(ScalarValue::Struct(_), _)));
}

#[test]
fn literal_datetime() {
    let mut arena = ExprArena::new();
    let id = arena.push(IrExpr::Literal(IrLiteral::DateTime(0)));
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    assert!(matches!(
        result,
        DfExpr::Literal(ScalarValue::TimestampMicrosecond(Some(0), Some(_)), _)
    ));
}

// -----------------------------------------------------------------------
// VarRef tests
// -----------------------------------------------------------------------

#[test]
fn var_ref_bound() {
    let mut arena = ExprArena::new();
    let id = arena.push(IrExpr::VarRef(VarId(0)));
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "node_id");
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    assert!(matches!(result, DfExpr::Column(_)));
    if let DfExpr::Column(col) = result {
        assert_eq!(col.name, "node_id");
    }
}

#[test]
fn var_ref_unbound_returns_error() {
    let mut arena = ExprArena::new();
    let id = arena.push(IrExpr::VarRef(VarId(99)));
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id);
    assert!(matches!(result, Err(LoweringError::UnboundVar(99))));
}

// -----------------------------------------------------------------------
// UnaryOp tests
// -----------------------------------------------------------------------

#[test]
fn unary_not() {
    let mut arena = ExprArena::new();
    let inner = arena.push(IrExpr::Literal(IrLiteral::Bool(true)));
    let id = arena.push(IrExpr::UnaryOp {
        op: UnaryOpKind::Not,
        expr: inner,
    });
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    assert!(matches!(result, DfExpr::Not(_)));
}

#[test]
fn unary_is_null() {
    let mut arena = ExprArena::new();
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "x");
    let inner = arena.push(IrExpr::VarRef(VarId(0)));
    let id = arena.push(IrExpr::UnaryOp {
        op: UnaryOpKind::IsNull,
        expr: inner,
    });
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    assert!(matches!(result, DfExpr::IsNull(_)));
}

#[test]
fn unary_is_not_null() {
    let mut arena = ExprArena::new();
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "x");
    let inner = arena.push(IrExpr::VarRef(VarId(0)));
    let id = arena.push(IrExpr::UnaryOp {
        op: UnaryOpKind::IsNotNull,
        expr: inner,
    });
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    assert!(matches!(result, DfExpr::IsNotNull(_)));
}

// -----------------------------------------------------------------------
// BinaryOp tests
// -----------------------------------------------------------------------

#[test]
fn binary_eq() {
    let mut arena = ExprArena::new();
    let l = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let r = arena.push(IrExpr::Literal(IrLiteral::Int(2)));
    let id = arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::Eq,
        left: l,
        right: r,
    });
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    // `=` lowers to the type-tolerant `cypher_eq` UDF (ADR 0009), not a
    // native `BinaryExpr` (which would plan-error on mismatched types).
    let DfExpr::ScalarFunction(sf) = result else {
        panic!("expected a cypher_eq scalar-function call, got {result:?}");
    };
    assert_eq!(sf.func.name(), "cypher_eq");
    assert_eq!(sf.args.len(), 2);
}

#[test]
fn binary_in_list() {
    let mut arena = ExprArena::new();
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "x");
    let l = arena.push(IrExpr::VarRef(VarId(0)));
    let one = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let two = arena.push(IrExpr::Literal(IrLiteral::Int(2)));
    let list = arena.push(IrExpr::ListLiteral(vec![one, two]));
    let id = arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::In,
        left: l,
        right: list,
    });
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    // Cypher `IN` lowers to the structural three-valued `cypher_in` UDF
    // (ADR 0011), not DataFusion's `in_list` (which treats the whole list as a
    // single element and type-errors on `3 IN ([1,2,3])`).
    let DfExpr::ScalarFunction(sf) = result else {
        panic!("expected a cypher_in scalar-function call, got {result:?}");
    };
    assert_eq!(sf.func.name(), "cypher_in");
    assert_eq!(sf.args.len(), 2);
}

fn label_membership_expr(arena: &mut ExprArena, left: IrExpr, node_var: VarId) -> ExprId {
    let left = arena.push(left);
    let node = arena.push(IrExpr::VarRef(node_var));
    let labels = arena.push(IrExpr::FunctionCall {
        name: "labels".into(),
        args: vec![node],
    });
    arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::In,
        left,
        right: labels,
    })
}

fn label_lowerer<'a>(arena: &'a ExprArena, var_map: &'a VarMap) -> ExprLowerer<'a> {
    ExprLowerer::with_prop_names_and_nodes(
        arena,
        var_map,
        HashMap::new(),
        HashMap::from([(0, NodeShape { prop_names: vec![] })]),
        HashMap::from([(
            graphforge_value::EntityTypeId::decode(7).unwrap(),
            "Known".to_owned(),
        )]),
        false,
    )
}

#[test]
fn known_literal_in_labels_lowers_to_direct_type_id_membership() {
    let mut arena = ExprArena::new();
    let id = label_membership_expr(
        &mut arena,
        IrExpr::Literal(IrLiteral::Str("Known".into())),
        VarId(0),
    );
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "var_0");

    let result = label_lowerer(&arena, &vm).lower(id).unwrap();
    let DfExpr::ScalarFunction(sf) = &result else {
        panic!("expected array_has scalar function, got {result:?}");
    };
    assert_eq!(sf.func.name(), "array_has");
    let rendered = result.to_string();
    assert!(rendered.contains("var_0.type_ids"));
    assert!(rendered.contains("UInt32(7)"));
    assert!(!rendered.contains("cypher_in"));
    assert!(!rendered.contains("array_concat"));
}

#[test]
fn unknown_literal_in_labels_retains_generic_membership() {
    let mut arena = ExprArena::new();
    let id = label_membership_expr(
        &mut arena,
        IrExpr::Literal(IrLiteral::Str("Unknown".into())),
        VarId(0),
    );
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "var_0");

    let result = label_lowerer(&arena, &vm).lower(id).unwrap();
    let DfExpr::ScalarFunction(sf) = result else {
        panic!("expected cypher_in scalar function");
    };
    assert_eq!(sf.func.name(), "cypher_in");
}

#[test]
fn dynamic_in_labels_retains_generic_membership() {
    let mut arena = ExprArena::new();
    let id = label_membership_expr(&mut arena, IrExpr::VarRef(VarId(1)), VarId(0));
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "var_0");
    vm.insert(VarId(1), "label_name");

    let result = label_lowerer(&arena, &vm).lower(id).unwrap();
    let DfExpr::ScalarFunction(sf) = result else {
        panic!("expected cypher_in scalar function");
    };
    assert_eq!(sf.func.name(), "cypher_in");
}

// -----------------------------------------------------------------------
// Compound predicate: a.age > 30 AND b.name = $name
// -----------------------------------------------------------------------

#[test]
fn compound_predicate() {
    let mut arena = ExprArena::new();
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "a.age"); // a.age resolved to column
    vm.insert(VarId(1), "b.name"); // b.name resolved to column

    let a_age = arena.push(IrExpr::VarRef(VarId(0)));
    let thirty = arena.push(IrExpr::Literal(IrLiteral::Int(30)));
    let gt = arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::Gt,
        left: a_age,
        right: thirty,
    });

    let b_name = arena.push(IrExpr::VarRef(VarId(1)));
    let param = arena.push(IrExpr::Parameter("name".into()));
    let eq = arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::Eq,
        left: b_name,
        right: param,
    });

    let and = arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::And,
        left: gt,
        right: eq,
    });

    let result = make_lowerer(&arena, &vm).lower(and).unwrap();
    assert!(matches!(result, DfExpr::ScalarFunction(_)));
    if let DfExpr::ScalarFunction(sf) = result {
        assert_eq!(sf.func.name(), "cypher_and");
    }
}

#[test]
fn xor_chain_lowering_has_one_udf_per_source_operator() {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

    let lower_and_count = |operands: usize| {
        let mut arena = ExprArena::new();
        let mut root = arena.push(IrExpr::Literal(IrLiteral::Bool(true)));
        for index in 1..operands {
            let right = match index % 3 {
                0 => IrLiteral::Bool(true),
                1 => IrLiteral::Bool(false),
                _ => IrLiteral::Null,
            };
            let right = arena.push(IrExpr::Literal(right));
            root = arena.push(IrExpr::BinaryOp {
                op: BinaryOpKind::Xor,
                left: root,
                right,
            });
        }

        let lowered = make_lowerer(&arena, &VarMap::new())
            .lower(root)
            .expect("XOR chain lowers");
        let mut udf_count = 0;
        lowered
            .apply(|expr| {
                if matches!(
                    expr,
                    DfExpr::ScalarFunction(function)
                        if function.func.name() == "cypher_xor"
                ) {
                    udf_count += 1;
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("expression traversal succeeds");
        udf_count
    };

    let eleven = lower_and_count(11);
    let twenty_two = lower_and_count(22);
    assert_eq!(eleven, 10);
    assert_eq!(twenty_two, 21);
    assert!(
        twenty_two <= eleven * 3,
        "doubling operands must keep deterministic lowering work within 3x"
    );
}

// -----------------------------------------------------------------------
// Function call tests
// -----------------------------------------------------------------------

#[test]
fn function_call_to_upper() {
    let mut arena = ExprArena::new();
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "n.name");
    let arg = arena.push(IrExpr::VarRef(VarId(0)));
    let id = arena.push(IrExpr::FunctionCall {
        name: "toUpper".into(),
        args: vec![arg],
    });
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    // upper() produces a ScalarFunction expr
    assert!(matches!(result, DfExpr::ScalarFunction(_)));
}

#[test]
fn function_call_unknown_returns_error() {
    let mut arena = ExprArena::new();
    let id = arena.push(IrExpr::FunctionCall {
        name: "unknownFn".into(),
        args: vec![],
    });
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id);
    assert!(matches!(result, Err(LoweringError::UnknownFunction(_))));
}

// -----------------------------------------------------------------------
// Relationship-list access lowering (#743)
// -----------------------------------------------------------------------

/// Lower `fn_name(VarRef("r"), <int args>)` over a var `r` and return the
/// rendered DataFusion expression string.
fn lower_rel_fn(name: &str, int_args: &[i64]) -> String {
    let mut arena = ExprArena::new();
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "var_1.rels");
    let mut args = vec![arena.push(IrExpr::VarRef(VarId(0)))];
    for &n in int_args {
        args.push(arena.push(IrExpr::Literal(IrLiteral::Int(n))));
    }
    let id = arena.push(IrExpr::FunctionCall {
        name: name.into(),
        args,
    });
    let expr = make_lowerer(&arena, &vm).lower(id).expect("lower");
    format!("{expr}")
}

#[test]
fn subscript_lowers_to_cypher_value_access() {
    // r[0] routes through Cypher's runtime subscript UDF so unknown and
    // parameterized list/map containers get Cypher error/null semantics.
    let s = lower_rel_fn("_subscript", &[0]);
    assert!(s.contains("cypher_value_access"), "got {s}");
    assert!(s.contains("var_1.rels"), "got {s}");
}

#[test]
fn head_and_last_lower_to_array_element() {
    assert!(lower_rel_fn("head", &[]).contains("array_element"));
    assert!(lower_rel_fn("last", &[]).contains("array_element"));
}

#[test]
fn slice_lowers_to_array_slice() {
    // r[0..2] → array_slice(var_1.rels, begin, end)
    let s = lower_rel_fn("_slice", &[0, 2]);
    assert!(s.contains("array_slice"), "got {s}");
    assert!(s.contains("var_1.rels"), "got {s}");
}

#[test]
fn slice_with_null_bounds_uses_array_length() {
    // r[..2]: start is Null → begin defaults to 1 (no array_length needed).
    // r[1..]: end is Null → end defaults to array_length(list).
    let mut arena = ExprArena::new();
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "var_1.rels");
    let list = arena.push(IrExpr::VarRef(VarId(0)));
    let start = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let end_null = arena.push(IrExpr::Literal(IrLiteral::Null));
    let id = arena.push(IrExpr::FunctionCall {
        name: "_slice".into(),
        args: vec![list, start, end_null],
    });
    let expr = make_lowerer(&arena, &vm).lower(id).expect("lower");
    let s = format!("{expr}");
    assert!(s.contains("array_slice"), "got {s}");
    assert!(
        s.contains("array_length"),
        "an unbounded end must default to array_length: {s}"
    );
}

#[test]
fn type_of_element_lowers_to_runtime_graph_metadata_dispatch() {
    let mut arena = ExprArena::new();
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "var_1.rels");
    let list = arena.push(IrExpr::VarRef(VarId(0)));
    let idx = arena.push(IrExpr::Literal(IrLiteral::Int(0)));
    let elem = arena.push(IrExpr::FunctionCall {
        name: "_subscript".into(),
        args: vec![list, idx],
    });
    let id = arena.push(IrExpr::FunctionCall {
        name: "type".into(),
        args: vec![elem],
    });
    let expr = make_lowerer(&arena, &vm).lower(id).expect("lower");
    let s = format!("{expr}");
    assert!(
        s.contains("cypher_relationship_type"),
        "must dispatch graph metadata by runtime value: {s}"
    );
    assert!(
        s.contains("cypher_value_access"),
        "over the indexed element: {s}"
    );
}

#[test]
fn size_lowers_to_cypher_size_udf() {
    let mut arena = ExprArena::new();
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "var_1.rels");
    let list = arena.push(IrExpr::VarRef(VarId(0)));
    let id = arena.push(IrExpr::FunctionCall {
        name: "size".into(),
        args: vec![list],
    });
    let expr = make_lowerer(&arena, &vm).lower(id).expect("lower");
    assert!(format!("{expr}").contains("cypher_size"), "got {expr}");
}

// -----------------------------------------------------------------------
// Parameter test
// -----------------------------------------------------------------------

#[test]
fn parameter_produces_placeholder() {
    let mut arena = ExprArena::new();
    let id = arena.push(IrExpr::Parameter("eid".into()));
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    if let DfExpr::Placeholder(p) = result {
        // The `$` is reattached so DataFusion's named-param binding resolves.
        assert_eq!(p.id, "$eid");
    } else {
        panic!("expected Placeholder, got {result:?}");
    }
}

// -----------------------------------------------------------------------
// List literal tests (#714)
// -----------------------------------------------------------------------

#[test]
fn list_literal_of_ints_folds_to_scalar_list() {
    use datafusion::arrow::array::Array;
    // [1, 2, 3] → a single ScalarValue::List literal with 3 Int64 elements.
    let mut arena = ExprArena::new();
    let e1 = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let e2 = arena.push(IrExpr::Literal(IrLiteral::Int(2)));
    let e3 = arena.push(IrExpr::Literal(IrLiteral::Int(3)));
    let id = arena.push(IrExpr::ListLiteral(vec![e1, e2, e3]));
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    let DfExpr::Literal(ScalarValue::List(arr), _) = result else {
        panic!("expected a ScalarValue::List literal, got {result:?}");
    };
    // One list row holding 3 elements.
    assert_eq!(arr.len(), 1);
    assert_eq!(arr.value(0).len(), 3);
    assert_eq!(arr.value(0).data_type(), &DataType::Int64);
}

#[test]
fn empty_list_literal_folds_to_empty_int64_list() {
    use datafusion::arrow::array::Array;
    let mut arena = ExprArena::new();
    let id = arena.push(IrExpr::ListLiteral(vec![]));
    let vm = VarMap::new();
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    let DfExpr::Literal(ScalarValue::List(arr), _) = result else {
        panic!("expected an empty ScalarValue::List literal, got {result:?}");
    };
    assert_eq!(arr.len(), 1);
    assert_eq!(arr.value(0).len(), 0, "no elements");
}

#[test]
fn list_literal_with_expression_element_uses_make_array() {
    // [n.age, 1] has a non-constant element, so it lowers to make_array(...).
    let mut arena = ExprArena::new();
    let var = arena.push(IrExpr::VarRef(VarId(0)));
    let age = arena.push(IrExpr::PropertyAccess {
        base: var,
        prop: PropertyId::runtime(graphforge_value::RuntimePropId::new(0).unwrap()),
    });
    let one = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let id = arena.push(IrExpr::ListLiteral(vec![age, one]));
    let mut vm = VarMap::new();
    vm.insert(VarId(0), "var_0");
    let result = make_lowerer(&arena, &vm).lower(id).unwrap();
    let DfExpr::ScalarFunction(f) = result else {
        panic!("expected a make_array ScalarFunction, got {result:?}");
    };
    assert_eq!(f.name(), "make_array");
    assert_eq!(f.args.len(), 2);
}

// -----------------------------------------------------------------------
// scalar_to_ir_literal (#791)
// -----------------------------------------------------------------------

#[test]
fn temporal_truncate_lowering_covers_arity_default_literal_override_and_rejection_paths() {
    for name in [
        "date.truncate",
        "localtime.truncate",
        "localdatetime.truncate",
        "time.truncate",
        "datetime.truncate",
    ] {
        let mut missing = ExprArena::new();
        let call = missing.push(IrExpr::FunctionCall {
            name: name.into(),
            args: vec![],
        });
        assert!(matches!(
            make_lowerer(&missing, &VarMap::new()).lower(call),
            Err(LoweringError::UnknownFunction(function)) if function == name
        ));

        let mut defaults = ExprArena::new();
        let unit = defaults.push(IrExpr::Literal(IrLiteral::Str("day".into())));
        let value = defaults.push(IrExpr::Literal(IrLiteral::Null));
        let call = defaults.push(IrExpr::FunctionCall {
            name: name.into(),
            args: vec![unit, value],
        });
        let lowered = make_lowerer(&defaults, &VarMap::new()).lower(call).unwrap();
        assert!(format!("{lowered}").contains("truncate"));

        let mut overrides = ExprArena::new();
        let unit = overrides.push(IrExpr::Literal(IrLiteral::Str("day".into())));
        let value = overrides.push(IrExpr::Literal(IrLiteral::Null));
        let one = overrides.push(IrExpr::Literal(IrLiteral::Int(1)));
        let zone = overrides.push(IrExpr::Literal(IrLiteral::Str("UTC".into())));
        let map = overrides.push(IrExpr::MapLiteral(vec![
            ("year".into(), one),
            ("month".into(), one),
            ("day".into(), one),
            ("week".into(), one),
            ("dayOfWeek".into(), one),
            ("ordinalDay".into(), one),
            ("quarter".into(), one),
            ("dayOfQuarter".into(), one),
            ("hour".into(), one),
            ("minute".into(), one),
            ("second".into(), one),
            ("millisecond".into(), one),
            ("microsecond".into(), one),
            ("nanosecond".into(), one),
            ("timezone".into(), zone),
        ]));
        let call = overrides.push(IrExpr::FunctionCall {
            name: name.into(),
            args: vec![unit, value, map],
        });
        assert!(make_lowerer(&overrides, &VarMap::new()).lower(call).is_ok());

        let mut dynamic = ExprArena::new();
        let unit = dynamic.push(IrExpr::Literal(IrLiteral::Str("day".into())));
        let value = dynamic.push(IrExpr::Literal(IrLiteral::Null));
        let parameter = dynamic.push(IrExpr::Parameter("overrides".into()));
        let call = dynamic.push(IrExpr::FunctionCall {
            name: name.into(),
            args: vec![unit, value, parameter],
        });
        assert!(
            make_lowerer(&dynamic, &VarMap::new())
                .lower(call)
                .unwrap_err()
                .to_string()
                .contains("override map must be a literal map")
        );
    }

    for name in [
        "duration.between",
        "duration.inmonths",
        "duration.indays",
        "duration.inseconds",
    ] {
        let mut arena = ExprArena::new();
        let call = arena.push(IrExpr::FunctionCall {
            name: name.into(),
            args: vec![],
        });
        assert!(matches!(
            make_lowerer(&arena, &VarMap::new()).lower(call),
            Err(LoweringError::UnknownFunction(function)) if function == name
        ));

        let left = arena.push(IrExpr::Literal(IrLiteral::Null));
        let right = arena.push(IrExpr::Literal(IrLiteral::Null));
        let call = arena.push(IrExpr::FunctionCall {
            name: name.into(),
            args: vec![left, right],
        });
        assert!(make_lowerer(&arena, &VarMap::new()).lower(call).is_ok());
    }
}

#[test]
fn expression_lowering_error_and_static_access_matrix_reaches_contract_branches() {
    let lower_call = |name: &str, args: Vec<IrExpr>| {
        let mut arena = ExprArena::new();
        let args = args
            .into_iter()
            .map(|expr| arena.push(expr))
            .collect::<Vec<_>>();
        let call = arena.push(IrExpr::FunctionCall {
            name: name.into(),
            args,
        });
        make_lowerer(&arena, &VarMap::new()).lower(call)
    };

    for (name, expected) in [
        ("_subscript", "expects two arguments"),
        ("_node_struct", "expects at least one argument"),
        (
            "_node_struct_list",
            "expects two nodes and one relationship",
        ),
        ("_rel_struct", "expects an edge variable"),
        ("_rel_struct_list", "expects an edge variable"),
        ("keys", "expects one argument"),
        ("properties", "expects one argument"),
        ("labels", "expects one argument"),
    ] {
        assert!(
            lower_call(name, vec![])
                .unwrap_err()
                .to_string()
                .contains(expected),
            "{name}"
        );
    }
    for name in ["nodes", "relationships"] {
        assert!(
            lower_call(name, vec![])
                .unwrap_err()
                .to_string()
                .contains("expects one path argument")
        );
    }

    assert!(
        lower_call("_node_struct", vec![IrExpr::Literal(IrLiteral::Int(1))])
            .unwrap_err()
            .to_string()
            .contains("must be a node variable")
    );
    assert!(
        lower_call("_rel_struct", vec![IrExpr::Literal(IrLiteral::Int(1))])
            .unwrap_err()
            .to_string()
            .contains("must be a relationship variable")
    );
    assert!(
        lower_call(
            "_node_struct_list",
            vec![
                IrExpr::Literal(IrLiteral::Int(1)),
                IrExpr::Literal(IrLiteral::Int(2)),
                IrExpr::Literal(IrLiteral::Null),
            ],
        )
        .unwrap_err()
        .to_string()
        .contains("node arguments must be variables")
    );

    for name in ["keys", "properties"] {
        assert!(
            lower_call(name, vec![IrExpr::ListLiteral(vec![])],)
                .unwrap_err()
                .to_string()
                .contains("requires a map, node, relationship, or null")
        );
        assert!(lower_call(name, vec![IrExpr::Literal(IrLiteral::Null)]).is_ok());
        assert!(lower_call(name, vec![IrExpr::MapLiteral(vec![])],).is_ok());
    }
    assert!(lower_call("labels", vec![IrExpr::Literal(IrLiteral::Null)]).is_ok());

    let mut arena = ExprArena::new();
    let null = arena.push(IrExpr::Literal(IrLiteral::Null));
    let null_key = arena.push(IrExpr::Literal(IrLiteral::Null));
    let access = arena.push(IrExpr::FunctionCall {
        name: "_subscript".into(),
        args: vec![null, null_key],
    });
    let null_access = make_lowerer(&arena, &VarMap::new()).lower(access).unwrap();
    assert!(format!("{null_access}").contains("cypher_value_access"));

    let mut arena = ExprArena::new();
    let answer = arena.push(IrExpr::Literal(IrLiteral::Int(42)));
    let map = arena.push(IrExpr::MapLiteral(vec![("answer".into(), answer)]));
    let key = arena.push(IrExpr::Literal(IrLiteral::Str("answer".into())));
    let missing = arena.push(IrExpr::Literal(IrLiteral::Str("missing".into())));
    let found = arena.push(IrExpr::FunctionCall {
        name: "_subscript".into(),
        args: vec![map, key],
    });
    let absent = arena.push(IrExpr::FunctionCall {
        name: "_subscript".into(),
        args: vec![map, missing],
    });
    assert_eq!(
        format!(
            "{}",
            make_lowerer(&arena, &VarMap::new()).lower(found).unwrap()
        ),
        "Int64(42)"
    );
    assert!(matches!(
        make_lowerer(&arena, &VarMap::new()).lower(absent).unwrap(),
        DfExpr::Literal(ScalarValue::Null, _)
    ));

    let mut arena = ExprArena::new();
    let scalar = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let index = arena.push(IrExpr::Literal(IrLiteral::Int(0)));
    let invalid = arena.push(IrExpr::FunctionCall {
        name: "_subscript".into(),
        args: vec![scalar, index],
    });
    assert!(
        make_lowerer(&arena, &VarMap::new())
            .lower(invalid)
            .unwrap_err()
            .to_string()
            .contains("subscript requires a list")
    );

    for argument in [
        IrExpr::Literal(IrLiteral::Str("abc".into())),
        IrExpr::ListLiteral(vec![]),
        IrExpr::Parameter("value".into()),
    ] {
        assert!(lower_call("reverse", vec![argument]).is_ok());
    }
}

#[test]
fn static_nested_list_map_access_handles_negative_oob_and_nonliteral_indices() {
    let mut arena = ExprArena::new();
    let one = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let two = arena.push(IrExpr::Literal(IrLiteral::Int(2)));
    let first_map = arena.push(IrExpr::MapLiteral(vec![("value".into(), one)]));
    let second_map = arena.push(IrExpr::MapLiteral(vec![("value".into(), two)]));
    let list = arena.push(IrExpr::ListLiteral(vec![first_map, second_map]));
    let negative = arena.push(IrExpr::Literal(IrLiteral::Int(-1)));
    let oob = arena.push(IrExpr::Literal(IrLiteral::Int(9)));
    let dynamic = arena.push(IrExpr::Parameter("index".into()));
    let key = arena.push(IrExpr::Literal(IrLiteral::Str("value".into())));

    for (index, expected) in [(negative, Some("Int64(2)")), (oob, None)] {
        let indexed = arena.push(IrExpr::FunctionCall {
            name: "_subscript".into(),
            args: vec![list, index],
        });
        let field = arena.push(IrExpr::FunctionCall {
            name: "_subscript".into(),
            args: vec![indexed, key],
        });
        let lowered = make_lowerer(&arena, &VarMap::new()).lower(field).unwrap();
        match expected {
            Some(expected) => assert_eq!(format!("{lowered}"), expected),
            None => assert!(matches!(lowered, DfExpr::Literal(ScalarValue::Null, _))),
        }
    }

    let indexed = arena.push(IrExpr::FunctionCall {
        name: "_subscript".into(),
        args: vec![list, dynamic],
    });
    let field = arena.push(IrExpr::FunctionCall {
        name: "_subscript".into(),
        args: vec![indexed, key],
    });
    assert!(
        format!(
            "{}",
            make_lowerer(&arena, &VarMap::new()).lower(field).unwrap()
        )
        .contains("cypher_value_access")
    );
}

pub(super) fn large_list_scalar(values: Vec<i64>, valid: bool) -> ScalarValue {
    use datafusion::arrow::array::{ArrayRef, Int64Array, LargeListArray};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::Field;
    let len = i64::try_from(values.len()).unwrap();
    ScalarValue::LargeList(Arc::new(LargeListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, len])),
        Arc::new(Int64Array::from(values)) as ArrayRef,
        Some(NullBuffer::from(vec![valid])),
    )))
}

#[test]
fn malformed_heterogeneous_values_fail_property_admission() {
    use graphforge_value::heterogeneous::{self as het, Scalar};
    let original = het::encode_scalar([Some(Scalar::Int(7))]);
    let mut columns = original.columns().to_vec();
    columns[0] = Arc::new(datafusion::arrow::array::Int8Array::from(vec![99]));
    let value = ScalarValue::Struct(Arc::new(datafusion::arrow::array::StructArray::new(
        het::scalar_fields(),
        columns,
        None,
    )));
    let error = scalar_to_ir_literal(&value).unwrap_err();
    assert!(error.to_string().contains("GF_VALUE_TAG"), "{error}");
    let valid = ScalarValue::Struct(Arc::new(original));
    assert_eq!(scalar_to_ir_literal(&valid).unwrap(), IrLiteral::Int(7));
}

#[test]
fn exact_zero_uncorrelated_list_comprehension_preserves_null_and_empty_rows() {
    use datafusion::arrow::array::{Array, Int64Builder, ListArray, ListBuilder};
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    let mut builder = ListBuilder::new(Int64Builder::new());
    builder.append_null();
    builder.append(true);
    builder.values().append_value(1);
    builder.values().append_value(2);
    builder.append(true);
    let input = Arc::new(builder.finish()) as datafusion::arrow::array::ArrayRef;
    let udf = CypherListComp::new(None, None, "__gf_elem".into(), vec![]);
    let return_type = udf.return_type(&[input.data_type().clone()]).unwrap();
    let output = udf
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input)],
            arg_fields: vec![Arc::new(Field::new(
                "list",
                DataType::new_list(DataType::Int64, true),
                true,
            ))],
            number_rows: 3,
            return_field: Arc::new(Field::new("out", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
        .into_array(3)
        .unwrap();
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert!(output.is_null(0));
    assert_eq!(output.value_length(1), 0);
    assert_eq!(output.value_length(2), 2);
}
