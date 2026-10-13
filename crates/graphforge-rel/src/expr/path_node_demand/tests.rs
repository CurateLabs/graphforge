use super::*;
use crate::expr::PathNodeHydration;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use graphforge_ir::expr::{BinaryOpKind, CaseArm, IrLiteral};
use graphforge_ir::{LoweringSnapshot, QuantifierKind};
use graphforge_value::{EntityTypeId, RuntimePropId};
use std::sync::Arc;

const LOOP: VarId = VarId(7);
const OTHER: VarId = VarId(8);

fn prop(n: u32) -> PropertyId {
    PropertyId::runtime(RuntimePropId::new(n).unwrap())
}

fn name_of(prop: PropertyId) -> String {
    [
        (self::prop(0), "id"),
        (self::prop(1), "name"),
        (self::prop(2), "labels"),
    ]
    .into_iter()
    .find_map(|(p, name)| (p == prop).then(|| name.to_owned()))
    .expect("test property")
}

fn demand_of(arena: &ExprArena, clauses: &[ExprId]) -> PathNodeDemand {
    PathNodeDemand::of_clauses(arena, LOOP, clauses, &name_of)
}

fn observed(labels: bool, names: &[&str]) -> PathNodeDemand {
    PathNodeDemand::Observed {
        labels,
        properties: names.iter().map(|n| (*n).to_owned()).collect(),
    }
}

fn var(arena: &mut ExprArena, var: VarId) -> ExprId {
    arena.push(IrExpr::VarRef(var))
}

fn access(arena: &mut ExprArena, var: VarId, p: u32) -> ExprId {
    let base = self::var(arena, var);
    arena.push(IrExpr::PropertyAccess {
        base,
        prop: prop(p),
    })
}

fn call(arena: &mut ExprArena, name: &str, args: Vec<ExprId>) -> ExprId {
    arena.push(IrExpr::FunctionCall {
        name: name.to_owned(),
        args,
    })
}

fn add(arena: &mut ExprArena, left: ExprId, right: ExprId) -> ExprId {
    arena.push(IrExpr::BinaryOp {
        op: BinaryOpKind::Add,
        left,
        right,
    })
}

#[test]
fn no_clause_reads_demand_nothing_beyond_identity() {
    let mut arena = ExprArena::new();
    let literal = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
    let other = access(&mut arena, OTHER, 0);
    assert_eq!(demand_of(&arena, &[]), observed(false, &[]));
    assert_eq!(demand_of(&arena, &[literal, other]), observed(false, &[]));
}

#[test]
fn property_reads_name_exactly_the_columns_read() {
    let mut arena = ExprArena::new();
    let id = access(&mut arena, LOOP, 0);
    let name = access(&mut arena, LOOP, 1);
    let sum = add(&mut arena, id, name);
    assert_eq!(demand_of(&arena, &[id]), observed(false, &["id"]));
    assert_eq!(demand_of(&arena, &[sum]), observed(false, &["id", "name"]));
}

#[test]
fn labels_function_and_labels_field_demand_the_label_list() {
    let mut arena = ExprArena::new();
    let n = var(&mut arena, LOOP);
    let labels_call = call(&mut arena, "labels", vec![n]);
    let size = call(&mut arena, "size", vec![labels_call]);
    let field = access(&mut arena, LOOP, 2);
    assert_eq!(demand_of(&arena, &[labels_call]), observed(true, &[]));
    assert_eq!(demand_of(&arena, &[size]), observed(true, &[]));
    // A stored property named `labels` is shadowed by the label field, so the
    // read demands the field and no stored column.
    assert_eq!(demand_of(&arena, &[field]), observed(true, &[]));
}

#[test]
fn labels_of_another_variable_is_not_a_demand() {
    let mut arena = ExprArena::new();
    let other = var(&mut arena, OTHER);
    let labels_call = call(&mut arena, "labels", vec![other]);
    assert_eq!(demand_of(&arena, &[labels_call]), observed(false, &[]));
}

#[test]
fn a_bare_loop_variable_demands_the_whole_node() {
    let mut arena = ExprArena::new();
    let n = var(&mut arena, LOOP);
    let id = access(&mut arena, LOOP, 0);
    let properties = call(&mut arena, "properties", vec![n]);
    let coalesced = call(&mut arena, "coalesce", vec![n, id]);
    let labels_call = call(&mut arena, "labels", vec![n, id]);
    let map = arena.push(IrExpr::MapLiteral(vec![("node".to_owned(), n)]));
    let list = arena.push(IrExpr::ListLiteral(vec![id, n]));
    for whole in [n, properties, coalesced, labels_call, map, list] {
        assert_eq!(
            demand_of(&arena, &[id, whole]),
            PathNodeDemand::Whole,
            "{:?}",
            arena.get(whole)
        );
    }
}

#[test]
fn reads_nested_in_every_expression_form_are_found() {
    let mut arena = ExprArena::new();
    let id = access(&mut arena, LOOP, 0);
    let name = access(&mut arena, LOOP, 1);
    let n = var(&mut arena, LOOP);
    let labels_call = call(&mut arena, "labels", vec![n]);
    let negated = arena.push(IrExpr::UnaryOp {
        op: graphforge_ir::UnaryOpKind::Not,
        expr: id,
    });
    let case = arena.push(IrExpr::Case {
        operand: Some(negated),
        arms: vec![CaseArm {
            when: name,
            then: labels_call,
        }],
        else_expr: None,
    });
    let map = arena.push(IrExpr::MapLiteral(vec![("k".to_owned(), case)]));
    let list = arena.push(IrExpr::ListLiteral(vec![map]));
    // The inner comprehension iterates something derived from the loop
    // variable and projects its own loop variable.
    let inner_list = access(&mut arena, LOOP, 1);
    let inner_var = var(&mut arena, OTHER);
    let comprehension = arena.push(IrExpr::ListComprehension {
        loop_var: OTHER,
        list: inner_list,
        filter: None,
        projection: Some(inner_var),
    });
    let quantifier = arena.push(IrExpr::Quantifier {
        kind: QuantifierKind::Any,
        loop_var: OTHER,
        list: comprehension,
        predicate: list,
    });
    assert_eq!(
        demand_of(&arena, &[quantifier]),
        observed(true, &["id", "name"])
    );
}

#[test]
fn a_shared_subexpression_is_walked_once_and_still_counted() {
    let mut arena = ExprArena::new();
    let id = access(&mut arena, LOOP, 0);
    let twice = add(&mut arena, id, id);
    let again = add(&mut arena, twice, twice);
    assert_eq!(demand_of(&arena, &[again, id]), observed(false, &["id"]));
}

fn snapshot() -> LoweringSnapshot {
    let schema = |columns: &[&str]| {
        Arc::new(Schema::new(
            std::iter::once(Field::new("node_uuid", DataType::FixedSizeBinary(16), false))
                .chain(
                    columns
                        .iter()
                        .map(|c| Field::new(*c, DataType::Utf8, false)),
                )
                .collect::<Vec<_>>(),
        ))
    };
    LoweringSnapshot {
        node_property_stems: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
        node_properties: [
            ("a".to_owned(), schema(&["id", "name"])),
            ("b".to_owned(), schema(&["id", "age"])),
            ("c".to_owned(), schema(&["weight"])),
        ]
        .into(),
        ..LoweringSnapshot::default()
    }
}

fn labels() -> Vec<(EntityTypeId, String)> {
    vec![(EntityTypeId::decode(0).unwrap(), "A".to_owned())]
}

fn hydration(demand: PathNodeDemand) -> PathNodeHydration {
    PathNodeHydration::build(&snapshot(), labels(), demand).unwrap()
}

fn field_names(hydration: &PathNodeHydration) -> Vec<&str> {
    hydration
        .fields
        .iter()
        .map(|f| f.name().as_str())
        .collect()
}

#[test]
fn whole_hydration_reads_every_stem_and_every_column() {
    let whole = hydration(PathNodeDemand::Whole);
    assert_eq!(
        field_names(&whole),
        ["node_uuid", "labels", "id", "name", "age", "weight"]
    );
    assert_eq!(whole.prop_stems, ["a", "b", "c"]);
    assert_eq!(whole.labels_by_type, labels());
    assert_eq!(whole.property_fields().len(), 4);
}

#[test]
fn narrowed_hydration_keeps_only_the_demanded_columns_and_stems() {
    let age = hydration(observed(false, &["age"]));
    assert_eq!(field_names(&age), ["node_uuid", "age"]);
    assert_eq!(age.prop_stems, ["b"]);
    assert!(age.labels_by_type.is_empty(), "labels are not scanned");
    assert!(age.fields.iter().all(|f| f.name() != "labels"));

    // `id` is stored under both `a` and `b`: both stems stay readable so the
    // values coalesce exactly as they do for whole hydration.
    let id = hydration(observed(false, &["id"]));
    assert_eq!(field_names(&id), ["node_uuid", "id"]);
    assert_eq!(id.prop_stems, ["a", "b"]);

    let both = hydration(observed(false, &["name", "weight"]));
    assert_eq!(field_names(&both), ["node_uuid", "name", "weight"]);
    assert_eq!(both.prop_stems, ["a", "c"]);
}

#[test]
fn narrowed_hydration_reads_labels_without_properties_when_only_labels_are_observed() {
    let only_labels = hydration(observed(true, &[]));
    assert_eq!(field_names(&only_labels), ["node_uuid", "labels"]);
    assert!(only_labels.prop_stems.is_empty());
    assert_eq!(only_labels.labels_by_type, labels());
    assert!(only_labels.property_fields().is_empty());
}

#[test]
fn a_property_no_stem_stores_reads_nothing() {
    let none = hydration(observed(false, &["nosuch"]));
    assert_eq!(field_names(&none), ["node_uuid"]);
    assert!(none.prop_stems.is_empty());
    assert!(none.property_fields().is_empty());
}

#[test]
fn narrowed_columns_are_nullable_like_whole_columns() {
    let narrowed = hydration(observed(false, &["id", "age"]));
    assert!(
        narrowed
            .property_fields()
            .iter()
            .all(|f| f.is_nullable() && f.data_type() == &DataType::Utf8)
    );
}

#[test]
fn hydration_rejects_a_stem_without_a_schema() {
    let mut broken = snapshot();
    broken.node_properties.remove("b");
    let err = PathNodeHydration::build(&broken, labels(), observed(false, &["age"]))
        .expect_err("stem b has no schema");
    assert!(err.to_string().contains("stem b"), "{err}");
}
