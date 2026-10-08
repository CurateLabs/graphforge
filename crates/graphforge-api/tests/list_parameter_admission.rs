//! List query parameters whose elements differ in Arrow type (#1887 D10): a
//! correct result where a list encoding holds them, a typed error where none
//! does, and never a panic.

use std::collections::HashMap;

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::{GraphForge, IrLiteral};

fn rows(
    gf: &GraphForge,
    query: &str,
    params: &HashMap<String, IrLiteral>,
) -> Vec<Vec<Option<String>>> {
    let result = gf
        .execute_with_params(query, params)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let mut rows = Vec::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        (!array.is_null(row))
                            .then(|| array_value_to_string(array, row).expect("render"))
                    })
                    .collect(),
            );
        }
    }
    rows
}

fn s(value: &str) -> IrLiteral {
    IrLiteral::Str(value.to_owned())
}

fn map(entries: &[(&str, IrLiteral)]) -> IrLiteral {
    IrLiteral::Map(
        entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), value.clone()))
            .collect(),
    )
}

fn param(name: &str, value: IrLiteral) -> HashMap<String, IrLiteral> {
    HashMap::from([(name.to_owned(), value)])
}

fn mixed_null_rows() -> HashMap<String, IrLiteral> {
    param(
        "rows",
        IrLiteral::List(vec![
            map(&[("a", IrLiteral::Null), ("b", s("x"))]),
            map(&[("a", s("y")), ("b", IrLiteral::Null)]),
            map(&[("a", s("z"))]),
        ]),
    )
}

#[test]
fn list_of_maps_mixing_null_and_string_values_reads_back() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    assert_eq!(
        rows(
            &gf,
            "UNWIND $rows AS row RETURN row.a, row.b",
            &mixed_null_rows()
        ),
        vec![
            vec![None, Some("x".to_owned())],
            vec![Some("y".to_owned()), None],
            vec![Some("z".to_owned()), None],
        ]
    );
}

#[test]
fn list_of_maps_mixing_null_and_string_values_creates_nodes() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute_with_params(
        "UNWIND $rows AS row CREATE (:Q {a: row.a, b: row.b})",
        &mixed_null_rows(),
    )
    .expect("create from mixed rows");
    assert_eq!(
        rows(
            &gf,
            "MATCH (q:Q) RETURN q.a, q.b ORDER BY q.a",
            &HashMap::new()
        ),
        vec![
            vec![Some("y".to_owned()), None],
            vec![Some("z".to_owned()), None],
            vec![None, Some("x".to_owned())],
        ]
    );
}

#[test]
fn heterogeneous_scalar_and_map_lists_match_their_inline_literals() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let scalars = param(
        "xs",
        IrLiteral::List(vec![IrLiteral::Int(1), s("a"), IrLiteral::Null]),
    );
    assert_eq!(
        rows(
            &gf,
            "RETURN 1 IN $xs, 'a' IN $xs, size($xs), $xs[1], 2 IN $xs",
            &scalars
        ),
        rows(
            &gf,
            "RETURN 1 IN [1, 'a', null], 'a' IN [1, 'a', null], size([1, 'a', null]), [1, 'a', null][1], 2 IN [1, 'a', null]",
            &HashMap::new()
        )
    );
    let conflicting = param(
        "xs",
        IrLiteral::List(vec![
            map(&[("k", IrLiteral::Int(1))]),
            map(&[("k", s("one"))]),
        ]),
    );
    // Maps whose shared key holds different types use the tagged encoding.
    assert_eq!(
        rows(
            &gf,
            "UNWIND $xs AS x RETURN count(x), size($xs)",
            &conflicting
        ),
        vec![vec![Some("2".to_owned()), Some("2".to_owned())]]
    );
}

#[test]
fn unrepresentable_list_parameter_is_a_typed_error() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let params = param(
        "xs",
        IrLiteral::List(vec![IrLiteral::Int(1), IrLiteral::Date(3)]),
    );
    for query in ["RETURN $xs", "UNWIND $xs AS x RETURN x"] {
        let error = gf
            .execute_with_params(query, &params)
            .expect_err("a number and a date cannot share a list value");
        assert!(
            error.to_string().contains("parameter $xs"),
            "{query}: {error}"
        );
    }
    let stream = gf.execute_stream_with_params("RETURN $xs", &params);
    assert!(stream.is_err(), "the streaming path admits parameters too");
}
