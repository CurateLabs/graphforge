//! Public parse-bind-lower coverage for expression families that do not need a graph catalog.

use std::sync::{Arc, Mutex};

use graphforge_ir::{Binder, OntologyMode, RuntimeCatalog};

fn lower(query: &str) -> graphforge_rel::LogicalPlan {
    let ast = graphforge_cypher::parse(query)
        .unwrap_or_else(|error| panic!("parse failed for {query:?}: {error}"));
    let plan = Binder::new(
        None,
        Arc::new(Mutex::new(RuntimeCatalog::new())),
        OntologyMode::Exploratory,
    )
    .bind(&ast)
    .unwrap_or_else(|errors| panic!("bind failed for {query:?}: {errors:?}"));
    graphforge_rel::lower(&plan)
        .unwrap_or_else(|error| panic!("lower failed for {query:?}: {error}"))
}

fn assert_output_schema(query: &str, expected: &[&str]) {
    let plan = lower(query);
    let actual = plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "wrong lowered output schema for {query:?}"
    );
}

#[test]
fn lowers_scalar_collection_string_and_temporal_expression_matrix() {
    let cases = [
        "RETURN CASE WHEN true THEN 1 ELSE 2 END AS value",
        "RETURN CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END AS value",
        "RETURN coalesce(null, null, 'value') AS value",
        "RETURN null IS NULL AS value, 1 IS NOT NULL AS present",
        "RETURN 1 + 2 * 3 AS value, 7 % 4 AS remainder, 2 ^ 3 AS power",
        "RETURN -7 AS negative, abs(-7) AS magnitude, sign(-7) AS signum",
        "RETURN [1, 2, 3][0] AS first, [1, 2, 3][-1] AS last",
        "RETURN [1, 2, 3][1..] AS tail, [1, 2, 3][..2] AS head",
        "RETURN [1, 2] + [3, 4] AS joined, 0 + [1, 2] AS prepended",
        "RETURN [1, 2] + 3 AS appended, reverse([1, 2, 3]) AS reversed",
        "RETURN size([1, 2, 3]) AS list_size, head([1, 2]) AS first",
        "RETURN last([1, 2]) AS final, tail([1, 2, 3]) AS rest",
        "RETURN range(1, 5) AS ascending, range(5, 1, -2) AS descending",
        "RETURN [x IN [1, 2, 3] WHERE x > 1 | x * 2] AS mapped",
        "RETURN any(x IN [1, 2, 3] WHERE x = 2) AS any_match",
        "RETURN all(x IN [1, 2, 3] WHERE x > 0) AS all_match",
        "RETURN none(x IN [1, 2, 3] WHERE x < 0) AS no_match",
        "RETURN single(x IN [1, 2, 3] WHERE x = 2) AS one_match",
        "RETURN keys({name: 'Ada', age: 37}) AS names",
        "RETURN toString(42) AS text, toInteger('42') AS integer",
        "RETURN toFloat('4.25') AS decimal, toBoolean('true') AS boolean",
        "RETURN trim('  Ada  ') AS trim, ltrim('  Ada') AS left",
        "RETURN rtrim('Ada  ') AS right, reverse('Ada') AS reverse",
        "RETURN toLower('ADA') AS lower, toUpper('ada') AS upper",
        "RETURN substring('GraphForge', 5) AS suffix",
        "RETURN substring('GraphForge', 0, 5) AS prefix",
        "RETURN replace('a-b-c', '-', ':') AS replaced",
        "RETURN split('a,b,c', ',') AS pieces",
        "RETURN date('2024-02-29') AS day, datetime('2024-02-29T12:30:00Z') AS instant",
        "RETURN localdatetime('2024-02-29T12:30:00') AS local",
        "RETURN time('12:30:00Z') AS time, localtime('12:30:00') AS local_time",
        "RETURN duration('P1Y2M3DT4H5M6S') AS duration",
        "RETURN date('2024-02-29') + duration('P1D') AS tomorrow",
        "RETURN datetime('2024-02-29T12:30:00Z') - duration('PT30M') AS earlier",
    ];

    for query in cases {
        let expected = query
            .split(" AS ")
            .skip(1)
            .map(|suffix| {
                suffix
                    .split(|character: char| character == ',' || character.is_whitespace())
                    .next()
                    .expect("AS has an alias")
            })
            .collect::<Vec<_>>();
        assert_output_schema(query, &expected);
    }
}

#[test]
fn lowers_unwind_with_ordering_pagination_and_distinct() {
    let cases = [
        "UNWIND [3, 1, 2] AS x RETURN x AS value ORDER BY value",
        "UNWIND [1, 1, 2] AS x RETURN DISTINCT x AS value",
        "UNWIND range(1, 10) AS x WITH x WHERE x % 2 = 0 RETURN x AS value",
        "UNWIND range(1, 10) AS x RETURN x AS value SKIP 2 LIMIT 3",
        "UNWIND [[1, 2], [3, 4]] AS values UNWIND values AS value RETURN value",
    ];

    for query in cases {
        assert_output_schema(query, &["value"]);
    }
}
