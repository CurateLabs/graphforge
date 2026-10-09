//! Temporal equality on stored values (#1887 D11), temporal parameters
//! (#1887 D2) and the map forms of the temporal constructors (#1887 D14).

use std::collections::HashMap;

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::{GraphForge, IrLiteral};

/// Every column of every row, rendered; `None` for a null cell.
fn rows_with(
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

fn rows(gf: &GraphForge, query: &str) -> Vec<Vec<Option<String>>> {
    rows_with(gf, query, &HashMap::new())
}

fn strings(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_owned())).collect()
}

/// One stored property per temporal type, with an equal constructor literal
/// and a different one.
const TEMPORALS: [(&str, &str, &str); 6] = [
    ("d", "date('2012-01-01')", "date('2012-01-02')"),
    ("lt", "localtime('10:20:30.5')", "localtime('10:20:31')"),
    ("tm", "time('10:20:30+01:00')", "time('10:20:31+01:00')"),
    (
        "ldt",
        "localdatetime('2012-01-01T10:20:30')",
        "localdatetime('2012-01-01T10:20:31')",
    ),
    (
        "dt",
        "datetime('2012-01-01T10:20:30Z')",
        "datetime('2012-01-01T10:20:31Z')",
    ),
    ("du", "duration('P1DT2H')", "duration('P1DT3H')"),
];

fn stored_temporals() -> GraphForge {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let props = TEMPORALS
        .iter()
        .map(|(key, value, _)| format!("{key}: {value}"))
        .collect::<Vec<_>>()
        .join(", ");
    gf.execute(&format!(
        "CREATE (:E {{id: 1, {props}, zdt: datetime('2012-06-01T10:20:30[Europe/London]')}})"
    ))
    .expect("create temporals");
    gf
}

#[test]
fn equality_on_every_stored_temporal_type_agrees_with_tostring() {
    let gf = stored_temporals();
    for (key, same, other) in TEMPORALS {
        let query = format!(
            "MATCH (n:E) RETURN n.{key} = {same}, n.{key} <> {same}, n.{key} IN [{same}], \
             toString(n.{key}) = toString({same}), n.{key} = {other}, n.{key} <> {other}"
        );
        assert_eq!(
            rows(&gf, &query),
            vec![strings(&["true", "false", "true", "true", "false", "true"])],
            "{key}"
        );
        let filtered = format!("MATCH (n:E) WHERE n.{key} = {same} RETURN count(n)");
        assert_eq!(rows(&gf, &filtered), vec![strings(&["1"])], "{key}");
        let excluded = format!("MATCH (n:E) WHERE n.{key} <> {same} RETURN count(n)");
        assert_eq!(rows(&gf, &excluded), vec![strings(&["0"])], "{key}");
    }
}

#[test]
fn durable_stored_temporal_equality_after_reopen() {
    let root = tempfile::tempdir().expect("temporary root");
    let project = root.path().join("project");
    let props = TEMPORALS
        .iter()
        .map(|(key, value, _)| format!("{key}: {value}"))
        .collect::<Vec<_>>()
        .join(", ");
    let forge = GraphForge::new(project.to_str()).expect("durable project");
    forge
        .execute(&format!("CREATE (:E {{{props}}})"))
        .expect("create temporals");
    drop(forge);
    let reopened = GraphForge::new(project.to_str()).expect("reopen");
    for (key, same, other) in TEMPORALS {
        let query =
            format!("MATCH (n:E) RETURN n.{key} = {same}, n.{key} <> {same}, n.{key} = {other}");
        assert_eq!(
            rows(&reopened, &query),
            vec![strings(&["true", "false", "false"])],
            "{key}"
        );
        let filtered = format!("MATCH (n:E) WHERE n.{key} = {same} RETURN count(n)");
        assert_eq!(rows(&reopened, &filtered), vec![strings(&["1"])], "{key}");
    }
}

#[test]
fn equality_on_orderable_stored_temporals_agrees_with_ordering() {
    let gf = stored_temporals();
    for (key, same, other) in &TEMPORALS[..5] {
        let query = format!(
            "MATCH (n:E) RETURN n.{key} = {same}, n.{key} <= {same} AND n.{key} >= {same}, \
             n.{key} < {other}, n.{key} = {other}"
        );
        assert_eq!(
            rows(&gf, &query),
            vec![strings(&["true", "true", "true", "false"])],
            "{key}"
        );
    }
}

#[test]
fn named_zone_datetime_equality() {
    let gf = stored_temporals();
    assert_eq!(
        rows(
            &gf,
            "MATCH (n:E) RETURN n.zdt = datetime('2012-06-01T10:20:30[Europe/London]'), \
             n.zdt = datetime('2012-06-01T10:20:30+01:00'), \
             toString(n.zdt) = toString(datetime('2012-06-01T10:20:30+01:00'))"
        ),
        vec![strings(&["true", "false", "false"])]
    );
}

#[test]
fn stored_and_computed_datetimes_are_one_distinct_value() {
    let gf = stored_temporals();
    assert_eq!(
        rows(
            &gf,
            "MATCH (n:E) UNWIND [n.dt, datetime('2012-01-01T10:20:30Z')] AS v \
             RETURN count(DISTINCT v)"
        ),
        vec![strings(&["1"])]
    );
}

fn datetime_param() -> HashMap<String, IrLiteral> {
    // 2012-01-01T10:20:30.123456789Z
    HashMap::from([(
        "d".to_owned(),
        IrLiteral::ZonedDateTime {
            days: 15_340,
            nanos: (10 * 3_600 + 20 * 60 + 30) * 1_000_000_000 + 123_456_789,
            offset: 0,
            zone: None,
        },
    )])
}

const DATETIME_COMPONENTS: [&str; 19] = [
    "year",
    "quarter",
    "month",
    "week",
    "weekYear",
    "day",
    "ordinalDay",
    "weekDay",
    "dayOfQuarter",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
    "nanosecond",
    "timezone",
    "offsetSeconds",
    "epochSeconds",
    "epochMillis",
];

#[test]
fn every_datetime_parameter_component_matches_the_literal() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let literal = "datetime('2012-01-01T10:20:30.123456789Z')";
    for component in DATETIME_COMPONENTS {
        let from_param = rows_with(&gf, &format!("RETURN $d.{component}"), &datetime_param());
        let from_literal = rows(&gf, &format!("RETURN {literal}.{component}"));
        assert_eq!(from_param, from_literal, "{component}");
        assert!(from_param[0][0].is_some(), "{component} must not be null");
    }
    assert_eq!(
        rows_with(
            &gf,
            "RETURN $d.year, $d.month, $d.day, $d.hour, $d.minute, $d.second, $d.nanosecond",
            &datetime_param()
        ),
        vec![strings(&["2012", "1", "1", "10", "20", "30", "123456789"])]
    );
}

#[test]
fn datetime_parameter_arithmetic_with_durations() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    assert_eq!(
        rows_with(
            &gf,
            "RETURN toString($d + duration('P1D')), toString($d - duration({hours: 1})), \
             toString(duration({days: 1}) + $d), ($d + duration('P1M')).month",
            &datetime_param()
        ),
        vec![strings(&[
            "2012-01-02T10:20:30.123456789Z",
            "2012-01-01T09:20:30.123456789Z",
            "2012-01-02T10:20:30.123456789Z",
            "2",
        ])]
    );
    // Through a WITH alias and in a filter against stored values.
    gf.execute("CREATE (:E {t: datetime('2012-01-01T10:20:30.123456789Z')})")
        .expect("create");
    assert_eq!(
        rows_with(
            &gf,
            "WITH $d AS d MATCH (n:E) WHERE n.t = d AND n.t < d + duration('PT1S') \
             RETURN d.year, count(n)",
            &datetime_param()
        ),
        vec![strings(&["2012", "1"])]
    );
}

#[test]
fn other_temporal_parameters_match_their_literals() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let cases: [(IrLiteral, &str, &[&str]); 5] = [
        (
            IrLiteral::Date(15_340),
            "date('2012-01-01')",
            &["year", "month", "day", "week", "weekDay", "quarter"],
        ),
        (
            IrLiteral::LocalDateTime {
                days: 15_340,
                nanos: 37_230_000_000_000,
            },
            "localdatetime('2012-01-01T10:20:30')",
            &["year", "month", "day", "hour", "minute", "second"],
        ),
        (
            IrLiteral::Time(37_230_000_000_000),
            "localtime('10:20:30')",
            &["hour", "minute", "second", "nanosecond"],
        ),
        (
            IrLiteral::ZonedTime {
                nanos: 37_230_000_000_000,
                offset: 3_600,
            },
            "time('10:20:30+01:00')",
            &["hour", "minute", "second", "timezone", "offsetSeconds"],
        ),
        (
            IrLiteral::Duration {
                months: 14,
                days: 3,
                seconds: 3_661,
                nanos: 0,
            },
            "duration('P1Y2M3DT1H1M1S')",
            &["years", "months", "days", "hours", "minutes", "seconds"],
        ),
    ];
    for (value, literal, components) in cases {
        let params = HashMap::from([("v".to_owned(), value)]);
        for component in components {
            assert_eq!(
                rows_with(&gf, &format!("RETURN $v.{component}"), &params),
                rows(&gf, &format!("RETURN {literal}.{component}")),
                "{literal}.{component}"
            );
        }
        assert_eq!(
            rows_with(&gf, &format!("RETURN $v = {literal}"), &params),
            vec![strings(&["true"])],
            "{literal}"
        );
        if !literal.starts_with("duration") {
            assert_eq!(
                rows_with(
                    &gf,
                    &format!(
                        "RETURN toString($v + duration('PT1H')) = toString({literal} + duration('PT1H'))"
                    ),
                    &params
                ),
                vec![strings(&["true"])],
                "{literal}"
            );
        }
    }
}

#[test]
fn temporal_components_of_stored_properties() {
    let gf = stored_temporals();
    assert_eq!(
        rows(
            &gf,
            "MATCH (n:E) RETURN n.dt.year, n.dt.month, n.d.day, n.ldt.hour, n.tm.offsetSeconds, \
             n.du.hours, n.zdt.timezone"
        ),
        vec![strings(&[
            "2012",
            "1",
            "1",
            "10",
            "3600",
            "2",
            "Europe/London"
        ])]
    );
}

#[test]
fn datetime_epoch_map_forms() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    assert_eq!(
        rows(
            &gf,
            "RETURN toString(datetime({epochMillis: 1325412000000})), \
             toString(datetime({epochSeconds: 1325412000})), \
             toString(datetime({epochSeconds: 1325412000, nanosecond: 5})), \
             toString(datetime({epochMillis: 1325412000123, timezone: '+01:00'})), \
             toString(datetime({epochSeconds: 1338546030, timezone: 'Europe/London'})), \
             toString(datetime({epochMillis: -1}))"
        ),
        vec![strings(&[
            "2012-01-01T10:00Z",
            "2012-01-01T10:00Z",
            "2012-01-01T10:00:00.000000005Z",
            "2012-01-01T11:00:00.123+01:00",
            "2012-06-01T11:20:30+01:00[Europe/London]",
            "1969-12-31T23:59:59.999Z",
        ])]
    );
    // Runtime (non-literal) epoch values, as LDBC SNB stores them.
    gf.execute("CREATE (:Person {birthday: 1325412000000}), (:Person {birthday: 0})")
        .expect("create");
    assert_eq!(
        rows(
            &gf,
            "MATCH (p:Person) WITH datetime({epochMillis: p.birthday}) AS b \
             RETURN b.year, b.month, b.day, b.epochMillis ORDER BY b.year"
        ),
        vec![
            strings(&["1970", "1", "1", "0"]),
            strings(&["2012", "1", "1", "1325412000000"]),
        ]
    );
    assert_eq!(
        rows(
            &gf,
            "RETURN datetime({epochMillis: 1325412000000}) = datetime('2012-01-01T10:00Z')"
        ),
        vec![strings(&["true"])]
    );
    assert_eq!(
        rows(&gf, "RETURN datetime({epochMillis: null})"),
        vec![vec![None]]
    );
}

#[test]
fn temporal_constructor_maps_refuse_unknown_keys() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    for query in [
        "RETURN date({epochMillis: 1})",
        "RETURN localdatetime({epochSeconds: 1})",
        "RETURN localtime({year: 2012})",
        "RETURN time({day: 1})",
        "RETURN date({hour: 1})",
        "RETURN datetime({year: 2012, epochMillis: 1})",
        "RETURN datetime({epochMillis: 1, epochSeconds: 1})",
        "RETURN datetime({yaer: 2012})",
        "RETURN duration({day: 1})",
    ] {
        let error = gf
            .execute(query)
            .expect_err("an unknown constructor key must not be ignored");
        assert!(
            error.to_string().contains("invalid argument type"),
            "{query}: {error}"
        );
    }
    // Every documented key still constructs.
    assert_eq!(
        rows(
            &gf,
            "RETURN toString(date({year: 2012, quarter: 2, dayOfQuarter: 3})), \
             toString(localtime({hour: 1, minute: 2, second: 3, millisecond: 4, microsecond: 5, nanosecond: 6})), \
             toString(duration({years: 1, quarters: 1, months: 1, weeks: 1, days: 1, hours: 1, minutes: 1, seconds: 1, milliseconds: 1, microseconds: 1, nanoseconds: 1}))"
        ),
        vec![strings(&[
            "2012-04-03",
            "01:02:03.004005006",
            "P1Y4M8DT1H1M1.001001001S",
        ])]
    );
}

#[test]
fn constructors_with_only_a_timezone_read_the_current_value_in_that_zone() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    // One query captures one `now`, so the UTC forms equal the clock forms.
    assert_eq!(
        rows(
            &gf,
            "RETURN date({timezone: 'UTC'}) = date(), localtime({timezone: 'UTC'}) = localtime(), \
             localdatetime({timezone: 'UTC'}) = localdatetime(), time({timezone: 'UTC'}) = time(), \
             datetime({timezone: 'Z'}) = datetime()"
        ),
        vec![strings(&["true", "true", "true", "true", "true"])]
    );
    assert_eq!(
        rows(
            &gf,
            "RETURN datetime({timezone: '+01:00'}).epochMillis = datetime().epochMillis, \
             datetime({timezone: '+01:00'}).offsetSeconds, time({timezone: '+01:00'}).offsetSeconds, \
             datetime({timezone: 'Europe/London'}).timezone, \
             duration.inSeconds(localtime(), localtime({timezone: '+01:00'})).seconds IN [3600, -82800], \
             date({timezone: 'UTC'}).year >= 2026"
        ),
        vec![strings(&[
            "true",
            "3600",
            "3600",
            "Europe/London",
            "true",
            "true"
        ])]
    );
}

#[test]
fn equality_and_ordering_agree_for_one_instant_at_two_offsets() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    for (x, y) in [
        (
            "datetime('2012-01-01T10:00+01:00')",
            "datetime('2012-01-01T09:00Z')",
        ),
        ("time('10:00+01:00')", "time('09:00Z')"),
        (
            "datetime('2012-06-01T10:00:00[Europe/London]')",
            "datetime('2012-06-01T10:00:00+01:00')",
        ),
    ] {
        let query = format!(
            "WITH {x} AS x, {y} AS y RETURN x = y, x <> y, (x < y) <> (x > y), \
             (x <= y) = (x < y OR x = y), (x >= y) = (x > y OR x = y), \
             (y <= x) = (y < x OR y = x), (x < y) = (y > x)"
        );
        assert_eq!(
            rows(&gf, &query),
            vec![strings(&[
                "false", "true", "true", "true", "true", "true", "true"
            ])],
            "{x} vs {y}"
        );
        let same = format!("WITH {x} AS x, {x} AS y RETURN x = y, x <= y, x >= y, x < y, x > y");
        assert_eq!(
            rows(&gf, &same),
            vec![strings(&["true", "true", "true", "false", "false"])],
            "{x}"
        );
    }
}

#[test]
fn a_map_never_equals_a_temporal() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    assert_eq!(
        rows(
            &gf,
            "RETURN date('2012-01-01') = {epoch_day: 15340}, {epoch_day: 15340} = date('2012-01-01'), \
             {months: 1, days: 0, seconds: 0, nanos: 0} = duration('P1M'), \
             {epoch_day: 15340} = {epoch_day: 15340}, {epoch_day: 15340}.epoch_day, \
             date('2012-01-01') IN [{epoch_day: 15340}]"
        ),
        vec![strings(&[
            "false", "false", "false", "true", "15340", "false"
        ])]
    );
    assert_eq!(
        rows(
            &gf,
            "UNWIND [15340, 15341] AS d WITH {epoch_day: d} AS m \
             RETURN m = date('2012-01-01'), m.epoch_day ORDER BY m.epoch_day"
        ),
        vec![strings(&["false", "15340"]), strings(&["false", "15341"])]
    );
    let params = HashMap::from([(
        "m".to_owned(),
        IrLiteral::Map(vec![("epoch_day".to_owned(), IrLiteral::Int(15_340))]),
    )]);
    assert_eq!(
        rows_with(&gf, "RETURN $m = date('2012-01-01'), $m.epoch_day", &params),
        vec![strings(&["false", "15340"])]
    );
}

#[test]
fn map_parameter_keys_are_readable() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    let params = HashMap::from([(
        "m".to_owned(),
        IrLiteral::Map(vec![
            ("k".to_owned(), IrLiteral::Int(7)),
            ("s".to_owned(), IrLiteral::Str("x".to_owned())),
        ]),
    )]);
    assert_eq!(
        rows_with(
            &gf,
            "WITH $m AS w RETURN $m.k, $m.s, $m['k'], w.k, $m = {k: 7, s: 'x'}",
            &params
        ),
        vec![strings(&["7", "x", "7", "7", "true"])]
    );
}
