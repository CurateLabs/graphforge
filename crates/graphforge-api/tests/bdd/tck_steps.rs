//! cucumber-rs step definitions for the openCypher TCK feature files
//! (`tests/tck/features/`, vendored at tag 2024.3 in #874).
//!
//! Scope (Milestone 17, #597): the first tier — basic scalar `RETURN`. This is
//! enough to un-skip `Literals1` (boolean / null literals). Step definitions are
//! shared with `api_steps` through cucumber's global step registry; the phrasings
//! here are openCypher-TCK-specific and do not collide with the GraphForge-API
//! steps. Tiers are added (and `@skip-rust` removed feature-by-feature) as more
//! step vocabulary and result-value types are supported (#598–#601).

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int8Array, Int32Array, Int64Array,
    ListArray, StringArray, StructArray, Time64NanosecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use cucumber::gherkin::Step;
use cucumber::{given, then, when};

use crate::GraphForgeWorld;

// ---------------------------------------------------------------------------
// GIVEN
// ---------------------------------------------------------------------------

/// `Given any graph` — the scenario does not depend on graph contents, so an
/// empty in-memory forge suffices.
#[given("any graph")]
async fn given_any_graph(world: &mut GraphForgeWorld) {
    crate::fixture::replace_with_fresh(&mut world.forge);
    world.nodes.clear();
    world.last_error = None;
    world.last_names = None;
    world.last_count = None;
    world.last_explanation = None;
    world.last_exec = None;
    world.params.clear();
}

/// Build a fresh forge and seed it with `create` (a single-clause `CREATE` so the
/// relationship patterns reference node variables bound in the same clause —
/// cross-clause visibility is a separate feature). Shared by the named-graph fixtures.
fn seed_graph(world: &mut GraphForgeWorld, create: &str) {
    crate::fixture::replace_with_fresh(&mut world.forge);
    let forge = world
        .forge
        .take()
        .expect("fixture replacement must install a forge");
    forge
        .execute(create)
        .unwrap_or_else(|e| panic!("named-graph fixture setup failed: {e}"));
    world.forge = Some(forge);
    world.nodes.clear();
    world.last_error = None;
    world.last_names = None;
    world.last_count = None;
    world.last_explanation = None;
    world.last_exec = None;
    world.params.clear();
}

/// `And parameters are:` followed by a two-column table of `$name` bindings.
#[given("parameters are:")]
async fn given_parameters_are(world: &mut GraphForgeWorld, step: &Step) {
    let table = step
        .table
        .as_ref()
        .expect("`parameters are:` requires a data table");
    world.params.clear();
    for row in &table.rows {
        assert_eq!(row.len(), 2, "parameter rows must be name/value pairs");
        world.params.insert(
            row[0].trim().to_owned(),
            parse_tck_param_literal(row[1].trim()),
        );
    }
}

/// Register the deterministic procedure table supplied by the openCypher TCK.
#[given(regex = r"^there exists a procedure (.+)$")]
async fn given_procedure(world: &mut GraphForgeWorld, signature: String, step: &Step) {
    let signature = signature.trim_end_matches(':').trim();
    let (left, outputs) = signature
        .split_once(") :: (")
        .unwrap_or_else(|| panic!("invalid procedure signature `{signature}`"));
    let open = left
        .find('(')
        .unwrap_or_else(|| panic!("invalid procedure signature `{signature}`"));
    let name = left[..open].trim().to_owned();
    let inputs = parse_procedure_fields(&left[open + 1..]);
    let outputs = parse_procedure_fields(outputs.trim_end_matches(')'));
    let width = inputs.len() + outputs.len();

    let rows = if width == 0 {
        vec![vec![]]
    } else {
        let table = step
            .table
            .as_ref()
            .expect("procedure fixture requires a data table");
        table
            .rows
            .iter()
            .skip(1)
            .map(|row| {
                assert_eq!(row.len(), width, "procedure fixture row width");
                row.iter()
                    .map(|value| parse_tck_param_literal(value.trim()))
                    .collect()
            })
            .collect()
    };

    world
        .forge
        .as_ref()
        .expect("graph fixture must initialize forge before procedure")
        .register_procedure(graphforge_api::ProcedureDefinition {
            name,
            inputs,
            outputs,
            rows,
        })
        .expect("valid TCK procedure fixture");
}

fn parse_procedure_fields(raw: &str) -> Vec<graphforge_api::ProcedureField> {
    let raw = raw.trim();
    if raw.is_empty() {
        return vec![];
    }
    raw.split(',')
        .map(|field| {
            let (name, type_name) = field
                .trim()
                .split_once("::")
                .unwrap_or_else(|| panic!("invalid procedure field `{field}`"));
            let type_name = type_name.trim();
            graphforge_api::ProcedureField {
                name: name.trim().to_owned(),
                type_name: type_name.trim_end_matches('?').to_owned(),
                nullable: type_name.ends_with('?'),
            }
        })
        .collect()
}

/// The openCypher TCK `binary-tree-1` graph (vendored definition): a root `:A`,
/// four `:X` b-nodes (KNOWS/FOLLOWS from the root) and eight `:X` c-nodes
/// (FRIEND from the b-nodes), plus a FRIEND ring across the b-nodes.
#[given("the binary-tree-1 graph")]
async fn given_binary_tree_1(world: &mut GraphForgeWorld) {
    seed_graph(
        world,
        "CREATE (a:A {name: 'a'}), \
                (b1:X {name: 'b1'}), (b2:X {name: 'b2'}), (b3:X {name: 'b3'}), (b4:X {name: 'b4'}), \
                (c11:X {name: 'c11'}), (c12:X {name: 'c12'}), (c21:X {name: 'c21'}), (c22:X {name: 'c22'}), \
                (c31:X {name: 'c31'}), (c32:X {name: 'c32'}), (c41:X {name: 'c41'}), (c42:X {name: 'c42'}), \
                (a)-[:KNOWS]->(b1), (a)-[:KNOWS]->(b2), (a)-[:FOLLOWS]->(b3), (a)-[:FOLLOWS]->(b4), \
                (b1)-[:FRIEND]->(c11), (b1)-[:FRIEND]->(c12), (b2)-[:FRIEND]->(c21), (b2)-[:FRIEND]->(c22), \
                (b3)-[:FRIEND]->(c31), (b3)-[:FRIEND]->(c32), (b4)-[:FRIEND]->(c41), (b4)-[:FRIEND]->(c42), \
                (b1)-[:FRIEND]->(b2), (b2)-[:FRIEND]->(b3), (b3)-[:FRIEND]->(b4), (b4)-[:FRIEND]->(b1)",
    );
}

/// The openCypher TCK `binary-tree-2` graph — identical to `binary-tree-1` except
/// the second c-node under each b-node (`c12`/`c22`/`c32`/`c42`) is labeled `:Y`.
#[given("the binary-tree-2 graph")]
async fn given_binary_tree_2(world: &mut GraphForgeWorld) {
    seed_graph(
        world,
        "CREATE (a:A {name: 'a'}), \
                (b1:X {name: 'b1'}), (b2:X {name: 'b2'}), (b3:X {name: 'b3'}), (b4:X {name: 'b4'}), \
                (c11:X {name: 'c11'}), (c12:Y {name: 'c12'}), (c21:X {name: 'c21'}), (c22:Y {name: 'c22'}), \
                (c31:X {name: 'c31'}), (c32:Y {name: 'c32'}), (c41:X {name: 'c41'}), (c42:Y {name: 'c42'}), \
                (a)-[:KNOWS]->(b1), (a)-[:KNOWS]->(b2), (a)-[:FOLLOWS]->(b3), (a)-[:FOLLOWS]->(b4), \
                (b1)-[:FRIEND]->(c11), (b1)-[:FRIEND]->(c12), (b2)-[:FRIEND]->(c21), (b2)-[:FRIEND]->(c22), \
                (b3)-[:FRIEND]->(c31), (b3)-[:FRIEND]->(c32), (b4)-[:FRIEND]->(c41), (b4)-[:FRIEND]->(c42), \
                (b1)-[:FRIEND]->(b2), (b2)-[:FRIEND]->(b3), (b3)-[:FRIEND]->(b4), (b4)-[:FRIEND]->(b1)",
    );
}

/// `And having executed:` followed by a `"""…"""` setup query (typically a
/// `CREATE`). Runs it against the forge and asserts it succeeded — the graph
/// state it builds is what the scenario's main query then reads. (`Given an
/// empty graph` is provided by `api_steps`; reused here to avoid a duplicate.)
#[given("having executed:")]
async fn given_having_executed(world: &mut GraphForgeWorld, step: &Step) {
    let query = step
        .docstring
        .as_deref()
        .expect("`And having executed:` requires a doc-string query")
        .trim();
    let forge = world
        .forge
        .as_ref()
        .expect("a forge must exist (set up by a Given step)");
    forge
        .execute(query)
        .unwrap_or_else(|e| panic!("setup `having executed` failed for {query:?}: {e}"));
}

// ---------------------------------------------------------------------------
// WHEN
// ---------------------------------------------------------------------------

/// Execute the step's doc-string query against the world's forge, capturing the
/// result or error. Shared by `executing query:` and `executing control query:`.
fn run_docstring_query(world: &mut GraphForgeWorld, step: &Step, label: &str) {
    let query = step
        .docstring
        .as_deref()
        .unwrap_or_else(|| panic!("`When {label}:` requires a doc-string query"))
        .trim();
    let forge = world
        .forge
        .as_ref()
        .expect("a forge must exist (set up by a Given step)");
    let result = if world.params.is_empty() {
        forge.execute(query)
    } else {
        forge.execute_with_params(query, &world.params)
    };
    match result {
        Ok(result) => {
            world.last_exec = Some(result);
            world.last_error = None;
        }
        Err(e) => {
            world.last_error = Some(e.to_string());
            world.last_exec = None;
        }
    }
}

/// `When executing query:` followed by a `"""…"""` doc-string holding the query.
#[when("executing query:")]
async fn when_executing_query(world: &mut GraphForgeWorld, step: &Step) {
    run_docstring_query(world, step, "executing query");
}

/// `When executing control query:` — a second query run after the main one to
/// observe graph state (e.g. verifying a write's side effects). Same execution
/// path as `executing query:`; it overwrites the captured result/error.
#[when("executing control query:")]
async fn when_executing_control_query(world: &mut GraphForgeWorld, step: &Step) {
    run_docstring_query(world, step, "executing control query");
}

// ---------------------------------------------------------------------------
// THEN
// ---------------------------------------------------------------------------

/// The column names a write **side-effect summary** batch carries (`GraphCreate`
/// / `GraphDelete` / `GraphSet` / `GraphRemove` and the statement driver). Used to
/// tell a bare write's summary batch apart from a real write-result `RETURN`.
const SUMMARY_COLS: [&str; 7] = [
    "nodes_created",
    "edges_created",
    "nodes_deleted",
    "edges_deleted",
    "properties_set",
    "properties_removed",
    "labels_added",
];

/// Whether `exec` is a bare write whose `batches` are the side-effect summary
/// (every column is a counter name) rather than a `RETURN` projection. A write
/// that DOES project rows (`... RETURN n.x`) carries real columns and is treated
/// as a normal result.
fn is_write_summary(exec: &graphforge_api::ExecutionResult) -> bool {
    exec.side_effects.is_some()
        && exec.batches.first().is_none_or(|b| {
            b.schema()
                .fields()
                .iter()
                .all(|f| SUMMARY_COLS.contains(&f.name().as_str()))
        })
}

/// Render the actual result rows as cells, one row per result row, projecting the
/// columns named in `header` (in header order). Panics with a clear message if the
/// query errored or a named column is absent. Shared by the result-table steps.
fn actual_result_rows(world: &GraphForgeWorld, header: &[String]) -> Vec<Vec<String>> {
    if let Some(err) = &world.last_error {
        panic!("expected a result table but the query errored: {err}");
    }
    let exec = world
        .last_exec
        .as_ref()
        .expect("a query result (from `When executing query:`)");
    // A bare write's `batches` carry the side-effect summary, not result rows, so
    // it has zero result rows. (A write that projects a RETURN carries real
    // columns and falls through to normal rendering.)
    if is_write_summary(exec) {
        return Vec::new();
    }
    let mut actual: Vec<Vec<String>> = Vec::new();
    for batch in &exec.batches {
        for row in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(header.len());
            for col in header {
                let array: &dyn Array = batch
                    .column_by_name(col)
                    .unwrap_or_else(|| panic!("result is missing expected column `{col}`"));
                cells.push(render_cell(array, row));
            }
            actual.push(cells);
        }
    }
    actual
}

/// `Then the result should be, in any order:` with an expected result table.
/// Order-insensitive multiset comparison of rendered cells.
#[then("the result should be, in any order:")]
async fn then_result_in_any_order(world: &mut GraphForgeWorld, step: &Step) {
    let table = step
        .table
        .as_ref()
        .expect("`the result should be` requires a data table");
    // First row is the header (column names); the rest are expected data rows.
    // Both sides are canonicalized (structural compare) before the multiset check.
    let header = &table.rows[0];
    let mut expected: Vec<Vec<String>> = canon_expected_rows(&table.rows[1..]);
    let mut actual = canon_rows(&actual_result_rows(world, header));

    expected.sort();
    actual.sort();
    assert_eq!(
        actual, expected,
        "result rows did not match (compared in any order)\n  expected: {expected:?}\n  actual:   {actual:?}"
    );
}

/// `Then the result should be, in order:` with an expected result table.
/// Order-SENSITIVE comparison — rows must match positionally (the query carries an
/// `ORDER BY`, so the produced order is the assertion).
#[then("the result should be, in order:")]
async fn then_result_in_order(world: &mut GraphForgeWorld, step: &Step) {
    let table = step
        .table
        .as_ref()
        .expect("`the result should be, in order:` requires a data table");
    let header = &table.rows[0];
    let expected: Vec<Vec<String>> = canon_expected_rows(&table.rows[1..]);
    let actual = canon_rows(&actual_result_rows(world, header));

    assert_eq!(
        actual, expected,
        "result rows did not match (order-sensitive)\n  expected: {expected:?}\n  actual:   {actual:?}"
    );
}

/// `Then the result should be (ignoring element order for lists):` — like
/// `in any order`, but list-valued cells compare as multisets (element order
/// within a list is ignored), per the openCypher result step.
#[then("the result should be (ignoring element order for lists):")]
async fn then_result_any_order_lists(world: &mut GraphForgeWorld, step: &Step) {
    let table = step
        .table
        .as_ref()
        .expect("`the result should be (ignoring element order for lists):` requires a data table");
    let header = &table.rows[0];
    let mut expected: Vec<Vec<String>> = canon_expected_rows_sorted(&table.rows[1..]);
    let mut actual = canon_rows_sorted(&actual_result_rows(world, header));

    expected.sort();
    actual.sort();
    assert_eq!(
        actual, expected,
        "result rows did not match (any row order, ignoring list element order)\n  expected: {expected:?}\n  actual:   {actual:?}"
    );
}

/// `Then the result should be, in order (ignoring element order for lists):` —
/// row order is significant (the query has `ORDER BY`), but list-valued cells
/// compare as multisets.
#[then("the result should be, in order (ignoring element order for lists):")]
async fn then_result_in_order_lists(world: &mut GraphForgeWorld, step: &Step) {
    let table = step
        .table
        .as_ref()
        .expect("`the result should be, in order (ignoring element order for lists):` requires a data table");
    let header = &table.rows[0];
    let expected: Vec<Vec<String>> = canon_expected_rows_sorted(&table.rows[1..]);
    let actual = canon_rows_sorted(&actual_result_rows(world, header));

    assert_eq!(
        actual, expected,
        "result rows did not match (in order, ignoring list element order)\n  expected: {expected:?}\n  actual:   {actual:?}"
    );
}

/// `Then the result should be empty` — the query produced zero rows.
#[then("the result should be empty")]
async fn then_result_empty(world: &mut GraphForgeWorld) {
    if let Some(err) = &world.last_error {
        panic!("expected an empty result but the query errored: {err}");
    }
    let exec = world
        .last_exec
        .as_ref()
        .expect("a query result (from `When executing query:`)");
    // A bare write's `batches` are the side-effect summary, not result rows.
    if is_write_summary(exec) {
        return;
    }
    let rows: usize = exec.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "expected an empty result but got {rows} row(s)");
}

/// `And no side effects` — the scenario asserts the query mutated nothing.
///
/// The current tier (#597) only un-skips read-only scalar-`RETURN` scenarios,
/// which have no side effects by construction. A real side-effect ledger lands
/// with the write-path tier (#601); until then this confirms the query produced
/// a result rather than failing.
#[then("no side effects")]
async fn then_no_side_effects(world: &mut GraphForgeWorld) {
    assert!(
        world.last_error.is_none(),
        "expected no side effects but the query errored: {:?}",
        world.last_error
    );
    assert!(
        world.last_exec.is_some(),
        "expected a completed query before asserting side effects"
    );
}

/// `Then the side effects should be:` — assert the openCypher write-effect ledger
/// (a table of `| +nodes | N |` rows; an unlisted counter is 0).
///
/// Compared against [`ExecutionResult::side_effects`]. `+labels`/`-labels` are not
/// computed yet (always 0), so a scenario asserting a non-zero label count fails
/// here rather than passing — conservative, no false pass.
#[then("the side effects should be:")]
async fn then_side_effects(world: &mut GraphForgeWorld, step: &Step) {
    if let Some(err) = &world.last_error {
        panic!("expected side effects but the query errored: {err}");
    }
    let exec = world
        .last_exec
        .as_ref()
        .expect("a completed write query before asserting side effects");
    let se = exec.side_effects.clone().unwrap_or_default();
    // The engine's actual ledger, keyed by the openCypher side-effect names.
    let actual: [(&str, u64); 8] = [
        ("+nodes", se.nodes_created),
        ("-nodes", se.nodes_deleted),
        ("+relationships", se.relationships_created),
        ("-relationships", se.relationships_deleted),
        ("+properties", se.properties_set),
        ("-properties", se.properties_removed),
        ("+labels", se.labels_added),
        ("-labels", se.labels_removed),
    ];
    // Expected ledger: every counter the table lists; the rest are 0.
    let table = step
        .table
        .as_ref()
        .expect("`the side effects should be:` requires a data table");
    let mut expected: std::collections::HashMap<&str, u64> =
        actual.iter().map(|(k, _)| (*k, 0u64)).collect();
    for row in &table.rows {
        let key = row[0].trim();
        let val: u64 = row[1]
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("non-numeric side-effect count in {row:?}"));
        let slot = expected
            .get_mut(key)
            .unwrap_or_else(|| panic!("unknown side-effect key `{key}`"));
        *slot = val;
    }
    for (key, got) in actual {
        let want = expected[key];
        assert_eq!(got, want, "side effect `{key}`: expected {want}, got {got}");
    }
}

/// `Then a <Error> should be raised at <compile time|runtime|any time>: <detail>`
/// — a negative scenario asserting the query was rejected.
///
/// GraphForge does not classify Cypher error *categories* (the corpus's
/// `SyntaxError` / `TypeError` / `ArgumentError` / …) — its [`GfError`] records
/// the pipeline *phase* instead. So we assert the phase, which is verifiable from
/// the error's `Display`:
/// - **compile time** → a `parse error` (lexer/grammar), a `bind error` (the
///   binder, span-rich #606), or a `plan error` (later planning): openCypher's
///   compile-time `SyntaxError` deliberately spans both true syntax errors AND
///   bind-time semantic ones (undefined/duplicate variable, type conflict),
///   which GraphForge surfaces as `Parse`, `Bind`, and `Plan` respectively.
/// - **runtime** → an `execution error` / `storage error`.
/// - **any time** → any genuine error.
///
/// CRUCIALLY, the error must be GraphForge **deliberately validating** the query
/// — not GraphForge failing because it cannot yet *run* the construct. The latter
/// (an unsupported feature, an unknown built-in, a DataFusion planning/schema
/// failure) happens to produce an error too, but counting it would be a false
/// conformance pass: the scenario would "pass" for the wrong reason. So a
/// capability-gap error is rejected even though it is technically an error. As
/// GraphForge implements those features, the real validation (or success) takes
/// over and the scenario flips to a genuine pass/fail. The error *detail* after
/// the `:` is not asserted (GraphForge does not emit openCypher's sub-codes).
///
/// [`GfError`]: graphforge_api::GfError
#[then(regex = r"^a (\w+) should be raised at (compile time|runtime|any time)(?::.*)?$")]
async fn then_error_raised(world: &mut GraphForgeWorld, error_type: String, phase: String) {
    let err = world.last_error.as_deref().unwrap_or_else(|| {
        panic!("expected a {error_type} to be raised at {phase}, but the query produced a result")
    });
    // A capability gap — GraphForge can't yet RUN the construct — is not a genuine
    // rejection of an invalid query. Recognise the phrasings GraphForge uses for
    // "I don't support this" and refuse to count them (conservative: a few genuine
    // rejections phrased this way are under-counted, but no false pass slips in).
    const CAPABILITY_GAP: [&str; 9] = [
        "not implemented",
        "not yet",
        "unsupported",
        "only supported",
        "unknown built-in function",
        "Schema error",          // DataFusion field/schema resolution failure
        "Error during planning", // DataFusion internal planning failure
        "type_coercion",         // DataFusion coercion failure (not a checked Cypher type error)
        "panicked",              // a caught panic is a bug, never a genuine rejection
    ];
    let deliberate_range_error = err.contains("range ")
        && (err.contains("must be an integer") || err.contains("must not be zero"));
    assert!(
        deliberate_range_error || !CAPABILITY_GAP.iter().any(|g| err.contains(g)),
        "query failed because GraphForge cannot run the construct, not because it \
         validated an invalid {error_type}: {err}"
    );
    let is_compile = err.starts_with("parse error")
        || err.starts_with("bind error")
        || err.starts_with("plan error");
    let is_runtime = err.starts_with("execution error") || err.starts_with("storage error");
    match phase.as_str() {
        "compile time" => assert!(
            is_compile,
            "expected a compile-time {error_type} (parse/plan), got: {err}"
        ),
        "runtime" => assert!(
            is_runtime,
            "expected a runtime {error_type} (execution/storage), got: {err}"
        ),
        // "any time": any genuine (non-unimplemented) error suffices.
        _ => {}
    }
}

/// Render an `f64` in the openCypher TCK canonical float form.
///
/// The TCK writes floats the way JavaScript's `Number.toString` does (verified
/// against every row of `Literals5.feature`): **decimal expansion** for
/// `1e-6 <= |v| < 1e21` — always with a fractional part, so a whole value gets a
/// trailing `.0` (`1.0`, `1000000000.0`, `0.00001`) — and **scientific notation**
/// outside that range, with a lowercase `e` and no `+` on a positive exponent
/// (`1e308`, `1.2635418652381264e305`, `1e-305`). Rust's `Display` gives the exact
/// decimal expansion (it never uses an exponent) and `LowerExp` (`{:e}`) gives the
/// shortest scientific mantissa with a lowercase `e` and no `+` — so each branch
/// matches openCypher directly. Special cases: `±0.0` → `0.0`, and the non-finite
/// values openCypher spells `Infinity`/`-Infinity`/`NaN`.
///
/// (Note: openCypher's reference TCK compares floats *numerically*; matching the
/// canonical string here is the equivalent within this harness's string compare.
/// A structural value comparator is the broader follow-up — see #27 report.)
fn render_float(v: f64) -> String {
    if v == 0.0 {
        // Covers both +0.0 and -0.0 (which compare equal); the TCK expects `0.0`.
        "0.0".to_string()
    } else if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else {
        let a = v.abs();
        if !(1e-6..1e21).contains(&a) {
            // Out of the decimal range → scientific (e.g. `1e308`, `1e-305`).
            format!("{v:e}")
        } else {
            // Decimal expansion; force a fractional part for a whole value.
            let s = format!("{v}");
            if s.contains('.') { s } else { format!("{s}.0") }
        }
    }
}

// ---------------------------------------------------------------------------
// Structural value comparison (#942 slice 2)
// ---------------------------------------------------------------------------

/// A parsed openCypher result value. Result cells are compared *structurally*
/// (via [`canon_cell`]) rather than by raw string, so map/node property order, a
/// label-less node's spacing, and absent (null) node properties don't cause a
/// spurious mismatch. Distinct kinds never canonicalize alike (`5` ≠ `5.0` ≠
/// `'5'`), so this only accepts genuine openCypher equivalences — never a false pass.
#[derive(Debug, Clone, PartialEq)]
enum Val {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    List(Vec<Val>),
    Map(Vec<(String, Val)>),
    Node {
        labels: Vec<String>,
        props: Vec<(String, Val)>,
    },
    Rel {
        rtype: Option<String>,
        props: Vec<(String, Val)>,
    },
}

fn parse_tck_param_literal(raw: &str) -> graphforge_api::IrLiteral {
    let value = ValParser::new(raw)
        .parse_all()
        .unwrap_or_else(|| panic!("unsupported TCK parameter literal `{raw}`"));
    val_to_ir_literal(value)
}

fn val_to_ir_literal(value: Val) -> graphforge_api::IrLiteral {
    match value {
        Val::Null => graphforge_api::IrLiteral::Null,
        Val::Bool(v) => graphforge_api::IrLiteral::Bool(v),
        Val::Int(v) => graphforge_api::IrLiteral::Int(v),
        Val::Float(v) => graphforge_api::IrLiteral::Float(v),
        Val::Str(v) => graphforge_api::IrLiteral::Str(v),
        Val::List(items) => {
            graphforge_api::IrLiteral::List(items.into_iter().map(val_to_ir_literal).collect())
        }
        Val::Map(entries) => graphforge_api::IrLiteral::Map(
            entries
                .into_iter()
                .map(|(key, value)| (key, val_to_ir_literal(value)))
                .collect(),
        ),
        Val::Node { .. } | Val::Rel { .. } => {
            panic!("node/relationship parameter literals are not supported")
        }
    }
}

/// Recursive-descent parser for the openCypher TCK value-literal grammar.
struct ValParser {
    chars: Vec<char>,
    pos: usize,
}

impl ValParser {
    fn new(s: &str) -> Self {
        Self {
            chars: s.chars().collect(),
            pos: 0,
        }
    }
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }
    fn bump(&mut self) -> Option<char> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }
    fn ws(&mut self) {
        while matches!(self.peek(), Some(c) if c.is_whitespace()) {
            self.pos += 1;
        }
    }
    fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// Parse the whole string as one value; `None` if it does not fully parse
    /// (the caller then falls back to a raw-string comparison).
    fn parse_all(mut self) -> Option<Val> {
        self.ws();
        let v = self.value()?;
        self.ws();
        (self.pos == self.chars.len()).then_some(v)
    }

    fn value(&mut self) -> Option<Val> {
        self.ws();
        match self.peek()? {
            '(' => self.node(),
            '{' => self.map().map(Val::Map),
            '[' => self.list_or_rel(),
            '\'' => self.string().map(Val::Str),
            _ => self.scalar(),
        }
    }

    fn node(&mut self) -> Option<Val> {
        self.eat('(').then_some(())?;
        self.ws();
        let mut labels = Vec::new();
        while self.eat(':') {
            labels.push(self.ident()?);
            self.ws();
        }
        let props = if self.peek() == Some('{') {
            self.map()?
        } else {
            Vec::new()
        };
        self.ws();
        self.eat(')').then_some(())?;
        Some(Val::Node { labels, props })
    }

    fn list_or_rel(&mut self) -> Option<Val> {
        self.eat('[').then_some(())?;
        self.ws();
        // A relationship literal starts `[:TYPE …]`; anything else is a list.
        if self.peek() == Some(':') {
            self.eat(':');
            let rtype = Some(self.ident()?);
            self.ws();
            let props = if self.peek() == Some('{') {
                self.map()?
            } else {
                Vec::new()
            };
            self.ws();
            self.eat(']').then_some(())?;
            return Some(Val::Rel { rtype, props });
        }
        let mut items = Vec::new();
        if self.eat(']') {
            return Some(Val::List(items));
        }
        loop {
            items.push(self.value()?);
            self.ws();
            if self.eat(',') {
                continue;
            }
            if self.eat(']') {
                return Some(Val::List(items));
            }
            return None;
        }
    }

    fn map(&mut self) -> Option<Vec<(String, Val)>> {
        self.eat('{').then_some(())?;
        self.ws();
        let mut entries = Vec::new();
        if self.eat('}') {
            return Some(entries);
        }
        loop {
            self.ws();
            let key = self.key()?;
            self.ws();
            self.eat(':').then_some(())?;
            entries.push((key, self.value()?));
            self.ws();
            if self.eat(',') {
                continue;
            }
            if self.eat('}') {
                return Some(entries);
            }
            return None;
        }
    }

    fn key(&mut self) -> Option<String> {
        if self.eat('`') {
            let mut s = String::new();
            while let Some(c) = self.bump() {
                if c == '`' {
                    return Some(s);
                }
                s.push(c);
            }
            None
        } else {
            self.ident()
        }
    }

    fn ident(&mut self) -> Option<String> {
        let mut s = String::new();
        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '_' {
                s.push(c);
                self.pos += 1;
            } else {
                break;
            }
        }
        (!s.is_empty()).then_some(s)
    }

    fn string(&mut self) -> Option<String> {
        self.eat('\'').then_some(())?;
        let mut s = String::new();
        while let Some(c) = self.bump() {
            if c == '\\' {
                // Inverse of `escape_string`: decode the openCypher escapes so the
                // expected literal and the actual rendering parse to the SAME value.
                match self.bump()? {
                    'n' => s.push('\n'),
                    't' => s.push('\t'),
                    'r' => s.push('\r'),
                    other => s.push(other), // `\\`→`\`, `\'`→`'`, `\"`→`"`, …
                }
            } else if c == '\'' {
                return Some(s);
            } else {
                s.push(c);
            }
        }
        None
    }

    fn scalar(&mut self) -> Option<Val> {
        let mut tok = String::new();
        while let Some(c) = self.peek() {
            if c.is_whitespace() || matches!(c, ',' | ']' | '}' | ')' | ':') {
                break;
            }
            tok.push(c);
            self.pos += 1;
        }
        match tok.as_str() {
            "" => None,
            "null" => Some(Val::Null),
            "true" => Some(Val::Bool(true)),
            "false" => Some(Val::Bool(false)),
            "NaN" => Some(Val::Float(f64::NAN)),
            "Infinity" => Some(Val::Float(f64::INFINITY)),
            "-Infinity" => Some(Val::Float(f64::NEG_INFINITY)),
            t if t.contains('.') || t.contains('e') || t.contains('E') => {
                t.parse::<f64>().ok().map(Val::Float)
            }
            t => t.parse::<i64>().ok().map(Val::Int),
        }
    }
}

/// Render a value in canonical form: map/node keys sorted, node null-properties
/// dropped (openCypher nodes never carry a null property), scalars via the same
/// renderers used elsewhere. Maps KEEP null values (a map may legitimately hold one).
fn canonical(v: &Val, sort_lists: bool) -> String {
    match v {
        Val::Null => "null".to_string(),
        Val::Bool(b) => b.to_string(),
        Val::Int(i) => i.to_string(),
        Val::Float(f) => render_float(*f),
        Val::Str(s) => format!("'{s}'"),
        Val::List(items) => {
            // `sort_lists` implements the `(ignoring element order for lists)`
            // result step: list elements (recursively) are sorted by canonical
            // form so two lists with the same multiset compare equal.
            let mut parts: Vec<String> = items.iter().map(|i| canonical(i, sort_lists)).collect();
            if sort_lists {
                parts.sort();
            }
            format!("[{}]", parts.join(", "))
        }
        Val::Map(entries) => format!("{{{}}}", canonical_entries(entries, false, sort_lists)),
        Val::Node { labels, props } => {
            let mut labs = labels.clone();
            labs.sort();
            let labels_str: String = labs.iter().map(|l| format!(":{l}")).collect();
            let body = canonical_entries(props, true, sort_lists);
            if body.is_empty() {
                format!("({labels_str})")
            } else if labels_str.is_empty() {
                format!("({{{body}}})")
            } else {
                format!("({labels_str} {{{body}}})")
            }
        }
        Val::Rel { rtype, props } => {
            let t = rtype.as_ref().map(|t| format!(":{t}")).unwrap_or_default();
            let body = canonical_entries(props, false, sort_lists);
            if body.is_empty() {
                format!("[{t}]")
            } else if t.is_empty() {
                format!("[{{{body}}}]")
            } else {
                format!("[{t} {{{body}}}]")
            }
        }
    }
}

/// Render map/node entries: sort by key; when `drop_null` (nodes) omit
/// null-valued properties — openCypher nodes never carry a null property.
fn canonical_entries(entries: &[(String, Val)], drop_null: bool, sort_lists: bool) -> String {
    let mut kept: Vec<&(String, Val)> = entries
        .iter()
        .filter(|entry| !(drop_null && entry.1 == Val::Null))
        .collect();
    kept.sort_by(|a, b| a.0.cmp(&b.0));
    kept.iter()
        .map(|(k, v)| format!("{k}: {}", canonical(v, sort_lists)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Canonicalize one rendered/expected cell for structural comparison. On parse
/// failure return the raw string — applied symmetrically to both sides, so an
/// identical (already-passing) cell can never change its verdict.
fn canon_cell(s: &str) -> String {
    match ValParser::new(s).parse_all() {
        Some(v) => canonical(&v, false),
        None => s.to_string(),
    }
}

/// Like [`canon_cell`] but canonicalizes every list (recursively) order-insensitively
/// — the `(ignoring element order for lists)` result step. Applied symmetrically to
/// both sides, so a cell with no list is identical to [`canon_cell`].
fn canon_cell_sorted(s: &str) -> String {
    match ValParser::new(s).parse_all() {
        Some(v) => canonical(&v, true),
        None => s.to_string(),
    }
}

/// Escape a string value into the openCypher single-quoted form: `\` → `\\`,
/// `'` → `\'`, and the control characters the TCK renders as escapes
/// (newline → `\n`, tab → `\t`, CR → `\r`). Other characters (including UTF-8) are
/// emitted verbatim — the TCK renders e.g. `'🧐'` unescaped. [`ValParser::string`]
/// performs the inverse, so a value round-trips to the same [`Val::Str`] on both
/// the expected and actual side.
fn escape_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out
}

/// Canonicalize every cell of a result-row table.
fn canon_rows(rows: &[Vec<String>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|c| canon_cell(c)).collect())
        .collect()
}

fn canon_expected_rows(rows: &[Vec<String>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| {
            r.iter()
                .map(|c| canon_cell(&unescape_gherkin_table_cell(c)))
                .collect()
        })
        .collect()
}

/// Canonicalize every cell order-insensitively for lists — the
/// `(ignoring element order for lists)` result steps.
fn canon_rows_sorted(rows: &[Vec<String>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|c| canon_cell_sorted(c)).collect())
        .collect()
}

fn canon_expected_rows_sorted(rows: &[Vec<String>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| {
            r.iter()
                .map(|c| canon_cell_sorted(&unescape_gherkin_table_cell(c)))
                .collect()
        })
        .collect()
}

// gherkin 0.14 stores table cells as trimmed raw text; decode only the table-cell
// escapes that the TCK uses before feeding expected Cypher literals to canon_cell.
fn unescape_gherkin_table_cell(cell: &str) -> String {
    let mut out = String::with_capacity(cell.len());
    let mut chars = cell.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => out.push('\\'),
                Some('|') => out.push('|'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Render one Arrow cell to the openCypher TCK string form, scoped to the value
/// types the current tier needs (boolean, null). Extend as tiers are un-skipped.
fn render_cell(array: &dyn Array, row: usize) -> String {
    // A `null` literal lowers to an all-null column of Arrow `Null` type, for
    // which `is_null(row)` is not reliable — match the type explicitly. Also
    // covers a null cell within an otherwise-typed column.
    if *array.data_type() == DataType::Null || array.is_null(row) {
        return "null".to_string();
    }
    match array.data_type() {
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Boolean array");
            if a.value(row) {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 array");
            a.value(row).to_string()
        }
        DataType::UInt64 => {
            let a = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("UInt64 array");
            a.value(row).to_string()
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64 array");
            render_float(a.value(row))
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 array");
            format!("'{}'", escape_string(a.value(row)))
        }
        // A `date` value (ADR 0009/0012): `Struct{epoch_day: Int64}` i64
        // days-since-epoch → quoted ISO date (signed for the expanded-year range).
        DataType::Struct(fields)
            if fields.len() == 1
                && fields[0].name() == "epoch_day"
                && *fields[0].data_type() == DataType::Int64 =>
        {
            let s = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Struct array");
            let days = s
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("epoch_day field")
                .value(row);
            format!("'{}'", graphforge_rel::temporal::format_date(days))
        }
        // A `localtime` value (ADR 0009): `Time64(Nanosecond)` nanos-of-day →
        // quoted canonical time, with render precision derived from the value.
        DataType::Time64(TimeUnit::Nanosecond) => {
            let a = array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .expect("Time64 array");
            format!(
                "'{}'",
                graphforge_rel::temporal::render_localtime_nanos(a.value(row))
            )
        }
        // A `duration` value (ADR 0009 / #1011): `Struct{months, days, seconds,
        // nanos}` all Int64 → quoted canonical `P…` designator string.
        DataType::Struct(fields)
            if fields.len() == 4
                && fields[0].name() == "months"
                && fields[1].name() == "days"
                && fields[2].name() == "seconds"
                && fields[3].name() == "nanos" =>
        {
            let s = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("duration struct array");
            let col = |idx: usize| {
                s.column(idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("duration child Int64")
                    .value(row)
            };
            format!(
                "'{}'",
                graphforge_rel::temporal::render_duration_value(
                    &graphforge_rel::temporal::DurationValue {
                        months: col(0),
                        days: col(1),
                        seconds: col(2),
                        nanos: col(3),
                    }
                )
            )
        }
        // A heterogeneous ("tagged") flat-scalar list element (ADR 0010, #943):
        // `Struct{__het_key, __het_tag, __het_int, __het_float, __het_str,
        // __het_bool}` — render the live field by tag (0=int, 1=float, 2=string,
        // 3=bool) so a mixed list keeps per-element types (`[1, 'a', 2.0]` →
        // `1`, `'a'`, `2.0`; `max([1, 2.0, 5])` → `5`).
        DataType::Struct(fields) if fields.iter().any(|f| f.name() == "__het_tag") => {
            let s = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Struct array");
            let col = |name: &str| s.column_by_name(name).expect("tagged field").as_ref();
            let tag = col("__het_tag")
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("__het_tag is Int8")
                .value(row);
            if let Some(value) = s.column_by_name(&format!("__het_value_{tag}")) {
                return render_cell(value.as_ref(), row);
            }
            match tag {
                0 => col("__het_int")
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("__het_int is Int64")
                    .value(row)
                    .to_string(),
                1 => render_float(
                    col("__het_float")
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("__het_float is Float64")
                        .value(row),
                ),
                2 => format!(
                    "'{}'",
                    escape_string(
                        col("__het_str")
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("__het_str is Utf8")
                            .value(row)
                    )
                ),
                3 => col("__het_bool")
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("__het_bool is Boolean")
                    .value(row)
                    .to_string(),
                // 4 = nested list (ADR 0011): recurse into the tagged children.
                4 => {
                    let l = col("__het_list")
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .expect("__het_list is List");
                    let elems = l.value(row);
                    let parts: Vec<String> = (0..elems.len())
                        .map(|i| render_cell(elems.as_ref(), i))
                        .collect();
                    format!("[{}]", parts.join(", "))
                }
                // 5 = map (ADR 0011 slice 2, #1005): render `{key: value, …}` from
                // the `__het_map` entry list, keys sorted, values recursed by tag.
                _ => {
                    let l = col("__het_map")
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .expect("__het_map is List");
                    let entries = l.value(row);
                    let es = entries
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .expect("map entries struct");
                    let mkeys = es
                        .column_by_name("__het_mkey")
                        .expect("__het_mkey")
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("__het_mkey is Utf8");
                    let mvals = es.column_by_name("__het_mval").expect("__het_mval");
                    let mut kv: Vec<(String, String)> = (0..es.len())
                        .map(|i| (mkeys.value(i).to_string(), render_cell(mvals.as_ref(), i)))
                        .collect();
                    kv.sort();
                    let body: Vec<String> =
                        kv.into_iter().map(|(k, v)| format!("{k}: {v}")).collect();
                    format!("{{{}}}", body.join(", "))
                }
            }
        }
        // A `localdatetime` value (ADR 0009): `Struct{date: Date32, time:
        // Time64(ns)}` → quoted canonical `YYYY-MM-DDTHH:MM[:SS[.fff…]]`.
        DataType::Struct(fields)
            if fields.len() == 2
                && fields[0].name() == "date"
                && *fields[0].data_type() == DataType::Int64
                && fields[1].name() == "time"
                && *fields[1].data_type() == DataType::Time64(TimeUnit::Nanosecond) =>
        {
            let s = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Struct array");
            let days = s
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("date field")
                .value(row);
            let nanos = s
                .column(1)
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .expect("time field")
                .value(row);
            format!(
                "'{}'",
                graphforge_rel::temporal::render_localdatetime(days, nanos)
            )
        }
        // A `time` value (ADR 0009): `Struct{time: Time64(ns), offset: Int32}`
        // → quoted canonical `HH:MM[:SS[.fff…]]±HH:MM` (`Z` for UTC).
        DataType::Struct(fields)
            if fields.len() == 2
                && fields[0].name() == "time"
                && *fields[0].data_type() == DataType::Time64(TimeUnit::Nanosecond)
                && fields[1].name() == "offset"
                && *fields[1].data_type() == DataType::Int32 =>
        {
            let s = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Struct array");
            let nanos = s
                .column(0)
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .expect("time field")
                .value(row);
            let offset = s
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("offset field")
                .value(row);
            format!(
                "'{}'",
                graphforge_rel::temporal::render_time_value(nanos, offset)
            )
        }
        // A `datetime` value (ADR 0009): `Struct{date: Date32, time: Time64(ns),
        // offset: Int32, zone: Utf8?}` → `YYYY-MM-DDTHH:MM…±HH:MM[Zone]`.
        DataType::Struct(fields)
            if fields.len() == 4
                && fields[0].name() == "date"
                && fields[1].name() == "time"
                && fields[2].name() == "offset"
                && fields[3].name() == "zone" =>
        {
            let s = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Struct array");
            let days = s
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("date field")
                .value(row);
            let nanos = s
                .column(1)
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .expect("time field")
                .value(row);
            let offset = s
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("offset field")
                .value(row);
            let zone_arr = s
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("zone field");
            let zone = (!zone_arr.is_null(row) && !zone_arr.value(row).is_empty())
                .then(|| zone_arr.value(row));
            format!(
                "'{}'",
                graphforge_rel::temporal::render_datetime_value(days, nanos, offset, zone)
            )
        }
        // A path value (#754/#806): `Struct{nodes, relationships}`.
        data_type if is_path_struct(data_type) => {
            let s = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Struct array");
            render_path_struct(s, row)
        }
        // A whole node value (#785/#889): `Struct{node_uuid, labels, <props>}`.
        DataType::Struct(fields) if fields.iter().any(|f| f.name() == "labels") => {
            let s = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Struct array");
            render_node_struct(s, row)
        }
        // A whole relationship value (#1023): the var-len edge-list struct
        // `Struct{edge_uuid, src_uuid, dst_uuid, rel_type, <props…>}`.
        DataType::Struct(fields)
            if fields.iter().any(|f| f.name() == "edge_uuid")
                && fields.iter().any(|f| f.name() == "rel_type") =>
        {
            let s = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Struct array");
            render_relationship_struct(s, row)
        }
        // A heterogeneous property scalar persisted as the executor's tagged
        // value representation. Decode before rendering so each node property
        // retains its original openCypher type.
        DataType::Struct(fields) if fields.iter().any(|f| f.name() == "__het_tag") => {
            let value = datafusion::scalar::ScalarValue::try_from_array(
                &arrow::array::make_array(array.to_data()),
                row,
            )
            .expect("tagged property scalar");
            let decoded = graphforge_rel::expr::decode_het_scalar(&value)
                .expect("valid tagged property scalar");
            let decoded = decoded.to_array().expect("decoded property scalar array");
            render_cell(decoded.as_ref(), 0)
        }
        // A map value (`{k: v, …}`, #600): a `Struct` with NO `labels` field.
        DataType::Struct(_) => {
            let s = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Struct array");
            render_map_struct(s, row)
        }
        // A list value (`collect(...)`, `nodes(p)`): render each element.
        DataType::List(_) => {
            let l = array
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("List array");
            let elems = l.value(row);
            let parts: Vec<String> = (0..elems.len())
                .map(|i| render_cell(elems.as_ref(), i))
                .collect();
            format!("[{}]", parts.join(", "))
        }
        other => panic!(
            "TCK result rendering is not implemented for Arrow type {other:?} \
             — extend render_cell() as new tiers are un-skipped (relationship / \
             path / map values are #889 slice 3)"
        ),
    }
}

fn is_path_struct(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    if fields.len() != 2 || fields[0].name() != "nodes" || fields[1].name() != "relationships" {
        return false;
    }
    let DataType::List(node_item) = fields[0].data_type() else {
        return false;
    };
    let DataType::Struct(node_fields) = node_item.data_type() else {
        return false;
    };
    // A zero-segment path has an empty relationship list whose item struct has
    // no fields, so node identity plus the canonical top-level field names are
    // the stable discriminator. Non-empty paths still validate connectivity in
    // `render_path_struct` using relationship src/dst identities.
    node_fields.iter().any(|field| field.name() == "node_uuid")
}

fn render_path_struct(path: &StructArray, row: usize) -> String {
    let nodes = path
        .column_by_name("nodes")
        .expect("path nodes")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("path nodes are a List")
        .value(row);
    let nodes = nodes
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("path node elements are Structs");
    let relationship_values = path
        .column_by_name("relationships")
        .expect("path relationships")
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("path relationships are a List")
        .value(row);
    if relationship_values.is_empty() {
        assert_eq!(nodes.len(), 1, "a zero-segment path contains one node");
        return format!("<{}>", render_node_struct(nodes, 0));
    }
    let relationships = relationship_values
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("path relationship elements are Structs");
    assert_eq!(
        nodes.len(),
        relationships.len() + 1,
        "a path must have one more node than relationship"
    );

    let mut rendered = String::from("<");
    rendered.push_str(&render_node_struct(nodes, 0));
    for i in 0..relationships.len() {
        let current = fixed_binary_16(nodes, "node_uuid", i);
        let next = fixed_binary_16(nodes, "node_uuid", i + 1);
        let src = fixed_binary_16(relationships, "src_uuid", i);
        let dst = fixed_binary_16(relationships, "dst_uuid", i);
        let relationship = render_relationship_struct(relationships, i);
        if current == src && next == dst {
            rendered.push('-');
            rendered.push_str(&relationship);
            rendered.push_str("->");
        } else if current == dst && next == src {
            rendered.push_str("<-");
            rendered.push_str(&relationship);
            rendered.push('-');
        } else {
            panic!(
                "path relationship {i} does not connect traversal nodes {i} and {}",
                i + 1
            );
        }
        rendered.push_str(&render_node_struct(nodes, i + 1));
    }
    rendered.push('>');
    rendered
}

fn fixed_binary_16(value: &StructArray, field: &str, row: usize) -> [u8; 16] {
    let bytes = value
        .column_by_name(field)
        .unwrap_or_else(|| panic!("missing {field} field"))
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap_or_else(|| panic!("{field} must be FixedSizeBinary"))
        .value(row);
    <[u8; 16]>::try_from(bytes).expect("graph UUIDs are 16 bytes")
}

/// Render a relationship-value `Struct` (#1023) as the openCypher TCK literal
/// `[:TYPE {key: value, …}]`.
///
/// - The identity fields (`edge_uuid`/`src_uuid`/`dst_uuid`) are omitted:
///   non-deterministic identities the TCK never references.
/// - NULL property values are skipped: under the wildcard union schema every
///   relation's columns appear on every edge, and NULL there means "this
///   edge's relation has no such property" (openCypher: a null-valued property
///   is an absent property).
fn render_relationship_struct(s: &StructArray, row: usize) -> String {
    use std::collections::BTreeMap;

    let rel_type = s
        .column_by_name("rel_type")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .filter(|a| !a.is_null(row))
        .map(|a| format!(":{}", a.value(row)))
        .unwrap_or_default();

    let mut props: BTreeMap<String, String> = BTreeMap::new();
    for (i, field) in s.fields().iter().enumerate() {
        let name = field.name();
        if ["edge_uuid", "src_uuid", "dst_uuid", "rel_type"].contains(&name.as_str())
            || s.column(i).is_null(row)
        {
            continue;
        }
        props.insert(name.clone(), render_cell(s.column(i).as_ref(), row));
    }
    let props_str = if props.is_empty() {
        String::new()
    } else {
        let body = props
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" {{{body}}}")
    };
    format!("[{rel_type}{props_str}]")
}

/// Render a map-value `Struct` (#600) as the openCypher map literal
/// `{key: value, …}` — keys unquoted and sorted (the canonical form openCypher
/// emits), values rendered recursively. An empty struct renders as `{}`.
fn render_map_struct(s: &StructArray, row: usize) -> String {
    let mut entries: Vec<(String, String)> = s
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().clone(), render_cell(s.column(i).as_ref(), row)))
        .collect();
    entries.sort();
    let body: Vec<String> = entries
        .into_iter()
        .map(|(k, v)| format!("{k}: {v}"))
        .collect();
    format!("{{{}}}", body.join(", "))
}

/// Render a node-value `Struct` (#785/#889) as the openCypher TCK node literal
/// `(:Label {key: value, …})`.
///
/// - `node_uuid` is omitted: a non-deterministic identity the TCK never
///   references in expected results.
/// - Labels come from the `labels` `List<Utf8>` field (`:L1:L2`); a node with no
///   labels renders none.
/// - Property keys are emitted in **sorted** order so an actual node compares
///   equal regardless of the struct's physical field order. (Features are only
///   un-skipped when their expected node literals are written sorted / have ≤1
///   property, until a structural comparison lands.)
fn render_node_struct(s: &StructArray, row: usize) -> String {
    use std::collections::BTreeMap;

    let mut labels = String::new();
    if let Some(labels_arr) = s.column_by_name("labels") {
        let list = labels_arr
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("labels is a List");
        if !list.is_null(row) {
            let vals = list.value(row);
            let strs = vals
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("labels are Utf8");
            for i in 0..strs.len() {
                if !strs.is_null(i) {
                    labels.push(':');
                    labels.push_str(strs.value(i));
                }
            }
        }
    }

    let mut props: BTreeMap<String, String> = BTreeMap::new();
    for (i, field) in s.fields().iter().enumerate() {
        let name = field.name();
        if name == "node_uuid" || name == "labels" {
            continue;
        }
        props.insert(name.clone(), render_cell(s.column(i).as_ref(), row));
    }

    let props_str = if props.is_empty() {
        String::new()
    } else {
        let body = props
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", ");
        let separator = if labels.is_empty() { "" } else { " " };
        format!("{separator}{{{body}}}")
    };
    format!("({labels}{props_str})")
}
