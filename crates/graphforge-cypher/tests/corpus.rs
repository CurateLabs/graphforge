//! Parser regression corpus (#607).
//!
//! A direct Rust regression suite that guards `graphforge_cypher::parse` against
//! regressions as the pipeline evolves. It has three parts:
//!
//! 1. **Valid corpus** — `tests/corpus/valid.json`, a frozen snapshot of every
//!    query docstring harvested from the vendored openCypher TCK corpus
//!    (`tests/tck/features/**/*.feature`, #874) that the parser currently
//!    accepts. Each must keep parsing `Ok`. This is a *parser* corpus, so
//!    "valid" means "the parser accepts it" — semantic validity is irrelevant.
//!    Queries using constructs the parser does not yet support are simply
//!    absent from the snapshot (that is the TCK feature arm's job, not this
//!    hardening corpus). Regenerate the snapshot with:
//!
//!    ```bash
//!    BLESS_CORPUS=1 cargo test -p graphforge-cypher --test corpus
//!    ```
//!
//!    which re-walks the feature files, re-partitions by `parse()` result, and
//!    rewrites `valid.json`. Adding a feature to the parser → re-bless to lock
//!    the newly-accepted queries into the regression floor.
//!
//! 2. **Curated categories** — hand-written queries exercising the specific
//!    edge cases the issue calls out: operator precedence, unicode identifiers
//!    (3+ scripts), parameter syntax (`$name`, `$0`), and comment stripping
//!    (`//` and `/* */`). Asserted `Ok`.
//!
//! 3. **Invalid corpus** — hand-written malformed queries asserted `Err`, with
//!    a verified subset pinning each of the 7 `ParseErrorKind` variants.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use graphforge_cypher::{ParseErrorKind, parse};

/// Absolute path to this crate's directory.
fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The committed valid-query snapshot.
fn valid_json_path() -> PathBuf {
    manifest_dir().join("tests/corpus/valid.json")
}

/// The vendored TCK feature corpus (`<repo>/tests/tck/features`).
fn tck_features_dir() -> PathBuf {
    manifest_dir().join("../../tests/tck/features")
}

// ---------------------------------------------------------------------------
// Extraction (bless mode only)
// ---------------------------------------------------------------------------

/// Recursively collect every `*.feature` file under `dir`.
fn feature_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            feature_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "feature") {
            out.push(path);
        }
    }
}

/// Strip the common leading-whitespace prefix from a Gherkin docstring body,
/// preserving relative indentation, and trim surrounding blank lines.
fn dedent(lines: &[&str]) -> String {
    let min_indent = lines
        .iter()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);
    let body: Vec<String> = lines
        .iter()
        .map(|l| {
            if l.len() >= min_indent {
                l[min_indent..].to_owned()
            } else {
                l.trim_start().to_owned()
            }
        })
        .collect();
    body.join("\n").trim().to_owned()
}

/// Harvest every query docstring from one feature file.
///
/// A docstring follows a step line ending in `executing query:`,
/// `executing control query:`, or `having executed:`, and is delimited by
/// `"""` on the immediately following line (the TCK's canonical layout).
fn harvest_file(content: &str, out: &mut BTreeSet<String>) {
    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0;
    while i < lines.len() {
        let t = lines[i].trim();
        let intro = t.ends_with("executing query:")
            || t.ends_with("executing control query:")
            || t.ends_with("having executed:");
        if intro && i + 1 < lines.len() && lines[i + 1].trim() == "\"\"\"" {
            let mut k = i + 2;
            let start = k;
            while k < lines.len() && lines[k].trim() != "\"\"\"" {
                k += 1;
            }
            let query = dedent(&lines[start..k]);
            if !query.is_empty() {
                out.insert(query);
            }
            i = k + 1;
        } else {
            i += 1;
        }
    }
}

/// All harvested query docstrings across the TCK corpus, deduplicated + sorted.
fn harvest_all() -> Vec<String> {
    let mut files = Vec::new();
    feature_files(&tck_features_dir(), &mut files);
    let mut set = BTreeSet::new();
    for f in &files {
        if let Ok(content) = fs::read_to_string(f) {
            harvest_file(&content, &mut set);
        }
    }
    set.into_iter().collect()
}

/// `BLESS_CORPUS=1`: re-walk the features, keep the queries the parser accepts,
/// and rewrite `valid.json`. Runs as an ordinary `#[test]` so it shares the
/// compiled parser; it is a no-op unless the env var is set.
#[test]
fn bless_valid_corpus() {
    if std::env::var_os("BLESS_CORPUS").is_none() {
        return;
    }
    let harvested = harvest_all();
    let total = harvested.len();
    let valid: Vec<String> = harvested.into_iter().filter(|q| parse(q).is_ok()).collect();
    let json = serde_json::to_string_pretty(&valid).expect("serialize corpus");
    fs::write(valid_json_path(), json + "\n").expect("write valid.json");
    eprintln!(
        "blessed corpus: {} / {} harvested queries parse Ok -> {}",
        valid.len(),
        total,
        valid_json_path().display()
    );
}

// ---------------------------------------------------------------------------
// Valid corpus
// ---------------------------------------------------------------------------

/// Load the frozen valid-query snapshot.
fn load_valid_corpus() -> Vec<String> {
    let raw = fs::read_to_string(valid_json_path()).unwrap_or_else(|e| {
        panic!(
            "missing {} ({e}); regenerate with BLESS_CORPUS=1 cargo test -p graphforge-cypher --test corpus",
            valid_json_path().display()
        )
    });
    serde_json::from_str(&raw).expect("valid.json is a JSON array of query strings")
}

#[test]
fn valid_corpus_parses() {
    let corpus = load_valid_corpus();
    assert!(
        corpus.len() >= 500,
        "valid corpus has {} queries; need >= 500 (re-bless with BLESS_CORPUS=1)",
        corpus.len()
    );
    let mut failures = Vec::new();
    for query in &corpus {
        if let Err(e) = parse(query) {
            failures.push(format!("  {e}: {}", query.replace('\n', " ⏎ ")));
        }
    }
    assert!(
        failures.is_empty(),
        "{} frozen corpus queries no longer parse (parser regression):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Curated categories
// ---------------------------------------------------------------------------

/// Precedence edge cases — the parser must accept these without grammar
/// ambiguity (correctness of the parse tree is covered by the in-module
/// Pratt-parser tests; here we only guard acceptance).
const PRECEDENCE: &[&str] = &[
    "RETURN 1 + 2 * 3",
    "RETURN (1 + 2) * 3",
    "RETURN 2 ^ 3 ^ 2",
    "RETURN -2 ^ 2",
    "RETURN NOT true AND false",
    "RETURN true OR false AND true",
    "RETURN NOT NOT true",
    "RETURN 1 < 2 = true",
    "RETURN 1 + 2 = 3 AND 4 < 5",
    "RETURN 1 = 2 OR 3 <> 4 XOR 5 > 6",
    "RETURN a.b.c.d",
    "RETURN -a.b + c.d * -e.f",
    "RETURN 1 + 2 + 3 - 4 - 5",
    "RETURN 10 % 3 * 2",
    "RETURN [1, 2, 3][0]",
    "RETURN {a: 1, b: {c: 2}}.b.c",
    "RETURN 1 IN [1, 2] AND 2 IN [3, 4]",
    "RETURN a IS NULL OR b IS NOT NULL",
    "RETURN n.p STARTS WITH 'a' AND n.q ENDS WITH 'z'",
    "WITH 1 AS x RETURN x + x * x ^ x",
];

/// Unicode identifiers across 3+ scripts (Latin-with-diacritics, Greek,
/// Cyrillic, CJK), plus backtick-escaped identifiers.
const UNICODE: &[&str] = &[
    "MATCH (café) RETURN café",
    "MATCH (Ω) WHERE Ω.μ > 0 RETURN Ω",
    "MATCH (Москва)-[:ЕДЕТ]->(Питер) RETURN Москва, Питер",
    "MATCH (東京) RETURN 東京.人口",
    "RETURN 'naïve café résumé' AS s",
    "MATCH (`weird name`) RETURN `weird name`",
    "WITH 1 AS αβγ RETURN αβγ",
    "RETURN '日本語のテキスト' AS jp",
    "MATCH (a)-[:`rel with spaces`]->(b) RETURN a, b",
    "RETURN 'emoji 🎉 string' AS e",
];

/// Parameter syntax: `$name`, `$0` (positional), in various positions.
const PARAMETERS: &[&str] = &[
    "RETURN $name",
    "RETURN $0",
    "MATCH (n) WHERE n.id = $id RETURN n",
    "MATCH (n {name: $name}) RETURN n",
    "RETURN $param + 1",
    "WITH $x AS y RETURN y",
    "MATCH (n) WHERE n.age > $minAge AND n.age < $maxAge RETURN n",
    "RETURN [$a, $b, $c]",
    "RETURN {key: $value}",
    "MATCH (n) RETURN n SKIP $skip LIMIT $limit",
];

/// Comment stripping: line (`//`) and block (`/* */`) comments anywhere.
const COMMENTS: &[&str] = &[
    "RETURN 1 // trailing line comment",
    "// leading comment\nRETURN 1",
    "RETURN /* inline block */ 1",
    "MATCH (n) // comment\nRETURN n",
    "RETURN 1 /* multi\nline\nblock */ + 2",
    "/* header */\nMATCH (n)\nRETURN n // tail",
    "RETURN 1, // first\n2 // second",
    "MATCH (n) /* c1 */ WHERE /* c2 */ n.x > 0 /* c3 */ RETURN n",
    "WITH 1 AS x // bind\nRETURN x",
    "RETURN 'not // a comment' AS s",
];

#[test]
fn curated_categories_parse() {
    let categories: &[(&str, &[&str])] = &[
        ("precedence", PRECEDENCE),
        ("unicode", UNICODE),
        ("parameters", PARAMETERS),
        ("comments", COMMENTS),
    ];
    let mut failures = Vec::new();
    for (name, queries) in categories {
        for query in *queries {
            if let Err(e) = parse(query) {
                failures.push(format!("  [{name}] {e}: {query:?}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "curated queries failed to parse:\n{}",
        failures.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Invalid corpus
// ---------------------------------------------------------------------------

/// Discriminant name for a `ParseErrorKind` (ignores inner fields).
fn kind_name(kind: &ParseErrorKind) -> &'static str {
    match kind {
        ParseErrorKind::UnexpectedChar => "UnexpectedChar",
        ParseErrorKind::UnexpectedToken { .. } => "UnexpectedToken",
        ParseErrorKind::UnterminatedString => "UnterminatedString",
        ParseErrorKind::UnterminatedBlockComment => "UnterminatedBlockComment",
        ParseErrorKind::InvalidNumericLiteral => "InvalidNumericLiteral",
        ParseErrorKind::InvalidParameter => "InvalidParameter",
        ParseErrorKind::UnexpectedEof { .. } => "UnexpectedEof",
        _ => "Other",
    }
}

/// Malformed queries with the exact `ParseErrorKind` variant they must produce.
/// Pins coverage of all 7 variants.
const INVALID_WITH_KIND: &[(&str, &str)] = &[
    // UnterminatedString
    ("RETURN 'abc", "UnterminatedString"),
    ("RETURN \"unclosed", "UnterminatedString"),
    ("MATCH (n {name: 'x}) RETURN n", "UnterminatedString"),
    ("WITH 'open AS s RETURN s", "UnterminatedString"),
    // UnterminatedBlockComment
    ("RETURN 1 /* never closed", "UnterminatedBlockComment"),
    ("/* start\nMATCH (n) RETURN n", "UnterminatedBlockComment"),
    ("MATCH (n) /* dangling RETURN n", "UnterminatedBlockComment"),
    // UnexpectedEof
    ("MATCH (n", "UnexpectedEof"),
    ("RETURN", "UnexpectedEof"),
    ("MATCH (n)-[", "UnexpectedEof"),
    ("WITH 1 AS", "UnexpectedEof"),
    ("MATCH (n) WHERE", "UnexpectedEof"),
    ("RETURN 1 +", "UnexpectedEof"),
    ("MATCH (n)-[:KNOWS]->", "UnexpectedEof"),
    // UnexpectedToken
    ("MATCH RETURN n", "UnexpectedToken"),
    ("RETURN 1 2", "UnexpectedToken"),
    ("MATCH (n) RETURN RETURN n", "UnexpectedToken"),
    ("CREATE WHERE", "UnexpectedToken"),
    ("RETURN * FROM n", "UnexpectedToken"),
    ("MATCH () () RETURN 1", "UnexpectedToken"),
    // InvalidParameter
    ("RETURN $", "InvalidParameter"),
    ("MATCH (n) WHERE n.x = $ RETURN n", "InvalidParameter"),
    // InvalidNumericLiteral
    (
        "RETURN 999999999999999999999999999999",
        "InvalidNumericLiteral",
    ), // i64 overflow
    ("RETURN 0xZZ", "InvalidNumericLiteral"),      // bad hex
    ("RETURN '\\uZZZZ'", "InvalidNumericLiteral"), // bad unicode escape
    // UnexpectedChar
    ("RETURN 1 # 2", "UnexpectedChar"),
];

/// Additional clearly-malformed queries asserted only to be `Err` (the precise
/// variant is not pinned, to keep the suite robust to classification tweaks).
const INVALID_ANY: &[&str] = &[
    "NOT A QUERY",
    "RETURN 1.2.3",
    "MATCH",
    "WHERE n.x > 0",
    "RETURN )",
    "RETURN (",
    "RETURN [",
    "RETURN ]",
    "RETURN {",
    "RETURN }",
    "MATCH (n RETURN n",
    "MATCH n) RETURN n",
    "MATCH (n)] RETURN n",
    "MATCH (n)-[r->(m) RETURN r",
    "MATCH (n)-r]->(m) RETURN r",
    "RETURN 1,",
    "RETURN ,1",
    "RETURN 1 AS",
    "RETURN AS x",
    "WITH RETURN n",
    "MATCH (n) WHERE AND n.x RETURN n",
    "MATCH (n) WHERE n.x = RETURN n",
    "MATCH (n) WHERE n.x AND RETURN n",
    "ORDER BY n.x",
    "SKIP 5",
    "LIMIT 5",
    "RETURN 1 ORDER",
    "RETURN 1 ORDER BY",
    "RETURN 1 SKIP",
    "RETURN 1 LIMIT",
    "MATCH (n)--",
    "MATCH (n)=[r]=(m) RETURN n",
    "CREATE (n RETURN n",
    "MERGE",
    "DELETE",
    "SET",
    "REMOVE",
    "UNWIND AS x RETURN x",
    "UNWIND [1,2,3] RETURN x",
    "RETURN 1 +* 2",
    "RETURN 1 ** 2",
    "RETURN * *",
    "RETURN 1 == 2",
    "RETURN 1 =< 2",
    "RETURN 1 => 2",
    "RETURN 1 !2",
    "RETURN !true",
    "RETURN 1 & 2",
    "RETURN @",
    "RETURN ;",
    "MATCH (n) ; RETURN n",
    "RETURN .",
    "RETURN ..",
    "RETURN 1..2",
    "RETURN [1..]",
    "MATCH (n {)} RETURN n",
    "MATCH (n {x}) RETURN n",
    "MATCH (n {x:}) RETURN n",
    "MATCH (:) RETURN 1",
    "MATCH (n:) RETURN n",
    "MATCH (n:`) RETURN n",
    "RETURN $$",
    "RETURN $.x",
    "CALL",
    "CALL (",
    "FOREACH",
    "FOREACH (x IN [1])",
    "RETURN CASE",
    "RETURN CASE WHEN",
    "RETURN CASE WHEN true",
    "RETURN CASE WHEN true THEN",
    "RETURN count(",
    "RETURN count(*",
    "RETURN [x IN",
    "RETURN [x IN [1,2]",
    "MATCH p = RETURN p",
    "MATCH (n)-[*a]-(m) RETURN n",
    "RETURN 0x",
    "RETURN 1e",
    "RETURN 1.0e",
    "RETURN 0b",
    "RETURN 0o",
];

#[test]
fn invalid_queries_rejected() {
    let total = INVALID_WITH_KIND.len() + INVALID_ANY.len();
    assert!(
        total >= 100,
        "invalid corpus has {total} queries; need >= 100"
    );

    let mut wrong = Vec::new();

    // Variant-pinned: must be Err with the exact kind.
    let mut seen_kinds = BTreeSet::new();
    for (query, expected) in INVALID_WITH_KIND {
        match parse(query) {
            Ok(_) => wrong.push(format!("  expected {expected}, but parsed OK: {query:?}")),
            Err(e) => {
                let got = kind_name(&e.kind);
                seen_kinds.insert(got);
                if got != *expected {
                    wrong.push(format!("  expected {expected}, got {got}: {query:?}"));
                }
            }
        }
    }

    // Loosely-pinned: must merely be Err.
    for query in INVALID_ANY {
        if parse(query).is_ok() {
            wrong.push(format!("  expected Err, but parsed OK: {query:?}"));
        }
    }

    assert!(
        wrong.is_empty(),
        "invalid-query mismatches:\n{}",
        wrong.join("\n")
    );

    // All 7 ParseErrorKind variants must be exercised by the pinned set.
    let all_variants = [
        "UnexpectedChar",
        "UnexpectedToken",
        "UnterminatedString",
        "UnterminatedBlockComment",
        "InvalidNumericLiteral",
        "InvalidParameter",
        "UnexpectedEof",
    ];
    let missing: Vec<&str> = all_variants
        .iter()
        .filter(|v| !seen_kinds.contains(*v))
        .copied()
        .collect();
    assert!(
        missing.is_empty(),
        "these ParseErrorKind variants are not covered by INVALID_WITH_KIND: {missing:?}"
    );
}
