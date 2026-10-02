//! Identity checks spill to sorted runs under a memory budget. These tests
//! run inputs many times larger than a deliberately tiny budget and compare
//! against a run whose buffer holds everything.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use gdc_scorecard::{
    Cause, ConvertError, DEFAULT_MEMORY_BUDGET_BYTES, KEY_RECORD_BYTES, MIN_MEMORY_BUDGET_BYTES,
    SPILL_DIR, convert_with_budget,
};
use serde_json::Value;

/// Ten key records: the fixture below holds 300 nodes and 1,200 endpoints.
const TINY_BUDGET: u64 = 10 * KEY_RECORD_BYTES;
const NODES_PER_FILE: i64 = 150;
const EDGES: u64 = 600;

const MAPPING: &str = r#"{
  "schema": "graphforge-gdc-load-mapping/1",
  "node_tables": [{"id": "person", "format": "ldbc-csv",
    "files": ["person_a.csv", "person_b.csv"], "label": "Person", "id_column": "id",
    "properties": [{"column": "id", "type": "int64"}, {"column": "age", "type": "int64"}]}],
  "edge_tables": [{"id": "knows", "format": "ldbc-csv", "files": ["knows.csv"],
    "rel_type": "KNOWS", "source": {"label": "Person", "column": "a"},
    "target": {"label": "Person", "column": "b"},
    "properties": [{"column": "w", "type": "float64"}]}]
}"#;

/// Person ids `0..300`, `NODES_PER_FILE` per file, so row `r` of `person_a`
/// holds id `r - 1` and row `r` of `person_b` holds id `r + 149`.
struct Input {
    person_a: Vec<i64>,
    person_b: Vec<i64>,
    edges: Vec<(i64, i64)>,
}

impl Input {
    fn new() -> Self {
        let mut state = 0x2545_f491_u64;
        let mut next = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            i64::try_from((state >> 33) % (2 * NODES_PER_FILE as u64)).unwrap()
        };
        let edges = (0..EDGES).map(|_| (next(), next())).collect();
        Self {
            person_a: (0..NODES_PER_FILE).collect(),
            person_b: (NODES_PER_FILE..2 * NODES_PER_FILE).collect(),
            edges,
        }
    }

    fn write(&self, root: &Path) {
        fs::create_dir_all(root).unwrap();
        let people = |ids: &[i64]| {
            let mut text = String::from("id|age\n");
            for id in ids {
                text.push_str(&format!("{id}|{}\n", id % 90));
            }
            text
        };
        fs::write(root.join("person_a.csv"), people(&self.person_a)).unwrap();
        fs::write(root.join("person_b.csv"), people(&self.person_b)).unwrap();
        let mut text = String::from("a|b|w\n");
        for (index, (source, target)) in self.edges.iter().enumerate() {
            text.push_str(&format!("{source}|{target}|{index}.5\n"));
        }
        fs::write(root.join("knows.csv"), text).unwrap();
    }
}

struct Run {
    _dir: tempfile::TempDir,
    out: PathBuf,
    result: Result<Value, ConvertError>,
}

fn run(input: &Input, budget: u64) -> Run {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("in");
    input.write(&root);
    let out = dir.path().join("out");
    let result = convert_with_budget(MAPPING.as_bytes(), &root, &out, budget)
        .map(|conversion| conversion.manifest);
    Run {
        _dir: dir,
        out,
        result,
    }
}

/// Every file under `root`, relative path to bytes.
fn tree(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let relative = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                files.insert(relative, fs::read(&path).unwrap());
            }
        }
    }
    files
}

fn names(root: &Path) -> Vec<String> {
    tree(root).into_keys().collect()
}

fn stat(manifest: &Value, kind: &str, field: &str) -> u64 {
    manifest["spill"][kind][field].as_u64().unwrap()
}

#[test]
fn tiny_budget_spills_and_matches_an_unbounded_run() {
    let input = Input::new();
    let input_key_bytes = (2 * NODES_PER_FILE as u64 + 2 * EDGES) * KEY_RECORD_BYTES;
    assert!(input_key_bytes >= 10 * TINY_BUDGET);
    let tiny = run(&input, TINY_BUDGET);
    let unbounded = run(&input, DEFAULT_MEMORY_BUDGET_BYTES);
    let (tiny_manifest, unbounded_manifest) = (tiny.result.unwrap(), unbounded.result.unwrap());

    for kind in ["node_keys", "endpoint_keys"] {
        assert!(stat(&tiny_manifest, kind, "runs") > 1, "{kind}");
        assert!(
            stat(&tiny_manifest, kind, "intermediate_merges") > 0,
            "{kind}"
        );
        assert_eq!(stat(&unbounded_manifest, kind, "runs"), 1, "{kind}");
        assert_eq!(stat(&unbounded_manifest, kind, "intermediate_merges"), 0);
    }
    let buffer = tiny_manifest["spill"]["budget"]["buffer_records"]
        .as_u64()
        .unwrap();
    assert!(buffer * KEY_RECORD_BYTES <= TINY_BUDGET);
    assert!(stat(&tiny_manifest, "node_keys", "peak_buffered_records") <= buffer);
    assert_eq!(stat(&tiny_manifest, "node_keys", "records"), 300);
    assert_eq!(stat(&tiny_manifest, "endpoint_keys", "records"), 1200);
    assert_eq!(
        tiny_manifest["spill"]["budget"]["memory_budget_bytes"],
        TINY_BUDGET
    );

    // Same tables, same bytes, nothing else left behind.
    let (tiny_tree, unbounded_tree) = (tree(&tiny.out), tree(&unbounded.out));
    assert_eq!(
        tiny_tree.keys().collect::<Vec<_>>(),
        [
            "conversion-manifest.json",
            "edges/knows.parquet",
            "nodes/person.parquet"
        ]
    );
    for table in ["edges/knows.parquet", "nodes/person.parquet"] {
        assert_eq!(tiny_tree[table], unbounded_tree[table], "{table}");
    }
    assert_eq!(tiny_manifest["outputs"], unbounded_manifest["outputs"]);
    assert_eq!(tiny_manifest["inputs"], unbounded_manifest["inputs"]);
    assert!(!tiny.out.join(SPILL_DIR).exists());
}

/// The failure's cause and its message with the scratch input root removed.
fn failure(input: &Input, budget: u64) -> (Cause, String) {
    let run = run(input, budget);
    let error = run.result.unwrap_err();
    let root = format!("{}/", run.out.with_file_name("in").display());
    let message = error.message().replace(&root, "");
    // Nothing is published and no spill file survives the failure.
    let left = names(&run.out);
    assert!(
        left.iter().all(|name| name.ends_with(".partial")),
        "{left:?}"
    );
    assert!(!run.out.join(SPILL_DIR).exists());
    (error.cause(), message)
}

/// The same error at a tiny budget and at a budget that never spills.
fn failure_at_every_budget(input: &Input) -> (Cause, String) {
    let tiny = failure(input, TINY_BUDGET);
    assert_eq!(tiny, failure(input, DEFAULT_MEMORY_BUDGET_BYTES));
    tiny
}

#[test]
fn duplicate_split_across_spill_runs_and_files_fails_typed() {
    // id 7 is row 8 of person_a; row 140 of person_b repeats it.
    let mut input = Input::new();
    input.person_b[139] = 7;
    assert_eq!(
        failure_at_every_budget(&input),
        (
            Cause::DuplicateNodeIdentity,
            "person_b.csv row 140: node (Person, 7) appears more than once (table person); \
             first defined at person_a.csv row 8 (table person)"
                .to_owned()
        )
    );

    // A second duplicate within person_a whose repeat comes earlier in input
    // order wins: rows 21 and 100 are many runs apart at the tiny budget.
    input.person_a[99] = 20;
    assert_eq!(
        failure_at_every_budget(&input),
        (
            Cause::DuplicateNodeIdentity,
            "person_a.csv row 100: node (Person, 20) appears more than once (table person); \
             first defined at person_a.csv row 21 (table person)"
                .to_owned()
        )
    );
}

fn dangling(row: u64, side: &str, id: i64) -> (Cause, String) {
    (
        Cause::DanglingEndpoint,
        format!("knows.csv row {row}: {side} node {id} is not defined by any node table"),
    )
}

#[test]
fn earliest_dangling_endpoint_in_input_order_fails_typed() {
    let mut input = Input::new();
    input.edges[299] = (9_999, 1);
    input.edges[119] = (1, 5_000);
    assert_eq!(
        failure_at_every_budget(&input),
        dangling(120, "target", 5_000)
    );

    // On one row the source is reported before the target.
    input.edges[79] = (7_777, 8_888);
    assert_eq!(
        failure_at_every_budget(&input),
        dangling(80, "source", 7_777)
    );

    // Ids past either end of the defined range dangle too.
    let mut input = Input::new();
    input.edges[599] = (i64::MAX, 0);
    input.edges[598] = (0, i64::MIN);
    assert_eq!(
        failure_at_every_budget(&input),
        dangling(599, "target", i64::MIN)
    );
    input.edges[598] = (0, 1);
    assert_eq!(
        failure_at_every_budget(&input),
        dangling(600, "source", i64::MAX)
    );
}

#[test]
fn budget_below_the_minimum_is_refused_before_any_output() {
    let run = run(&Input::new(), MIN_MEMORY_BUDGET_BYTES - 1);
    assert_eq!(run.result.unwrap_err().cause(), Cause::InvalidMemoryBudget);
    assert!(!run.out.exists());
}
