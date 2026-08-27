//! Physical-explain golden tests (#769): the plan text must honestly surface
//! whether the adjacency provider serves a traversal
//! (`adjacency=hit | miss | building`) while the `ExpandExec` shape remains
//! stable across index states.
//!
//! Snapshots live in `tests/explain_goldens/`. **Updating:**
//! ```text
//! INSTA_UPDATE=no cargo test -p graphforge-exec --test explain_snapshots   # review
//! INSTA_UPDATE=always cargo test -p graphforge-exec --test explain_snapshots
//! ```
//!
//! TempDir paths and other run-specific noise are normalized via insta
//! filters so snapshots are stable.

use std::path::Path;
use std::sync::{Arc, Mutex};

use tempfile::TempDir;

use graphforge_exec::ExecutionSession;
use graphforge_ir::{Binder, GraphPlan, OntologyMode, RuntimeCatalog};
use graphforge_storage::GraphCatalog;
use graphforge_storage::adjacency::build_adjacency_index;

const TS: i64 = 1_700_000_000_000_000;

fn bind(query: &str, rc: &Arc<Mutex<RuntimeCatalog>>) -> GraphPlan {
    let binder = Binder::new(None, Arc::clone(rc), OntologyMode::Advisory);
    let ast = graphforge_cypher::parse(query).expect("parse");
    binder.bind(&ast).expect("bind")
}

fn session(dir: &Path, rc: &Arc<Mutex<RuntimeCatalog>>) -> ExecutionSession {
    let catalog = GraphCatalog::open(dir, None, &rc.lock().unwrap()).unwrap();
    ExecutionSession::new_with_target(catalog, None, dir.to_path_buf(), OntologyMode::Advisory)
        .unwrap()
}

/// Tiny Advisory-mode fixture: Alice→Bob→Carol KNOWS chain with one edge prop.
async fn seed(dir: &Path) -> Arc<Mutex<RuntimeCatalog>> {
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let create = bind("CREATE (:Person {name: 'Alice'})", &rc);
    session(dir, &rc).execute_create(&create).await.unwrap();
    for stmt in [
        "MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})",
        "MATCH (b:Person {name: 'Bob'}) CREATE (b)-[:KNOWS {since: 2021}]->(c:Person {name: 'Carol'})",
    ] {
        let plan = bind(stmt, &rc);
        session(dir, &rc)
            .execute_write_statement(&plan)
            .await
            .unwrap_or_else(|e| panic!("seed {stmt:?}: {e}"));
    }
    rc
}

/// Escape regex metacharacters so a literal path can be used as a filter
/// pattern (avoids a regex-crate dev-dependency).
fn escape_for_regex(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for c in s.chars() {
        if "\\.^$|?*+()[]{}".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Snapshot settings: goldens directory + filters normalizing the TempDir
/// path and any partition/byte-size noise DataFusion embeds in scan nodes.
fn manifest_dir() -> std::path::PathBuf {
    let raw = Path::new(env!("CARGO_MANIFEST_DIR"));
    if raw.is_absolute() {
        return raw.to_path_buf();
    }
    std::env::current_dir().map_or_else(|_| raw.to_path_buf(), |cwd| cwd.join(raw))
}

fn golden_settings(dir: &Path) -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    settings.set_snapshot_path(manifest_dir().join("tests").join("explain_goldens"));
    settings.set_omit_expression(true);
    settings.add_filter(&escape_for_regex(&dir.display().to_string()), "<DIR>");
    // Parquet scan lines embed file sizes / row-group offsets that vary with
    // encoder details — normalize the numbers inside file lists.
    settings.add_filter(r"\.parquet(:\d+\.\.\d+)?", ".parquet");
    // Repartition fan-out tracks the machine's core count.
    settings.add_filter(r"RoundRobinBatch\(\d+\)", "RoundRobinBatch(<N>)");
    settings
}

async fn explain(dir: &Path, rc: &Arc<Mutex<RuntimeCatalog>>, query: &str) -> String {
    let plan = bind(query, rc);
    session(dir, rc).explain_physical(&plan).await.unwrap()
}

const VAR_LEN: &str = "MATCH (a:Person {name: 'Alice'})-[r:KNOWS*1..2]->(b:Person) RETURN 1 AS one";
const SINGLE_HOP: &str = "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN b.name AS bn";
const LIMITED_SINGLE_HOP: &str =
    "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN b.name AS bn LIMIT 3";

#[tokio::test]
async fn explain_goldens_index_absent() {
    let dir = TempDir::new().unwrap();
    let rc = seed(dir.path()).await;

    let var_len = explain(dir.path(), &rc, VAR_LEN).await;
    let single_hop = explain(dir.path(), &rc, SINGLE_HOP).await;
    let limited = explain(dir.path(), &rc, LIMITED_SINGLE_HOP).await;
    assert!(var_len.contains("adjacency=building"), "{var_len}");
    assert!(
        single_hop.contains("ExpandExec") && single_hop.contains("adjacency=building"),
        "{single_hop}"
    );
    assert!(limited.contains("fetch=3"), "{limited}");

    golden_settings(dir.path()).bind(|| {
        insta::assert_snapshot!("var_len_index_absent", var_len);
        insta::assert_snapshot!("single_hop_index_absent_expand_exec", single_hop);
    });
}

#[tokio::test]
async fn explain_goldens_index_present() {
    let dir = TempDir::new().unwrap();
    let rc = seed(dir.path()).await;
    build_adjacency_index(dir.path(), TS).unwrap();

    let var_len = explain(dir.path(), &rc, VAR_LEN).await;
    let single_hop = explain(dir.path(), &rc, SINGLE_HOP).await;
    assert!(var_len.contains("adjacency=hit"), "{var_len}");
    assert!(
        single_hop.contains("ExpandExec") && single_hop.contains("adjacency=hit"),
        "{single_hop}"
    );

    golden_settings(dir.path()).bind(|| {
        insta::assert_snapshot!("var_len_index_present", var_len);
        insta::assert_snapshot!("single_hop_index_present_expand_exec", single_hop);
    });
}
