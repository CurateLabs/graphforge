//! Integration tests for `PersistentAdjacencyProvider` (#761): build → load
//! identity with scan-build, staleness fallback, lazy rebuild, corrupt-artifact
//! degradation, and the session wiring that makes explain report
//! `adjacency=hit`.

use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;

use graphforge_core::uuid::{Uuid, new_v7};
use graphforge_core::{OntologyMode, TypeId};
use graphforge_ir::Direction;
use graphforge_storage::GraphWriter;
use graphforge_storage::adjacency::build_adjacency_index;

use graphforge_exec::{
    AdjacencyProvider, AdjacencyStatus, PersistentAdjacencyProvider, ScanBuildAdjacencyProvider,
};

const TS: i64 = 1_700_000_000_000_000;
const PERSON: TypeId = TypeId(0);

fn force_stale_generation_fixture(dir: &Path) {
    let topology = graphforge_storage::read_topology_generation(dir).unwrap() + 1;
    let search = graphforge_storage::read_search_generation(dir).unwrap();
    let property = graphforge_storage::generation::read_property_generation(dir).unwrap();
    std::fs::write(
        dir.join("topology/generation.json"),
        format!(
            r#"{{"topology_generation":{topology},"search_generation":{search},"property_generation":{property}}}"#
        ),
    )
    .unwrap();
}

/// Strict-mode diamond a→b, a→c, b→d, c→d plus a parallel a→b and a self-loop
/// d→d, all KNOWS — the fixture whose self-loop pins the undirected merge
/// order. Returns the surrogate node ids.
#[allow(clippy::many_single_char_names)]
fn write_diamond(dir: &Path) -> [u64; 4] {
    let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let uuids: Vec<Uuid> = (0..4).map(|_| new_v7()).collect();
    let ids: Vec<u64> = uuids
        .iter()
        .map(|u| w.create_node(*u, PERSON).unwrap())
        .collect();
    let (a, b, c, d) = (&uuids[0], &uuids[1], &uuids[2], &uuids[3]);
    for (src, dst) in [(a, b), (a, c), (b, d), (c, d), (a, b), (d, d)] {
        w.create_edge(new_v7(), "KNOWS", src, dst).unwrap();
    }
    w.flush().unwrap();
    [ids[0], ids[1], ids[2], ids[3]]
}

fn persistent(dir: &Path, mode: OntologyMode) -> PersistentAdjacencyProvider {
    PersistentAdjacencyProvider::new(dir.to_path_buf(), mode)
}

fn scan(dir: &Path, mode: OntologyMode) -> ScanBuildAdjacencyProvider {
    ScanBuildAdjacencyProvider::new(dir.to_path_buf(), mode)
}

/// Seed a 3-node KNOWS chain `a→b→c`, build the index, then DELETE the `a→b`
/// edge. A delete bumps the generation but writes no delta segment, breaking
/// the chain — so the index is **genuinely stale** (a post-build CREATE would
/// instead be served via the overlay, #765). The surviving `b→c` keeps KNOWS
/// non-empty so a rebuild still produces a KNOWS CSR (→ Hit).
fn stale_via_delete(dir: &Path) {
    use std::collections::HashSet;
    let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let (a, b, c) = (new_v7(), new_v7(), new_v7());
    for u in [a, b, c] {
        w.create_node(u, PERSON).unwrap();
    }
    let victim = new_v7();
    w.create_edge(victim, "KNOWS", &a, &b).unwrap();
    w.create_edge(new_v7(), "KNOWS", &b, &c).unwrap();
    w.flush().unwrap();
    build_adjacency_index(dir, TS).unwrap();
    let victims: HashSet<[u8; 16]> = std::iter::once(*victim.as_bytes()).collect();
    graphforge_storage::delete_edges(dir, &victims).unwrap();
}

// ---------------------------------------------------------------------------
// Acceptance 1: build → reopen → load identical to fresh in-memory build
// ---------------------------------------------------------------------------

#[test]
fn loaded_view_is_identical_to_scan_build_strict() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), TS).unwrap();

    let loaded = persistent(dir.path(), OntologyMode::Strict);
    let rebuilt = scan(dir.path(), OntologyMode::Strict);
    for direction in [Direction::Out, Direction::In, Direction::Undirected] {
        assert_eq!(loaded.status("KNOWS", direction), AdjacencyStatus::Hit);
        assert_eq!(
            loaded.adjacency("KNOWS", direction).unwrap(),
            rebuilt.adjacency("KNOWS", direction).unwrap(),
            "direction {direction:?}: loaded view must equal a fresh scan-build \
             (including per-node entry order — the undirected merge case)"
        );
    }
}

#[test]
fn loaded_view_is_identical_to_scan_build_exploratory_including_wildcard() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
    let (a, b, c) = (new_v7(), new_v7(), new_v7());
    for u in [a, b, c] {
        w.create_node(u, PERSON).unwrap();
    }
    w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
    w.create_edge(new_v7(), "OWNS", &a, &c).unwrap();
    w.flush().unwrap();
    build_adjacency_index(dir.path(), TS).unwrap();

    let loaded = persistent(dir.path(), OntologyMode::Exploratory);
    let rebuilt = scan(dir.path(), OntologyMode::Exploratory);
    for rel in ["KNOWS", "OWNS", "*"] {
        for direction in [Direction::Out, Direction::In, Direction::Undirected] {
            assert_eq!(loaded.status(rel, direction), AdjacencyStatus::Hit, "{rel}");
            assert_eq!(
                loaded.adjacency(rel, direction).unwrap(),
                rebuilt.adjacency(rel, direction).unwrap(),
                "rel {rel} direction {direction:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Acceptance 2: missing or stale index falls back with identical results
// ---------------------------------------------------------------------------

#[test]
fn absent_capability_dir_is_building_and_scan_builds() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());

    let provider = persistent(dir.path(), OntologyMode::Strict);
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Building
    );
    assert_eq!(
        provider.adjacency("KNOWS", Direction::Out).unwrap(),
        scan(dir.path(), OntologyMode::Strict)
            .adjacency("KNOWS", Direction::Out)
            .unwrap()
    );
}

#[test]
fn absent_index_bounded_build_is_cached_across_queries() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    let provider = persistent(dir.path(), OntologyMode::Strict);

    let first = provider.adjacency("KNOWS", Direction::Out).unwrap();
    let second = provider.adjacency("KNOWS", Direction::Out).unwrap();
    assert!(
        Arc::ptr_eq(&first, &second),
        "bounded derived CSR must be reused instead of rebuilding per input batch"
    );
    provider.revalidate();
    let next_query = provider.adjacency("KNOWS", Direction::Out).unwrap();
    assert!(
        Arc::ptr_eq(&first, &next_query),
        "the authenticated unchanged source must retain its bounded derived CSR"
    );
}

#[test]
fn stale_index_is_miss_then_lazy_rebuild_repairs() {
    let dir = TempDir::new().unwrap();
    // A DELETE breaks the delta chain ⇒ the index is genuinely stale.
    stale_via_delete(dir.path());

    let provider = persistent(dir.path(), OntologyMode::Strict);
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Miss
    );
    // Stale access serves correct (post-write) results and lazily rebuilds.
    assert_eq!(
        provider.adjacency("KNOWS", Direction::Out).unwrap(),
        scan(dir.path(), OntologyMode::Strict)
            .adjacency("KNOWS", Direction::Out)
            .unwrap()
    );
    // A fresh provider over the repaired index now reports a Hit.
    assert_eq!(
        persistent(dir.path(), OntologyMode::Strict).status("KNOWS", Direction::Out),
        AdjacencyStatus::Hit
    );
}

#[test]
fn fresh_index_with_unknown_rel_scan_builds_without_rebuild() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    let rows = build_adjacency_index(dir.path(), TS).unwrap();
    let built_at = rows[0].built_at_micros;

    let provider = persistent(dir.path(), OntologyMode::Strict);
    assert_eq!(
        provider.status("UNKNOWN_REL", Direction::Out),
        AdjacencyStatus::Miss
    );
    let view = provider.adjacency("UNKNOWN_REL", Direction::Out).unwrap();
    assert!(view.is_empty());

    // No rebuild happened: the manifest still carries the original built_at.
    let manifest = graphforge_storage::adjacency::read_manifest(dir.path()).unwrap();
    assert!(manifest.iter().all(|r| r.built_at_micros == built_at));
}

// ---------------------------------------------------------------------------
// Corrupt artifacts repair when authority is readable and fail closed when it is not
// ---------------------------------------------------------------------------

#[test]
fn corrupt_generation_counter_refuses_unbounded_scan_fallback() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), TS).unwrap();
    std::fs::write(
        graphforge_storage::generation::generation_path(dir.path()),
        "not json",
    )
    .unwrap();

    let provider = persistent(dir.path(), OntologyMode::Strict);
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Miss
    );
    let error = provider
        .adjacency("KNOWS", Direction::Out)
        .expect_err("unreadable authority must never select the O(E)-memory scan oracle");
    assert!(
        error
            .to_string()
            .contains("bounded adjacency index build failed")
            && error.to_string().contains("corrupt"),
        "{error}"
    );
}

#[test]
fn missing_csr_artifact_with_fresh_manifest_rebuilds_and_repairs() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), TS).unwrap();
    let csr_path = graphforge_storage::adjacency::csr_path(
        dir.path(),
        "KNOWS",
        graphforge_storage::adjacency::Direction::Out,
    );
    std::fs::remove_file(csr_path.with_extension("csr.json")).unwrap();

    let provider = persistent(dir.path(), OntologyMode::Strict);
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Miss,
        "manifest row present but CSR file gone"
    );
    assert_eq!(
        provider.adjacency("KNOWS", Direction::Out).unwrap(),
        scan(dir.path(), OntologyMode::Strict)
            .adjacency("KNOWS", Direction::Out)
            .unwrap()
    );
    // The lazy rebuild restored the sharded artifact.
    assert!(graphforge_storage::adjacency::sharded_csr_exists(&csr_path));
}

// ---------------------------------------------------------------------------
// No-scan proof: a Hit never touches the edge files
// ---------------------------------------------------------------------------

#[test]
fn hit_serves_without_reading_edge_files() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), TS).unwrap();
    let expected = scan(dir.path(), OntologyMode::Strict)
        .adjacency("KNOWS", Direction::Out)
        .unwrap();

    // Remove the edge files WITHOUT bumping the generation: the index still
    // reads as fresh, and only a provider that never scans can serve it.
    std::fs::remove_dir_all(dir.path().join("topology").join("edges")).unwrap();

    let provider = persistent(dir.path(), OntologyMode::Strict);
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Hit
    );
    assert_eq!(
        provider.adjacency("KNOWS", Direction::Out).unwrap(),
        expected
    );
    // The scan-build provider on the same project sees nothing.
    assert!(
        scan(dir.path(), OntologyMode::Strict)
            .adjacency("KNOWS", Direction::Out)
            .unwrap()
            .is_empty()
    );
}

// ---------------------------------------------------------------------------
// Typed-mode "*" is the union: `_all` CSR Hit == scan-build union (#823)
// ---------------------------------------------------------------------------

/// The #823 bug-fix invariant in typed mode: an untyped `"*"` pattern is a
/// `Hit` served by the `_all` union CSR, and that loaded view is identical
/// (including per-node entry order) to the scan-build union over
/// `read_edges(dir, "*")` — for every relation key and direction. Mirrors
/// `loaded_view_is_identical_to_scan_build_exploratory_including_wildcard` in
/// Strict mode. (Was `typed_mode_wildcard_stays_empty_and_building_with_built_index`,
/// which locked the pre-#823 empty bypass.)
#[test]
fn typed_mode_wildcard_is_hit_and_unions_all_relations() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    let (a, b, c) = (new_v7(), new_v7(), new_v7());
    for u in [a, b, c] {
        w.create_node(u, PERSON).unwrap();
    }
    w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
    w.create_edge(new_v7(), "OWNS", &a, &c).unwrap();
    w.flush().unwrap();
    build_adjacency_index(dir.path(), TS).unwrap();

    let loaded = persistent(dir.path(), OntologyMode::Strict);
    let rebuilt = scan(dir.path(), OntologyMode::Strict);
    for rel in ["KNOWS", "OWNS", "*"] {
        for direction in [Direction::Out, Direction::In, Direction::Undirected] {
            assert_eq!(
                loaded.status(rel, direction),
                AdjacencyStatus::Hit,
                "{rel} {direction:?}: served by a CSR, not the pre-#823 empty bypass"
            );
            assert_eq!(
                loaded.adjacency(rel, direction).unwrap(),
                rebuilt.adjacency(rel, direction).unwrap(),
                "rel {rel} direction {direction:?}: Hit (_all CSR) == scan-build union"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Acceptance 3: load << rebuild (run with --ignored; numbers in the PR body)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "timing benchmark — run manually with: cargo test -p graphforge-exec --test persistent_adjacency -- --ignored --nocapture"]
fn load_is_faster_than_rebuild() {
    let dir = TempDir::new().unwrap();
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    // Hub-and-spoke + chain: ~50k edges over ~25k nodes.
    let uuids: Vec<Uuid> = (0..25_000).map(|_| new_v7()).collect();
    for u in &uuids {
        w.create_node(*u, PERSON).unwrap();
    }
    for pair in uuids.windows(2) {
        w.create_edge(new_v7(), "KNOWS", &pair[0], &pair[1])
            .unwrap();
    }
    for spoke in uuids.iter().skip(1).take(25_000) {
        w.create_edge(new_v7(), "KNOWS", &uuids[0], spoke).unwrap();
    }
    w.flush().unwrap();
    build_adjacency_index(
        dir.path(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX)),
    )
    .unwrap();

    let time = |f: &dyn Fn() -> Arc<graphforge_exec::Adjacency>| {
        let start = std::time::Instant::now();
        let view = f();
        (start.elapsed(), view)
    };
    // Fresh provider per measurement: cold interior cache.
    let (rebuild, scanned) = time(&|| {
        scan(dir.path(), OntologyMode::Strict)
            .adjacency("KNOWS", Direction::Out)
            .unwrap()
    });
    let (load, loaded) = time(&|| {
        persistent(dir.path(), OntologyMode::Strict)
            .adjacency("KNOWS", Direction::Out)
            .unwrap()
    });
    assert_eq!(loaded, scanned, "identical views");
    println!("scan-build: {rebuild:?}, csr load: {load:?}");
    assert!(
        load < rebuild,
        "load ({load:?}) must beat scan-build ({rebuild:?})"
    );
}

// ---------------------------------------------------------------------------
// Session wiring: the injected provider reaches VarLenExpandExec
// ---------------------------------------------------------------------------

mod session_wiring {
    use std::sync::Mutex;

    use graphforge_exec::ExecutionSession;
    use graphforge_ir::{Binder, GraphPlan, RuntimeCatalog};
    use graphforge_storage::GraphCatalog;

    use super::*;

    fn bind(query: &str, rc: &Arc<Mutex<RuntimeCatalog>>) -> GraphPlan {
        let binder = Binder::new(None, Arc::clone(rc), OntologyMode::Exploratory);
        let ast = graphforge_cypher::parse(query).expect("parse");
        binder.bind(&ast).expect("bind")
    }

    fn session(dir: &Path, rc: &Arc<Mutex<RuntimeCatalog>>) -> ExecutionSession {
        let catalog = GraphCatalog::open(dir, None, &rc.lock().unwrap()).unwrap();
        ExecutionSession::new_with_target(
            catalog,
            None,
            dir.to_path_buf(),
            OntologyMode::Exploratory,
        )
        .unwrap()
    }

    /// Seed a 3-node KNOWS chain through the session (node CREATE plus two
    /// MATCH…CREATE edge statements — the driver's supported shapes) and
    /// return the shared runtime catalog.
    async fn seeded_chain(dir: &Path) -> Arc<Mutex<RuntimeCatalog>> {
        let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
        let create = bind("CREATE (:P {name: 'a'})", &rc);
        session(dir, &rc).execute_create(&create).await.unwrap();
        for stmt in [
            "MATCH (a:P {name: 'a'}) CREATE (a)-[:KNOWS]->(b:P {name: 'b'})",
            "MATCH (b:P {name: 'b'}) CREATE (b)-[:KNOWS]->(c:P {name: 'c'})",
        ] {
            let plan = bind(stmt, &rc);
            session(dir, &rc)
                .execute_write_statement(&plan)
                .await
                .unwrap();
        }
        rc
    }

    /// Only the session-injected persistent provider can report `hit`, so an
    /// `adjacency=hit` line in the physical explain pins the whole
    /// SessionConfig-extension wiring end to end.
    #[tokio::test]
    async fn explain_physical_shows_hit_over_built_index() {
        let dir = TempDir::new().unwrap();
        let rc = seeded_chain(dir.path()).await;

        let plan = bind("MATCH (a:P)-[:KNOWS*1..2]->(b:P) RETURN 1 AS one", &rc);
        let before = session(dir.path(), &rc)
            .explain_physical(&plan)
            .await
            .unwrap();
        assert!(
            before.contains("adjacency=building"),
            "no capability dir yet: {before}"
        );

        build_adjacency_index(dir.path(), TS).unwrap();
        let after = session(dir.path(), &rc)
            .explain_physical(&plan)
            .await
            .unwrap();
        assert!(after.contains("adjacency=hit"), "got: {after}");
    }

    /// Result identity across the hit/fallback boundary: the same traversal
    /// returns the same rows before the index exists, with it fresh, and with
    /// it stale.
    #[tokio::test]
    async fn traversal_results_identical_before_and_after_index_build() {
        let dir = TempDir::new().unwrap();
        let rc = seeded_chain(dir.path()).await;
        let plan = bind(
            "MATCH (a:P {name: 'a'})-[:KNOWS*1..2]->(b:P) RETURN 1 AS one",
            &rc,
        );

        let rows = |result: &graphforge_exec::ExecutionResult| result.stats.rows_produced;
        let before = session(dir.path(), &rc).execute_plan(&plan).await.unwrap();
        assert_eq!(rows(&before), 2, "a reaches b (1 hop) and c (2 hops)");

        build_adjacency_index(dir.path(), TS).unwrap();
        let on_hit = session(dir.path(), &rc).execute_plan(&plan).await.unwrap();
        assert_eq!(rows(&on_hit), rows(&before), "hit path identical");

        // Another write makes the index stale; results must not change.
        let extend = bind("CREATE (:P {name: 'd'})", &rc);
        session(dir.path(), &rc)
            .execute_create(&extend)
            .await
            .unwrap();
        let on_stale = session(dir.path(), &rc).execute_plan(&plan).await.unwrap();
        assert_eq!(rows(&on_stale), rows(&before), "stale fallback identical");
    }

    /// CodeRabbit regression (#824): one long-lived session that reads
    /// (caching a loaded view), writes (bumping the generation), and reads
    /// again must observe the post-write adjacency — successful writes
    /// invalidate the session provider's memoized state and cache.
    #[tokio::test]
    async fn same_session_read_write_read_observes_the_write() {
        let dir = TempDir::new().unwrap();
        let rc = seeded_chain(dir.path()).await;
        build_adjacency_index(dir.path(), TS).unwrap();

        let plan = bind(
            "MATCH (a:P {name: 'a'})-[:KNOWS*1..3]->(b:P) RETURN 1 AS one",
            &rc,
        );
        let session = session(dir.path(), &rc);

        let before = session.execute_plan(&plan).await.unwrap();
        assert_eq!(before.stats.rows_produced, 2, "a reaches b and c");

        // Extend the chain THROUGH THE SAME SESSION: c -> d.
        let extend = bind(
            "MATCH (c:P {name: 'c'}) CREATE (c)-[:KNOWS]->(d:P {name: 'd'})",
            &rc,
        );
        session.execute_write_statement(&extend).await.unwrap();

        let after = session.execute_plan(&plan).await.unwrap();
        assert_eq!(
            after.stats.rows_produced, 3,
            "the same session must see the new edge (a now also reaches d)"
        );
    }
}

/// #830: a zero-hop-only traversal reads NO edge bytes at all — a real
/// `*0..0` query through the session succeeds with the index built and the
/// edge files DELETED (the empty traversed-id set never opens the file).
#[tokio::test]
async fn zero_hop_traversal_on_hit_never_opens_edge_files() {
    use std::sync::Mutex;

    use graphforge_exec::ExecutionSession;
    use graphforge_ir::{Binder, RuntimeCatalog};
    use graphforge_storage::GraphCatalog;

    let dir = TempDir::new().unwrap();
    let rc = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let bind = |query: &str| {
        let binder = Binder::new(None, Arc::clone(&rc), OntologyMode::Advisory);
        binder
            .bind(&graphforge_cypher::parse(query).expect("parse"))
            .expect("bind")
    };
    let session = || {
        let catalog = GraphCatalog::open(dir.path(), None, &rc.lock().unwrap()).unwrap();
        ExecutionSession::new_with_target(
            catalog,
            None,
            dir.path().to_path_buf(),
            OntologyMode::Advisory,
        )
        .unwrap()
    };

    let create = bind("CREATE (:Person {name: 'Alice'})");
    session().execute_create(&create).await.unwrap();
    let edge =
        bind("MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(b:Person {name: 'Bob'})");
    session().execute_write_statement(&edge).await.unwrap();
    build_adjacency_index(dir.path(), TS).unwrap();

    // Delete the edge files WITHOUT bumping the generation: only a traversal
    // that truly reads zero edge bytes can succeed now.
    std::fs::remove_dir_all(dir.path().join("topology").join("edges")).unwrap();

    let zero_hop = bind("MATCH (a:Person {name: 'Alice'})-[r:KNOWS*0..0]->(b) RETURN 1 AS one");
    let result = session().execute_plan(&zero_hop).await.unwrap();
    assert_eq!(result.stats.rows_produced, 1, "the 0-hop self path");
}

// ---------------------------------------------------------------------------
// Shared facade provider (#832)
// ---------------------------------------------------------------------------

/// A warm shared provider serves repeat queries from its view cache: after
/// the first load, even deleting every index file does not affect the second
/// query (nothing is re-read).
#[test]
fn shared_provider_serves_second_query_from_cache() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), TS).unwrap();

    let provider = persistent(dir.path(), OntologyMode::Strict);
    let first = provider.adjacency("KNOWS", Direction::Out).unwrap();

    // Remove the whole index AND the edge files; cache must still serve.
    std::fs::remove_dir_all(dir.path().join("indexes")).unwrap();
    std::fs::remove_dir_all(dir.path().join("topology").join("edges")).unwrap();
    let second = provider.adjacency("KNOWS", Direction::Out).unwrap();
    assert_eq!(first, second);

    // revalidate() with an unchanged generation keeps the cache too (the
    // session-construction call must not defeat the amortization).
    provider.revalidate();
    let third = provider.adjacency("KNOWS", Direction::Out).unwrap();
    assert_eq!(first, third);
}

/// An external topology write (generation bump) is observed by the next
/// session: revalidate drops the memoized state + cache.
#[test]
fn revalidate_drops_cache_on_external_generation_bump() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), TS).unwrap();

    let provider = persistent(dir.path(), OntologyMode::Strict);
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Hit
    );
    provider.adjacency("KNOWS", Direction::Out).unwrap();

    // Simulate an external writer: bump the counter directly.
    force_stale_generation_fixture(dir.path());

    // Without revalidation the memoized state still says Hit…
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Hit,
        "within-query memoization is intentional"
    );
    // …revalidate (what a new session does) observes the bump.
    provider.revalidate();
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Miss,
        "stale after the external bump"
    );
}

/// A stale (`fresh: false`) index repaired in place at the *same* topology
/// generation — what `forge.index("adjacency")` does after a write — is
/// observed by the next session. Regression for #832: the pre-#832 per-query
/// provider re-read state every query and would pick this up; the shared
/// provider must not pin a `fresh: false` state across a same-generation
/// rebuild (CodeRabbit #835), or single-hop lowers to a join forever.
#[test]
fn revalidate_picks_up_same_generation_rebuild() {
    let dir = TempDir::new().unwrap();
    // A DELETE breaks the chain so the index is genuinely stale to begin with.
    stale_via_delete(dir.path());

    // The shared provider memoizes the stale state (fresh: false → Miss).
    let provider = persistent(dir.path(), OntologyMode::Strict);
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Miss
    );

    // Rebuild in place — the topology generation does NOT change.
    build_adjacency_index(dir.path(), TS).unwrap();

    // The memoized fresh:false state still says Miss within the query…
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Miss,
        "within-query memoization is intentional"
    );
    // …but the next session's revalidate must drop it and observe the repair.
    provider.revalidate();
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Hit,
        "same-generation in-place rebuild must be picked up"
    );
}

/// #765: a post-build CREATE is served from the base CSR + delta chain WITHOUT
/// a rebuild — the view matches scan-build for every direction, and the
/// manifest still records the base generation (proving the overlay served).
#[test]
fn delta_overlay_serves_new_edges_without_rebuild() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());
    build_adjacency_index(dir.path(), TS).unwrap();
    let base_gen = graphforge_storage::read_topology_generation(dir.path()).unwrap();

    // New KNOWS edge through the writer ⇒ a delta segment is written.
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    let (x, y) = (new_v7(), new_v7());
    w.create_node(x, PERSON).unwrap();
    w.create_node(y, PERSON).unwrap();
    w.create_edge(new_v7(), "KNOWS", &x, &y).unwrap();
    w.flush().unwrap();
    assert!(graphforge_storage::read_topology_generation(dir.path()).unwrap() > base_gen);

    let provider = persistent(dir.path(), OntologyMode::Strict);
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Hit,
        "base + chain serves a Hit, not a rebuild"
    );
    for direction in [Direction::Out, Direction::In, Direction::Undirected] {
        assert_eq!(
            provider.adjacency("KNOWS", direction).unwrap(),
            scan(dir.path(), OntologyMode::Strict)
                .adjacency("KNOWS", direction)
                .unwrap(),
            "overlay must equal scan-build for {direction:?}"
        );
    }
    // The index was NOT rebuilt: the manifest still records the base generation.
    let manifest = graphforge_storage::adjacency::read_manifest(dir.path()).unwrap();
    assert!(
        manifest.iter().all(|r| r.topology_generation == base_gen),
        "served from the base+chain overlay, not a rebuild"
    );
}

/// An index built AFTER the provider first observed `Absent` is picked up by
/// the next revalidation.
#[test]
fn revalidate_picks_up_externally_built_index() {
    let dir = TempDir::new().unwrap();
    write_diamond(dir.path());

    let provider = persistent(dir.path(), OntologyMode::Strict);
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Building
    );

    build_adjacency_index(dir.path(), TS).unwrap();
    provider.revalidate();
    assert_eq!(
        provider.status("KNOWS", Direction::Out),
        AdjacencyStatus::Hit
    );
}
