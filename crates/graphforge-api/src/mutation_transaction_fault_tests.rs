//! Publication authority parity for real Cypher and analyst mutations.
use crate::GraphForge;
use arrow::array::{Array, Float64Array};
use graphforge_core::RankOptions;
use graphforge_core::algorithms::RankAlgorithm;
use std::process::Command;

fn rank_options(write: bool) -> RankOptions {
    RankOptions {
        by: RankAlgorithm::Degree,
        via: Some("KNOWS".into()),
        directed: true,
        write_property: write.then(|| "fault_metric".into()),
    }
}

fn values(graph: &GraphForge, published: bool, score: f64) {
    let result = graph
        .execute("MATCH (n:Person) RETURN n.fault_metric ORDER BY n.name")
        .unwrap();
    assert_eq!(
        result.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        3
    );
    for batch in result.batches {
        let column = batch.column(0);
        if published {
            let column = column.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(column.null_count(), 0);
            for row in 0..column.len() {
                assert_eq!(column.value(row), score);
            }
        } else {
            assert_eq!(column.logical_null_count(), column.len());
        }
    }
}

#[test]
fn mutation_authority_fault_helper() {
    let Ok(root) = std::env::var("GF_MUTATION_AUTHORITY_ROOT") else {
        return;
    };
    let analyst = std::env::var("GF_MUTATION_AUTHORITY_ANALYST").unwrap() == "1";
    let hook = std::env::var("GRAPHFORGE_PROJECT_FAILPOINT").unwrap();
    let published = hook == "project.after_current_replace.error";
    let graph = GraphForge::new(Some(&root)).unwrap();
    let before = graphforge_storage::resolve_project_generation(std::path::Path::new(&root))
        .unwrap()
        .generation_uuid();
    let topology = graphforge_storage::read_topology_generation(&graph.dir).unwrap();
    let edges = graph
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name")
        .unwrap()
        .batches;
    // A directed cycle gives every node the same real Degree output. Derive
    // the SET value from that output rather than assuming degree conventions.
    let dry = graph.rank("Person", rank_options(false)).unwrap();
    let scores = dry
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(scores.len(), 3);
    assert_eq!(scores.null_count(), 0);
    let score = scores.value(0);
    assert!(score > 0.0);
    assert!((0..scores.len()).all(|row| scores.value(row) == score));
    {
        let catalog = graph.runtime_catalog.lock().unwrap();
        assert!(!catalog.contains_property("fault_metric", None));
        assert!(!catalog.contains_property("fault_metric", Some("Person")));
    }
    let error = if analyst {
        graph.rank("Person", rank_options(true)).unwrap_err()
    } else {
        graph
            .execute(&format!("MATCH (n:Person) SET n.fault_metric = {score:?}"))
            .unwrap_err()
    };
    if hook == "project.before_current_replace.error" {
        // Native replacement wraps and bounds the nested I/O cause. Its public
        // error retains the publication phase and authoritative commit state.
        assert_eq!(error.code(), "GF_PUBLICATION_FAILED");
        assert!(
            error.to_string().contains("phase=CURRENT committed=false"),
            "{error}"
        );
    } else {
        assert!(
            error.to_string().contains("cause=injected_failpoint"),
            "{hook}: {error}"
        );
    }
    let selected = graphforge_storage::resolve_project_generation(std::path::Path::new(&root))
        .unwrap()
        .generation_uuid();
    assert_eq!(selected != before, published, "{hook}");
    let reopened = GraphForge::new(Some(&root)).unwrap();
    for owner in [&graph, &reopened] {
        assert_eq!(*owner.current_generation_uuid.lock().unwrap(), selected);
        let catalog = owner.runtime_catalog.lock().unwrap();
        // These ownership scopes intentionally differ between the two APIs.
        assert_eq!(
            catalog.contains_property("fault_metric", if analyst { Some("Person") } else { None }),
            published
        );
        drop(catalog);
        values(owner, published, score);
        assert_eq!(
            graphforge_storage::read_topology_generation(&owner.dir).unwrap(),
            topology
        );
        let actual = owner
            .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name")
            .unwrap()
            .batches;
        assert_eq!(actual.len(), edges.len());
        for (actual, expected) in actual.iter().zip(&edges) {
            assert_eq!(actual.columns(), expected.columns());
        }
        assert_eq!(owner.node_count("Person").unwrap(), 3);
    }
}

#[test]
fn cypher_and_rank_failpoints_preserve_selected_generation_authority() {
    let mut failures = Vec::new();
    for hook in [
        "rewrite.before_intent.error",
        "rewrite.after_durable_intent.error",
        "project.before_current_replace.error",
        "project.after_current_replace.error",
    ] {
        for analyst in [false, true] {
            let root = tempfile::TempDir::new().unwrap();
            let graph = GraphForge::new(root.path().to_str()).unwrap();
            graph.execute("CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), (c:Person {name:'c'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a)").unwrap();
            drop(graph);
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "mutation_transaction_fault_tests::mutation_authority_fault_helper",
                    "--nocapture",
                ])
                .env("GF_MUTATION_AUTHORITY_ROOT", root.path())
                .env(
                    "GF_MUTATION_AUTHORITY_ANALYST",
                    if analyst { "1" } else { "0" },
                )
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", hook)
                .status()
                .unwrap();
            if !status.success() {
                failures.push(format!("{hook}, analyst={analyst}: {status}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
