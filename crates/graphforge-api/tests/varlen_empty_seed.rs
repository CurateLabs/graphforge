//! Empty var-len expand must return a typed empty batch (DF54 regression / #467).
use std::collections::HashMap;

use graphforge_api::{GraphForge, IrLiteral};

fn forge_with_knows() -> (tempfile::TempDir, GraphForge) {
    let dir = tempfile::tempdir().unwrap();
    let forge = GraphForge::new(Some(dir.path().to_str().unwrap())).unwrap();
    forge
        .execute("CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})")
        .unwrap();
    (dir, forge)
}

#[test]
fn empty_seed_with_varlen_expand_returns_empty() {
    let (_dir, forge) = forge_with_knows();
    let params = HashMap::from([("canonical".into(), IrLiteral::Str("NonExistent".into()))]);
    let result = forge
        .execute_with_params(
            "MATCH (seed:Person {name: $canonical})-[*1..2]-(neighbour:Person) \
             WHERE neighbour.name <> $canonical \
             RETURN DISTINCT neighbour.name AS name, labels(neighbour) AS labels",
            &params,
        )
        .expect("empty seed var-len expand");
    assert_eq!(result.stats.rows_produced, 0);
}

#[test]
fn nonempty_varlen_still_works() {
    let (_dir, forge) = forge_with_knows();
    let params = HashMap::from([("canonical".into(), IrLiteral::Str("Alice".into()))]);
    let result = forge
        .execute_with_params(
            "MATCH (seed:Person {name: $canonical})-[*1..2]-(neighbour:Person) \
             WHERE neighbour.name <> $canonical \
             RETURN DISTINCT neighbour.name AS name, labels(neighbour) AS labels",
            &params,
        )
        .expect("nonempty neighbourhood");
    assert!(result.stats.rows_produced >= 1);
}
