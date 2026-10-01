//! #1688: print the plan each #1619 read-path candidate chooses for the
//! Graph500 ladder queries, for the protocol's path verification (§6.2 of
//! docs/development/cypher-read-path-inventory.md).
//!
//! `GF_READ_PATH_CANDIDATE=<candidate> cargo run -p graphforge-api \
//!   --features read-path-experiment --example read_path_explain -- <project>`

use graphforge_api::GraphForge;

const QUERIES: [(&str, &str); 4] = [
    ("recount-nodes", "MATCH (n) RETURN count(n)"),
    ("recount-edges", "MATCH ()-[r]->() RETURN count(r)"),
    (
        "one-hop",
        "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000",
    ),
    (
        "two-hop",
        "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000",
    ),
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let project = std::env::args()
        .nth(1)
        .ok_or("usage: read_path_explain <project>")?;
    let forge = GraphForge::new(Some(&project))?;
    for (name, query) in QUERIES {
        println!("== {name}: {query}\n{}\n", forge.explain(query)?);
    }
    Ok(())
}
