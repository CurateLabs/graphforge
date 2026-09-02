use graphforge_api::GraphForge;
use graphforge_exec::demand;

const CANONICAL: &str =
    "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000";
const TYPED_ANONYMOUS: &str =
    "MATCH (a)-[:LINK]->(b)-[:LINK]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000";

#[test]
fn reproduce_ordered_two_hop_matcher() {
    let forge = GraphForge::new(None).unwrap();
    for _ in 0..64 {
        forge
            .execute("CREATE (:N)-[:LINK]->(:N)-[:LINK]->(:N)")
            .unwrap();
    }

    for query in [CANONICAL, TYPED_ANONYMOUS] {
        println!("QUERY={query}\nPLAN={}", forge.explain(query).unwrap());
        let (result, snapshot) = demand::capture(|| forge.execute(query));
        let result = result.unwrap();
        println!(
            "ROWS={} HOPS={:#?} SORTS={:#?}",
            result.stats.rows_produced, snapshot.hops, snapshot.sorts
        );
    }
}
