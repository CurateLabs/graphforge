use graphforge_api::GraphForge;

fn create_nodes(graph: &GraphForge, count: usize) {
    let pattern = std::iter::repeat("(:Seed)")
        .take(count)
        .collect::<Vec<_>>()
        .join(",");
    graph
        .execute(&format!("CREATE {pattern}"))
        .expect("seed nodes");
}

#[test]
fn property_and_relationship_writes_do_not_decode_unrelated_node_topology() {
    let mut measured = Vec::new();
    for node_count in [16, 256] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        create_nodes(&graph, node_count);

        let set = graph
            .execute("MATCH (n:Seed) WITH n LIMIT 1 SET n.value = 1")
            .expect("property SET");
        let set_work = set.stats.write_initialization;
        assert_eq!(set_work.node_rows_decoded, 0);
        assert_eq!(set_work.node_fragments_read, 0);
        assert!(set_work.summary_bytes_read > 0);

        graph
            .execute("MATCH (n:Seed) WITH n LIMIT 1 REMOVE n.value")
            .expect("property REMOVE");

        let edge = graph
            .execute("MATCH (a:Seed), (b:Seed) WITH a, b LIMIT 1 CREATE (a)-[:LINKS]->(b)")
            .expect("relationship CREATE");
        assert_eq!(edge.stats.write_initialization.node_rows_decoded, 0);
        assert_eq!(edge.stats.write_initialization.node_fragments_read, 0);
        measured.push((node_count, set_work));
    }

    assert_eq!(measured[0].0, 16);
    assert_eq!(measured[1].0, 256);
    assert_eq!(
        measured[0].1.node_rows_decoded,
        measured[1].1.node_rows_decoded
    );
    assert_eq!(
        measured[0].1.node_fragments_read,
        measured[1].1.node_fragments_read
    );
}

#[test]
fn label_counts_remain_exact_across_clauses_delete_rollback_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().to_str().unwrap();
    let graph = GraphForge::new(Some(path)).unwrap();

    let created = graph
        .execute("CREATE (:Anchor), (:Anchor), (:Temporary:Multi)")
        .unwrap();
    assert_eq!(created.side_effects.unwrap().labels_added, 3);

    let added = graph
        .execute("MATCH (n:Anchor) SET n:Known")
        .expect("add known label to multiple nodes");
    assert_eq!(added.side_effects.unwrap().labels_added, 1);

    let multiple_clauses = graph
        .execute("MATCH (n:Anchor) SET n:Transient REMOVE n:Transient")
        .expect("label addition and removal in one statement");
    let effects = multiple_clauses.side_effects.unwrap();
    assert_eq!(effects.labels_added, 1);
    assert_eq!(effects.labels_removed, 1);

    let removed = graph
        .execute("MATCH (n:Temporary) REMOVE n:Temporary")
        .expect("remove label");
    assert_eq!(removed.side_effects.unwrap().labels_removed, 1);

    let failed = graph.execute("MATCH (n:Anchor) SET n:RolledBack DELETE n RETURN n");
    assert!(failed.is_err());
    let after_rollback = graph
        .execute("MATCH (n:Anchor) SET n:RolledBack")
        .expect("retry rolled-back label addition");
    assert_eq!(after_rollback.side_effects.unwrap().labels_added, 1);

    let deleted = graph
        .execute("MATCH (n:Multi) DELETE n")
        .expect("delete last node carrying Multi");
    assert_eq!(deleted.side_effects.unwrap().labels_removed, 1);
    drop(graph);

    let reopened = GraphForge::new(Some(path)).unwrap();
    let existing = reopened
        .execute("CREATE (:Anchor)")
        .expect("create with existing label after reopen");
    assert_eq!(existing.side_effects.unwrap().labels_added, 0);

    let now_new = reopened
        .execute("CREATE (:Temporary)")
        .expect("create label whose last node was deleted");
    assert_eq!(now_new.side_effects.unwrap().labels_added, 1);
}
