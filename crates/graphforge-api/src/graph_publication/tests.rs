use super::*;
use crate::ProcedureDefinition;
use std::path::PathBuf;

#[test]
fn clear_repopulation_does_not_reuse_private_adjacency_at_same_generation() {
    use graphforge_exec::AdjacencyProvider;
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute("CREATE (:Person)-[:KNOWS]->(:Person)")
        .unwrap();
    let retained = graph
        .adjacency_provider_for_session()
        .adjacency("KNOWS", graphforge_ir::Direction::Out)
        .unwrap();
    let generation =
        graphforge_storage::generation::read_topology_generation(&graph.dir()).unwrap();
    graph.clear().unwrap();
    graph
        .execute("CREATE (:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(:Person)")
        .unwrap();
    assert_eq!(
        graphforge_storage::generation::read_topology_generation(&graph.dir()).unwrap(),
        generation
    );
    let count = graph
        .execute("MATCH ()-[r]->() RETURN count(r) AS n")
        .unwrap();
    let count = count.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 2);
    assert_eq!(
        graph
            .execute("MATCH ()-[r:KNOWS]->() RETURN r")
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        2
    );
    // The old view has not touched a shard yet. Its first lazy read must
    // still see the old graph after a different CSR has been published.
    assert_eq!(retained.neighbors(1).unwrap().len(), 1);
    assert!(retained.neighbors(2).unwrap().is_empty());
}

#[cfg(unix)]
#[test]
fn clear_resets_in_memory_state_after_partial_filesystem_failure() {
    use std::os::unix::fs::PermissionsExt;

    struct PermissionGuard {
        path: PathBuf,
        original: Option<std::fs::Permissions>,
    }

    impl PermissionGuard {
        fn restore(&mut self) {
            if let Some(original) = self.original.take() {
                std::fs::set_permissions(&self.path, original)
                    .expect("restore fixture directory permissions");
            }
        }
    }

    impl Drop for PermissionGuard {
        fn drop(&mut self) {
            self.restore();
        }
    }

    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {name: 'Alice'})")
        .expect("seed fixture files and runtime catalog");
    gf.register_procedure(ProcedureDefinition {
        name: "test.fixture".into(),
        inputs: vec![],
        outputs: vec![],
        rows: vec![vec![]],
    })
    .expect("register fixture procedure");

    let original = std::fs::metadata(&gf.dir())
        .expect("fixture directory metadata")
        .permissions();
    let mut restricted = original.clone();
    restricted.set_mode(0o500);
    std::fs::set_permissions(&gf.dir(), restricted)
        .expect("restrict fixture directory permissions");
    let mut guard = PermissionGuard {
        path: gf.dir().to_path_buf(),
        original: Some(original),
    };

    let error = gf
        .clear()
        .expect_err("filesystem cleanup must report the permission failure");
    guard.restore();

    assert!(matches!(error, GfError::Storage(_)));
    let catalog = gf.runtime_catalog.lock().expect("runtime catalog lock");
    assert!(catalog.entity_types().is_empty());
    assert!(catalog.relation_types().is_empty());
    assert_eq!(catalog.property_names().count(), 0);
    drop(catalog);
    assert!(
        gf.execute("CALL test.fixture()").is_err(),
        "procedure registry must reset even when filesystem cleanup fails"
    );

    gf.clear()
        .expect("cleanup succeeds after permissions are restored");
}

#[test]
fn private_wire_boundary_matches_public_error_domain() {
    for encoding in ["parquet", "arrow", "json"] {
        assert!(participant_encoding(encoding).is_ok());
    }
    let encoding = participant_encoding("PARQUET").unwrap_err();
    assert_eq!(encoding.code(), "GF_VALIDATION");
    assert_eq!(
        encoding.to_string(),
        "validation error: committed participant has unsupported encoding"
    );
}
