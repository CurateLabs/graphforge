//! Returned publication errors must not leave a usable stale composition facade.
use super::*;

fn upgrade(graph: &GraphForge) -> CompositionChangeRequest {
    request(
        graph,
        841_551,
        composition("2", ActivationMode::Strict, &["Person"], Vec::new()),
    )
}

fn assert_person(graph: &GraphForge) {
    let result = graph
        .execute("MATCH (n:Person) RETURN count(n) AS count")
        .unwrap();
    assert_eq!(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[test]
fn returned_error_child() {
    let Ok(path) = std::env::var("GF_COMPOSITION_RETURNED_ERROR_ROOT") else {
        return;
    };
    let committed = std::env::var("GF_COMPOSITION_RETURNED_ERROR_COMMITTED").unwrap() == "true";
    let mut graph = GraphForge::new(Some(&path)).unwrap();
    let change = upgrade(&graph);
    let preview = graph
        .preview_ontology_composition_change(&change, None)
        .unwrap();
    assert!(preview.diagnostics.is_empty(), "{:?}", preview.diagnostics);
    let error = graph
        .publish_ontology_composition_change(&change, &preview, None)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains(&format!("committed={committed}")),
        "{error}"
    );
    let authority =
        graphforge_storage::resolve_project_generation(std::path::Path::new(&path)).unwrap();
    if committed {
        assert_ne!(
            authority.generation_uuid(),
            change.expected_project_generation_uuid
        );
        assert!(graph.graph_visibility.health.check().is_err());
        assert!(graph.execute("MATCH (n:Person) RETURN count(n)").is_err());
        assert!(
            graph
                .publish_ontology_composition_change(&change, &preview, None)
                .is_err()
        );
    } else {
        assert_eq!(
            authority.generation_uuid(),
            change.expected_project_generation_uuid
        );
        graph.graph_visibility.health.check().unwrap();
        assert_person(&graph);
        assert_eq!(
            graph
                .workspace_ontology_composition()
                .unwrap()
                .unwrap()
                .modules[0]
                .document
                .version,
            "1"
        );
    }
}

#[test]
fn returned_errors_preserve_authority_and_reopen_replay() {
    for committed in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_str().unwrap();
        let mut graph = GraphForge::new(Some(path)).unwrap();
        let initial = request(
            &graph,
            841_550,
            composition("1", ActivationMode::Strict, &["Person"], Vec::new()),
        );
        let preview = graph
            .preview_ontology_composition_change(&initial, None)
            .unwrap();
        graph
            .publish_ontology_composition_change(&initial, &preview, None)
            .unwrap();
        graph.execute("CREATE (:Person)").unwrap();
        let change = upgrade(&graph);
        let preview = graph
            .preview_ontology_composition_change(&change, None)
            .unwrap();
        assert!(preview.diagnostics.is_empty(), "{:?}", preview.diagnostics);
        drop(graph);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "ontology_composition_lifecycle::tests::returned_errors::returned_error_child",
                "--nocapture",
            ])
            .env("GF_COMPOSITION_RETURNED_ERROR_ROOT", path)
            .env(
                "GF_COMPOSITION_RETURNED_ERROR_COMMITTED",
                committed.to_string(),
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT_TRANSACTION",
                change.context.operation_uuid.0.to_string(),
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT",
                if committed {
                    "project.after_current_replace.error"
                } else {
                    "project.before_current_replace.error"
                },
            )
            .status()
            .unwrap();
        assert!(status.success(), "committed={committed}: {status}");
        let mut reopened = GraphForge::new(Some(path)).unwrap();
        assert_person(&reopened);
        assert_eq!(
            reopened
                .workspace_ontology_composition()
                .unwrap()
                .unwrap()
                .modules[0]
                .document
                .version,
            if committed { "2" } else { "1" }
        );
        let receipt = reopened
            .publish_ontology_composition_change(&change, &preview, None)
            .unwrap();
        assert_eq!(
            reopened
                .publish_ontology_composition_change(&change, &preview, None)
                .unwrap(),
            receipt
        );
        assert_eq!(
            receipt.project_generation_uuid,
            reopened.generation_for_read().unwrap().generation_uuid()
        );
        assert_eq!(
            receipt.composition_fingerprint,
            change.candidate.composition_fingerprint
        );
        assert_person(&reopened);
        drop(reopened);
        let reopened = GraphForge::new(Some(path)).unwrap();
        assert_person(&reopened);
        assert_eq!(
            reopened
                .workspace_ontology_composition()
                .unwrap()
                .unwrap()
                .modules[0]
                .document
                .version,
            "2"
        );
    }
}
