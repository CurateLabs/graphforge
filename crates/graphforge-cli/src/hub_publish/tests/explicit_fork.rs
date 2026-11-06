//! Explicit forks publish and clone through the existing native HTTP harness.
use super::*;
use graphforge_api::{ModuleAdoptionRequest, ModuleSelector, OntologyAuthorityExpectation};
use graphforge_discovery::{ItemId, OntologyForkOrigin};

#[test]
fn receipt_free_fork_publishes_authenticated_provenance_and_clones_real_values() {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    let old_id = "ont_aaaaaaaaaaaaaaaa";
    let old_uri = ItemId::parse(old_id).unwrap().to_uri();
    let candidate = graph
        .create_ontology_module(
            serde_json::from_value(json!({
                "ontology_id":old_uri,"version":"v1","label":"Authored things",
                "description":"Exact source text.","license":"MIT OR  Apache-2.0",
                "entity_types":[{"name":"Thing"}],
                "properties":[{"owner":"Thing","name":"value","type":"utf8"}]
            }))
            .unwrap(),
            Vec::new(),
            None,
        )
        .unwrap();
    let source = candidate.id.clone();
    let state = graph.ontology_authority_state().unwrap();
    graph
        .adopt_ontology_module(
            &ModuleAdoptionRequest {
                authority: OntologyAuthorityExpectation {
                    context: WriteContext {
                        operation_uuid: OperationId(Uuid::now_v7()),
                        actor_uuid: None,
                    },
                    expected_project_generation_uuid: state.project_generation_uuid,
                    expected_composition_fingerprint: state.composition_fingerprint,
                },
                candidate,
            },
            None,
        )
        .unwrap();
    graph
        .execute("CREATE (:Thing {value:'retained explicit fork value'})")
        .unwrap();
    let (_, base) = create_branch(&mut graph, "main", current());
    let old_version = graph.research_version(base).unwrap();
    assert!(graph.id_taken_receipt(old_id).unwrap().is_none());
    let replacement = graph
        .fork_ontology_id(old_id, Uuid::now_v7(), None)
        .unwrap();
    let new_id = ItemId::parse(&replacement.id).unwrap();
    let new = graph
        .ontology_modules()
        .unwrap()
        .into_iter()
        .find(|m| m.id.ontology_id == new_id.to_uri())
        .unwrap()
        .id;
    let origin = OntologyForkOrigin::from(&source);
    let document = graph
        .inspect_ontology_module(&ModuleSelector::Exact(new.clone()))
        .unwrap()
        .doc;
    assert_eq!(document.forked_from, Some(origin.clone()));
    assert_eq!(document.license.as_deref(), Some("MIT OR  Apache-2.0"));
    assert_eq!(graph.research_version(base).unwrap(), old_version);
    let hub = LoopbackHub::start();
    let outcome = publish_ok(&hub, &graph, &on_ref("curate/explicit-fork", "main"))
        .result
        .unwrap();
    assert_eq!(
        outcome.published.unwrap().version_uuid,
        replacement.version_uuid
    );
    let summary = credit::served_summary(&hub, "curate/explicit-fork");
    let modules = summary.facts.ontology_composition.unwrap().modules;
    assert_eq!(modules.len(), 1);
    assert_eq!(modules[0].forked_from, Some(origin.clone()));
    let (manifest, lineage) = served_lineage(&hub, "curate/explicit-fork");
    let published = &manifest.ontology.as_ref().unwrap().modules;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].id, new_id.to_uri());
    assert_eq!(published[0].version, source.authored_version);
    assert_eq!(
        published[0].content_digest.0,
        format!("sha256:{}", new.canonical_digest)
    );
    assert!(lineage.descends_from(&replacement.version_uuid.to_string(), &base.to_string()));
    let clone_path = root.path().join("cloned");
    clone_into(
        &hub,
        "curate/explicit-fork",
        &clone_path,
        Some("main"),
        None,
    );
    for _ in 0..2 {
        let reopened = GraphForge::new(clone_path.to_str()).unwrap();
        assert_eq!(
            reopened
                .inspect_ontology_module(&ModuleSelector::Exact(new.clone()))
                .unwrap()
                .doc,
            document
        );
        let values = reopened.execute("MATCH (n:Thing) RETURN n.value").unwrap();
        assert_eq!(values.batches[0].num_rows(), 1);
        assert_eq!(
            values.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0),
            "retained explicit fork value"
        );
        assert!(reopened.id_taken_receipt(old_id).unwrap().is_none());
    }
    assert!(graph.id_taken_receipt(old_id).unwrap().is_none());
}
