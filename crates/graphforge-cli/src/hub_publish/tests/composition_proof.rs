//! A selected Version and the ontology advertised for it must agree.
use super::*;
use graphforge_api::{ModuleAdoptionRequest, OntologyAuthorityExpectation, OntologyDoc};

fn module(id: &str) -> OntologyDoc {
    serde_json::from_value(json!({
        "ontology_id": id, "version": "1",
        "entity_types": [{"name": "Thing"}],
    }))
    .unwrap()
}

fn adopt(graph: &mut GraphForge, id: &str) -> graphforge_api::OntologyModuleId {
    let candidate = graph
        .create_ontology_module(module(id), Vec::new(), None)
        .unwrap();
    let identity = candidate.id.clone();
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
    identity
}

fn authority(graph: &GraphForge) -> OntologyAuthorityExpectation {
    let state = graph.ontology_authority_state().unwrap();
    OntologyAuthorityExpectation {
        context: WriteContext {
            operation_uuid: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        },
        expected_project_generation_uuid: state.project_generation_uuid,
        expected_composition_fingerprint: state.composition_fingerprint,
    }
}

fn assert_mismatch(hub: &LoopbackHub, graph: &GraphForge, arguments: &PublishArgs) {
    let before = hub.log().len();
    let run = run_publish_with(
        hub,
        graph,
        arguments,
        Credential::Token(token(hub, "curate/claims")),
    );
    let error = run
        .result
        .expect_err("different compositions must refuse publication");
    assert_eq!(
        error_code(&error),
        "hub.publish.ontology_composition_mismatch"
    );
    assert_eq!(crate::error_exit_code(&error.into()), 2);
    let document: serde_json::Value = serde_json::from_slice(&run.stderr).unwrap();
    let message = document["error"]["message"].as_str().unwrap();
    assert!(message.contains("check out"), "{message}");
    assert!(message.contains("commit"), "{message}");
    assert!(run.stdout.is_empty());
    assert!(
        hub.log()[before..]
            .iter()
            .all(|entry| entry.method != "PUT" && entry.method != "POST")
    );
}

#[test]
fn mismatched_workspace_refuses_exact_version_branch_and_receipt_replay() {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    graph.execute("CREATE (:Item {score:0})").unwrap();
    adopt(&mut graph, "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa");
    let (branch, base) = create_branch(&mut graph, "main", current());
    edit(&mut graph, branch, 1);
    let hub = LoopbackHub::start();
    let arguments = PublishArgs {
        operation_uuid: Some(Uuid::now_v7().to_string()),
        ..on_ref("curate/claims", "main")
    };
    assert!(
        !publish_ok(&hub, &graph, &arguments)
            .result
            .unwrap()
            .replayed
    );
    assert!(
        publish_ok(&hub, &graph, &arguments)
            .result
            .unwrap()
            .replayed
    );
    let (before, _) = served_lineage(&hub, "curate/claims");
    let published_modules = &before.ontology.as_ref().unwrap().modules;
    assert_eq!(published_modules.len(), 1);
    assert_eq!(
        published_modules[0].id,
        "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa"
    );
    let version_arguments = PublishArgs {
        version_uuid: Some(base.to_string()),
        operation_uuid: Some(Uuid::now_v7().to_string()),
        ..args("curate/claims")
    };
    publish_ok(&hub, &graph, &version_arguments).result.unwrap();
    adopt(&mut graph, "https://graphforge.sh/ont_bbbbbbbbbbbbbbbb");
    assert_mismatch(&hub, &graph, &version_arguments);
    assert_mismatch(
        &hub,
        &graph,
        &PublishArgs {
            version_uuid: Some(base.to_string()),
            ..args("curate/claims")
        },
    );
    assert_mismatch(&hub, &graph, &on_ref("curate/claims", "main"));
    assert_mismatch(&hub, &graph, &arguments);
    let (after, _) = served_lineage(&hub, "curate/claims");
    assert_eq!(before.ontology, after.ontology);
}

#[test]
fn changed_module_digest_refuses_even_with_the_same_id_and_authored_version() {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    graph.execute("CREATE (:Item {score:0})").unwrap();
    let prior = adopt(&mut graph, "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa");
    create_branch(&mut graph, "main", current());
    let hub = LoopbackHub::start();
    publish_ok(&hub, &graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let document = serde_json::from_value(json!({
        "ontology_id": prior.ontology_id, "version": prior.authored_version,
        "entity_types": [{"name": "Thing"}, {"name": "Other"}],
    }))
    .unwrap();
    graph
        .delete_ontology_module(
            &graphforge_api::ModuleDeleteRequest {
                authority: authority(&graph),
                selector: graphforge_api::ModuleSelector::Exact(prior.clone()),
            },
            None,
        )
        .unwrap();
    let candidate = graph
        .create_ontology_module(document, Vec::new(), None)
        .unwrap();
    assert_eq!(candidate.id.ontology_id, prior.ontology_id);
    assert_eq!(candidate.id.authored_version, prior.authored_version);
    assert_ne!(candidate.id.canonical_digest, prior.canonical_digest);
    graph
        .adopt_ontology_module(
            &ModuleAdoptionRequest {
                authority: authority(&graph),
                candidate,
            },
            None,
        )
        .unwrap();
    assert_mismatch(&hub, &graph, &on_ref("curate/claims", "main"));
}

#[test]
fn bridge_only_change_refuses_publication() {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    graph.execute("CREATE (:Item {score:0})").unwrap();
    let source = adopt(&mut graph, "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa");
    let target = adopt(&mut graph, "https://graphforge.sh/ont_bbbbbbbbbbbbbbbb");
    create_branch(&mut graph, "main", current());
    let hub = LoopbackHub::start();
    publish_ok(&hub, &graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let candidate = graph.create_ontology_bridge(serde_json::from_value(json!({
        "bridge_id": "https://graphforge.sh/brg_aaaaaaaaaaaaaaaa", "authored_version": "1",
        "source_modules": [source], "target_modules": [target],
        "assertions": [{
            "source": {"module": source, "kind": "entity", "local_id": "Thing"},
            "target": {"module": target, "kind": "entity", "local_id": "Thing"},
            "predicate": "equivalent", "directional": true,
            "provenance": {"method": "authored", "justification": "curated mapping", "evidence_refs": ["evidence:review"]}
        }]
    })).unwrap()).unwrap();
    graph
        .adopt_ontology_bridge(
            &graphforge_api::BridgeAdoptionRequest {
                authority: authority(&graph),
                candidate,
            },
            None,
        )
        .unwrap();
    assert_mismatch(&hub, &graph, &on_ref("curate/claims", "main"));
}

#[test]
fn activation_profile_change_refuses_even_with_the_same_inventory_fingerprint() {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    graph.execute("CREATE (:Item {score:0})").unwrap();
    adopt(&mut graph, "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa");
    create_branch(&mut graph, "main", current());
    let hub = LoopbackHub::start();
    publish_ok(&hub, &graph, &on_ref("curate/claims", "main"))
        .result
        .unwrap();
    let before = graph
        .ontology_authority_state()
        .unwrap()
        .composition_fingerprint;
    graph
        .change_ontology_activation_profile(
            &graphforge_api::ActivationProfileChangeRequest {
                authority: authority(&graph),
                profile_default: graphforge_api::ActivationMode::Advisory,
                activation: Vec::new(),
            },
            None,
        )
        .unwrap();
    assert_eq!(
        before,
        graph
            .ontology_authority_state()
            .unwrap()
            .composition_fingerprint
    );
    assert_mismatch(&hub, &graph, &on_ref("curate/claims", "main"));
}

#[test]
fn matching_composition_inspection_preserves_replay_and_staging() {
    const CHILD: &str = "GRAPHFORGE_TEST_COMPOSITION_STAGING_CHILD";
    let Some(root) = std::env::var_os(CHILD) else {
        let root = tempfile::tempdir().unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "hub_publish::tests::composition_proof::matching_composition_inspection_preserves_replay_and_staging",
            ])
            .env(CHILD, root.path())
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            std::fs::read(root.path().join("verified")).unwrap(),
            b"verified"
        );
        return;
    };
    let root = PathBuf::from(root);
    let mut graph = GraphForge::new(root.join("project").to_str()).unwrap();
    graph.execute("CREATE (:Item {score:0})").unwrap();
    adopt(&mut graph, "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa");
    let (_, version) = create_branch(&mut graph, "main", current());
    let hub = LoopbackHub::start();
    let stage = root.join("explicit-stage");
    std::fs::create_dir(&stage).unwrap();
    let arguments = PublishArgs {
        operation_uuid: Some(Uuid::now_v7().to_string()),
        staging_directory: Some(stage.clone()),
        ..on_ref("curate/claims", "main")
    };
    publish_ok(&hub, &graph, &arguments).result.unwrap();
    // Confine the default-temp override to this child after engine setup.
    // It must be impossible to create the historical Project the old guard opened.
    let default_temp = root.join("unavailable-default-temp");
    tempfile::env::override_temp_dir(&default_temp).unwrap();
    assert!(graph.open_research_version(version).is_err());
    let replay = run_publish_with(
        &hub,
        &graph,
        &arguments,
        Credential::Token(token(&hub, "curate/claims")),
    );
    assert!(
        replay
            .result
            .expect("matching replay must return its receipt without staging")
            .replayed
    );
    let fresh_arguments = PublishArgs {
        staging_directory: Some(stage.clone()),
        ..on_ref("curate/fresh", "main")
    };
    let before = std::fs::read_dir(&stage).unwrap().count();
    check_ontology_composition(&graph, &select(&graph, &fresh_arguments).unwrap()).unwrap();
    assert_eq!(
        std::fs::read_dir(&stage).unwrap().count(),
        before,
        "composition inspection must not allocate historical staging before preflight"
    );
    let scratch = prepare_staging(&graph, Some(&stage)).unwrap();
    assert_eq!(scratch.path().parent(), Some(stage.as_path()));
    drop(scratch);
    // The existing research export path separately requires writable default
    // temp. Restore it after proving that inspection, replay and preflight do not.
    std::fs::create_dir(&default_temp).unwrap();
    let fresh = run_publish_with(
        &hub,
        &graph,
        &fresh_arguments,
        Credential::Token(token(&hub, "curate/fresh")),
    );
    fresh
        .result
        .expect("fresh matching publication must use explicit staging");
    std::fs::write(root.join("verified"), b"verified").unwrap();
}
