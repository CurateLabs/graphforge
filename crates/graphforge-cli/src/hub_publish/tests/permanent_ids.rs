use super::*;
use graphforge_discovery::ItemKind;

const IDS: [&str; 2] = ["ont_aaaaaaaaaaaaaaaa", "ont_bbbbbbbbbbbbbbbb"];

fn adopt(graph: &mut GraphForge, id: &str, version: &str) {
    let candidate = graph.create_ontology_module(serde_json::from_value(json!({
        "ontology_id":format!("https://graphforge.sh/{id}"),"version":version,"entity_types":[{"name":"Thing"}]
    })).unwrap(), Vec::new(), None).unwrap();
    let state = graph.ontology_authority_state().unwrap();
    graph
        .adopt_ontology_module(
            &graphforge_api::ModuleAdoptionRequest {
                authority: graphforge_api::OntologyAuthorityExpectation {
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
}

fn recording_publish(
    hub: &LoopbackHub,
    graph: &mut GraphForge,
    arguments: &PublishArgs,
    token: PublishToken,
) -> Result<(), GfError> {
    publish_and_report(
        &hub.transport(),
        graph,
        arguments,
        &mut PublishEnvironment {
            bypass: None,
            credential: Credential::Token(token),
            wait: &|_| {},
            notices: &mut Vec::new(),
            resolution: None,
        },
        true,
        &mut Vec::new(),
    )
}

#[test]
fn refused_ids_render_local_actions_and_retain_private_authenticated_receipts() {
    verify_private_refusal_receipts(false);
}

#[test]
fn asynchronous_refusals_retain_the_same_private_receipts() {
    verify_private_refusal_receipts(true);
}

fn verify_private_refusal_receipts(large_objects: bool) {
    let root = tempfile::tempdir().unwrap();
    let owner_path = root.path().join("owner");
    let mut owner = GraphForge::new(owner_path.to_str()).unwrap();
    for id in IDS {
        adopt(&mut owner, id, "1");
    }
    create_branch(&mut owner, "main", current());
    let mut config = ReferenceHubConfig {
        base_url: HUB.into(),
        large_objects,
        ..Default::default()
    };
    config.limits.preferred_chunk_bytes = large_objects.then_some(config.limits.max_chunk_bytes);
    let hub = LoopbackHub::with_config(config);
    publish_ok(&hub, &owner, &on_ref("orgsecret/source", "main"))
        .result
        .unwrap();

    let foreign_path = root.path().join("foreign");
    let mut foreign = GraphForge::new(foreign_path.to_str()).unwrap();
    for id in IDS {
        adopt(&mut foreign, id, "2");
    }
    let (_, version) = create_branch(&mut foreign, "main", current());
    let before = foreign.publication_content_identity().unwrap();
    let version_before = foreign.research_version_retention().unwrap().versions;
    for (visible, publish_as) in [(false, false), (true, false), (true, true), (false, true)] {
        let credential = token(&hub, "alice/copy");
        for id in IDS {
            hub.hub
                .set_item_permissions(credential.expose(), id, visible, publish_as)
                .unwrap();
        }
        let arguments = PublishArgs {
            operation_uuid: Some(Uuid::now_v7().to_string()),
            ..on_ref("alice/copy", "main")
        };
        let error = recording_publish(&hub, &mut foreign, &arguments, credential).unwrap_err();
        assert_eq!(error_code(&error), "hub.publish.id_taken");
        let message = error.to_string();
        for id in IDS {
            assert!(
                message.contains(&format!("gf ontology regenerate-id {id}")),
                "{message}"
            );
        }
        assert_eq!(message.contains("publish as orgsecret"), visible);
        assert_eq!(
            message.contains("you can publish as this organization"),
            publish_as && visible
        );
        assert_eq!(
            message.contains("publish as the owning organization"),
            publish_as && !visible
        );
        assert_eq!(crate::error_exit_code(&error.into()), 2);
        let state = foreign.publish_id_state().unwrap();
        let receipt = state
            .id_taken
            .values()
            .find(|receipt| {
                receipt.publish_operation_uuid.to_string()
                    == arguments.operation_uuid.as_deref().unwrap()
            })
            .unwrap();
        assert_eq!(receipt.version_uuid, version);
        assert_eq!(receipt.repository_url, repository_url("alice/copy"));
        assert_eq!(
            receipt
                .items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            IDS
        );
        for item in &receipt.items {
            assert_eq!(item.kind, ItemKind::OntologyModule);
            assert_eq!(item.owner.as_deref(), visible.then_some("orgsecret"));
            assert_eq!(item.publish_as_owner, publish_as.then_some(true));
        }
        assert_eq!(foreign.publication_content_identity().unwrap(), before);
        // Rerun the same Hub operation after recording moved CURRENT. It still
        // refuses the ID, rather than treating tracking as changed publication intent.
        let credential = token(&hub, "alice/copy");
        for id in IDS {
            hub.hub
                .set_item_permissions(credential.expose(), id, visible, publish_as)
                .unwrap();
        }
        let generation_before_replay = generation(&foreign);
        let error = recording_publish(&hub, &mut foreign, &arguments, credential).unwrap_err();
        assert_eq!(error_code(&error), "hub.publish.id_taken");
        assert_eq!(generation(&foreign), generation_before_replay);
    }
    assert_eq!(
        foreign.research_version_retention().unwrap().versions,
        version_before
    );
    let state = foreign.publish_id_state().unwrap();
    drop(foreign);
    let mut foreign = GraphForge::new(foreign_path.to_str()).unwrap();
    assert_eq!(foreign.publish_id_state().unwrap(), state);

    // Explicit research export and native import never carry private refusals.
    let package = root.path().join("export");
    foreign
        .export_research(
            &ExportResearchRequest {
                version_uuid: version,
                output: package.clone(),
                bundled: false,
                projection: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let destination = root.path().join("imported");
    GraphForge::import_portable(
        &destination,
        &graphforge_api::PortableImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    let imported = GraphForge::new(destination.to_str()).unwrap();
    assert!(imported.publish_id_state().unwrap().is_empty());
    for archive in imported
        .research_version_retention()
        .unwrap()
        .interchange
        .values()
    {
        let json = serde_json::to_string(archive).unwrap();
        assert!(!json.contains("orgsecret"));
        assert!(!json.contains("publish_ids"));
        assert!(!json.contains("publish_as_owner"));
    }
    // The owning publisher can publish this new authored version. Neither the
    // discovery documents nor its clonable packages expose local refusal hints.
    recording_publish(
        &hub,
        &mut foreign,
        &on_ref("orgsecret/accepted", "main"),
        token(&hub, "orgsecret/accepted"),
    )
    .unwrap();
    let (manifest, lineage) = served_lineage(&hub, "orgsecret/accepted");
    for json in [
        serde_json::to_string(&manifest).unwrap(),
        serde_json::to_string(&lineage).unwrap(),
    ] {
        assert!(!json.contains("publish_ids"));
        assert!(!json.contains("id_taken"));
        assert!(!json.contains("publish_as_owner"));
    }
    let cloned = root.path().join("cloned");
    clone_into(&hub, "orgsecret/accepted", &cloned, Some("main"), None);
    assert!(
        GraphForge::new(cloned.to_str())
            .unwrap()
            .publish_id_state()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn old_version_refusal_points_to_the_regenerated_version_and_ignores_hub_text() {
    let item = graphforge_api::PublishIdTakenItem {
        kind: ItemKind::OntologyModule,
        id: IDS[0].into(),
        owner: None,
        publish_as_owner: None,
    };
    let operation = Uuid::now_v7();
    let target = Uuid::now_v7();
    let relation = graphforge_api::OntologyIdRegeneration {
        operation_uuid: operation,
        kind: item.kind,
        old_id: item.id.clone(),
        new_id: "ont_cccccccccccccccc".into(),
        source_version_uuid: Uuid::now_v7(),
        version_uuid: target,
    };
    let state = graphforge_api::PublishIdState {
        regenerations: BTreeMap::from([(operation, relation)]),
        ..Default::default()
    };
    let error = render_id_taken(std::slice::from_ref(&item), &state);
    assert!(
        error
            .to_string()
            .contains(&format!("publish from the regenerated Version {target}"))
    );
    assert!(!error.to_string().contains("regenerate-id"));
    let graph = GraphForge::new(None).unwrap();
    let context = graphforge_api::PublishIdTakenReceipt {
        publish_operation_uuid: operation,
        repository_url: repository_url("alice/copy"),
        version_uuid: Uuid::now_v7(),
        items: vec![item.clone()],
    };
    let response = HubResponse {
        status: 409,
        headers: Vec::new(),
        body: serde_json::to_vec(
            &json!({"code":"id_taken","message":"TOKEN_SECRET_MUST_NEVER_RENDER","items":[item]}),
        )
        .unwrap(),
    };
    let mut refusal = None;
    let error = publication_failure(&response, &graph, &context, &mut refusal);
    assert!(!error.to_string().contains("TOKEN_SECRET"));
    assert!(refusal.is_some());
    let mut outside = context.clone();
    outside.items.clear();
    let mut refusal = None;
    let error = publication_failure(&response, &graph, &outside, &mut refusal);
    assert_eq!(error_code(&error), "hub.publish.integrity_failure");
    assert!(refusal.is_none());
    let optional = HubResponse {
        status: 409,
        headers: Vec::new(),
        body: serde_json::to_vec(&json!({"code":"id_taken","message":"TOKEN_SECRET"})).unwrap(),
    };
    let error = publication_failure(&optional, &graph, &context, &mut refusal);
    assert!(error.to_string().contains("gf ontology regenerate-id <id>"));
    assert!(!error.to_string().contains("TOKEN_SECRET"));
    assert!(refusal.is_none());
}
