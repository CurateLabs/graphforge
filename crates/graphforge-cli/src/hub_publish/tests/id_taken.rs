use super::*;
use graphforge_api::{
    ModuleAdoptionRequest, ModuleDeleteRequest, ModuleSelector, OntologyAuthorityExpectation,
};
use graphforge_discovery::{ItemId, ItemKind};
use graphforge_hub_publish::IdTakenItem;

const ID: &str = "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa";
const NEW: &str = "https://graphforge.sh/ont_bbbbbbbbbbbbbbbb";
const SERVER_TEXT: &str = "untrusted-server-instructions";

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

fn adopt(graph: &mut GraphForge, id: &str, version: &str) -> graphforge_api::OntologyModuleId {
    let document = serde_json::from_value(
        json!({"ontology_id": id, "version": version, "entity_types": [{"name": "Thing"}]}),
    )
    .unwrap();
    let candidate = graph
        .create_ontology_module(document, Vec::new(), None)
        .unwrap();
    let id = candidate.id.clone();
    graph
        .adopt_ontology_module(
            &ModuleAdoptionRequest {
                authority: authority(graph),
                candidate,
            },
            None,
        )
        .unwrap();
    id
}

fn project(version: &str) -> (tempfile::TempDir, GraphForge, Uuid) {
    let root = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    adopt(&mut graph, ID, version);
    let (_, version) = create_branch(&mut graph, "main", current());
    (root, graph, version)
}

/// Alter only server-controlled prose; all transport and admission stays real.
struct ServerTextTransport(LoopbackTransport);
impl Transport for ServerTextTransport {
    fn get(
        &self,
        url: &Url,
        range: Option<u64>,
        if_range: Option<&str>,
        limit: u64,
    ) -> Result<HttpResponse, GfError> {
        self.0.get(url, range, if_range, limit)
    }
    fn get_discovery(
        &self,
        url: &Url,
        headers: &[(&str, ureq::http::HeaderValue)],
        limit: u64,
    ) -> Result<HttpResponse, GfError> {
        self.0.get_discovery(url, headers, limit)
    }
    fn exchange(&self, request: &HubRequest, limit: usize) -> Result<HubResponse, GfError> {
        let mut response = self.0.exchange(request, limit)?;
        if response.status == 409 {
            let mut value: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
            if value["code"] == "id_taken" {
                value["message"] = json!(format!(
                    "{SERVER_TEXT} {}",
                    request.header("authorization").unwrap_or("")
                ));
                response.body = serde_json::to_vec(&value).unwrap();
            }
        }
        Ok(response)
    }
}

fn refused(hub: &LoopbackHub, graph: &GraphForge, args: &PublishArgs) -> (String, String) {
    let token = token(hub, "curate/refused");
    let secret = token.expose().to_owned();
    let mut notices = Vec::new();
    let mut environment = PublishEnvironment {
        bypass: None,
        credential: Credential::Token(token),
        wait: &|_| panic!("small fixture must finish within one verification poll"),
        notices: &mut notices,
        resolution: None,
    };
    let error = publish(
        &ServerTextTransport(hub.transport()),
        graph,
        args,
        &mut environment,
    )
    .unwrap_err();
    assert_eq!(error_code(&error), "hub.publish.id_taken", "{error}");
    let text = error.to_string();
    assert_eq!(crate::error_exit_code(&error.into()), 2);
    assert!(!text.contains(SERVER_TEXT));
    assert!(!text.contains(&secret));
    (text, secret)
}

#[test]
fn sync_and_async_refusals_persist_filtered_metadata_and_client_owned_guidance() {
    for asynchronous in [false, true] {
        let mut config = ReferenceHubConfig {
            base_url: HUB.into(),
            large_objects: asynchronous,
            ..ReferenceHubConfig::default()
        };
        if asynchronous {
            config.limits.preferred_chunk_bytes = Some(MAX_CLIENT_CHUNK_BYTES);
        }
        let hub = LoopbackHub::with_config(config);
        let (_owner_root, owner, _) = project("1");
        publish_ok(&hub, &owner, &on_ref("organization/owned", "main"))
            .result
            .unwrap();
        let (_root, graph, version) = project("2");
        let arguments = on_ref("curate/refused", "main");
        for (visible, organization) in [(false, false), (true, false), (true, true)] {
            let id = ItemId::parse_reference(ID).unwrap();
            hub.hub.set_item_visibility("curate", &id, visible).unwrap();
            hub.hub
                .set_organization_publish_permission("curate", "organization", organization)
                .unwrap();
            let before = hub.log().len();
            let (text, secret) = refused(&hub, &graph, &arguments);
            let receipt = graph.id_taken_receipt(ID).unwrap().unwrap();
            assert_eq!(receipt.item.id, id.as_str());
            assert_eq!(receipt.item.kind, ItemKind::OntologyModule);
            assert_eq!(
                receipt.item.owner.as_deref(),
                visible.then_some("organization")
            );
            assert_eq!(receipt.item.publish_as_owner, organization.then_some(true));
            assert_eq!(receipt.refused_version_uuid, version);
            if organization {
                assert!(text.contains("publish as organization instead of regenerating"));
            } else {
                assert!(text.contains("gf ontology regenerate-id ont_aaaaaaaaaaaaaaaa"));
            }
            if visible {
                assert!(text.contains("publish as organization"));
            } else {
                assert!(!text.contains("organization"));
            }
            let reopened = GraphForge::new(graph.path().unwrap().to_str()).unwrap();
            assert_eq!(reopened.id_taken_receipt(ID).unwrap(), Some(receipt));
            let bytes = std::fs::read(
                graph
                    .path()
                    .unwrap()
                    .join(".publish-refusals/id-taken.json"),
            )
            .unwrap();
            let stored = String::from_utf8(bytes).unwrap();
            assert!(!stored.contains(&secret));
            assert!(!stored.contains("Bearer"));
            assert!(!stored.contains(SERVER_TEXT));
            let log = hub.log();
            let after = &log[before..];
            let commit = after
                .iter()
                .rposition(|entry| entry.method == "POST" && entry.path.ends_with("/commit"))
                .unwrap();
            assert_eq!(
                after[commit + 1..]
                    .iter()
                    .any(|entry| entry.method == "GET"
                        && entry.path.contains("/publish/operations/")),
                asynchronous
            );
        }
    }
}

#[test]
fn old_version_refusal_uses_the_retained_replacement_relation_after_restore_and_reopen() {
    let hub = LoopbackHub::start();
    let (_owner_root, owner, _) = project("1");
    publish_ok(&hub, &owner, &on_ref("organization/owned", "main"))
        .result
        .unwrap();
    let (_root, mut graph, base) = project("2");
    refused(&hub, &graph, &on_ref("curate/refused", "main"));
    let old = graph.ontology_modules().unwrap()[0].id.clone();
    graph
        .delete_ontology_module(
            &ModuleDeleteRequest {
                authority: authority(&graph),
                selector: ModuleSelector::Exact(old),
            },
            None,
        )
        .unwrap();
    adopt(&mut graph, NEW, "2");
    // Exercise only the receipt seam after explicit native identity changes;
    // the regenerate command and reference-rewrite operation are separate work.
    let (_, replacement) = create_branch(&mut graph, "replacement", current());
    graph
        .record_item_id_regeneration(ID, NEW, replacement)
        .unwrap();
    // Publish the new identity, creating the repository needed by explicit
    // --version-uuid publication. Its old ancestor is lineage-only.
    publish_ok(&hub, &graph, &on_ref("curate/refused", "replacement"))
        .result
        .unwrap();
    // Restore only the authored ontology inventory, preserving retained
    // research and the replacement Version. Whole-Project checkpoint revert
    // intentionally refuses to rewind research history.
    let new = graph.ontology_modules().unwrap()[0].id.clone();
    graph
        .delete_ontology_module(
            &ModuleDeleteRequest {
                authority: authority(&graph),
                selector: ModuleSelector::Exact(new),
            },
            None,
        )
        .unwrap();
    adopt(&mut graph, ID, "2");
    let path = graph.path().unwrap().to_owned();
    drop(graph);
    let graph = GraphForge::new(path.to_str()).unwrap();
    let (text, _) = refused(
        &hub,
        &graph,
        &PublishArgs {
            version_uuid: Some(base.to_string()),
            operation_uuid: Some(Uuid::now_v7().to_string()),
            ..args("curate/refused")
        },
    );
    assert!(text.contains(&replacement.to_string()));
    assert!(text.contains("ont_bbbbbbbbbbbbbbbb"));
    assert!(text.contains("publish from that regenerated Version"));
    assert!(!text.contains("gf ontology regenerate-id"));
}

#[test]
fn malformed_details_credential_echo_and_unrelated_ids_never_authorize_receipts() {
    let token = PublishToken::new("gfp_privatecredential".into());
    let capture = refusals::Capture::default();
    capture.select(Uuid::now_v7());
    for items in [
        json!([{"kind": "ontology_module", "id": "ont_aaaaaaaaaaaaaaaa", "publish_as_owner": false}]),
        json!([{"kind": "bridge_set", "id": "ont_aaaaaaaaaaaaaaaa"}]),
        json!([{"kind": "ontology_module", "id": "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa"}]),
    ] {
        let response = HubResponse {
            status: 409,
            headers: Vec::new(),
            body: serde_json::to_vec(
                &json!({"code": "id_taken", "message": SERVER_TEXT, "items": items}),
            )
            .unwrap(),
        };
        capture.observe(&response, &token, None).unwrap();
        assert!(capture.take().is_none());
        assert_ne!(error_code(&hub_failure(&response)), "hub.publish.id_taken");
    }
    for (secret, bypass) in [
        (token.expose(), None),
        ("private-bypass-secret", Some("private-bypass-secret")),
    ] {
        let response = HubResponse {
            status: 409,
            headers: Vec::new(),
            body: serde_json::to_vec(&json!({
                "code": "id_taken", "message": SERVER_TEXT,
                "items": [{"kind": "ontology_module", "id": "ont_aaaaaaaaaaaaaaaa", "owner": secret}]
            })).unwrap(),
        };
        let error = capture.observe(&response, &token, bypass).unwrap_err();
        assert!(!error.to_string().contains(secret));
        assert!(capture.take().is_none());
    }
    let root = tempfile::tempdir().unwrap();
    let graph = GraphForge::new(root.path().join("project").to_str()).unwrap();
    let error = refusals::record(
        &graph,
        Uuid::now_v7(),
        &[IdTakenItem {
            kind: ItemKind::OntologyModule,
            id: "ont_aaaaaaaaaaaaaaaa".into(),
            owner: None,
            publish_as_owner: None,
        }],
    );
    assert_eq!(error_code(&error), "hub.publish.invalid_input");
    assert!(graph.id_taken_receipt(ID).unwrap().is_none());
}
