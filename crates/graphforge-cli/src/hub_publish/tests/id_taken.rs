use super::*;
use graphforge_api::{
    ItemIdRefusalReceipt, ModuleAdoptionRequest, ModuleSelector, OntologyAuthorityExpectation,
    PrepareResearchVersionRequest, ResearchMutation, ResearchOperation,
};
use graphforge_discovery::{ItemId, ItemKind};
use graphforge_hub_publish::IdTakenItem;

const ID: &str = "https://graphforge.sh/ont_aaaaaaaaaaaaaaaa";
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
        json!({"ontology_id": id, "version": version,
            "label":"Authored things", "description":"Exact source text.", "license":"MIT OR  Apache-2.0",
            "entity_types": [{"name": "Thing"}],
            "properties":[{"owner":"Thing","name":"value","type":"utf8"}]}),
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
    graph
        .execute("CREATE (:Thing {value:'retained fork value'})")
        .unwrap();
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
        clock: &std::time::Instant::now,
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
    assert_eq!(crate::error_exit_code(&error), 2);
    for json in [false, true] {
        let mut rendered = Vec::new();
        crate::write_runtime_error(&error, json, &mut rendered, &[]).unwrap();
        let rendered = String::from_utf8(rendered).unwrap();
        assert!(rendered.contains(SERVER_TEXT));
        assert!(!rendered.contains(&secret));
        if json {
            let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();
            assert_eq!(
                value["error"]["details"]["semantic_code"],
                "hub.publish.id_taken"
            );
        }
    }
    assert!(text.contains(SERVER_TEXT));
    assert!(!text.contains(&secret));
    (text, secret)
}

#[test]
fn sync_and_async_refusals_preserve_hub_and_client_guidance_with_verified_receipts() {
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
#[allow(clippy::too_many_lines)] // one native collision, regeneration, publication and restore flow
fn regenerated_head_publishes_without_its_colliding_ancestor_and_guides_old_version_retries() {
    let hub = LoopbackHub::start();
    let (_owner_root, owner, _) = project("1");
    publish_ok(&hub, &owner, &on_ref("organization/owned", "main"))
        .result
        .unwrap();
    let (_root, mut graph, base) = project("2");
    let arguments = on_ref("curate/refused", "main");
    let old_version = graph.research_version(base).unwrap();
    let old_identity = graph.research_version_retention().unwrap().identities[&base];
    let old = graph.ontology_modules().unwrap()[0].id.clone();
    let old_composition = graph
        .research_version_portable_ontology_composition(base, PortableLimits::default())
        .unwrap()
        .unwrap();
    assert_eq!(old_composition.modules.len(), 1);
    assert_eq!(old_composition.modules[0].ontology_id, ID);
    assert_eq!(old_composition.modules[0].version, "2");
    // Retain the complete source for the later Project restore. A Branch
    // restore intentionally preserves the active ontology composition.
    let restore_source = Uuid::now_v7();
    let capture = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: restore_source,
            context_uuid: Uuid::now_v7(),
            created_at: 2,
            required_versions: BTreeSet::new(),
            label: None,
            description: None,
            author: None,
            committer: None,
        })
        .unwrap();
    graph
        .commit_research_version_operation(capture, &CancellationToken::new())
        .unwrap();
    let id = ItemId::parse_reference(ID).unwrap();
    let (text, _) = refused(&hub, &graph, &arguments);
    assert!(text.contains(&format!("gf ontology regenerate-id {id}")));
    let mut receipt = ItemIdRefusalReceipt {
        item: IdTakenItem {
            kind: ItemKind::OntologyModule,
            id: id.as_str().into(),
            owner: None,
            publish_as_owner: None,
        },
        refused_version_uuid: base,
        regenerated: None,
    };
    assert_eq!(graph.id_taken_receipt(ID).unwrap(), Some(receipt.clone()));

    let replacement = graph
        .regenerate_item_id(id.as_str(), Uuid::now_v7(), None)
        .unwrap();
    let new_id = ItemId::parse(&replacement.id).unwrap();
    assert_eq!(new_id.as_str(), replacement.id);
    assert_eq!(new_id.kind(), id.kind());
    assert_ne!(new_id, id);
    assert_ne!(replacement.version_uuid, base);
    assert!(!replacement.version_uuid.is_nil());
    let modules = graph.ontology_modules().unwrap();
    assert_eq!(modules.len(), 1);
    let new = modules[0].id.clone();
    let source = graphforge_discovery::OntologyForkOrigin::from(&old);
    let document = graph
        .inspect_ontology_module(&ModuleSelector::Exact(new.clone()))
        .unwrap()
        .doc;
    assert_eq!(document.forked_from, Some(source.clone()));
    assert_eq!(document.label.as_deref(), Some("Authored things"));
    assert_eq!(document.description.as_deref(), Some("Exact source text."));
    assert_eq!(document.license.as_deref(), Some("MIT OR  Apache-2.0"));
    assert_eq!(new.ontology_id, new_id.to_uri());
    assert_eq!(new.authored_version, old.authored_version);
    assert_ne!(new.canonical_digest, old.canonical_digest);
    let replacement_version = graph.research_version(replacement.version_uuid).unwrap();
    assert_eq!(replacement_version.parents, vec![base]);
    receipt.regenerated = Some(replacement.clone());
    assert_eq!(graph.id_taken_receipt(ID).unwrap(), Some(receipt.clone()));
    let outcome = publish_ok(&hub, &graph, &arguments).result.unwrap();
    assert_eq!(
        outcome.published.unwrap().version_uuid,
        replacement.version_uuid
    );
    let summary = super::credit::served_summary(&hub, "curate/refused");
    assert_eq!(
        summary.facts.ontology_composition.unwrap().modules[0].forked_from,
        Some(source.clone())
    );
    let clone_root = tempfile::tempdir().unwrap();
    let clone_path = clone_root.path().join("fork");
    clone_into(&hub, "curate/refused", &clone_path, Some("main"), None);
    for _ in 0..2 {
        let reopened = GraphForge::new(clone_path.to_str()).unwrap();
        assert_eq!(
            reopened
                .inspect_ontology_module(&ModuleSelector::Exact(new.clone()))
                .unwrap()
                .doc
                .forked_from,
            Some(source.clone())
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
            "retained fork value"
        );
    }
    let (manifest, lineage) = served_lineage(&hub, "curate/refused");
    let published = &manifest.ontology.as_ref().unwrap().modules;
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].id, new_id.to_uri());
    assert_eq!(published[0].version, old.authored_version);
    assert_eq!(
        published[0].content_digest.0,
        format!("sha256:{}", new.canonical_digest)
    );
    assert!(
        lineage
            .version(&base.to_string())
            .unwrap()
            .package
            .is_none()
    );
    assert!(
        lineage
            .version(&replacement.version_uuid.to_string())
            .unwrap()
            .package
            .is_some()
    );
    assert!(lineage.descends_from(&replacement.version_uuid.to_string(), &base.to_string()));

    // Restore the authored composition through the native facade, preserving
    // private refusal metadata and retained research history.
    let source = graph.research_version(restore_source).unwrap();
    graph
        .commit_research_version_operation(
            ResearchOperation {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(&graph),
                mutation: ResearchMutation::RestoreProject {
                    context_uuid: source.context_uuid,
                    source_version: source.version_uuid,
                    version_uuid: Uuid::now_v7(),
                    created_at: 3,
                    author: None,
                    committer: None,
                },
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let path = graph.path().unwrap().to_owned();
    drop(graph);
    let graph = GraphForge::new(path.to_str()).unwrap();
    assert_eq!(
        graph
            .portable_ontology_composition(PortableLimits::default())
            .unwrap(),
        Some(old_composition.clone())
    );
    assert_eq!(graph.id_taken_receipt(ID).unwrap(), Some(receipt.clone()));
    let (text, _) = refused(
        &hub,
        &graph,
        &PublishArgs {
            version_uuid: Some(base.to_string()),
            operation_uuid: Some(Uuid::now_v7().to_string()),
            ..args("curate/refused")
        },
    );
    assert!(text.contains(&replacement.version_uuid.to_string()));
    assert!(text.contains(&replacement.id));
    assert!(text.contains("publish from that regenerated Version"));
    assert!(!text.contains("gf ontology regenerate-id"));
    assert_eq!(graph.id_taken_receipt(ID).unwrap(), Some(receipt));
    assert_eq!(graph.research_version(base).unwrap(), old_version);
    assert_eq!(
        graph.research_version_retention().unwrap().identities[&base],
        old_identity
    );
    assert_eq!(
        graph
            .research_version_portable_ontology_composition(base, PortableLimits::default())
            .unwrap(),
        Some(old_composition)
    );
    assert_eq!(
        graph.research_version(replacement.version_uuid).unwrap(),
        replacement_version
    );
    let (_, after_refusal) = served_lineage(&hub, "curate/refused");
    assert_eq!(after_refusal, lineage);
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
        assert_ne!(
            core_error_code(&hub_failure(&response, &Url::parse(HUB).unwrap())),
            "hub.publish.id_taken"
        );
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
        SERVER_TEXT,
    );
    assert_eq!(core_error_code(&error), "hub.publish.invalid_input");
    assert!(graph.id_taken_receipt(ID).unwrap().is_none());
}
