use super::*;
use crate::{
    ActivationMode, BranchSource, BridgeAdoptionRequest, BridgeDocument,
    CreateResearchBranchRequest, ExecuteResearchBranchRequest, ModuleAdoptionRequest,
    SelectResearchBranchRequest,
};
use arrow::array::{Int64Array, StringArray};
use graphforge_hub_publish::IdTakenItem;
use graphforge_ontology::BridgeSetId;

mod multi_version;

const OLD: &str = "ont_aaaaaaaaaaaaaaaa";
const OTHER: &str = "ont_bbbbbbbbbbbbbbbb";
const BRIDGE: &str = "brg_aaaaaaaaaaaaaaaa";

fn uri(id: &str) -> String {
    ItemId::parse_reference(id).unwrap().to_uri()
}
fn current(graph: &GraphForge) -> Uuid {
    graph
        .committed_generation_identity()
        .unwrap()
        .generation_uuid
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
fn module(
    graph: &mut GraphForge,
    id: &str,
    other: bool,
    dependencies: Vec<crate::OntologyModuleId>,
) -> crate::OntologyModuleId {
    let document = serde_json::from_value(if other {
        serde_json::json!({"ontology_id":uri(id),"version":"1","entity_types":[{"name":"Other"}]})
    } else {
        serde_json::json!({"ontology_id":uri(id),"version":"1","entity_types":[{"name":"Thing"}],
            "relation_types":[{"name":"LINK","src":"Thing","dst":"Thing"}],
            "properties":[{"owner":"Thing","name":"name","type":"utf8"},{"owner":"LINK","name":"weight","type":"int64"}]})
    }).unwrap();
    let candidate = graph
        .create_ontology_module(document, dependencies, Some(ActivationMode::Advisory))
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
fn bridge(
    graph: &mut GraphForge,
    id: &str,
    old: &crate::OntologyModuleId,
    other: &crate::OntologyModuleId,
    dependencies: Vec<BridgeSetId>,
) -> BridgeSetId {
    let document: BridgeDocument = serde_json::from_value(serde_json::json!({
        "bridge_id":uri(id),"authored_version":"1","source_modules":[old,other],"target_modules":[old,other],"dependencies":dependencies,
        "shared_surfaces":["research_actions"], "enforcement":"advisory",
        "assertions":[
            {"source":{"module":old,"kind":"entity","local_id":"Thing"},"target":{"module":other,"kind":"entity","local_id":"Other"},"predicate":"related","directional":true,"provenance":{"method":"authored","justification":"related symbols","evidence_refs":[]}},
            {"source":{"module":other,"kind":"entity","local_id":"Other"},"target":{"module":old,"kind":"entity","local_id":"Thing"},"predicate":"related","directional":true,"provenance":{"method":"authored","justification":"reverse symbols","evidence_refs":[]}}
        ]
    })).unwrap();
    let candidate = graph.create_ontology_bridge(document).unwrap();
    let id = candidate.id.clone();
    graph
        .adopt_ontology_bridge(
            &BridgeAdoptionRequest {
                authority: authority(graph),
                candidate,
            },
            None,
        )
        .unwrap();
    let (profile_default, mut activation) = graph.ontology_activation_profile().unwrap();
    activation.push(graphforge_ontology::ActivationRecord {
        scope: graphforge_ontology::ActivationScope::Bridge,
        subject: id.display_ref(),
        mode: ActivationMode::Advisory,
    });
    graph
        .change_ontology_activation_profile(
            &crate::ActivationProfileChangeRequest {
                authority: authority(graph),
                profile_default,
                activation,
            },
            None,
        )
        .unwrap();
    id
}
fn branch(graph: &mut GraphForge, name: &str) -> (Uuid, Uuid) {
    let branch_uuid = Uuid::now_v7();
    let version_uuid = Uuid::now_v7();
    let request = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(graph),
        branch_uuid,
        version_uuid,
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: name.into(),
        description: None,
        author: None,
        committer: None,
    };
    graph
        .create_research_branch(&request, &CancellationToken::new())
        .unwrap();
    graph
        .select_research_branch(
            &SelectResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(graph),
                branch_uuid,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    (branch_uuid, version_uuid)
}
fn refusal(graph: &GraphForge, id: &str, version: Uuid, owner: bool) {
    let id = ItemId::parse_reference(id).unwrap();
    graph
        .record_publish_id_taken(
            version,
            &[IdTakenItem {
                kind: id.kind(),
                id: id.as_str().into(),
                owner: Some("organization".into()),
                publish_as_owner: owner.then_some(true),
            }],
        )
        .unwrap();
}
fn data(graph: &GraphForge) {
    let result = graph
        .execute(
            "MATCH (a:Thing)-[r:LINK]->(b:Thing) RETURN a.name AS a,b.name AS b,r.weight AS weight",
        )
        .unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    let batch = &result.batches[0];
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "Ada"
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "Bea"
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
}
struct Fixture {
    root: tempfile::TempDir,
    graph: GraphForge,
    branch: Uuid,
    base: Uuid,
}
impl Fixture {
    fn new(full: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("project");
        let mut graph = GraphForge::new(path.to_str()).unwrap();
        let old = module(&mut graph, OLD, false, vec![]);
        if full {
            let other = module(&mut graph, OTHER, true, vec![old.clone()]);
            let first = bridge(&mut graph, BRIDGE, &old, &other, vec![]);
            let second = bridge(
                &mut graph,
                "brg_bbbbbbbbbbbbbbbb",
                &old,
                &other,
                vec![first],
            );
            bridge(
                &mut graph,
                "brg_cccccccccccccccc",
                &old,
                &other,
                vec![second],
            );
            let mut metadata = graph.research_project_metadata().unwrap();
            metadata.ontologies = vec![uri(OLD)];
            graph
                .update_research_metadata(UpdateResearchMetadataRequest {
                    context: WriteContext {
                        operation_uuid: OperationId(Uuid::now_v7()),
                        actor_uuid: None,
                    },
                    metadata,
                })
                .unwrap();
        }
        graph
            .execute(
                "CREATE (a:Thing {name:'Ada'}),(b:Thing {name:'Bea'}),(a)-[:LINK {weight:42}]->(b)",
            )
            .unwrap();
        let (branch, base) = branch(&mut graph, "main");
        refusal(&graph, OLD, base, false);
        Self {
            root,
            graph,
            branch,
            base,
        }
    }
    fn path(&self) -> std::path::PathBuf {
        self.root.path().join("project")
    }
}

#[test]
fn module_regeneration_rewrites_all_references_and_stored_routes_in_one_version() {
    let mut fixture = Fixture::new(true);
    let graph = &mut fixture.graph;
    let (sibling, sibling_head) = branch(graph, "sibling");
    graph
        .select_research_branch(
            &SelectResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(graph),
                branch_uuid: fixture.branch,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    // CURRENT edits after the Branch capture belong in the regenerated head too.
    graph
        .execute("CREATE (:Thing {name:'Current only'})")
        .unwrap();
    let before = graph.workspace_ontology_composition().unwrap().unwrap();
    let old_registry = graph.research_version_retention().unwrap();
    let old_bindings =
        graphforge_storage::semantic_storage_bindings(&graph.generation_for_read().unwrap())
            .unwrap()
            .unwrap();
    let old_fields = crate::branches::baseline::read(
        graph.open_research_branch(fixture.branch).unwrap().graph(),
    )
    .unwrap();
    let operation = Uuid::now_v7();
    let result = graph.regenerate_item_id(OLD, operation, None).unwrap();
    let new_id = ItemId::parse_reference(&result.id).unwrap();
    assert_eq!(new_id.kind(), ItemKind::OntologyModule);
    assert_ne!(result.id, OLD);
    let after = graph.workspace_ontology_composition().unwrap().unwrap();
    assert_ne!(
        after.composition_fingerprint,
        before.composition_fingerprint
    );
    assert!(!serde_json::to_string(&after).unwrap().contains(&uri(OLD)));
    let old = before
        .modules
        .iter()
        .find(|module| module.id.ontology_id == uri(OLD))
        .unwrap();
    let new = after
        .modules
        .iter()
        .find(|module| module.id.ontology_id == new_id.to_uri())
        .unwrap();
    let mut expected = old.document.clone();
    expected.ontology_id = new_id.to_uri();
    assert_eq!(new.document, expected);
    assert_ne!(new.id.canonical_digest, old.id.canonical_digest);
    assert_eq!(
        after
            .modules
            .iter()
            .find(|module| module.id.ontology_id == uri(OTHER))
            .unwrap()
            .dependencies,
        vec![new.id.clone()]
    );
    for prior in &before.bridges {
        let rewritten = after
            .bridges
            .iter()
            .find(|bridge| bridge.bridge_id == prior.bridge_id)
            .unwrap();
        assert_ne!(bridge_id(prior).unwrap(), bridge_id(rewritten).unwrap());
        for dependency in &rewritten.dependencies {
            assert!(
                after
                    .bridges
                    .iter()
                    .any(|bridge| bridge_id(bridge).unwrap() == *dependency)
            );
        }
        assert!(rewritten.source_modules.contains(&new.id));
        assert!(rewritten.target_modules.contains(&new.id));
        assert_eq!(rewritten.assertions[0].source.module, new.id);
        assert_eq!(rewritten.assertions[1].target.module, new.id);
    }
    assert!(
        after
            .activation
            .iter()
            .any(|record| record.subject == new.id.display_ref())
    );
    for bridge in &after.bridges {
        assert!(
            after
                .activation
                .iter()
                .any(|record| record.subject == bridge_id(bridge).unwrap().display_ref())
        );
    }
    assert_eq!(
        graph.research_project_metadata().unwrap().ontologies,
        vec![new_id.to_uri()]
    );
    let new_bindings =
        graphforge_storage::semantic_storage_bindings(&graph.generation_for_read().unwrap())
            .unwrap()
            .unwrap();
    for prior in old_bindings.bindings {
        let expected_id = if prior.symbol.module.ontology_id == uri(OLD) {
            new.id.clone()
        } else {
            prior.symbol.module.clone()
        };
        let replacement = new_bindings
            .bindings
            .iter()
            .find(|binding| {
                binding.symbol.module == expected_id
                    && binding.symbol.kind == prior.symbol.kind
                    && binding.symbol.local_id == prior.symbol.local_id
            })
            .unwrap();
        assert_eq!(replacement.storage_id, prior.storage_id);
        if prior.symbol.module.ontology_id == uri(OLD) {
            assert_ne!(replacement.route, prior.route);
        }
    }
    data(graph);
    let count = graph.execute("MATCH (n:Thing) RETURN count(n)").unwrap();
    assert_eq!(
        count.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    let registry = graph.research_version_retention().unwrap();
    assert_eq!(registry.current_branch, Some(fixture.branch));
    assert_eq!(registry.heads[&fixture.branch], result.version_uuid);
    assert_eq!(registry.heads[&sibling], sibling_head);
    for (id, version) in old_registry.versions {
        assert_eq!(registry.versions[&id], version);
    }
    let version = &registry.versions[&result.version_uuid];
    assert_eq!(version.parents, vec![fixture.base]);
    assert!(version.content.source_version.is_some());
    assert!(registry.materialized.contains(&result.version_uuid));
    let view = graph.open_research_branch(fixture.branch).unwrap();
    data(view.graph());
    let count = view
        .graph()
        .execute("MATCH (n:Thing) RETURN count(n)")
        .unwrap();
    assert_eq!(
        count.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    let new_fields = crate::branches::baseline::read(view.graph()).unwrap();
    for row in old_fields.values() {
        let replacement = new_fields
            .values()
            .find(|next| next.contribution == row.contribution)
            .unwrap();
        assert_eq!(replacement.origin, row.origin);
        assert_eq!(replacement.incorporated, row.incorporated);
        assert_eq!(replacement.original, row.original);
        assert_eq!(replacement.baseline, row.baseline);
    }
    let old_view = crate::research_versions::materialize_version(
        graph,
        &graph.research_version(fixture.base).unwrap(),
    )
    .unwrap();
    data(&old_view);
    let count = old_view.execute("MATCH (n:Thing) RETURN count(n)").unwrap();
    assert_eq!(
        count.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    let generation = current(graph);
    assert_eq!(
        graph
            .regenerate_item_id(&OLD.to_uppercase(), operation, None)
            .unwrap(),
        result
    );
    assert_eq!(current(graph), generation);
    assert!(graph.regenerate_item_id(OTHER, operation, None).is_err());
    let path = fixture.path();
    drop(fixture.graph);
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    data(&graph);
    assert_eq!(
        graph.regenerate_item_id(OLD, operation, None).unwrap(),
        result
    );
    // A subsequent native Branch edit consumes the remapped fields and routes.
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid: fixture.branch,
                version_uuid: Uuid::now_v7(),
                query: "MATCH (a:Thing)-[r:LINK]->(b:Thing) WHERE a.name='Ada' SET a.name='Ada edited',r.weight=43".into(),
                created_at: 3,
                label: None,
                description: None,
                author: None,
                committer: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let edited = graph.open_research_branch(fixture.branch).unwrap();
    let result_data = edited
        .graph()
        .execute("MATCH (a:Thing)-[r:LINK]->(b:Thing) RETURN a.name,r.weight")
        .unwrap();
    assert_eq!(
        result_data.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "Ada edited"
    );
    assert_eq!(
        result_data.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        43
    );
    let later = current(&graph);
    assert_eq!(
        graph.regenerate_item_id(OLD, operation, None).unwrap(),
        result
    );
    assert_eq!(current(&graph), later);
}

#[test]
fn bridge_regeneration_cascades_dependencies_activation_and_reopens_data() {
    let mut fixture = Fixture::new(true);
    refusal(&fixture.graph, BRIDGE, fixture.base, false);
    let before = fixture
        .graph
        .workspace_ontology_composition()
        .unwrap()
        .unwrap();
    let result = fixture
        .graph
        .regenerate_item_id(BRIDGE, Uuid::now_v7(), None)
        .unwrap();
    assert_eq!(
        ItemId::parse_reference(&result.id).unwrap().kind(),
        ItemKind::BridgeSet
    );
    let after = fixture
        .graph
        .workspace_ontology_composition()
        .unwrap()
        .unwrap();
    assert_eq!(before.modules, after.modules);
    assert_ne!(
        before.composition_fingerprint,
        after.composition_fingerprint
    );
    assert!(
        !serde_json::to_string(&after)
            .unwrap()
            .contains(&uri(BRIDGE))
    );
    for bridge in &after.bridges {
        for dependency in &bridge.dependencies {
            assert!(
                after
                    .bridges
                    .iter()
                    .any(|entry| bridge_id(entry).unwrap() == *dependency)
            );
        }
    }
    let path = fixture.path();
    drop(fixture.graph);
    let graph = GraphForge::new(path.to_str()).unwrap();
    data(&graph);
    assert_eq!(
        graph.research_version_retention().unwrap().heads[&fixture.branch],
        result.version_uuid
    );
}

#[test]
fn receipt_guards_cancellation_and_ordinary_migration_preserve_authority() {
    let mut fixture = Fixture::new(false);
    let generation = current(&fixture.graph);
    for id in [
        OTHER,
        "brg_bbbbbbbbbbbbbbbb",
        "https://example.org/ontology",
    ] {
        assert!(
            fixture
                .graph
                .regenerate_item_id(id, Uuid::now_v7(), None)
                .is_err()
        );
        assert_eq!(current(&fixture.graph), generation);
    }
    refusal(&fixture.graph, OLD, fixture.base, true);
    let error = fixture
        .graph
        .regenerate_item_id(OLD, Uuid::now_v7(), None)
        .unwrap_err();
    assert!(error.to_string().contains("owning organization"));
    refusal(&fixture.graph, OLD, fixture.base, false);
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(
        fixture
            .graph
            .regenerate_item_id(OLD, Uuid::now_v7(), Some(&token))
            .unwrap_err()
            .code(),
        "GF_CANCELLED"
    );
    assert_eq!(current(&fixture.graph), generation);
    let composition = fixture
        .graph
        .workspace_ontology_composition()
        .unwrap()
        .unwrap();
    let old = &composition.modules[0];
    let mut document = old.document.clone();
    document.ontology_id = uri(OTHER);
    assert!(
        fixture
            .graph
            .preview_migrate_ontology_module(&ModuleMigrationRequest {
                authority: authority(&fixture.graph),
                selector: ModuleSelector::Exact(old.id.clone()),
                document,
                dependencies: old.dependencies.clone(),
                enforcement: None
            })
            .unwrap_err()
            .to_string()
            .contains("retain ontology_id")
    );
    let prior = fixture.graph.research_version(fixture.base).unwrap();
    assert!(
        fixture
            .graph
            .commit_research_version_operation(
                ResearchOperation {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: generation,
                    mutation: ResearchMutation::RegenerateItemId {
                        intent_sha256: [0; 32],
                        version: Box::new(prior)
                    }
                },
                &CancellationToken::new()
            )
            .unwrap_err()
            .to_string()
            .contains("native receipt")
    );
}

fn fault(binding: &RegenerationBinding) -> Option<String> {
    (std::env::var("GF_ITEM_ID_OPERATION").ok().as_deref()
        == Some(&binding.operation_uuid.to_string()))
    .then(|| std::env::var("GF_ITEM_ID_FAULT").ok())
    .flatten()
}
pub(super) fn before_publication(
    graph: &mut GraphForge,
    binding: &RegenerationBinding,
    token: &CancellationToken,
) -> Result<(), GfError> {
    match fault(binding).as_deref() {
        Some("cancel_before") => {
            token.cancel();
            token.checkpoint()?;
        }
        Some("race") => {
            graph.execute("CREATE (:Thing {name:'Race winner'})")?;
        }
        _ => {}
    }
    Ok(())
}
pub(super) fn after_publication(
    binding: &RegenerationBinding,
    token: &CancellationToken,
) -> Result<(), GfError> {
    match fault(binding).as_deref() {
        Some("private_ack_error") => {
            return Err(GfError::Storage(
                "private acknowledgement injected error".into(),
            ));
        }
        Some("private_ack_crash") => std::process::exit(83),
        Some("cancel_after") => token.cancel(),
        _ => {}
    }
    Ok(())
}

#[test]
fn regeneration_fault_helper() {
    let Ok(root) = std::env::var("GF_ITEM_ID_ROOT") else {
        return;
    };
    let operation = Uuid::parse_str(&std::env::var("GF_ITEM_ID_OPERATION").unwrap()).unwrap();
    let phase = std::env::var("GF_ITEM_ID_FAULT").unwrap();
    let mut graph = GraphForge::new(Some(&root)).unwrap();
    let generation = current(&graph);
    let result = graph.regenerate_item_id(OLD, operation, None);
    if phase == "cancel_after" {
        result.unwrap();
        data(&graph);
        return;
    }
    assert!(result.is_err(), "{phase}");
    if phase.contains("after_current") || phase == "private_ack_error" {
        assert_ne!(current(&graph), generation);
        data(&graph);
        assert!(
            !serde_json::to_string(&graph.workspace_ontology_composition().unwrap())
                .unwrap()
                .contains(&uri(OLD))
        );
        if phase.contains("after_current") {
            assert!(result.unwrap_err().to_string().contains("committed=true"));
        }
    } else if phase == "race" {
        assert_ne!(current(&graph), generation);
        assert!(
            !graph
                .research_version_retention()
                .unwrap()
                .receipts
                .contains_key(&operation)
        );
    } else {
        assert_eq!(current(&graph), generation);
    }
}

fn run_fault(path: &std::path::Path, operation: Uuid, phase: &str) -> std::process::ExitStatus {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "multi_ontology::regenerate::tests::regeneration_fault_helper",
            "--nocapture",
        ])
        .env("GF_ITEM_ID_ROOT", path)
        .env("GF_ITEM_ID_OPERATION", operation.to_string())
        .env("GF_ITEM_ID_FAULT", phase);
    if phase.starts_with("project.") {
        command
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINT_TRANSACTION",
                operation.to_string(),
            )
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env("GRAPHFORGE_PROJECT_FAILPOINT", phase);
    }
    command.status().unwrap()
}

#[test]
fn native_publication_faults_and_cancellation_preserve_the_atomic_boundary() {
    for phase in [
        "project.before_current_replace.error",
        "project.after_current_replace.error",
        "project.before_current_replace",
        "project.after_current_replace",
        "cancel_before",
        "cancel_after",
    ] {
        let fixture = Fixture::new(false);
        let path = fixture.path();
        let generation = current(&fixture.graph);
        let operation = Uuid::now_v7();
        drop(fixture.graph);
        let status = run_fault(&path, operation, phase);
        assert_eq!(
            status.success(),
            phase.ends_with(".error") || phase.starts_with("cancel_"),
            "{phase}: {status}"
        );
        let mut graph = GraphForge::new(path.to_str()).unwrap();
        let committed = phase.contains("after_current") || phase == "cancel_after";
        assert_eq!(
            graph
                .research_version_retention()
                .unwrap()
                .receipts
                .contains_key(&operation),
            committed
        );
        if !committed {
            assert_eq!(current(&graph), generation);
        }
        data(&graph);
        let result = graph.regenerate_item_id(OLD, operation, None).unwrap();
        let replay_generation = current(&graph);
        assert_eq!(
            graph.regenerate_item_id(OLD, operation, None).unwrap(),
            result
        );
        assert_eq!(current(&graph), replay_generation);
    }
}

#[test]
fn pending_binding_survives_refusal_recording_and_competing_operation() {
    let fixture = Fixture::new(false);
    let path = fixture.path();
    let operation = Uuid::now_v7();
    drop(fixture.graph);
    assert!(run_fault(&path, operation, "cancel_before").success());
    let before: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path.join(".publish-refusals/id-taken.json")).unwrap(),
    )
    .unwrap();
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    // Fresh hints gate new attempts while the original execution retains its
    // exact authorization. Protection from eviction does not freeze guidance.
    refusal(&graph, OLD, fixture.base, true);
    assert!(graph.regenerate_item_id(OLD, Uuid::now_v7(), None).is_err());
    assert!(
        !graph
            .research_version_retention()
            .unwrap()
            .receipts
            .contains_key(&operation)
    );
    let after: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path.join(".publish-refusals/id-taken.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(before["regenerations"], after["regenerations"]);
    assert_eq!(
        graph
            .id_taken_receipt(OLD)
            .unwrap()
            .unwrap()
            .item
            .publish_as_owner,
        Some(true)
    );
}

#[test]
fn private_acknowledgement_recovers_from_exact_native_receipt_after_restore() {
    for phase in ["private_ack_error", "private_ack_crash"] {
        let mut fixture = Fixture::new(false);
        let context = Uuid::now_v7();
        let source_version = Uuid::now_v7();
        let capture = fixture
            .graph
            .prepare_research_version(crate::PrepareResearchVersionRequest {
                operation_uuid: Uuid::now_v7(),
                version_uuid: source_version,
                context_uuid: context,
                created_at: 1,
                required_versions: BTreeSet::new(),
                label: None,
                description: None,
                author: None,
                committer: None,
            })
            .unwrap();
        fixture
            .graph
            .commit_research_version_operation(capture, &CancellationToken::new())
            .unwrap();
        let path = fixture.path();
        let operation = Uuid::now_v7();
        drop(fixture.graph);
        let status = run_fault(&path, operation, phase);
        assert_eq!(status.success(), phase.ends_with("error"));
        let raw: serde_json::Value = serde_json::from_slice(
            &std::fs::read(path.join(".publish-refusals/id-taken.json")).unwrap(),
        )
        .unwrap();
        assert!(raw["receipts"][OLD].get("regenerated").is_none());
        let mut graph = GraphForge::new(path.to_str()).unwrap();
        let result = ItemIdRegeneration {
            id: raw["regenerations"][operation.to_string()]["replacement"]["id"]
                .as_str()
                .unwrap()
                .into(),
            version_uuid: Uuid::parse_str(
                raw["regenerations"][operation.to_string()]["replacement"]["version_uuid"]
                    .as_str()
                    .unwrap(),
            )
            .unwrap(),
        };
        // Restore old content BEFORE reading or replacing the private refusal.
        graph
            .commit_research_version_operation(
                ResearchOperation {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: current(&graph),
                    mutation: ResearchMutation::RestoreProject {
                        context_uuid: context,
                        source_version,
                        version_uuid: Uuid::now_v7(),
                        created_at: 3,
                        author: None,
                        committer: None,
                    },
                },
                &CancellationToken::new(),
            )
            .unwrap();
        let restored = current(&graph);
        assert!(
            serde_json::to_string(&graph.workspace_ontology_composition().unwrap())
                .unwrap()
                .contains(&uri(OLD))
        );
        refusal(&graph, OLD, fixture.base, true);
        assert_eq!(
            graph.id_taken_receipt(OLD).unwrap().unwrap().regenerated,
            Some(result.clone())
        );
        assert_eq!(
            graph.regenerate_item_id(OLD, operation, None).unwrap(),
            result
        );
        assert_eq!(
            current(&graph),
            restored,
            "replay must not reinstall regenerated content"
        );
        data(&graph);
        assert!(graph.regenerate_item_id(OTHER, operation, None).is_err());
    }
}

#[test]
fn explicit_new_operation_recovers_a_proven_stale_preparation_without_rebasing_it() {
    let fixture = Fixture::new(false);
    let path = fixture.path();
    let operation = Uuid::now_v7();
    drop(fixture.graph);
    assert!(run_fault(&path, operation, "race").success());
    let read = || -> serde_json::Value {
        serde_json::from_slice(
            &std::fs::read(path.join(".publish-refusals/id-taken.json")).unwrap(),
        )
        .unwrap()
    };
    let original_binding = read()["regenerations"][operation.to_string()].clone();
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    assert_eq!(
        graph
            .regenerate_item_id(OLD, operation, None)
            .unwrap_err()
            .code(),
        "GF_WRITE_CONFLICT"
    );
    refusal(&graph, OLD, fixture.base, false);
    let new_operation = Uuid::now_v7();
    let replacement = graph.regenerate_item_id(OLD, new_operation, None).unwrap();
    assert_ne!(
        replacement.id,
        original_binding["replacement"]["id"].as_str().unwrap()
    );
    assert_eq!(
        read()["regenerations"][operation.to_string()],
        original_binding
    );
    let generation = current(&graph);
    assert_eq!(
        graph
            .regenerate_item_id(OLD, operation, None)
            .unwrap_err()
            .code(),
        "GF_WRITE_CONFLICT"
    );
    assert_eq!(
        graph.regenerate_item_id(OLD, new_operation, None).unwrap(),
        replacement
    );
    assert_eq!(current(&graph), generation);
    data(&graph);
}

#[test]
fn newer_owner_hint_gates_a_new_attempt_after_a_generation_race() {
    let fixture = Fixture::new(false);
    let path = fixture.path();
    let operation = Uuid::now_v7();
    drop(fixture.graph);
    assert!(run_fault(&path, operation, "race").success());
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    refusal(&graph, OLD, fixture.base, true);
    let generation = current(&graph);
    let error = graph
        .regenerate_item_id(OLD, Uuid::now_v7(), None)
        .unwrap_err();
    assert!(error.to_string().contains("owning organization"), "{error}");
    assert_eq!(current(&graph), generation);
    assert_eq!(
        graph
            .regenerate_item_id(OLD, operation, None)
            .unwrap_err()
            .code(),
        "GF_WRITE_CONFLICT"
    );
    let document: serde_json::Value = serde_json::from_slice(
        &std::fs::read(path.join(".publish-refusals/id-taken.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(document["regenerations"].as_object().unwrap().len(), 1);
    assert!(
        document["regenerations"][operation.to_string()]["refusal"]["item"]
            .get("publish_as_owner")
            .is_none()
    );
    assert_eq!(
        graph
            .id_taken_receipt(OLD)
            .unwrap()
            .unwrap()
            .item
            .publish_as_owner,
        Some(true)
    );
}

#[test]
fn a_foreign_native_uuid_consumer_conflicts_locally_without_poisoning_receipt_storage() {
    let fixture = Fixture::new(false);
    let path = fixture.path();
    let operation = Uuid::now_v7();
    drop(fixture.graph);
    assert!(run_fault(&path, operation, "cancel_before").success());
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    let capture = graph
        .prepare_research_version(crate::PrepareResearchVersionRequest {
            operation_uuid: operation,
            version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
            created_at: 1,
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
    assert!(graph.id_taken_receipt(OTHER).unwrap().is_none());
    refusal(&graph, OTHER, fixture.base, false);
    assert!(graph.id_taken_receipt(OTHER).unwrap().is_some());
    assert!(
        graph
            .id_taken_receipt(OLD)
            .unwrap()
            .unwrap()
            .regenerated
            .is_none()
    );
    assert_eq!(
        graph
            .regenerate_item_id(OLD, operation, None)
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    let result = graph.regenerate_item_id(OLD, Uuid::now_v7(), None).unwrap();
    assert_eq!(
        graph.id_taken_receipt(OLD).unwrap().unwrap().regenerated,
        Some(result)
    );
    assert_eq!(
        graph
            .regenerate_item_id(OLD, operation, None)
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
}

#[test]
fn unrelated_authored_versions_keep_exact_identities_and_stored_values() {
    let mut fixture = Fixture::new(false);
    let unrelated = "https://example.org/unrelated";
    let mut ids = Vec::new();
    for (version, name) in [("1", "Alpha"), ("2", "Beta")] {
        let candidate = fixture.graph.create_ontology_module(serde_json::from_value(serde_json::json!({"ontology_id":unrelated,"version":version,"entity_types":[{"name":name}],"properties":[{"owner":name,"name":"value","type":"utf8"}]})).unwrap(), vec![], None).unwrap();
        ids.push(candidate.id.clone());
        fixture
            .graph
            .adopt_ontology_module(
                &ModuleAdoptionRequest {
                    authority: authority(&fixture.graph),
                    candidate,
                },
                None,
            )
            .unwrap();
        fixture
            .graph
            .execute(&format!("CREATE (:`{name}` {{value:'{name} value'}})"))
            .unwrap();
    }
    fixture
        .graph
        .regenerate_item_id(OLD, Uuid::now_v7(), None)
        .unwrap();
    let path = fixture.path();
    drop(fixture.graph);
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    let composition = graph.workspace_ontology_composition().unwrap().unwrap();
    for id in ids {
        assert!(composition.modules.iter().any(|module| module.id == id));
    }
    for name in ["Alpha", "Beta"] {
        let values = graph
            .execute(&format!("MATCH (n:`{name}`) RETURN n.value"))
            .unwrap();
        assert_eq!(
            values.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            format!("{name} value")
        );
    }
    for name in ["Alpha", "Beta"] {
        let nodes = graph
            .execute(&format!("MATCH (n:`{name}`) RETURN n.node_uuid"))
            .unwrap();
        let node = Uuid::from_slice(
            nodes.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
        )
        .unwrap();
        let pattern = crate::branches::semantic_fields::property_pattern(
            &graph,
            "node",
            node,
            "value",
            &CancellationToken::new(),
        )
        .unwrap();
        let values = graph
            .execute(&format!("MATCH {pattern} RETURN n.value"))
            .unwrap();
        assert_eq!(values.batches[0].num_rows(), 1);
        graph
            .execute_research_branch(
                &ExecuteResearchBranchRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: current(&graph),
                    branch_uuid: fixture.branch,
                    version_uuid: Uuid::now_v7(),
                    query: format!("MATCH (n:`{name}`) SET n.value='{name} edited'"),
                    created_at: 4,
                    label: None,
                    description: None,
                    author: None,
                    committer: None,
                },
                &CancellationToken::new(),
            )
            .unwrap();
    }
    let view = graph.open_research_branch(fixture.branch).unwrap();
    for name in ["Alpha", "Beta"] {
        let values = view
            .graph()
            .execute(&format!("MATCH (n:`{name}`) RETURN n.value"))
            .unwrap();
        assert_eq!(
            values.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            format!("{name} edited")
        );
    }
    data(&graph);
}

#[test]
fn inconsistent_bound_intent_is_corruption_instead_of_a_foreign_uuid_consumer() {
    let mut fixture = Fixture::new(false);
    let operation = Uuid::now_v7();
    fixture
        .graph
        .regenerate_item_id(OLD, operation, None)
        .unwrap();
    let path = fixture.path().join(".publish-refusals/id-taken.json");
    let mut document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    document["regenerations"][operation.to_string()]["created_at"] = serde_json::json!(123);
    std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
    assert!(fixture.graph.id_taken_receipt(OTHER).is_err());
    assert_eq!(
        fixture
            .graph
            .regenerate_item_id(OLD, operation, None)
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
}
