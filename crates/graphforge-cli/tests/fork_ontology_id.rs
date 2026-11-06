//! Receipt-free native forks through the facade and the same-build parsed CLI.
use arrow::array::{Int64Array, StringArray};
use graphforge_api::{
    ActivationMode, BranchSource, BridgeAdoptionRequest, CancellationToken,
    CreateResearchBranchRequest, GraphForge, ItemIdRegeneration, ModuleAdoptionRequest,
    OntologyAuthorityExpectation, OntologyModuleId, OperationId, SelectResearchBranchRequest,
    WriteContext,
};
use graphforge_discovery::{ItemId, ItemKind};
use graphforge_hub_publish::IdTakenItem;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use uuid::Uuid;

const OLD: &str = "ont_aaaaaaaaaaaaaaaa";
const OTHER: &str = "ont_bbbbbbbbbbbbbbbb";
fn uri(id: &str) -> String {
    ItemId::parse_reference(id).unwrap().to_uri()
}
fn generation(graph: &GraphForge) -> Uuid {
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
fn adopt(
    graph: &mut GraphForge,
    id: &str,
    dependencies: Vec<OntologyModuleId>,
) -> OntologyModuleId {
    let document = if id == OLD {
        json!({"ontology_id":uri(id),"version":"v1","label":"Exact label", "description":"Exact prose", "license":"MIT OR  Apache-2.0",
            "entity_types":[{"name":"Thing"}],"relation_types":[{"name":"LINK","src":"Thing","dst":"Thing"}],
            "properties":[{"owner":"Thing","name":"name","type":"utf8"},{"owner":"LINK","name":"weight","type":"int64"}]})
    } else {
        json!({"ontology_id":uri(id),"version":"v1","entity_types":[{"name":"Other"}]})
    };
    let candidate = graph
        .create_ontology_module(
            serde_json::from_value(document).unwrap(),
            dependencies,
            Some(ActivationMode::Advisory),
        )
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
fn branch(graph: &mut GraphForge, label: &str) -> (Uuid, Uuid) {
    let branch_uuid = Uuid::now_v7();
    let version_uuid = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(graph),
                branch_uuid,
                version_uuid,
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: label.into(),
                description: None,
                author: None,
                committer: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    graph
        .select_research_branch(
            &SelectResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(graph),
                branch_uuid,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    (branch_uuid, version_uuid)
}
struct Project {
    _root: tempfile::TempDir,
    path: PathBuf,
    branch: Uuid,
    base: Uuid,
    sibling: Uuid,
    sibling_head: Uuid,
}
impl Project {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("project");
        let mut graph = GraphForge::new(path.to_str()).unwrap();
        let old = adopt(&mut graph, OLD, vec![]);
        let other = adopt(&mut graph, OTHER, vec![old.clone()]);
        let mut dependencies = Vec::new();
        for id in ["brg_aaaaaaaaaaaaaaaa", "brg_bbbbbbbbbbbbbbbb"] {
            let bridge = graph.create_ontology_bridge(serde_json::from_value(json!({
                "bridge_id":uri(id),"authored_version":"v1","source_modules":[old,other],"target_modules":[old,other],"dependencies":dependencies,
                "shared_surfaces":["research_actions"],"enforcement":"advisory",
                "assertions":[{"source":{"module":old,"kind":"entity","local_id":"Thing"},"target":{"module":other,"kind":"entity","local_id":"Other"},"predicate":"related","directional":true,"provenance":{"method":"authored","justification":"related symbols","evidence_refs":[]}}]
            })).unwrap()).unwrap();
            dependencies = vec![bridge.id.clone()];
            graph
                .adopt_ontology_bridge(
                    &BridgeAdoptionRequest {
                        authority: authority(&graph),
                        candidate: bridge,
                    },
                    None,
                )
                .unwrap();
        }
        graph
            .execute(
                "CREATE (a:Thing {name:'Ada'}),(b:Thing {name:'Bea'}),(a)-[:LINK {weight:42}]->(b)",
            )
            .unwrap();
        let mut metadata = graph.research_project_metadata().unwrap();
        metadata.ontologies = vec![uri(OLD), uri(OTHER)];
        graph
            .update_research_metadata(graphforge_api::UpdateResearchMetadataRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                metadata,
            })
            .unwrap();
        let (sibling, sibling_head) = branch(&mut graph, "sibling");
        let (branch, base) = branch(&mut graph, "main");
        Self {
            _root: root,
            path,
            branch,
            base,
            sibling,
            sibling_head,
        }
    }
    fn open(&self) -> GraphForge {
        GraphForge::new(self.path.to_str()).unwrap()
    }
    fn command(&self, verb: &str, id: &str, operation: Uuid, yes: bool) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_gf"));
        command
            .arg("--project")
            .arg(&self.path)
            .args(["--json", "ontology", verb, id, "--operation-uuid"])
            .arg(operation.to_string())
            .stdin(Stdio::null());
        if yes {
            command.arg("--yes");
        }
        command
    }
    fn journal_path(&self) -> PathBuf {
        self.path.join(".publish-refusals/id-taken.json")
    }
    fn journal(&self) -> Value {
        serde_json::from_slice(&std::fs::read(self.journal_path()).unwrap()).unwrap()
    }
}
fn success(output: Output) -> ItemIdRegeneration {
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
fn data(graph: &GraphForge) {
    let result = graph
        .execute("MATCH (a:Thing)-[r:LINK]->(b:Thing) RETURN a.name,b.name,r.weight")
        .unwrap();
    assert_eq!(
        result.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
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
fn routes(path: &Path) -> Value {
    let source = graphforge_storage::resolve_project_generation(path).unwrap();
    serde_json::to_value(
        graphforge_storage::semantic_storage_bindings(&source)
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

#[test]
fn parsed_cli_forks_without_receipt_preserving_project_and_replays_after_later_work() {
    let project = Project::new();
    let mut graph = project.open();
    assert_eq!(graph.id_taken_receipt(OLD).unwrap(), None);
    let before = graph.workspace_ontology_composition().unwrap().unwrap();
    let old_versions = graph.research_version_retention().unwrap().versions;
    let old_routes = routes(&project.path);
    drop(graph);
    let operation = Uuid::now_v7();
    let result = success(
        project
            .command("fork-id", &uri(OLD), operation, true)
            .output()
            .unwrap(),
    );
    assert_ne!(result.id, OLD);
    assert_eq!(
        ItemId::parse(&result.id).unwrap().kind(),
        ItemKind::OntologyModule
    );
    graph = project.open();
    let after = graph.workspace_ontology_composition().unwrap().unwrap();
    let old = before
        .modules
        .iter()
        .find(|m| m.id.ontology_id == uri(OLD))
        .unwrap();
    let new = after
        .modules
        .iter()
        .find(|m| m.id.ontology_id == uri(&result.id))
        .unwrap();
    let mut expected = old.document.clone();
    expected.ontology_id = uri(&result.id);
    expected.forked_from = Some((&old.id).into());
    assert_eq!(new.document, expected);
    assert_eq!(after.profile_default, before.profile_default);
    assert_eq!(after.activation.len(), before.activation.len());
    let original_mode = before
        .activation
        .iter()
        .find(|r| r.subject == old.id.display_ref())
        .unwrap();
    let rewritten_mode = after
        .activation
        .iter()
        .find(|r| r.subject == new.id.display_ref())
        .unwrap();
    assert_eq!(rewritten_mode.mode, original_mode.mode);
    assert_eq!(rewritten_mode.scope, original_mode.scope);
    let mut expected_ontologies = vec![uri(&result.id), uri(OTHER)];
    expected_ontologies.sort();
    assert_eq!(
        graph.research_project_metadata().unwrap().ontologies,
        expected_ontologies
    );
    assert!(after.modules.iter().all(|m| m.id.ontology_id != uri(OLD)));
    assert_eq!(
        after
            .modules
            .iter()
            .find(|m| m.id.ontology_id == uri(OTHER))
            .unwrap()
            .dependencies,
        vec![new.id.clone()]
    );
    assert_eq!(after.bridges.len(), before.bridges.len());
    for bridge in &after.bridges {
        assert!(bridge.source_modules.contains(&new.id));
        assert!(bridge.target_modules.contains(&new.id));
        assert_eq!(bridge.assertions[0].source.module, new.id);
        for dependency in &bridge.dependencies {
            assert!(
                graph
                    .ontology_bridges()
                    .unwrap()
                    .iter()
                    .any(|b| b.id == *dependency)
            );
        }
    }
    let new_routes = routes(&project.path);
    for prior in old_routes["bindings"].as_array().unwrap() {
        let replacement = new_routes["bindings"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| {
                b["symbol"]["kind"] == prior["symbol"]["kind"]
                    && b["symbol"]["local_id"] == prior["symbol"]["local_id"]
            })
            .unwrap();
        assert_eq!(replacement["storage_id"], prior["storage_id"]);
        assert_eq!(
            replacement["symbol"]["local_id"],
            prior["symbol"]["local_id"]
        );
        if prior["symbol"]["module"]["ontology_id"] == uri(OLD) {
            assert_ne!(replacement["route"], prior["route"]);
            assert_eq!(
                replacement["symbol"]["module"]["ontology_id"],
                uri(&result.id)
            );
        }
    }
    data(&graph);
    let registry = graph.research_version_retention().unwrap();
    assert_eq!(registry.current_branch, Some(project.branch));
    assert_eq!(registry.heads[&project.branch], result.version_uuid);
    assert_eq!(registry.heads[&project.sibling], project.sibling_head);
    for (id, version) in old_versions {
        assert_eq!(registry.versions[&id], version);
    }
    assert_eq!(
        registry.versions[&result.version_uuid].parents,
        vec![project.base]
    );
    data(graph.open_research_branch(project.branch).unwrap().graph());
    assert_eq!(graph.id_taken_receipt(OLD).unwrap(), None);
    assert!(
        project.journal()["receipts"]
            .as_object()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        project.journal()["regenerations"][operation.to_string()]["refusal"],
        json!({"mode":"explicit_ontology_fork"})
    );
    // Replay is independent of current composition and selected Branch.
    graph
        .select_research_branch(
            &SelectResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(&graph),
                branch_uuid: project.sibling,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    graph.execute("CREATE (:Other)").unwrap();
    let later = generation(&graph);
    drop(graph);
    assert_eq!(
        success(
            project
                .command("fork-id", OLD, operation, true)
                .output()
                .unwrap()
        ),
        result
    );
    assert_eq!(generation(&project.open()), later);
    assert_eq!(
        project
            .open()
            .research_version_retention()
            .unwrap()
            .current_branch,
        Some(project.sibling)
    );
    assert!(
        !project
            .command("regenerate-id", OLD, operation, true)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert!(
        !project
            .command("fork-id", OTHER, operation, true)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(generation(&project.open()), later);
}

#[test]
fn facade_refusals_and_confirmation_fail_without_mutation() {
    let project = Project::new();
    let mut graph = project.open();
    let before = generation(&graph);
    for id in [
        "https://example.org/module",
        "brg_aaaaaaaaaaaaaaaa",
        "ont_cccccccccccccccc",
    ] {
        assert!(graph.fork_ontology_id(id, Uuid::now_v7(), None).is_err());
    }
    assert!(graph.fork_ontology_id(OLD, Uuid::nil(), None).is_err());
    assert!(graph.regenerate_item_id(OLD, Uuid::now_v7(), None).is_err());
    let token = CancellationToken::new();
    token.cancel();
    assert!(
        graph
            .fork_ontology_id(OLD, Uuid::now_v7(), Some(&token))
            .is_err()
    );
    assert!(!project.journal_path().exists());
    drop(graph);
    assert!(
        !project
            .command("fork-id", OLD, Uuid::now_v7(), false)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert_eq!(generation(&project.open()), before);
    assert!(!project.journal_path().exists());
    let mut empty = GraphForge::new(project._root.path().join("empty").to_str()).unwrap();
    assert!(empty.fork_ontology_id(OLD, Uuid::now_v7(), None).is_err());
    assert!(!empty.path().unwrap().join(".publish-refusals").exists());
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyBinding {
    operation_uuid: Uuid,
    old_id: String,
    refusal: graphforge_api::ItemIdRefusalReceipt,
    replacement: ItemIdRegeneration,
    source_generation: Uuid,
    branch_uuid: Uuid,
    source_version: Uuid,
    created_at: i64,
    request_sha256: Option<[u8; 32]>,
}

#[test]
fn owner_receipts_stay_refused_by_legacy_command_and_cross_mode_replay_fails() {
    for owner in [false, true] {
        let project = Project::new();
        let mut graph = project.open();
        graph
            .record_publish_id_taken(
                project.base,
                &[IdTakenItem {
                    kind: ItemKind::OntologyModule,
                    id: OLD.into(),
                    owner: Some("organization".into()),
                    publish_as_owner: owner.then_some(true),
                }],
            )
            .unwrap();
        let receipt = graph.id_taken_receipt(OLD).unwrap();
        let operation = Uuid::now_v7();
        if owner {
            assert!(graph.regenerate_item_id(OLD, operation, None).is_err());
            graph.fork_ontology_id(OLD, operation, None).unwrap();
            assert_eq!(graph.id_taken_receipt(OLD).unwrap(), receipt);
            assert!(graph.regenerate_item_id(OLD, operation, None).is_err());
        } else {
            graph.regenerate_item_id(OLD, operation, None).unwrap();
            assert!(graph.fork_ontology_id(OLD, operation, None).is_err());
            let serialized = &project.journal()["regenerations"][operation.to_string()]["refusal"];
            assert_eq!(*serialized, serde_json::to_value(receipt.unwrap()).unwrap());
            let journal = project.journal();
            let raw = &journal["regenerations"][operation.to_string()];
            let mut legacy: LegacyBinding = serde_json::from_value(raw.clone()).unwrap();
            assert_eq!(serde_json::to_value(&legacy).unwrap(), *raw);
            legacy.request_sha256 = None;
            use sha2::Digest as _;
            let mut digest = sha2::Sha256::new();
            digest.update(b"graphforge-item-id-regeneration/1");
            digest.update(serde_json::to_vec(&legacy).unwrap());
            let original_digest: [u8; 32] = digest.finalize().into();
            assert_eq!(
                graph.research_version_retention().unwrap().receipts[&operation].intent_sha256,
                Some(original_digest)
            );
        }
    }
}

#[test]
fn fork_fault_helper() {
    let Ok(path) = std::env::var("GF_EXPLICIT_FORK_ROOT") else {
        return;
    };
    let operation = Uuid::parse_str(&std::env::var("GF_EXPLICIT_FORK_OPERATION").unwrap()).unwrap();
    let mut graph = GraphForge::new(Some(&path)).unwrap();
    assert!(graph.fork_ontology_id(OLD, operation, None).is_err());
}
fn interrupt(project: &Project, operation: Uuid, phase: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "fork_fault_helper", "--nocapture"])
        .env("GF_EXPLICIT_FORK_ROOT", &project.path)
        .env("GF_EXPLICIT_FORK_OPERATION", operation.to_string())
        .env(
            "GRAPHFORGE_PROJECT_FAILPOINT_TRANSACTION",
            operation.to_string(),
        )
        .env(
            "GRAPHFORGE_PROJECT_FAILPOINTS",
            "graphforge-internal-subprocess-v1",
        )
        .env("GRAPHFORGE_PROJECT_FAILPOINT", phase)
        .output()
        .unwrap();
    if phase.ends_with(".error") {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    } else {
        assert!(!output.status.success());
    }
}
#[test]
fn native_current_interruptions_recover_the_original_id_and_frozen_intent() {
    for phase in [
        "project.before_current_replace.error",
        "project.after_current_replace.error",
        "project.before_current_replace",
        "project.after_current_replace",
    ] {
        let project = Project::new();
        let before = generation(&project.open());
        let operation = Uuid::now_v7();
        interrupt(&project, operation, phase);
        let journal = project.journal();
        let pending = &journal["regenerations"][operation.to_string()];
        let expected: ItemIdRegeneration =
            serde_json::from_value(pending["replacement"].clone()).unwrap();
        assert_eq!(pending["source_generation"], before.to_string());
        assert_eq!(pending["source_version"], project.base.to_string());
        assert_eq!(pending["branch_uuid"], project.branch.to_string());
        let mut graph = project.open();
        data(&graph);
        let committed = phase.contains("after_current");
        assert_eq!(
            graph
                .research_version_retention()
                .unwrap()
                .receipts
                .contains_key(&operation),
            committed
        );
        if !committed {
            assert_eq!(generation(&graph), before);
        }
        assert_eq!(
            graph.fork_ontology_id(OLD, operation, None).unwrap(),
            expected
        );
        let after = generation(&graph);
        drop(graph);
        assert_eq!(
            project
                .open()
                .fork_ontology_id(&uri(OLD), operation, None)
                .unwrap(),
            expected
        );
        assert_eq!(generation(&project.open()), after);
        assert_eq!(project.open().id_taken_receipt(OLD).unwrap(), None);
    }
}
#[test]
fn pending_fork_never_rebases_and_new_operations_cannot_cross_authorize_it() {
    let project = Project::new();
    let operation = Uuid::now_v7();
    interrupt(&project, operation, "project.before_current_replace.error");
    let pending = project.journal()["regenerations"][operation.to_string()].clone();
    let mut graph = project.open();
    assert!(graph.regenerate_item_id(OLD, operation, None).is_err());
    assert!(graph.fork_ontology_id(OLD, Uuid::now_v7(), None).is_err());
    graph.execute("CREATE (:Other)").unwrap();
    let before = generation(&graph);
    let error = graph.fork_ontology_id(OLD, operation, None).unwrap_err();
    assert!(error.to_string().contains("cannot be rebased"), "{error}");
    assert_eq!(generation(&graph), before);
    assert_eq!(
        project.journal()["regenerations"][operation.to_string()],
        pending
    );
    let result = graph.fork_ontology_id(OLD, Uuid::now_v7(), None).unwrap();
    assert_ne!(result.id, pending["replacement"]["id"]);
    assert!(graph.fork_ontology_id(OLD, operation, None).is_err());
    assert_eq!(
        project.journal()["regenerations"][operation.to_string()],
        pending
    );
}
#[test]
fn persisted_authority_missing_null_malformed_or_changed_mode_fails_closed() {
    for authority in [
        None,
        Some(Value::Null),
        Some(json!({})),
        Some(json!({"mode":"regenerate"})),
        Some(json!({"mode":"explicit_ontology_fork","extra":true})),
    ] {
        let project = Project::new();
        let operation = Uuid::now_v7();
        interrupt(&project, operation, "project.before_current_replace.error");
        let mut document = project.journal();
        let binding = document["regenerations"][operation.to_string()]
            .as_object_mut()
            .unwrap();
        if let Some(authority) = authority {
            binding.insert("refusal".into(), authority);
        } else {
            binding.remove("refusal");
        }
        std::fs::write(
            project.journal_path(),
            serde_json::to_vec(&document).unwrap(),
        )
        .unwrap();
        let mut graph = project.open();
        let before = generation(&graph);
        assert!(graph.fork_ontology_id(OLD, operation, None).is_err());
        assert!(graph.regenerate_item_id(OLD, operation, None).is_err());
        assert_eq!(generation(&graph), before);
    }
    // A committed receipt binds authorization mode cryptographically as well.
    let project = Project::new();
    let operation = Uuid::now_v7();
    let mut graph = project.open();
    graph
        .record_publish_id_taken(
            project.base,
            &[IdTakenItem {
                kind: ItemKind::OntologyModule,
                id: OLD.into(),
                owner: None,
                publish_as_owner: None,
            }],
        )
        .unwrap();
    graph.regenerate_item_id(OLD, operation, None).unwrap();
    let mut document = project.journal();
    document["regenerations"][operation.to_string()]["refusal"] =
        json!({"mode":"explicit_ontology_fork"});
    std::fs::write(
        project.journal_path(),
        serde_json::to_vec(&document).unwrap(),
    )
    .unwrap();
    assert!(graph.fork_ontology_id(OLD, operation, None).is_err());
}
