//! Guarded recovery binds immutable refused content without mixing CURRENT edits.
use super::*;

fn selection(fixture: &Fixture) -> ItemIdRegenerationSelection {
    ItemIdRegenerationSelection {
        branch_uuid: fixture.branch,
        head_version_uuid: fixture.base,
    }
}

pub(super) fn advance_head(graph: &mut GraphForge, branch_uuid: Uuid) -> Uuid {
    let version_uuid = Uuid::now_v7();
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(graph),
                branch_uuid,
                version_uuid,
                query: "CREATE (:Thing {name:'Branch only'})".into(),
                created_at: 2,
                author: None,
                committer: None,
                label: None,
                description: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    version_uuid
}

fn names(graph: &GraphForge) -> Vec<String> {
    let result = graph
        .execute("MATCH (n:Thing) RETURN n.name AS name ORDER BY name")
        .unwrap();
    result
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|name| name.unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn guarded_recovery_keeps_branch_and_workspace_content_independent() {
    let mut fixture = Fixture::new(true);
    fixture.base = advance_head(&mut fixture.graph, fixture.branch);
    refusal(&fixture.graph, OLD, fixture.base, false);
    fixture
        .graph
        .execute("CREATE (:Thing {name:'Current only'})")
        .unwrap();
    let expected = selection(&fixture);
    let before = fixture.graph.research_version_retention().unwrap();
    let prior_composition = fixture
        .graph
        .open_research_branch(fixture.branch)
        .unwrap()
        .graph()
        .workspace_ontology_composition()
        .unwrap()
        .unwrap();
    let operation = Uuid::now_v7();
    let result = fixture
        .graph
        .regenerate_item_id_at_selection(OLD, operation, expected, None)
        .unwrap();
    assert_eq!(names(&fixture.graph), ["Ada", "Bea", "Current only"]);
    assert_no_old_references(
        &fixture
            .graph
            .workspace_ontology_composition()
            .unwrap()
            .unwrap(),
    );
    let head = fixture.graph.open_research_branch(fixture.branch).unwrap();
    assert_eq!(head.version_uuid(), result.version_uuid);
    assert_eq!(names(head.graph()), ["Ada", "Bea", "Branch only"]);
    let composition = head
        .graph()
        .workspace_ontology_composition()
        .unwrap()
        .unwrap();
    assert_no_old_references(&composition);
    assert!(
        composition
            .modules
            .iter()
            .any(|module| module.id.ontology_id == uri(&result.id))
    );
    multi_version::assert_history(&fixture.graph, &before, fixture.branch, &result);
    let subjects: BTreeMap<_, _> = prior_composition
        .modules
        .iter()
        .map(|old| {
            let id = if old.id.ontology_id == uri(OLD) {
                uri(&result.id)
            } else {
                old.id.ontology_id.clone()
            };
            let next = composition
                .modules
                .iter()
                .find(|next| {
                    next.id.ontology_id == id && next.id.authored_version == old.id.authored_version
                })
                .unwrap();
            (old.id.display_ref(), next.id.display_ref())
        })
        .chain(prior_composition.bridges.iter().map(|old| {
            let next = composition
                .bridges
                .iter()
                .find(|next| {
                    next.bridge_id == old.bridge_id && next.authored_version == old.authored_version
                })
                .unwrap();
            (
                bridge_id(old).unwrap().display_ref(),
                bridge_id(next).unwrap().display_ref(),
            )
        }))
        .collect();
    let mut identities = vec![("ontology_module", uri(OLD), uri(&result.id))];
    identities.extend(prior_composition.activation.iter().map(|old| {
        (
            "ontology_activation",
            format!("{:?}:{}", old.scope, old.subject),
            format!("{:?}:{}", old.scope, subjects.get(&old.subject).unwrap()),
        )
    }));
    multi_version::assert_field_credit(&fixture.graph, fixture.base, fixture.branch, &identities);
    let exact = fixture.graph.research_version(result.version_uuid).unwrap();
    advance_head(&mut fixture.graph, fixture.branch);
    branch(&mut fixture.graph, "other");
    let path = fixture.path();
    drop(fixture.graph);
    let mut graph = GraphForge::new(path.to_str()).unwrap();
    let generation = current(&graph);
    assert_eq!(
        graph
            .regenerate_item_id_at_selection(OLD, operation, expected, None)
            .unwrap(),
        result
    );
    assert_eq!(current(&graph), generation);
    assert_eq!(graph.research_version(result.version_uuid).unwrap(), exact);
    for changed in [
        ItemIdRegenerationSelection {
            branch_uuid: Uuid::now_v7(),
            ..expected
        },
        ItemIdRegenerationSelection {
            head_version_uuid: Uuid::now_v7(),
            ..expected
        },
    ] {
        assert!(
            graph
                .regenerate_item_id_at_selection(OLD, operation, changed, None)
                .is_err()
        );
    }
    assert!(graph.regenerate_item_id(OLD, operation, None).is_err());
    assert_eq!(current(&graph), generation);
}

#[test]
fn guarded_selection_refusals_leave_native_and_private_authority_unchanged() {
    let mut fixture = Fixture::new(false);
    let expected = selection(&fixture);
    let generation = current(&fixture.graph);
    let before = fixture.graph.workspace_ontology_composition().unwrap();
    let receipt = fixture.graph.id_taken_receipt(OLD).unwrap();
    for changed in [
        ItemIdRegenerationSelection {
            branch_uuid: Uuid::now_v7(),
            ..expected
        },
        ItemIdRegenerationSelection {
            head_version_uuid: Uuid::now_v7(),
            ..expected
        },
        ItemIdRegenerationSelection {
            branch_uuid: Uuid::nil(),
            ..expected
        },
        ItemIdRegenerationSelection {
            head_version_uuid: Uuid::nil(),
            ..expected
        },
    ] {
        assert!(
            fixture
                .graph
                .regenerate_item_id_at_selection(OLD, Uuid::now_v7(), changed, None)
                .is_err()
        );
        assert_eq!(current(&fixture.graph), generation);
        assert_eq!(
            fixture.graph.workspace_ontology_composition().unwrap(),
            before
        );
        assert_eq!(fixture.graph.id_taken_receipt(OLD).unwrap(), receipt);
    }
    let other_version = advance_head(&mut fixture.graph, fixture.branch);
    let expected = ItemIdRegenerationSelection {
        head_version_uuid: other_version,
        ..expected
    };
    let generation = current(&fixture.graph);
    let error = fixture
        .graph
        .regenerate_item_id_at_selection(OLD, Uuid::now_v7(), expected, None)
        .unwrap_err();
    assert!(error.to_string().contains("refused Version"));
    assert_eq!(current(&fixture.graph), generation);
    assert_eq!(fixture.graph.id_taken_receipt(OLD).unwrap(), receipt);
    let bytes = std::fs::read(fixture.path().join(".publish-refusals/id-taken.json")).unwrap();
    let document: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(document.get("regenerations").is_none());
}

#[test]
fn guarded_operation_cannot_replay_legacy_intent() {
    let mut fixture = Fixture::new(false);
    let expected = selection(&fixture);
    let operation = Uuid::now_v7();
    fixture
        .graph
        .regenerate_item_id(OLD, operation, None)
        .unwrap();
    let generation = current(&fixture.graph);
    assert!(
        fixture
            .graph
            .regenerate_item_id_at_selection(OLD, operation, expected, None)
            .is_err()
    );
    assert_eq!(current(&fixture.graph), generation);
}

#[test]
fn guarded_fault_helper() {
    let Ok(root) = std::env::var("GF_ITEM_ID_ROOT") else {
        return;
    };
    let operation = Uuid::parse_str(&std::env::var("GF_ITEM_ID_OPERATION").unwrap()).unwrap();
    let phase = std::env::var("GF_ITEM_ID_FAULT").unwrap();
    let mut graph = GraphForge::new(Some(&root)).unwrap();
    let registry = graph.research_version_retention().unwrap();
    let branch_uuid = registry.current_branch.unwrap();
    let expected = ItemIdRegenerationSelection {
        branch_uuid,
        head_version_uuid: registry.heads[&branch_uuid],
    };
    let result = graph.regenerate_item_id_at_selection(OLD, operation, expected, None);
    if phase == "private_ack_error" {
        assert!(result.is_err());
        let result = graph
            .regenerate_item_id_at_selection(OLD, operation, expected, None)
            .unwrap();
        assert_eq!(
            graph
                .open_research_branch(branch_uuid)
                .unwrap()
                .version_uuid(),
            result.version_uuid
        );
        return;
    }
    assert!(result.is_err());
    assert!(
        graph
            .id_taken_receipt(OLD)
            .unwrap()
            .unwrap()
            .regenerated
            .is_none()
    );
    assert!(
        !graph
            .research_version_retention()
            .unwrap()
            .receipts
            .contains_key(&operation)
    );
    assert!(
        graph
            .ontology_modules()
            .unwrap()
            .iter()
            .any(|module| module.id.ontology_id == uri(OLD))
    );
}

#[test]
fn guarded_publication_refuses_intervening_branch_switch_and_head_advance() {
    for phase in [
        "switch_race",
        "head_race",
        "cancel_before",
        "private_ack_error",
    ] {
        let fixture = Fixture::new(false);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "multi_ontology::regenerate::tests::selection::guarded_fault_helper",
                "--nocapture",
            ])
            .env("GF_ITEM_ID_ROOT", fixture.path())
            .env("GF_ITEM_ID_OPERATION", Uuid::now_v7().to_string())
            .env("GF_ITEM_ID_FAULT", phase)
            .status()
            .unwrap();
        assert!(status.success(), "{phase}");
    }
}

#[test]
fn guarded_recovery_remaps_existing_workspace_credit_after_a_manual_fork() {
    let mut fixture = Fixture::new(false);
    let first = fixture
        .graph
        .regenerate_item_id(OLD, Uuid::now_v7(), None)
        .unwrap();
    fixture
        .graph
        .execute("CREATE (:Thing {name:'Uncommitted current edit'})")
        .unwrap();
    let prior = crate::branches::baseline::read(&fixture.graph).unwrap();
    assert!(!prior.is_empty());
    refusal(&fixture.graph, &first.id, first.version_uuid, false);
    let result = fixture
        .graph
        .regenerate_item_id_at_selection(
            &first.id,
            Uuid::now_v7(),
            ItemIdRegenerationSelection {
                branch_uuid: fixture.branch,
                head_version_uuid: first.version_uuid,
            },
            None,
        )
        .unwrap();
    let after = crate::branches::baseline::read(&fixture.graph).unwrap();
    assert_eq!(
        after.len(),
        prior.len(),
        "uncommitted edits must not acquire credit from the Branch Version"
    );
    let old_key =
        crate::branches::context_fields::context_identity("ontology_module", &uri(&first.id));
    let new_key =
        crate::branches::context_fields::context_identity("ontology_module", &uri(&result.id));
    let old = prior
        .values()
        .find(|row| row.key.0 == "ontology_module" && row.key.1 == old_key)
        .unwrap();
    let mut key = old.key.clone();
    key.1 = new_key;
    let new = after
        .get(&key)
        .expect("CURRENT credit must follow the rewritten ID");
    assert!(!after.contains_key(&old.key));
    assert_eq!(new.origin, old.origin);
    assert_eq!(new.original, old.original);
    assert_eq!(new.baseline, old.baseline);
    assert_eq!(new.contribution, old.contribution);
    assert_ne!(new.current, old.current);
}
