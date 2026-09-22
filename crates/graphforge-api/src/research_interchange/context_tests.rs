//! Imported immutable suppression context remains the original Project after Fork.
use super::*;
use crate::*;
use arrow::array::FixedSizeBinaryArray;
use graphforge_knowledge::research::{
    ResearchCategory, ResearchSuppressionLedger, ResearchSuppressionRecord,
};
use graphforge_storage::{ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome};

#[test]
fn fork_historical_project_suppression_uses_original_authority_after_reopen() {
    let owner = tempfile::tempdir().unwrap();
    let root = owner.path().join("source");
    let mut source = GraphForge::new(root.to_str()).unwrap();
    let (assertion, creator, project) = seed_suppression(&mut source);
    drop(source);
    let mut source = GraphForge::new(root.to_str()).unwrap();
    let version = Uuid::now_v7();
    let capture = source
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: version,
            context_uuid: Uuid::now_v7(),
            label: None,
            description: None,
            created_at: 12,
            required_versions: Default::default(),
        })
        .unwrap();
    source
        .commit_research_version_operation(capture, &CancellationToken::new())
        .unwrap();
    assert_suppressed(&source, version, assertion);
    let fork_project = Uuid::now_v7();
    let target = owner.path().join("fork");
    source
        .fork_research(
            &ForkResearchRequest {
                operation_uuid: Uuid::now_v7(),
                project_uuid: fork_project,
                version_uuid: version,
                projection: None,
                target: target.clone(),
                actor_uuid: creator,
                governance: "Independent decisions".into(),
                adopt_selected_ontology: true,
                metadata: graphforge_storage::WorkspaceResearchMetadata::empty(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let fork = GraphForge::new(target.to_str()).unwrap();
    let current = fork.generation_for_read().unwrap();
    assert_eq!(
        crate::research_claims::authority::project_uuid(&current).unwrap(),
        fork_project
    );
    assert_ne!(project, fork_project);
    let registry = graphforge_storage::research_versions::read_research_registry(&current).unwrap();
    assert_eq!(registry.historical_project(version), Some(project));
    assert_suppressed(&fork, version, assertion);
    // The historical event must not become a current decision of the Fork.
    let live = load(
        &fork,
        &current,
        &registry,
        &ResearchComparisonEndpoint::Project,
        None,
        &CancellationToken::new(),
    )
    .unwrap();
    assert!(!live.suppressed.contains(&("assertion".into(), assertion)));
}

fn seed_suppression(source: &mut GraphForge) -> (Uuid, Uuid, Uuid) {
    for capability_id in [
        CapabilityId::Provenance,
        CapabilityId::Knowledge,
        CapabilityId::Epistemic,
    ] {
        source
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    source.execute("CREATE (:Subject)").unwrap();
    let nodes = source
        .execute("MATCH (n:Subject) RETURN n.node_uuid")
        .unwrap();
    let node = Uuid::from_slice(
        nodes.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let assertion = Uuid::now_v7();
    let creator = Uuid::now_v7();
    source
        .create_research_claim(
            &CreateResearchClaimRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: source.generation_for_read().unwrap().generation_uuid(),
                assertion_uuid: assertion,
                claim: "historical interpretation".into(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: node,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
                category: ResearchCategory::Interpretation,
                creator_uuid: creator,
                run_uuid: None,
                created_at: 10,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let row = source.assertion(assertion, None).unwrap();
    let provenance = Uuid::from_slice(
        row.batches[0]
            .column_by_name("provenance_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let project =
        crate::research_claims::authority::project_uuid(&source.generation_for_read().unwrap())
            .unwrap();
    // The native ledger admits Project-scoped history; the public suppression
    // command currently targets Branches, so construct this owner-valid fixture.
    publish_suppression(
        source,
        ResearchSuppressionRecord {
            suppression_uuid: Uuid::now_v7(),
            assertion_uuid: assertion,
            context_uuid: project,
            creator_uuid: creator,
            provenance_uuid: provenance,
            recorded_at: 11,
        },
    );
    (assertion, creator, project)
}

fn assert_suppressed(graph: &GraphForge, version: Uuid, assertion: Uuid) {
    let current = graph.generation_for_read().unwrap();
    let registry = graphforge_storage::research_versions::read_research_registry(&current).unwrap();
    let state = load(
        graph,
        &current,
        &registry,
        &ResearchComparisonEndpoint::Version {
            version_uuid: version,
        },
        None,
        &CancellationToken::new(),
    )
    .unwrap();
    assert!(state.suppressed.contains(&("assertion".into(), assertion)));
}

fn publish_suppression(graph: &GraphForge, record: ResearchSuppressionRecord) {
    let current = graph.generation_for_read().unwrap();
    let mut participants = current
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .filter(|p| p.record_family_id != "claim_suppressions")
        .map(crate::knowledge::ledger::snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    participants.extend(
        crate::research_claims::ledger::encode_suppressions(
            &ResearchSuppressionLedger::new(vec![record]).unwrap(),
        )
        .unwrap(),
    );
    participants.sort_by(|a, b| {
        (&a.capability_id, &a.record_family_id).cmp(&(&b.capability_id, &b.record_family_id))
    });
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        participants,
        capabilities: current
            .capabilities()
            .into_iter()
            .map(|c| ProjectCapability {
                capability_id: c.capability_id,
                capability_version: c.capability_version,
            })
            .collect(),
    };
    let ProjectStageOutcome::Staged(stage) = graph.stage_project_generation(&request).unwrap()
    else {
        panic!("fresh fixture transaction")
    };
    stage
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
}
