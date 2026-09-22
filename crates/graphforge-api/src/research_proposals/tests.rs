//! Direct native facade proof; no mocked plans or wrapper-only assertions.
mod admission;
mod claims;
mod edge_dependencies;
mod helpers;
mod historical_comparison;
mod interchange;
mod mixed_authority_tests;
mod nested;
mod ontology_dependencies;
mod partial_review;
mod recovery;
mod restore;
mod results;
mod retention;
mod two_stories;
use crate::*;
use arrow::array::{FixedSizeBinaryArray, Int64Array};
use helpers::*;
use uuid::Uuid;

fn current(graph: &GraphForge) -> Uuid {
    graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}

fn edit(graph: &mut GraphForge, branch: Uuid, query: &str) -> Uuid {
    let version_uuid = Uuid::now_v7();
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(graph),
                branch_uuid: branch,
                version_uuid,
                created_at: 2,
                query: query.into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    version_uuid
}

#[test]
fn frozen_submission_survives_continued_branch_edits_and_retains_only_selected_fields() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Character {score:0, private_note:'unselected'})")
        .unwrap();
    let branch_uuid = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                branch_uuid,
                version_uuid: Uuid::now_v7(),
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "Research".into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let source_version_uuid = edit(&mut graph, branch_uuid, "MATCH (n:Character) SET n.score=1");
    let ids = graph
        .execute("MATCH (n:Character) RETURN n.node_uuid AS id")
        .unwrap();
    let node = Uuid::from_slice(
        ids.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    let frozen = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: source_version_uuid,
                },
                selector: SliceSelector::Direct {
                    members: SliceMembers {
                        nodes: [node].into(),
                        ..Default::default()
                    },
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let mut frozen_ipc = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut frozen_ipc, &frozen.schema).unwrap();
    for batch in &frozen.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    let request = SubmitResearchProposalRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        proposal_uuid: Uuid::now_v7(),
        source_branch_uuid: branch_uuid,
        source_version_uuid,
        frozen_ipc,
        fields: vec![ResearchFieldIdentity {
            object_kind: "node".into(),
            object_uuid: node,
            field: "property:score".into(),
        }],
        actor_uuid: Uuid::now_v7(),
        created_at: 3,
        motivation: "Review score only".into(),
        policy: String::new(),
    };
    let receipt = graph
        .submit_research_proposal(&request, &CancellationToken::new())
        .unwrap();
    let before_preview = current(&graph);
    let preview = graph
        .preview_research_proposal(
            &PreviewResearchProposalRequest {
                proposal_uuid: request.proposal_uuid,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(current(&graph), before_preview);
    let conflict = preview.batches[0]
        .column_by_name("conflict")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(!conflict.value(0));
    let missing = preview.batches[0]
        .column_by_name("unavailable_dependencies")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .unwrap();
    assert_eq!(missing.value_length(0), 0);
    graph.execute("MATCH (n:Character) SET n.score=3").unwrap();
    let conflict_preview = graph
        .preview_research_proposal(
            &PreviewResearchProposalRequest {
                proposal_uuid: request.proposal_uuid,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert!(
        conflict_preview.batches[0]
            .column_by_name("conflict")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_ne!(
        preview.batches[0].column_by_name("preview_sha256"),
        conflict_preview.batches[0].column_by_name("preview_sha256")
    );
    assert_eq!(
        graph
            .open_research_branch(branch_uuid)
            .unwrap()
            .version_uuid(),
        source_version_uuid
    );
    let registry = graph.research_version_retention().unwrap();
    let proposal = registry.proposals.proposals[&request.proposal_uuid].clone();
    let version = &registry.versions[&proposal.payload_version_uuid];
    let payload = crate::research_versions::materialize_version(&graph, version).unwrap();
    let fields = crate::branches::fields::read(&payload, &CancellationToken::new()).unwrap();
    assert!(!fields.keys().any(|key| key.2 == "property:private_note"));
    let baseline = crate::branches::baseline::read(&payload).unwrap();
    assert_eq!(baseline.len(), 1);
    assert!(baseline.keys().all(|key| key.2 == "property:score"));
    edit(&mut graph, branch_uuid, "MATCH (n:Character) SET n.score=2");
    drop(payload);
    drop(graph);
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        graph
            .submit_research_proposal(&request, &CancellationToken::new())
            .unwrap(),
        receipt
    );
    let payload = crate::research_versions::materialize_version(
        &graph,
        &graph
            .research_version(proposal.payload_version_uuid)
            .unwrap(),
    )
    .unwrap();
    let score = payload
        .execute("MATCH (n:Character) RETURN n.score AS score")
        .unwrap();
    assert_eq!(
        score.batches[0]
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        graph
            .research_version_retention()
            .unwrap()
            .proposals
            .proposals[&request.proposal_uuid],
        proposal
    );
    let preview =
        super::preview::load(&graph, request.proposal_uuid, &CancellationToken::new()).unwrap();
    let item = proposal.items[0].item_uuid;
    let mut review = ReviewResearchProposalRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: preview.generation,
        proposal_uuid: request.proposal_uuid,
        preview_sha256: preview.digest,
        decisions: [(
            item,
            graphforge_storage::research_versions::ResearchProposalDecision::Accept,
        )]
        .into(),
        resolve_conflicts: [item].into(),
        acknowledge_evidence: Default::default(),
        promotions: vec![],
        community_uuid: None,
        actor_uuid: Uuid::now_v7(),
        created_at: 4,
        explanation: "Use reviewed score, preserve private fields".into(),
        policy: String::new(),
    };
    let accepted = graph
        .review_research_proposal(&review, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        graph
            .research_canonical_choices(&ResearchContext::Project, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        0
    );
    let parent = graph
        .execute("MATCH (n:Character) RETURN n.score AS score, n.private_note AS private_note")
        .unwrap();
    assert_eq!(
        parent.batches[0]
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        parent.batches[0]
            .column_by_name("private_note")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0),
        "unselected"
    );
    graph.execute("MATCH (n:Character) SET n.score=99").unwrap();
    let advanced = current(&graph);
    assert_eq!(
        graph
            .review_research_proposal(&review, &CancellationToken::new())
            .unwrap(),
        accepted
    );
    assert_eq!(current(&graph), advanced);
    let preview =
        super::preview::load(&graph, request.proposal_uuid, &CancellationToken::new()).unwrap();
    assert!(preview.rows[0].already_accepted);
    review.operation_uuid = Uuid::now_v7();
    review.expected_generation_uuid = preview.generation;
    review.preview_sha256 = preview.digest;
    review.resolve_conflicts.clear();
    review.promotions = vec![ResearchDecisionInput {
        decision_uuid: Uuid::now_v7(),
        subject_kind: graphforge_knowledge::research::ResearchSubjectKind::Node,
        subject_uuid: node,
        kind: graphforge_knowledge::research::ResearchDecisionKind::Promote,
        source_version_uuid: Some(source_version_uuid),
    }];
    let duplicate = graph
        .review_research_proposal(&review, &CancellationToken::new())
        .unwrap();
    assert!(duplicate.version_uuid.is_none());
    assert_eq!(
        graph
            .research_canonical_choices(&ResearchContext::Project, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    let decisions =
        crate::research_claims::ledger::read_decisions(&graph.generation_for_read().unwrap())
            .unwrap();
    assert_eq!(decisions.events().len(), 2);
    assert_eq!(
        decisions.events()[0].kind,
        graphforge_knowledge::research::ResearchDecisionKind::Integrate
    );
    assert_eq!(
        decisions.events()[1].kind,
        graphforge_knowledge::research::ResearchDecisionKind::Promote
    );
    assert_eq!(
        graph
            .research_version_retention()
            .unwrap()
            .proposals
            .accepted
            .len(),
        1
    );
    let parent = graph
        .execute("MATCH (n:Character) RETURN n.score AS score")
        .unwrap();
    assert_eq!(
        parent.batches[0]
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        99
    );
    let history_request = ResearchProposalHistoryRequest {
        proposal_uuid: request.proposal_uuid,
        detail: ResearchProposalHistoryDetail::Reviews,
        page_size: 1,
        after: None,
    };
    let page = graph
        .research_proposal_history(&history_request, &CancellationToken::new())
        .unwrap();
    let next = page.batches[0]
        .column_by_name("next")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap()
        .value(0)
        .to_string();
    let mut continued = history_request.clone();
    continued.after = Some(next);
    let page = graph
        .research_proposal_history(&continued, &CancellationToken::new())
        .unwrap();
    assert_eq!(page.batches[0].num_rows(), 1);
    graph
        .release_research_proposal(
            &ReleaseResearchProposalRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                proposal_uuid: request.proposal_uuid,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(
        graph
            .research_proposal_history(&continued, &CancellationToken::new())
            .unwrap_err()
            .code(),
        "GF_PAGE_SNAPSHOT_GONE"
    );
    for version_uuid in [proposal.payload_version_uuid, source_version_uuid] {
        graph
            .commit_research_version_operation(
                graphforge_storage::research_versions::ResearchOperation {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: current(&graph),
                    mutation:
                        graphforge_storage::research_versions::ResearchMutation::DeleteVersion {
                            version_uuid,
                        },
                },
                &CancellationToken::new(),
            )
            .unwrap();
    }
    assert!(
        graph
            .research_version(proposal.payload_version_uuid)
            .is_err()
    );
    let accepted_history = graph
        .research_proposal_history(
            &ResearchProposalHistoryRequest {
                detail: ResearchProposalHistoryDetail::Accepted,
                ..history_request
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(accepted_history.batches[0].num_rows(), 1);
    drop(graph);
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        graph
            .submit_research_proposal(&request, &CancellationToken::new())
            .unwrap(),
        receipt
    );
    assert_eq!(
        graph
            .review_research_proposal(&review, &CancellationToken::new())
            .unwrap(),
        duplicate
    );
    let comparison = graph
        .compare_research(
            &ResearchComparisonRequest {
                left: ResearchComparisonEndpoint::Branch { branch_uuid },
                right: ResearchComparisonEndpoint::Project,
                left_authority: None,
                right_authority: None,
                detail: ResearchComparisonDetail::Changes,
                accepted: vec![],
                max_fields: 40000,
                max_bytes: 64 * 1024 * 1024,
                page_size: 1000,
                after: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let batch = &comparison.batches[0];
    let fields = batch
        .column_by_name("field")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let row = (0..batch.num_rows())
        .find(|i| fields.value(*i) == "property:score")
        .unwrap();
    let source = batch
        .column_by_name("accepted_source_version_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(
        Uuid::from_slice(source.value(row)).unwrap(),
        source_version_uuid
    );
    let changes = batch
        .column_by_name("change")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(changes.value(row), "conflict");
    let mut changed = request;
    changed.motivation.push_str(" changed");
    assert_eq!(
        graph
            .submit_research_proposal(&changed, &CancellationToken::new())
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
}
