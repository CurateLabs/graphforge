//! Selected immutable claims retain classification without disclosing private siblings.
use super::*;
use graphforge_knowledge::{AssertionGraphRole, GraphObjectKind, research::ResearchCategory};
use graphforge_storage::research_versions::ResearchProposalDecision::Accept;

fn assert_claims(graph: &GraphForge, expected: &[Uuid]) {
    let fields = crate::branches::fields::read(graph, &CancellationToken::new()).unwrap();
    let actual: std::collections::BTreeSet<_> = fields
        .keys()
        .filter(|key| key.0 == "assertion" && key.2 == "$object")
        .map(|key| key.1)
        .collect();
    assert_eq!(actual, expected.iter().copied().collect());
}

fn assert_public_claim(graph: &GraphForge, expected: Uuid) {
    let result = graph
        .inspect_research_claims(&InspectResearchClaimsRequest {
            context: ResearchContext::Project,
            community_uuid: None,
            include_suppressed: true,
        })
        .unwrap();
    assert_eq!(
        result
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    let batch = result
        .batches
        .iter()
        .find(|batch| batch.num_rows() != 0)
        .unwrap();
    let id = batch
        .column_by_name("assertion_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(Uuid::from_slice(id.value(0)).unwrap(), expected);
}

#[test]
fn selected_claim_preserves_native_fields_and_excludes_private_claim_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (:Character)").unwrap();
    for capability_id in [
        CapabilityId::Provenance,
        CapabilityId::Knowledge,
        CapabilityId::Epistemic,
    ] {
        graph
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
    let subject = node(&mut graph);
    let branch_uuid = branch(&mut graph);
    let selected = Uuid::now_v7();
    let private = Uuid::now_v7();
    let mut version = Uuid::nil();
    for (assertion_uuid, claim) in [
        (selected, "Selected interpretation for review"),
        (private, "PRIVATE_UNSELECTED_CLAIM_SENTINEL"),
    ] {
        version = Uuid::now_v7();
        graph
            .change_research_branch_claim(
                &ChangeResearchBranchClaimRequest {
                    operation_uuid: Uuid::now_v7(),
                    expected_generation_uuid: current(&graph),
                    branch_uuid,
                    version_uuid: version,
                    creator_uuid: Uuid::now_v7(),
                    created_at: 2,
                    change: ResearchClaimChange::Create {
                        claim: ResearchClaimDraft {
                            assertion_uuid,
                            claim: claim.into(),
                            graph_refs: vec![AssertionGraphRefInput {
                                graph_uuid: subject,
                                graph_kind: GraphObjectKind::Node,
                                role: AssertionGraphRole::Subject,
                                ordinal: 0,
                            }],
                            category: ResearchCategory::Interpretation,
                            run_uuid: None,
                        },
                    },
                },
                &CancellationToken::new(),
            )
            .unwrap();
    }
    assert_claims(&graph, &[]);
    let view = graph.open_research_branch(branch_uuid).unwrap();
    assert_claims(view.graph(), &[selected, private]);
    let source_fields =
        crate::branches::fields::read(view.graph(), &CancellationToken::new()).unwrap();
    let selected_fields: std::collections::BTreeMap<_, _> = source_fields
        .into_iter()
        .filter(|(key, _)| key.0 == "assertion" && key.1 == selected)
        .collect();
    assert!(selected_fields.contains_key(&(
        "assertion".into(),
        selected,
        "$research_classification".into()
    )));
    assert!(selected_fields.contains_key(&("assertion".into(), selected, "$object".into())));
    drop(view);
    let frozen = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: version,
                },
                selector: SliceSelector::Direct {
                    members: SliceMembers {
                        assertions: [selected].into(),
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
    let submission = SubmitResearchProposalRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        proposal_uuid: Uuid::now_v7(),
        source_branch_uuid: branch_uuid,
        source_version_uuid: version,
        frozen_ipc,
        fields: selected_fields
            .keys()
            .map(|key| ResearchFieldIdentity {
                object_kind: key.0.clone(),
                object_uuid: key.1,
                field: key.2.clone(),
            })
            .collect(),
        actor_uuid: Uuid::now_v7(),
        created_at: 3,
        motivation: "Review one interpretation and its required subject".into(),
        policy: String::new(),
    };
    graph
        .submit_research_proposal(&submission, &CancellationToken::new())
        .unwrap();
    let preview =
        super::super::preview::load(&graph, submission.proposal_uuid, &CancellationToken::new())
            .unwrap();
    assert_claims(&preview.source, &[selected]);
    assert!(
        preview
            .proposal
            .items
            .iter()
            .all(|item| item.unit.object_uuid == selected)
    );
    drop(preview);
    let review = decision(&graph, submission.proposal_uuid, |_| Accept);
    let receipt = graph
        .review_research_proposal(&review, &CancellationToken::new())
        .unwrap();
    assert_claims(&graph, &[selected]);
    assert_public_claim(&graph, selected);
    let actual = crate::branches::fields::read(&graph, &CancellationToken::new()).unwrap();
    for (key, value) in &selected_fields {
        assert_eq!(
            actual.get(key),
            Some(value),
            "accepted native field {key:?}"
        );
    }
    assert!(
        !actual
            .keys()
            .any(|key| key.0 == "assertion" && key.1 == private)
    );
    let branch_view = graph.open_research_branch(branch_uuid).unwrap();
    assert_claims(branch_view.graph(), &[selected, private]);
    drop(branch_view);
    drop(graph);
    let mut reopened = GraphForge::new(root.to_str()).unwrap();
    assert_claims(&reopened, &[selected]);
    assert_public_claim(&reopened, selected);
    let before = current(&reopened);
    assert_eq!(
        reopened
            .review_research_proposal(&review, &CancellationToken::new())
            .unwrap(),
        receipt
    );
    assert_eq!(current(&reopened), before);
}
