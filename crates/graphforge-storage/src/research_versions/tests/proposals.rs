//! Storage publication fixtures; facade tests must prove typed selected application.
use super::*;

fn branch(root: &Path) -> ResearchVersionRecord {
    let origin = execute(root, &register(root, Uuid::now_v7()))
        .version_uuid
        .unwrap();
    execute(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation: ResearchMutation::Compact {
                versions: BTreeSet::from([origin]),
            },
        },
    );
    let mut version = state(root).versions[&origin].clone();
    version.version_uuid = Uuid::now_v7();
    version.context_uuid = Uuid::now_v7();
    version.content.source_version = Some(origin);
    execute(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation: ResearchMutation::PublishBranch {
                intent_sha256: [1; 32],
                origin_capture: None,
                creation: Some(ResearchBranchRecord {
                    branch_uuid: version.context_uuid,
                    project_uuid: Uuid::now_v7(),
                    parent_branch_uuid: None,
                    origin_version_uuid: origin,
                    base_version_uuid: version.version_uuid,
                    creator_uuid: Uuid::now_v7(),
                    created_at: 1,
                    label: "foundation fixture".into(),
                    selection_sha256: [0; 32],
                }),
                version: Box::new(version.clone()),
            },
        },
    );
    version
}

fn submission(root: &Path, source: &ResearchVersionRecord) -> ResearchOperation {
    let mut payload = source.clone();
    payload.version_uuid = Uuid::now_v7();
    payload.context_uuid = Uuid::now_v7();
    payload.content.source_version = Some(source.version_uuid);
    let operation_uuid = Uuid::now_v7();
    let proposal = ResearchProposalRecord {
        proposal_uuid: Uuid::now_v7(),
        source_branch_uuid: source.context_uuid,
        source_version_uuid: source.version_uuid,
        destination: ResearchProposalDestination::Project {
            project_uuid: state(root).branches[&source.context_uuid].project_uuid,
        },
        payload_version_uuid: payload.version_uuid,
        operation_uuid,
        actor_uuid: Uuid::now_v7(),
        created_at: 2,
        motivation: "storage fixture".into(),
        policy: String::new(),
        items: vec![ResearchProposalItem {
            item_uuid: Uuid::now_v7(),
            unit: ResearchProposalUnit {
                object_kind: "node".into(),
                object_uuid: Uuid::now_v7(),
                field: "name".into(),
            },
            contribution_uuid: Uuid::now_v7(),
            value_sha256: Some([7; 32]),
            baseline_sha256: Some([3; 32]),
            required_items: BTreeSet::new(),
        }],
    };
    ResearchOperation {
        operation_uuid,
        expected_generation_uuid: current(root),
        mutation: ResearchMutation::SubmitProposal {
            intent_sha256: [2; 32],
            proposal: Box::new(proposal),
            payload: Box::new(payload),
        },
    }
}

#[test]
fn frozen_payload_never_advances_branch_and_release_keeps_identity_and_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "frozen content");
    let source = branch(root);
    let operation = submission(root, &source);
    let before = state(root).heads;
    let receipt = execute(root, &operation);
    let ResearchMutation::SubmitProposal {
        proposal, payload, ..
    } = &operation.mutation
    else {
        unreachable!()
    };
    assert_eq!(state(root).heads, before);
    assert!(state(root).roots.contains_key(&proposal.proposal_uuid));
    let invalid_restore = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(root),
        mutation: ResearchMutation::Restore {
            context_uuid: source.context_uuid,
            source_version: payload.version_uuid,
            version_uuid: Uuid::now_v7(),
            created_at: 3,
        },
    };
    assert!(publish_research_operation(root, &invalid_restore, &AtomicBool::new(false)).is_err());
    execute(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation: ResearchMutation::ReleaseProposal {
                intent_sha256: [3; 32],
                proposal_uuid: proposal.proposal_uuid,
            },
        },
    );
    execute(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation: ResearchMutation::DeleteVersion {
                version_uuid: payload.version_uuid,
            },
        },
    );
    crate::project_recovery::recover_project_on_open(root).unwrap();
    assert_eq!(execute(root, &operation), receipt);
    assert!(state(root).identities.contains_key(&payload.version_uuid));
    assert_eq!(
        state(root).proposals.proposals[&proposal.proposal_uuid],
        **proposal
    );
}

#[test]
fn project_acceptance_installs_content_review_mapping_and_receipt_in_one_generation() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fixture(root, "parent before acceptance");
    let source = branch(root);
    let submit = submission(root, &source);
    execute(root, &submit);
    let ResearchMutation::SubmitProposal {
        proposal, payload, ..
    } = &submit.mutation
    else {
        unreachable!()
    };
    let ResearchProposalDestination::Project { project_uuid } = proposal.destination else {
        unreachable!()
    };
    let ResearchMutation::Register(mut spec) = register(root, project_uuid).mutation else {
        unreachable!()
    };
    spec.created_at = 3;
    let draft = prepare_project_draft(root, &spec, &AtomicBool::new(false)).unwrap();
    fixture(draft.path(), "accepted parent content");
    let destination = draft.finish(vec![], &AtomicBool::new(false)).unwrap();
    let mut proof = *payload.clone();
    proof.version_uuid = Uuid::now_v7();
    proof.context_uuid = Uuid::now_v7();
    let operation_uuid = Uuid::now_v7();
    let item = &proposal.items[0];
    let mut mapping = ResearchAcceptedMapping {
        mapping_uuid: Uuid::nil(),
        destination: proposal.destination.clone(),
        unit: item.unit.clone(),
        contribution_uuid: item.contribution_uuid,
        value_sha256: item.value_sha256,
        source_branch_uuid: source.context_uuid,
        source_version_uuid: source.version_uuid,
        destination_version_uuid: spec.version_uuid,
        proof_version_uuid: proof.version_uuid,
        operation_uuid,
        item_uuid: item.item_uuid,
    };
    mapping.mapping_uuid = mapping.identity().unwrap();
    let review = ResearchProposalReview {
        sequence: 1,
        operation_uuid,
        proposal_uuid: proposal.proposal_uuid,
        preview_generation_uuid: current(root),
        preview_sha256: [0; 32],
        resolved_conflicts: BTreeSet::new(),
        acknowledged_evidence: BTreeSet::new(),
        destination_version_uuid: Some(spec.version_uuid),
        actor_uuid: Uuid::now_v7(),
        created_at: 3,
        explanation: "accepted".into(),
        policy: String::new(),
        decisions: BTreeMap::from([(item.item_uuid, ResearchProposalDecision::Accept)]),
        mappings: BTreeSet::from([mapping.mapping_uuid]),
    };
    let operation = ResearchOperation {
        operation_uuid,
        expected_generation_uuid: current(root),
        mutation: ResearchMutation::ReviewProposal {
            intent_sha256: [4; 32],
            review: Box::new(review),
            destination: Some(Box::new(destination.version.clone())),
            proof: Some(Box::new(proof)),
            mappings: vec![mapping.clone()],
            decisions: None,
        },
    };
    let before = current(root);
    assert!(publish_research_operation(root, &operation, &AtomicBool::new(true)).is_err());
    assert_eq!(current(root), before);
    let receipt = execute(root, &operation);
    drop(destination);
    drop(draft);
    crate::project_recovery::recover_project_on_open(root).unwrap();
    let after = crate::resolve_project_generation(root).unwrap();
    assert_eq!(after.generation_uuid(), receipt.generation_uuid);
    assert_eq!(
        after
            .participant_snapshot("workspace", "research_fixture")
            .unwrap()
            .unwrap()
            .bytes,
        json(&"accepted parent content").unwrap()
    );
    let registry = state(root);
    assert_eq!(registry.proposals.accepted[&mapping.mapping_uuid], mapping);
    assert_eq!(registry.receipts[&operation_uuid], receipt);
    assert_eq!(registry.heads[&source.context_uuid], source.version_uuid);
    assert_eq!(execute(root, &operation), receipt);
    let mut changed = operation.clone();
    let ResearchMutation::ReviewProposal { review, .. } = &mut changed.mutation else {
        unreachable!()
    };
    review.explanation.push_str(" changed");
    let error = publish_research_operation(root, &changed, &AtomicBool::new(false)).unwrap_err();
    assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
}
