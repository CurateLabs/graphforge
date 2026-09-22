use super::*;

#[test]
fn generic_publication_rejects_forged_upstream_origin_and_receipt_generation() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fixture(root, "research");
    let project = Uuid::now_v7();
    let origin = execute(root, &register(root, project))
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
    let branch = ResearchBranchRecord {
        branch_uuid: version.context_uuid,
        project_uuid: project,
        parent_branch_uuid: None,
        origin_version_uuid: origin,
        base_version_uuid: version.version_uuid,
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "review".into(),
        selection_sha256: [1; 32],
    };
    execute(
        root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(root),
            mutation: ResearchMutation::PublishBranch {
                intent_sha256: [2; 32],
                origin_capture: None,
                creation: Some(branch.clone()),
                version: Box::new(version.clone()),
            },
        },
    );
    let parent = crate::resolve_project_generation(root).unwrap();
    let before = state(root);
    let capture = RegisterResearchVersion {
        version_uuid: Uuid::now_v7(),
        context_uuid: project,
        source_generation_uuid: parent.generation_uuid(),
        source_version: None,
        selection: None,
        required_versions: BTreeSet::new(),
        label: None,
        description: None,
        created_at: 2,
        evidence: Vec::new(),
    };
    let mut after = before.clone();
    branches::stage_origin(root, &mut after, &capture).unwrap();
    let next = Uuid::now_v7();
    version.version_uuid = next;
    branches::publish(root, &mut after, None, &version).unwrap();
    after.versions.remove(&capture.version_uuid);
    let operation = Uuid::now_v7();
    let generation = Uuid::now_v7();
    let mut receipt = before.receipts.values().next().unwrap().clone();
    receipt.operation_uuid = operation;
    receipt.generation_uuid = generation;
    receipt.version_uuid = Some(next);
    receipt.intent_sha256 = Some([3; 32]);
    after.receipts.insert(operation, receipt);
    after.upstream.reviews.insert(
        operation,
        ResearchUpstreamReview {
            sequence: 1,
            operation_uuid: operation,
            branch_uuid: branch.branch_uuid,
            original_base_version_uuid: branch.base_version_uuid,
            prior_version_uuid: branch.base_version_uuid,
            upstream_version_uuid: capture.version_uuid,
            version_uuid: next,
            preview_generation_uuid: parent.generation_uuid(),
            preview_sha256: [4; 32],
            fields: vec![ResearchUpstreamFieldReview {
                unit: ResearchUpstreamField {
                    object_kind: "node".into(),
                    object_uuid: Uuid::now_v7(),
                    field: "property:x".into(),
                },
                baseline_sha256: Some([0; 32]),
                local_sha256: Some([0; 32]),
                upstream_sha256: Some([1; 32]),
                result_sha256: Some([0; 32]),
                resolution: ResearchUpstreamResolutionRecord::KeepLocal,
            }],
            acknowledged_evidence: BTreeSet::new(),
            actor_uuid: Uuid::now_v7(),
            created_at: 2,
            explanation: String::new(),
        },
    );
    super::super::upstream::preserve(&before, &after, &parent, generation, Some(&capture)).unwrap();
    let mut forged = after.clone();
    let review = forged.upstream.reviews.get_mut(&operation).unwrap();
    review.upstream_version_uuid = origin;
    assert!(
        super::super::upstream::preserve(&before, &forged, &parent, generation, Some(&capture))
            .unwrap_err()
            .to_string()
            .contains("Project capture")
    );
    assert!(
        super::super::upstream::preserve(&before, &after, &parent, generation, None)
            .unwrap_err()
            .to_string()
            .contains("immediate parent")
    );
    let mut wrong_context = capture.clone();
    wrong_context.context_uuid = Uuid::now_v7();
    assert!(
        super::super::upstream::preserve(
            &before,
            &after,
            &parent,
            generation,
            Some(&wrong_context)
        )
        .unwrap_err()
        .to_string()
        .contains("Project capture")
    );
    let mut forged = after.clone();
    forged.receipts.get_mut(&operation).unwrap().generation_uuid = Uuid::now_v7();
    assert!(
        super::super::upstream::preserve(&before, &forged, &parent, generation, Some(&capture))
            .unwrap_err()
            .to_string()
            .contains("publication transition")
    );
    assert_eq!(current(root), parent.generation_uuid());
    assert_eq!(state(root), before);
}
