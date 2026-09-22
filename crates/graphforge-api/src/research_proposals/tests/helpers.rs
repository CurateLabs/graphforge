use super::*;

pub(super) fn branch(graph: &mut GraphForge) -> Uuid {
    let branch_uuid = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(graph),
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
    branch_uuid
}

pub(super) fn node(graph: &mut GraphForge) -> Uuid {
    let result = graph
        .execute("MATCH (n:Character) RETURN n.node_uuid AS id")
        .unwrap();
    Uuid::from_slice(
        result.batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap()
}

pub(super) fn submit(
    graph: &mut GraphForge,
    branch: Uuid,
    version: Uuid,
    node: Uuid,
    fields: &[&str],
) -> SubmitResearchProposalRequest {
    let frozen = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: version,
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
        expected_generation_uuid: current(graph),
        proposal_uuid: Uuid::now_v7(),
        source_branch_uuid: branch,
        source_version_uuid: version,
        frozen_ipc,
        fields: fields
            .iter()
            .map(|field| ResearchFieldIdentity {
                object_kind: "node".into(),
                object_uuid: node,
                field: (*field).into(),
            })
            .collect(),
        actor_uuid: Uuid::now_v7(),
        created_at: 3,
        motivation: "Selected review".into(),
        policy: String::new(),
    };
    graph
        .submit_research_proposal(&request, &CancellationToken::new())
        .unwrap();
    request
}

pub(super) fn decision(
    graph: &GraphForge,
    proposal: Uuid,
    choose: impl Fn(&str) -> graphforge_storage::research_versions::ResearchProposalDecision,
) -> ReviewResearchProposalRequest {
    let preview = super::super::preview::load(graph, proposal, &CancellationToken::new()).unwrap();
    ReviewResearchProposalRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: preview.generation,
        proposal_uuid: proposal,
        preview_sha256: preview.digest,
        decisions: preview
            .proposal
            .items
            .iter()
            .map(|item| (item.item_uuid, choose(&item.unit.field)))
            .collect(),
        resolve_conflicts: Default::default(),
        acknowledge_evidence: Default::default(),
        promotions: vec![],
        community_uuid: None,
        actor_uuid: Uuid::now_v7(),
        created_at: 4,
        explanation: "Explicit review".into(),
        policy: String::new(),
    }
}
