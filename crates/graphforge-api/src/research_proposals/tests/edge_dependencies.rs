//! A new relationship cannot be accepted while its new endpoint is deferred.
use super::*;
use graphforge_storage::research_versions::ResearchProposalDecision::{Accept, Defer};

fn uuid_column(result: &ExecutionResult, name: &str) -> Uuid {
    Uuid::from_slice(
        result.batches[0]
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap()
}

#[test]
fn edge_acceptance_requires_new_endpoint_and_excludes_private_properties() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Character {private_note:'parent private'})")
        .unwrap();
    let original = node(&mut graph);
    let branch_uuid = branch(&mut graph);
    let version = edit(
        &mut graph,
        branch_uuid,
        "MATCH (a:Character) CREATE (b:NewCharacter {private_note:'branch private'}), (a)-[:RELATED {private_note:'edge private'}]->(b)",
    );
    let view = graph.open_research_branch(branch_uuid).unwrap();
    let ids = view.graph().execute(
        "MATCH (a:Character)-[r:RELATED]->(b:NewCharacter) RETURN r.edge_uuid AS edge, b.node_uuid AS endpoint"
    ).unwrap();
    let edge = uuid_column(&ids, "edge");
    let endpoint = uuid_column(&ids, "endpoint");
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
                        nodes: [endpoint].into(),
                        edges: [edge].into(),
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
    let mut fields = Vec::new();
    for (kind, id, names) in [
        ("node", endpoint, &["$object", "$labels"][..]),
        (
            "edge",
            edge,
            &[
                "$object",
                "$relationship_type",
                "$source_uuid",
                "$target_uuid",
            ][..],
        ),
    ] {
        fields.extend(names.iter().map(|name| ResearchFieldIdentity {
            object_kind: kind.into(),
            object_uuid: id,
            field: (*name).into(),
        }));
    }
    let submission = SubmitResearchProposalRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        proposal_uuid: Uuid::now_v7(),
        source_branch_uuid: branch_uuid,
        source_version_uuid: version,
        frozen_ipc,
        fields,
        actor_uuid: Uuid::now_v7(),
        created_at: 3,
        motivation: "Review relationship and required new endpoint only".into(),
        policy: String::new(),
    };
    graph
        .submit_research_proposal(&submission, &CancellationToken::new())
        .unwrap();
    let preview =
        super::super::preview::load(&graph, submission.proposal_uuid, &CancellationToken::new())
            .unwrap();
    let endpoint_items: std::collections::BTreeSet<_> = preview
        .proposal
        .items
        .iter()
        .filter(|item| item.unit.object_uuid == endpoint)
        .map(|item| item.item_uuid)
        .collect();
    assert_eq!(endpoint_items.len(), 2);
    for item in preview
        .proposal
        .items
        .iter()
        .filter(|item| item.unit.object_uuid == edge)
    {
        let row = preview
            .rows
            .iter()
            .find(|row| row.item_uuid == item.item_uuid)
            .unwrap();
        assert!(
            endpoint_items.is_subset(&row.required_items),
            "edge preview must require endpoint structural items"
        );
        assert!(row.unavailable.is_empty());
    }
    let mut review = decision(&graph, submission.proposal_uuid, |_| Accept);
    for id in &endpoint_items {
        review.decisions.insert(*id, Defer);
    }
    let before = graph.research_version_retention().unwrap();
    let generation = current(&graph);
    assert!(
        graph
            .review_research_proposal(&review, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(current(&graph), generation);
    assert_eq!(graph.research_version_retention().unwrap(), before);
    let empty = graph
        .execute("MATCH ()-[r:RELATED]->() RETURN count(r) AS count")
        .unwrap();
    assert_eq!(
        empty.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    let review = decision(&graph, submission.proposal_uuid, |_| Accept);
    graph
        .review_research_proposal(&review, &CancellationToken::new())
        .unwrap();
    let result = graph.execute("MATCH (a:Character)-[r:RELATED]->(b:NewCharacter) RETURN a.node_uuid AS original, r.edge_uuid AS edge, b.node_uuid AS endpoint").unwrap();
    assert_eq!(
        result.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        1
    );
    assert_eq!(uuid_column(&result, "original"), original);
    assert_eq!(uuid_column(&result, "edge"), edge);
    assert_eq!(uuid_column(&result, "endpoint"), endpoint);
    let values = crate::branches::fields::read(&graph, &CancellationToken::new()).unwrap();
    assert!(values.contains_key(&("node".into(), original, "property:private_note".into())));
    for (kind, id) in [("node", endpoint), ("edge", edge)] {
        assert!(!values.contains_key(&(kind.into(), id, "property:private_note".into())));
    }
    let registry = graph.research_version_retention().unwrap();
    assert_eq!(registry.proposals.accepted.len(), 6);
    let proof_id = registry
        .proposals
        .accepted
        .values()
        .next()
        .unwrap()
        .proof_version_uuid;
    let proof =
        crate::research_versions::materialize_version(&graph, &registry.versions[&proof_id])
            .unwrap();
    let retained = crate::branches::fields::read(&proof, &CancellationToken::new()).unwrap();
    assert!(retained.keys().all(|key| key.2 != "property:private_note"));
    drop(proof);
    drop(graph);
    let graph = GraphForge::new(root.to_str()).unwrap();
    let result = graph.execute("MATCH ()-[r:RELATED]->(b:NewCharacter) RETURN r.edge_uuid AS edge, b.node_uuid AS endpoint").unwrap();
    assert_eq!(uuid_column(&result, "edge"), edge);
    assert_eq!(uuid_column(&result, "endpoint"), endpoint);
}
