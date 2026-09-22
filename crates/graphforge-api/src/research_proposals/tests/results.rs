//! Executed Arrow results are proposed as explicit owned Artifacts, not ephemeral runs.
use super::*;
use arrow::array::BinaryArray;
use graphforge_storage::research_versions::{
    ResearchMutation, ResearchOperation, ResearchProposalDecision::Accept,
};

fn context() -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    }
}
fn ipc(result: &ExecutionResult) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &result.schema).unwrap();
    for batch in &result.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    bytes
}
fn assert_result(graph: &GraphForge, version: Uuid, artifact: Uuid, expected: &[u8]) {
    let result = graph
        .open_research_version(version)
        .unwrap()
        .artifact_payload(artifact)
        .unwrap();
    let bytes = result.batches[0]
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(bytes.value(0), expected);
}

#[test]
fn selected_result_artifact_preserves_bytes_and_source_provenance_with_explicit_evidence_ack() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Character {score:2}), (:Character {score:5})")
        .unwrap();
    for capability_id in [CapabilityId::Provenance, CapabilityId::Knowledge] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    let baseline = Uuid::now_v7();
    let owner = Uuid::now_v7();
    let capture = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: baseline,
            context_uuid: owner,
            label: None,
            description: None,
            created_at: 1,
            required_versions: Default::default(),
        })
        .unwrap();
    graph
        .commit_research_version_operation(capture, &CancellationToken::new())
        .unwrap();
    let result = graph
        .execute("MATCH (n:Character) RETURN count(n) AS population, sum(n.score) AS total")
        .unwrap();
    assert_eq!(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    assert_eq!(
        result.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    let payload = ipc(&result);
    let private_payload = ipc(&graph
        .execute("RETURN 'PRIVATE_RESULT_SENTINEL' AS private_result")
        .unwrap());
    let source = Uuid::now_v7();
    let selected = Uuid::now_v7();
    let external = Uuid::now_v7();
    let private = Uuid::now_v7();
    graph
        .register_source(RegisterSourceRequest {
            context: context(),
            source_uuid: source,
            label: "Explicit analysis output".into(),
            source_kind: SourceKind::Other,
            identity_uri: None,
        })
        .unwrap();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            source_uuid: source,
            artifact_uuid: external,
            artifact_kind: ArtifactKind::Other,
            media_type: "text/plain".into(),
            payload: ArtifactPayloadRequest::ExternalReference {
                uri: "https://example.invalid/historical-input".into(),
                fingerprint: None,
            },
            derivation_inputs: vec![],
            run_uuid: None,
        })
        .unwrap();
    for (id, bytes) in [(selected, payload.clone()), (private, private_payload)] {
        graph
            .register_artifact(RegisterArtifactRequest {
                context: context(),
                source_uuid: source,
                artifact_uuid: id,
                artifact_kind: ArtifactKind::Other,
                media_type: "application/vnd.apache.arrow.stream".into(),
                payload: ArtifactPayloadRequest::LocalBytes(bytes),
                derivation_inputs: vec![],
                run_uuid: None,
            })
            .unwrap();
    }
    let branch_uuid = branch(&mut graph);
    let view = graph.open_research_branch(branch_uuid).unwrap();
    let version = view.version_uuid();
    let token = CancellationToken::new();
    let expected: std::collections::BTreeMap<_, _> =
        crate::branches::fields::read(view.graph(), &token)
            .unwrap()
            .into_iter()
            .filter(|(key, _)| {
                (key.0 == "source" && key.1 == source)
                    || (key.0 == "artifact" && [selected, external].contains(&key.1))
            })
            .collect();
    assert!(!expected.is_empty());
    assert_result(&graph, version, selected, &payload);
    drop(view);
    graph
        .commit_research_version_operation(
            ResearchOperation {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                mutation: ResearchMutation::RestoreProject {
                    context_uuid: owner,
                    source_version: baseline,
                    version_uuid: Uuid::now_v7(),
                    created_at: 2,
                },
            },
            &token,
        )
        .unwrap();
    assert!(graph.source(source).is_err());
    assert!(graph.artifact(selected).is_err());
    let frozen = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: version,
                },
                selector: SliceSelector::Direct {
                    members: SliceMembers {
                        sources: [source].into(),
                        artifacts: [selected, external].into(),
                        ..Default::default()
                    },
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &token,
        )
        .unwrap();
    let submission = SubmitResearchProposalRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        proposal_uuid: Uuid::now_v7(),
        source_branch_uuid: branch_uuid,
        source_version_uuid: version,
        frozen_ipc: ipc(&frozen),
        fields: expected
            .keys()
            .map(|key| ResearchFieldIdentity {
                object_kind: key.0.clone(),
                object_uuid: key.1,
                field: key.2.clone(),
            })
            .collect(),
        actor_uuid: Uuid::now_v7(),
        created_at: 3,
        motivation: "Publish one explicit result and a separately selected external reference"
            .into(),
        policy: String::new(),
    };
    graph.submit_research_proposal(&submission, &token).unwrap();
    let preview = super::super::preview::load(&graph, submission.proposal_uuid, &token).unwrap();
    assert_result(
        &graph,
        preview.proposal.payload_version_uuid,
        selected,
        &payload,
    );
    assert!(preview.source.artifact(private).is_err());
    assert!(
        preview
            .proposal
            .items
            .iter()
            .all(|item| item.unit.object_uuid != private)
    );
    let mut review = decision(&graph, submission.proposal_uuid, |_| Accept);
    review.resolve_conflicts = preview
        .rows
        .iter()
        .filter(|row| row.conflict)
        .map(|row| row.item_uuid)
        .collect();
    let evidence: std::collections::BTreeSet<_> = preview
        .rows
        .iter()
        .flat_map(|row| row.evidence_gaps.iter().map(|(id, _)| *id))
        .collect();
    assert!(evidence.contains(&external));
    drop(preview);
    let before = current(&graph);
    assert!(graph.review_research_proposal(&review, &token).is_err());
    assert_eq!(current(&graph), before);
    assert!(graph.source(source).is_err());
    review.acknowledge_evidence = evidence;
    let receipt = graph.review_research_proposal(&review, &token).unwrap();
    for reopen in [false, true] {
        if reopen {
            drop(graph);
            graph = GraphForge::new(root.to_str()).unwrap();
        }
        assert_result(&graph, receipt.version_uuid.unwrap(), selected, &payload);
        assert!(graph.artifact(private).is_err());
        let actual = crate::branches::fields::read(&graph, &token).unwrap();
        for (key, value) in &expected {
            assert_eq!(
                actual.get(key),
                Some(value),
                "accepted result/source field {key:?}"
            );
        }
        let before = current(&graph);
        assert_eq!(
            graph.review_research_proposal(&review, &token).unwrap(),
            receipt
        );
        assert_eq!(current(&graph), before);
    }
}
