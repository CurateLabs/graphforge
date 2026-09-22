use super::*;
use crate::PrepareResearchVersionRequest;
use graphforge_storage::research_versions::{ResearchMutation, ResearchOperation};

#[test]
fn committed_fork_replays_after_source_version_release_without_resetting_destination() {
    let mut source = GraphForge::new(None).unwrap();
    source.execute("CREATE (:Item {score:7})").unwrap();
    let context = Uuid::now_v7();
    let version = Uuid::now_v7();
    capture(&mut source, context, version);
    let owner = tempfile::tempdir().unwrap();
    let request = ForkResearchRequest {
        operation_uuid: Uuid::now_v7(),
        project_uuid: Uuid::now_v7(),
        version_uuid: version,
        projection: None,
        target: owner.path().join("fork"),
        actor_uuid: Uuid::now_v7(),
        governance: "Independent local review".into(),
        adopt_selected_ontology: true,
        metadata: graphforge_storage::WorkspaceResearchMetadata::empty(),
    };
    let first = source
        .fork_research(&request, &CancellationToken::new())
        .unwrap();
    let destination = GraphForge::new(Some(request.target.to_str().unwrap())).unwrap();
    destination.execute("MATCH(n:Item) SET n.score=9").unwrap();
    let authority = destination.generation_for_read().unwrap().generation_uuid();
    capture(&mut source, context, Uuid::now_v7());
    source
        .commit_research_version_operation(
            ResearchOperation {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: source.generation_for_read().unwrap().generation_uuid(),
                mutation: ResearchMutation::DeleteVersion {
                    version_uuid: version,
                },
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert!(source.research_version(version).is_err());
    let replay = source
        .fork_research(&request, &CancellationToken::new())
        .unwrap();
    assert!(replay.idempotent_replay);
    assert_eq!(replay.generation_uuid, first.generation_uuid);
    let mut changed = request.clone();
    changed.metadata.title = Some("different public intent".into());
    assert_eq!(
        source
            .fork_research(&changed, &CancellationToken::new())
            .unwrap_err()
            .code,
        PortableV2ErrorCode::ConcurrentMutation
    );
    let reopened = GraphForge::new(Some(request.target.to_str().unwrap())).unwrap();
    assert_eq!(
        reopened.generation_for_read().unwrap().generation_uuid(),
        authority
    );
    let result = reopened.execute("MATCH(n:Item) RETURN n.score").unwrap();
    let scores = result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(scores.value(0), 9);
}

fn capture(graph: &mut GraphForge, context_uuid: Uuid, version_uuid: Uuid) {
    let operation = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid,
            context_uuid,
            label: None,
            description: None,
            created_at: 1,
            required_versions: Default::default(),
        })
        .unwrap();
    graph
        .commit_research_version_operation(operation, &CancellationToken::new())
        .unwrap();
}
