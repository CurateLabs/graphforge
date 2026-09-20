//! Durable Source and Artifact registration (#1349).

use arrow::array::{FixedSizeBinaryArray, StringArray};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    ArtifactKind, ArtifactPayloadRequest, CapabilityId, EnableCapabilityRequest, GraphForge,
    ListArtifactsRequest, ListSourcesRequest, OperationId, RegisterArtifactRequest,
    RegisterSourceRequest, SourceKind, WriteContext,
};
use graphforge_core::{GfError, ProjectErrorCode};
use tempfile::TempDir;
use uuid::Uuid;

fn open_admitted_graph(path: &str) -> Option<GraphForge> {
    match GraphForge::new(Some(path)) {
        Ok(graph) => Some(graph),
        Err(GfError::Project {
            code: ProjectErrorCode::UnsupportedFilesystem,
            ..
        }) => None,
        Err(error) => panic!("{error}"),
    }
}

fn write_context(seed: u128) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::from_u128(seed)),
        actor_uuid: None,
    }
}

fn enable_knowledge_capabilities(graph: &GraphForge) {
    for (seed, capability_id) in [
        (100_u128, CapabilityId::Provenance),
        (101_u128, CapabilityId::Knowledge),
    ] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: write_context(seed),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
}

fn only_batch(result: graphforge_api::ExecutionResult) -> RecordBatch {
    assert_eq!(result.batches.len(), 1, "expected one deterministic batch");
    result.batches.into_iter().next().unwrap()
}

fn uuid_column(batch: &RecordBatch, name: &str) -> Vec<Uuid> {
    let column = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    column
        .iter()
        .map(|value| Uuid::from_bytes(value.unwrap().try_into().unwrap()))
        .collect()
}

fn string_column(batch: &RecordBatch, name: &str) -> Vec<String> {
    let column = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    column
        .iter()
        .map(|value| value.unwrap().to_string())
        .collect()
}

#[test]
fn source_and_artifact_survive_reopen_and_list() {
    let root = TempDir::new().unwrap();
    let path = root.path().to_str().unwrap();
    if open_admitted_graph(path).is_none() {
        return;
    }

    let source_uuid = Uuid::now_v7();
    let artifact_uuid = Uuid::now_v7();
    let payload = b"manuscript page 1".to_vec();

    let graph = GraphForge::new(Some(path)).unwrap();
    enable_knowledge_capabilities(&graph);
    graph
        .register_source(RegisterSourceRequest {
            context: write_context(1),
            source_uuid,
            label: "Codex A".into(),
            source_kind: SourceKind::Manuscript,
            identity_uri: Some("https://example.org/codex-a".into()),
        })
        .unwrap();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: write_context(2),
            artifact_uuid,
            source_uuid,
            artifact_kind: ArtifactKind::RawScan,
            media_type: "image/tiff".into(),
            payload: ArtifactPayloadRequest::LocalBytes(payload.clone()),
            derivation_inputs: Vec::new(),
            run_uuid: None,
        })
        .unwrap();

    let reopened = GraphForge::new(Some(path)).unwrap();
    let source_batch = only_batch(reopened.source(source_uuid).unwrap());
    assert_eq!(uuid_column(&source_batch, "source_uuid"), vec![source_uuid]);
    assert_eq!(
        string_column(&source_batch, "label"),
        vec!["Codex A".to_string()]
    );

    let artifact_batch = only_batch(reopened.artifact(artifact_uuid).unwrap());
    assert_eq!(
        uuid_column(&artifact_batch, "artifact_uuid"),
        vec![artifact_uuid]
    );
    assert_eq!(
        uuid_column(&artifact_batch, "source_uuid"),
        vec![source_uuid]
    );
    assert_eq!(
        string_column(&artifact_batch, "media_type"),
        vec!["image/tiff".to_string()]
    );

    let listed_sources = only_batch(
        reopened
            .list_sources(ListSourcesRequest::default())
            .unwrap(),
    );
    assert_eq!(listed_sources.num_rows(), 1);
    assert_eq!(
        uuid_column(&listed_sources, "source_uuid"),
        vec![source_uuid]
    );

    let listed_artifacts = only_batch(
        reopened
            .list_artifacts(ListArtifactsRequest {
                source_uuid: Some(source_uuid),
                page: Default::default(),
            })
            .unwrap(),
    );
    assert_eq!(listed_artifacts.num_rows(), 1);
    assert_eq!(
        uuid_column(&listed_artifacts, "artifact_uuid"),
        vec![artifact_uuid]
    );
}
