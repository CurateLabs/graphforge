//! Source and Artifact lifecycle: lineage, preference, and retention (#1349).

use arrow::array::FixedSizeBinaryArray;
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    ArtifactKind, ArtifactPayloadRequest, CapabilityId, DerivationInput, DerivationSubjectKind,
    EnableCapabilityRequest, GraphForge, LineageDirection, OperationId, RegisterArtifactRequest,
    RegisterSourceRequest, ReplacementImpactRequest, ResearchLineageRequest,
    RetentionDependencyClosureRequest, SetPreferredArtifactRequest, SourceKind, WriteContext,
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

#[test]
fn scan_to_ocr_lineage_preference_and_impact_survive_reopen() {
    let root = TempDir::new().unwrap();
    let path = root.path().to_str().unwrap();
    if open_admitted_graph(path).is_none() {
        return;
    }

    let source_uuid = Uuid::now_v7();
    let scan_uuid = Uuid::now_v7();
    let ocr_uuid = Uuid::now_v7();
    let preference_uuid = Uuid::now_v7();

    let graph = GraphForge::new(Some(path)).unwrap();
    enable_knowledge_capabilities(&graph);
    graph
        .register_source(RegisterSourceRequest {
            context: write_context(1),
            source_uuid,
            label: "Codex A".into(),
            source_kind: SourceKind::Manuscript,
            identity_uri: None,
        })
        .unwrap();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: write_context(2),
            artifact_uuid: scan_uuid,
            source_uuid,
            artifact_kind: ArtifactKind::RawScan,
            media_type: "image/tiff".into(),
            payload: ArtifactPayloadRequest::LocalBytes(b"scan bytes".to_vec()),
            derivation_inputs: Vec::new(),
            run_uuid: None,
        })
        .unwrap();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: write_context(3),
            artifact_uuid: ocr_uuid,
            source_uuid,
            artifact_kind: ArtifactKind::OcrText,
            media_type: "text/plain".into(),
            payload: ArtifactPayloadRequest::LocalBytes(b"ocr text".to_vec()),
            derivation_inputs: vec![DerivationInput {
                input_uuid: scan_uuid,
                input_kind: DerivationSubjectKind::Artifact,
            }],
            run_uuid: None,
        })
        .unwrap();
    graph
        .set_preferred_artifact(SetPreferredArtifactRequest {
            context: write_context(4),
            preference_event_uuid: preference_uuid,
            source_uuid,
            artifact_uuid: scan_uuid,
            reason: "initial preferred scan".into(),
        })
        .unwrap();
    let impact = only_batch(
        graph
            .replacement_impact(ReplacementImpactRequest {
                source_uuid,
                artifact_uuid: ocr_uuid,
            })
            .unwrap(),
    );
    assert_eq!(impact.num_rows(), 2);
    let impacted = uuid_column(&impact, "artifact_uuid");
    assert!(impacted.contains(&scan_uuid));
    assert!(impacted.contains(&ocr_uuid));
    graph
        .set_preferred_artifact(SetPreferredArtifactRequest {
            context: write_context(5),
            preference_event_uuid: Uuid::now_v7(),
            source_uuid,
            artifact_uuid: ocr_uuid,
            reason: "better OCR available".into(),
        })
        .unwrap();

    let reopened = GraphForge::new(Some(path)).unwrap();
    let backward = only_batch(
        reopened
            .research_lineage(ResearchLineageRequest {
                subject_uuid: ocr_uuid,
                subject_kind: DerivationSubjectKind::Artifact,
                direction: LineageDirection::Backward,
                max_depth: 4,
                page: Default::default(),
            })
            .unwrap(),
    );
    assert_eq!(backward.num_rows(), 1);
    assert_eq!(uuid_column(&backward, "input_uuid"), vec![scan_uuid]);

    let closure = only_batch(
        reopened
            .retention_dependency_closure(RetentionDependencyClosureRequest {
                scope_uuid: source_uuid,
                page: Default::default(),
            })
            .unwrap(),
    );
    assert_eq!(closure.num_rows(), 0);

    let post_reopen_impact = only_batch(
        reopened
            .replacement_impact(ReplacementImpactRequest {
                source_uuid,
                artifact_uuid: scan_uuid,
            })
            .unwrap(),
    );
    assert_eq!(post_reopen_impact.num_rows(), 1);
    assert_eq!(
        uuid_column(&post_reopen_impact, "artifact_uuid"),
        vec![ocr_uuid]
    );
}
