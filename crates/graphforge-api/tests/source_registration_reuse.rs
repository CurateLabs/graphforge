//! Source registration reuses unchanged sibling ledgers (#1811).

use std::collections::BTreeMap;

use graphforge_api::{
    ArtifactKind, ArtifactPayloadRequest, CapabilityId, DerivationInput, DerivationSubjectKind,
    EnableCapabilityRequest, GraphForge, OperationId, PortableExportRequest, PortableImportRequest,
    PortableSelection, RegisterArtifactRequest, RegisterSourceRequest, SetPreferredArtifactRequest,
    SourceKind, WriteContext,
};
use graphforge_storage::concurrency_attribution::{RegionCapture, RegionSnapshot};
use tempfile::TempDir;
use uuid::Uuid;

fn context(seed: u128) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::from_u128(seed)),
        actor_uuid: None,
    }
}

fn enable_knowledge(graph: &GraphForge) {
    for (seed, capability_id) in [
        (100_u128, CapabilityId::Provenance),
        (101_u128, CapabilityId::Knowledge),
    ] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(seed),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
}

fn register_source(graph: &GraphForge, source_uuid: Uuid, seed: u128, label: &str) {
    graph
        .register_source(RegisterSourceRequest {
            context: context(seed),
            source_uuid,
            label: label.into(),
            source_kind: SourceKind::Manuscript,
            identity_uri: None,
        })
        .unwrap();
}

fn participant_contents(root: &std::path::Path) -> BTreeMap<(String, String), (u64, Vec<u8>)> {
    graphforge_storage::resolve_project_generation(root)
        .unwrap()
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .map(|snapshot| {
            (
                (snapshot.capability_id, snapshot.record_family_id),
                (snapshot.row_count, snapshot.bytes),
            )
        })
        .collect()
}

fn participant_row_count(
    contents: &BTreeMap<(String, String), (u64, Vec<u8>)>,
    capability: &str,
    family: &str,
) -> u64 {
    contents
        .get(&(capability.into(), family.into()))
        .map(|(rows, _)| *rows)
        .unwrap_or(0)
}

fn participant_bytes(
    contents: &BTreeMap<(String, String), (u64, Vec<u8>)>,
    capability: &str,
    family: &str,
) -> u64 {
    contents
        .get(&(capability.into(), family.into()))
        .map(|(_, bytes)| u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn work(snapshot: &RegionSnapshot, region: &str, unit: &str) -> u64 {
    snapshot
        .regions
        .get(region)
        .and_then(|row| row.work.get(unit))
        .copied()
        .unwrap_or(0)
}

fn assert_source_registration_work(
    snapshot: &RegionSnapshot,
    before: &BTreeMap<(String, String), (u64, Vec<u8>)>,
    after: &BTreeMap<(String, String), (u64, Vec<u8>)>,
) {
    assert!(snapshot.complete);
    let root = "knowledge_facade/source_publication";
    let materialization = format!("{root}/participant_materialization");
    let source_bytes = participant_bytes(before, "knowledge", "sources");
    let event_bytes = participant_bytes(before, "provenance", "events");
    let lineage_bytes = participant_bytes(before, "provenance", "lineage");
    let published_source_bytes = participant_bytes(after, "knowledge", "sources");
    assert_eq!(
        snapshot
            .regions
            .get(&materialization)
            .map_or(0, |region| region.calls),
        4,
        "only the changed source/provenance participants may be materialized: {snapshot:?}"
    );
    assert_eq!(
        work(snapshot, &materialization, "participant_materialized_bytes"),
        source_bytes + event_bytes + lineage_bytes + published_source_bytes,
        "source registration materialized bytes outside the changed source/provenance ledgers: {snapshot:?}"
    );

    let source_rows = participant_row_count(before, "knowledge", "sources");
    let source_bytes = participant_bytes(before, "knowledge", "sources");
    let event_rows = participant_row_count(before, "provenance", "events");
    let lineage_rows = participant_row_count(before, "provenance", "lineage");
    let event_bytes = participant_bytes(before, "provenance", "events");
    let lineage_bytes = participant_bytes(before, "provenance", "lineage");
    let written_rows = [
        ("knowledge", "sources"),
        ("provenance", "events"),
        ("provenance", "lineage"),
    ]
    .iter()
    .map(|(capability, family)| participant_row_count(after, capability, family))
    .sum::<u64>();
    let written_bytes = [
        ("knowledge", "sources"),
        ("provenance", "events"),
        ("provenance", "lineage"),
    ]
    .iter()
    .map(|(capability, family)| participant_bytes(after, capability, family))
    .sum::<u64>();
    assert_eq!(
        work(snapshot, root, "rows"),
        source_rows
            + event_rows
            + lineage_rows
            + participant_row_count(after, "knowledge", "sources")
            + written_rows,
        "source publication decoded or encoded sibling ledger rows: {snapshot:?}"
    );
    assert_eq!(
        work(snapshot, root, "bytes"),
        source_bytes
            + event_bytes
            + lineage_bytes
            + participant_bytes(after, "knowledge", "sources")
            + written_bytes,
        "source publication decoded or encoded sibling ledger bytes: {snapshot:?}"
    );
}

fn populate_siblings(graph: &GraphForge, source_uuid: Uuid, count: usize) -> Vec<Uuid> {
    let mut artifacts = Vec::with_capacity(count);
    for index in 0..count {
        let artifact_uuid = Uuid::now_v7();
        graph
            .register_artifact(RegisterArtifactRequest {
                context: context(1_000 + u128::try_from(index).unwrap()),
                artifact_uuid,
                source_uuid,
                artifact_kind: ArtifactKind::OcrText,
                media_type: "text/plain".into(),
                payload: ArtifactPayloadRequest::Absent,
                derivation_inputs: artifacts
                    .last()
                    .copied()
                    .map(|input_uuid| {
                        vec![DerivationInput {
                            input_uuid,
                            input_kind: DerivationSubjectKind::Artifact,
                        }]
                    })
                    .unwrap_or_default(),
                run_uuid: None,
            })
            .unwrap();
        artifacts.push(artifact_uuid);
    }
    graph
        .set_preferred_artifact(SetPreferredArtifactRequest {
            context: context(2_000),
            preference_event_uuid: Uuid::now_v7(),
            source_uuid,
            artifact_uuid: *artifacts.last().unwrap(),
            reason: "fixture preference".into(),
        })
        .unwrap();
    artifacts
}

fn source_registration_cost(count: usize) {
    let directory = TempDir::new().unwrap();
    let root = directory.path();
    let path = root.to_str().unwrap();
    let graph = GraphForge::new(Some(path))
        .expect("test_environment must provide a durable ext4/xfs/btrfs project root");
    enable_knowledge(&graph);
    let original_source = Uuid::now_v7();
    register_source(&graph, original_source, 1, "original");
    let artifacts = populate_siblings(&graph, original_source, count);
    assert_eq!(artifacts.len(), count);

    let before = participant_contents(root);
    let source_uuid = Uuid::now_v7();
    let request = RegisterSourceRequest {
        context: context(3_000),
        source_uuid,
        label: "new source".into(),
        source_kind: SourceKind::Manuscript,
        identity_uri: Some("https://example.org/new-source".into()),
    };
    let capture = RegionCapture::start("knowledge_facade");
    graph.register_source(request.clone()).unwrap();
    let diagnostic = capture.finish();
    let after = participant_contents(root);
    assert_source_registration_work(&diagnostic, &before, &after);

    for family in [
        ("knowledge", "artifacts"),
        ("knowledge", "artifact_derivations"),
        ("knowledge", "artifact_preference_events"),
        ("knowledge", "retention_dependencies"),
    ] {
        assert_eq!(
            before.get(&(family.0.into(), family.1.into())),
            after.get(&(family.0.into(), family.1.into())),
            "{family:?}"
        );
    }

    // The existing exact retry and conflicting UUID behavior remains intact.
    let generation_uuid = graphforge_storage::resolve_project_generation(root)
        .unwrap()
        .generation_uuid();
    graph.register_source(request).unwrap();
    assert_eq!(
        graphforge_storage::resolve_project_generation(root)
            .unwrap()
            .generation_uuid(),
        generation_uuid,
        "exact retry must not publish another generation"
    );
    let conflict = graph
        .register_source(RegisterSourceRequest {
            context: context(3_001),
            source_uuid,
            label: "conflicting source".into(),
            source_kind: SourceKind::Manuscript,
            identity_uri: None,
        })
        .unwrap_err();
    assert_eq!(conflict.code(), "GF_IDEMPOTENCY_CONFLICT");
    assert_eq!(
        graphforge_storage::resolve_project_generation(root)
            .unwrap()
            .generation_uuid(),
        generation_uuid,
    );

    drop(graph);
    let reopened = GraphForge::new(Some(path)).unwrap();
    assert_eq!(participant_contents(root), after);
    let package = root.join("roundtrip.gfp");
    reopened
        .export_portable(PortableExportRequest {
            selection: PortableSelection::Current,
            output: package.clone(),
        })
        .unwrap();
    let import_root = TempDir::new().unwrap();
    GraphForge::import_portable(
        import_root.path(),
        &PortableImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
        },
    )
    .unwrap();
    assert_eq!(participant_contents(import_root.path()), after);
}

#[test]
fn fixed_source_registration_does_not_decode_or_encode_sibling_ledgers() {
    // Provenance is changed and remains proportional to its own existing ledger.
    // Artifact, derivation, preference and retention rows are carried forward as
    // verified participant bytes, independent of their sizes.
    source_registration_cost(64);
    source_registration_cost(256);
}
