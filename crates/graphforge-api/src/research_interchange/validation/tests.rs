use super::*;
use crate::{BranchSource, CreateResearchBranchRequest};
use uuid::Uuid;

#[test]
fn archived_native_baseline_rejects_unknown_origin_without_mutating_source() {
    let mut graph = GraphForge::new(None).unwrap();
    graph.execute("CREATE (:Item {score:7})").unwrap();
    let version_uuid = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: graph.generation_for_read().unwrap().generation_uuid(),
                branch_uuid: Uuid::now_v7(),
                version_uuid,
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "validation".into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    low_level_import_requires_native_owner(&graph, version_uuid);
    let before = graph.generation_for_read().unwrap();
    let mut registry =
        graphforge_storage::research_versions::read_research_registry(&before).unwrap();
    let version = registry.versions[&version_uuid].clone();
    let directory = tempfile::tempdir().unwrap();
    let historical = graphforge_storage::research_versions::materialize_research_project(
        before.container_root(),
        &version,
        directory.path(),
    )
    .unwrap();
    validate(&historical, &version, &registry).unwrap();
    let origin = registry
        .branches
        .values()
        .next()
        .unwrap()
        .origin_version_uuid;
    let identity = registry.identities.remove(&origin).unwrap();
    assert!(validate(&historical, &version, &registry).is_err());
    registry.identities.insert(origin, identity);
    unsupported_historical_capability_is_rejected(&historical, &version, &registry);
    malformed_owner_is_rejected(&historical, &version, &registry);
    assert_eq!(
        graph.generation_for_read().unwrap().generation_uuid(),
        before.generation_uuid()
    );
    assert_eq!(
        graph
            .execute("MATCH(n:Item) RETURN n.score")
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
}

fn malformed_owner_is_rejected(
    historical: &ResolvedProjectGeneration,
    version: &ResearchVersionRecord,
    registry: &ResearchRegistry,
) {
    use graphforge_storage::{
        ProjectCapability, ProjectGenerationRequest, ProjectParticipant,
        ProjectParticipantEncoding, ProjectStageOutcome,
    };
    let mut participants: Vec<_> = historical
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .map(|p| ProjectParticipant {
            capability_id: p.capability_id,
            capability_version: p.capability_version,
            record_family_id: p.record_family_id,
            record_version: p.record_version,
            encoding: match p.encoding.as_str() {
                "json" => ProjectParticipantEncoding::Json,
                "parquet" => ProjectParticipantEncoding::Parquet,
                other => panic!("unexpected fixture encoding {other}"),
            },
            schema_fingerprint: p.schema_fingerprint,
            row_count: p.row_count,
            bytes: p.bytes,
        })
        .collect();
    let fields = participants
        .iter_mut()
        .find(|p| p.record_family_id == "branch_fields")
        .unwrap();
    let batches = crate::knowledge::ledger::read_parquet(&fields.bytes).unwrap();
    let mut bytes = Vec::new();
    {
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut bytes, batches[0].schema(), None).unwrap();
        for batch in batches {
            let mut columns = batch.columns().to_vec();
            // Hashes, schema and Parquet remain internally authenticated, but the
            // native baseline owner must reject a nil immutable origin identity.
            columns[3] = std::sync::Arc::new(arrow::array::StringArray::from(vec![
                Uuid::nil()
                    .to_string();
                batch.num_rows()
            ]));
            writer
                .write(&arrow::record_batch::RecordBatch::try_new(batch.schema(), columns).unwrap())
                .unwrap();
        }
        writer.close().unwrap();
    }
    fields.bytes = bytes;
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities: historical
            .capabilities()
            .into_iter()
            .map(|c| ProjectCapability {
                capability_id: c.capability_id,
                capability_version: c.capability_version,
            })
            .collect(),
        participants,
    };
    let stage = graphforge_storage::stage_project_generation_with_graph_tree_mode(
        historical.container_root(),
        &request,
        Some(&historical.graph_tree_root()),
        graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral,
    )
    .unwrap();
    let ProjectStageOutcome::Staged(stage) = stage else {
        panic!("fresh fixture transaction")
    };
    stage
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    let malformed =
        graphforge_storage::resolve_project_generation(historical.container_root()).unwrap();
    malformed.validate_complete_participant_inventory().unwrap();
    assert!(validate(&malformed, version, registry).is_err());
}

fn low_level_import_requires_native_owner(graph: &GraphForge, version_uuid: Uuid) {
    let owner = tempfile::tempdir().unwrap();
    let package = owner.path().join("package");
    graph
        .export_research(
            &crate::ExportResearchRequest {
                version_uuid,
                output: package.clone(),
                bundled: false,
                projection: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let capabilities = graph
        .generation_for_read()
        .unwrap()
        .capabilities()
        .into_iter()
        .map(|c| graphforge_storage::ProjectCapability {
            capability_id: c.capability_id,
            capability_version: c.capability_version,
        })
        .collect::<Vec<_>>();
    let target = owner.path().join("destination");
    let error = graphforge_storage::import_complete_portable_v2(
        &package,
        &target,
        Uuid::now_v7(),
        Uuid::now_v7(),
        &capabilities,
        graphforge_core::portable::PortableV2Limits::default(),
        None,
    )
    .unwrap_err();
    assert_eq!(
        error.code,
        graphforge_core::portable::PortableV2ErrorCode::Incompatible
    );
    assert!(error.committed_import.is_none());
    assert!(
        !target.exists(),
        "native owner refusal must precede destination admission"
    );
}

fn unsupported_historical_capability_is_rejected(
    historical: &ResolvedProjectGeneration,
    version: &ResearchVersionRecord,
    registry: &ResearchRegistry,
) {
    use graphforge_storage::{ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome};
    // An authenticated, structurally valid generation can still require a reader
    // that this binary does not implement. Historical proof roots need the same
    // admission as the package's selected working generation.
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities: vec![
            ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
            ProjectCapability {
                capability_id: "graph".into(),
                capability_version: 1,
            },
            ProjectCapability {
                capability_id: "future_research_owner".into(),
                capability_version: 999,
            },
        ],
        participants: Vec::new(),
    };
    let stage = graphforge_storage::stage_project_generation_with_graph_tree_mode(
        historical.container_root(),
        &request,
        None,
        graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral,
    )
    .unwrap();
    let ProjectStageOutcome::Staged(stage) = stage else {
        panic!("fresh transaction")
    };
    stage
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    let unsupported =
        graphforge_storage::resolve_project_generation(historical.container_root()).unwrap();
    let error = validate(&unsupported, version, registry).unwrap_err();
    assert!(error.to_string().contains("capability is unsupported"));
}
