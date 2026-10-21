use super::*;
use crate::{BranchSource, CancellationToken, CreateResearchBranchRequest};
use uuid::Uuid;

#[test]
fn archived_native_baseline_rejects_unknown_origin_without_mutating_source() {
    let mut graph = GraphForge::new(None).unwrap();
    graph.execute("CREATE (:Item {score:7})").unwrap();
    let version_uuid = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                author: None,
                committer: None,
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
    let graph_objects =
        graphforge_storage::begin_graph_object_publication(historical.container_root()).unwrap();
    let stage = graphforge_storage::stage_project_generation_with_graph_tree_mode(
        historical.container_root(),
        &request,
        None,
        graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral,
    )
    .unwrap();
    let ProjectStageOutcome::Staged(stage) = stage else {
        panic!("fresh fixture transaction")
    };
    stage
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish_with_graph_objects(&graph_objects)
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
    let error = graphforge_storage::import_complete_portable(
        &package,
        &target,
        Uuid::now_v7(),
        Uuid::now_v7(),
        &capabilities,
        graphforge_core::portable::PortableLimits::default(),
        None,
    )
    .unwrap_err();
    assert_eq!(
        error.code,
        graphforge_core::portable::PortableErrorCode::Incompatible
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

#[test]
fn accepted_proof_mappings_stream_compare_against_retained_fields() {
    let mut graph = GraphForge::new(None).unwrap();
    graph.execute("CREATE (:Item {score:7})").unwrap();
    let version_uuid = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                author: None,
                committer: None,
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
                label: "streaming mapping".into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let generation = graph.generation_for_read().unwrap();
    let registry =
        graphforge_storage::research_versions::read_research_registry(&generation).unwrap();
    let version = registry.versions[&version_uuid].clone();
    let directory = tempfile::tempdir().unwrap();
    let historical = graphforge_storage::research_versions::materialize_research_project(
        generation.container_root(),
        &version,
        directory.path(),
    )
    .unwrap();
    let fields = crate::branches::fields::read(&graph, &CancellationToken::new()).unwrap();
    let (key, digest) = fields
        .iter()
        .find(|(key, _)| key.0 == "node" && key.2 == "property:score")
        .expect("fixture node property field");
    let mut altered = *digest;
    altered[0] ^= 0xff;

    // A mapping committing the exact retained value verifies.
    let mut matching = registry.clone();
    insert_accepted_mapping(
        &mut matching,
        version_uuid,
        Uuid::now_v7(),
        key.clone(),
        *digest,
    );
    validate(&historical, &version, &matching).unwrap();

    // A changed digest refuses.
    let mut changed = registry.clone();
    insert_accepted_mapping(
        &mut changed,
        version_uuid,
        Uuid::now_v7(),
        key.clone(),
        altered,
    );
    assert!(validate(&historical, &version, &changed).is_err());

    // A mapping whose key is absent from the Version refuses.
    let mut absent = registry.clone();
    insert_accepted_mapping(
        &mut absent,
        version_uuid,
        Uuid::now_v7(),
        ("node".into(), Uuid::now_v7(), "property:absent".into()),
        *digest,
    );
    assert!(validate(&historical, &version, &absent).is_err());

    // A mapping exported as a different proof Version is ignored.
    let mut foreign = registry;
    insert_accepted_mapping(
        &mut foreign,
        Uuid::now_v7(),
        Uuid::now_v7(),
        key.clone(),
        altered,
    );
    validate(&historical, &version, &foreign).unwrap();
}

fn insert_accepted_mapping(
    registry: &mut graphforge_storage::research_versions::ResearchRegistry,
    exported_version_uuid: Uuid,
    proof_version_uuid: Uuid,
    key: (String, Uuid, String),
    digest: [u8; 32],
) {
    use graphforge_storage::research_versions::{
        ResearchAcceptedMapping, ResearchInterchangeManifest, ResearchProposalDestination,
        ResearchProposalUnit,
    };
    let archive = registry
        .interchange
        .entry(Uuid::now_v7())
        .or_insert_with(|| ResearchInterchangeManifest {
            contract_version: 1,
            research_capability_version: 1,
            producer: "validation fixture".into(),
            source_project_uuid: Uuid::now_v7(),
            fork_project_uuid: None,
            fork: None,
            selected_version_uuid: exported_version_uuid,
            selection:
                graphforge_storage::research_versions::ResearchInterchangeSelection::Complete {
                    source_version_uuid: exported_version_uuid,
                },
            versions: std::collections::BTreeMap::new(),
            history: std::collections::BTreeMap::new(),
            version_projects: std::collections::BTreeMap::new(),
            identities: std::collections::BTreeMap::new(),
            ancestry: std::collections::BTreeMap::new(),
            genealogy: std::collections::BTreeMap::new(),
            accepted: std::collections::BTreeMap::new(),
            proof_exports: std::collections::BTreeMap::new(),
        });
    archive
        .proof_exports
        .insert(proof_version_uuid, exported_version_uuid);
    let mapping = ResearchAcceptedMapping {
        mapping_uuid: Uuid::now_v7(),
        destination: ResearchProposalDestination::Project {
            project_uuid: Uuid::now_v7(),
        },
        unit: ResearchProposalUnit {
            object_kind: key.0,
            object_uuid: key.1,
            field: key.2,
        },
        contribution_uuid: Uuid::now_v7(),
        value_sha256: Some(digest),
        source_branch_uuid: Uuid::now_v7(),
        source_version_uuid: Uuid::now_v7(),
        destination_version_uuid: exported_version_uuid,
        proof_version_uuid,
        operation_uuid: Uuid::now_v7(),
        item_uuid: Uuid::now_v7(),
    };
    archive.accepted.insert(mapping.mapping_uuid, mapping);
}
