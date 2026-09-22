//! Native source/evidence/ontology closure survives selected interchange and cleanup.
use super::*;
use crate::*;
use arrow::array::BinaryArray;
use graphforge_ontology::{
    ActivationMode, AuthoredModule, CompositionLimits, EntityTypeDef, InventoryCompileRequest,
    OntologyModuleId, compile_inventory, module_document_digest,
};
use uuid::Uuid;

#[test]
fn selected_artifact_bytes_external_limits_and_ontology_survive_reopen() {
    let owner = tempfile::tempdir().unwrap();
    let mut graph = GraphForge::new(owner.path().join("source").to_str()).unwrap();
    graph
        .execute("CREATE (:Character), (:Character), (:Character)")
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
    let source = Uuid::now_v7();
    let unrelated_source = Uuid::now_v7();
    for id in [source, unrelated_source] {
        graph
            .register_source(RegisterSourceRequest {
                context: context(),
                source_uuid: id,
                label: if id == source {
                    "Selected"
                } else {
                    "Private ancestor"
                }
                .into(),
                source_kind: SourceKind::Other,
                identity_uri: None,
            })
            .unwrap();
    }
    let local = Uuid::now_v7();
    let external = Uuid::now_v7();
    let unrelated = Uuid::now_v7();
    let payload = b"exact selected OCR transcription".to_vec();
    for (source_uuid, artifact_uuid, content) in [
        (
            source,
            local,
            ArtifactPayloadRequest::LocalBytes(payload.clone()),
        ),
        (
            source,
            external,
            ArtifactPayloadRequest::ExternalReference {
                uri: "https://example.invalid/selected-historical".into(),
                fingerprint: None,
            },
        ),
        (
            unrelated_source,
            unrelated,
            ArtifactPayloadRequest::LocalBytes(b"PRIVATE_ANCESTOR_ARTIFACT".to_vec()),
        ),
    ] {
        graph
            .register_artifact(RegisterArtifactRequest {
                context: context(),
                source_uuid,
                artifact_uuid,
                artifact_kind: ArtifactKind::Other,
                media_type: "text/plain".into(),
                payload: content,
                derivation_inputs: vec![],
                run_uuid: None,
            })
            .unwrap();
    }
    let branch = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(&graph),
                branch_uuid: branch,
                version_uuid: Uuid::now_v7(),
                source: BranchSource::Current {
                    origin_version_uuid: Uuid::now_v7(),
                    context_uuid: Uuid::now_v7(),
                },
                creator_uuid: Uuid::now_v7(),
                created_at: 1,
                label: "Evidence study".into(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let version = install_ontology(&mut graph, branch);
    let selected = [source, local, external];
    let original = graph.open_research_version(version).unwrap();
    let original_external_error = original
        .artifact_payload(external)
        .unwrap_err()
        .code()
        .to_owned();
    let original_record = graph.research_version(version).unwrap();
    let original_graph =
        crate::research_versions::materialize_version(&graph, &original_record).unwrap();
    let fields = crate::branches::fields::read(&original_graph, &CancellationToken::new()).unwrap();
    let selected_fields = fields
        .keys()
        .filter(|key| selected.contains(&key.1) && matches!(key.0.as_str(), "source" | "artifact"))
        .map(|key| ResearchFieldIdentity {
            object_kind: key.0.clone(),
            object_uuid: key.1,
            field: key.2.clone(),
        })
        .collect();
    let projection = Uuid::now_v7();
    let package = owner.path().join("package");
    let ipc = freeze_evidence(&graph, version, source, local, external);
    graph
        .export_research(
            &ExportResearchRequest {
                version_uuid: version,
                output: package.clone(),
                bundled: false,
                projection: Some(ResearchExportProjection {
                    version_uuid: projection,
                    frozen_ipc: ipc,
                    fields: selected_fields,
                    created_at: 4,
                }),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_no_private_payload(&package);
    let target = owner.path().join("imported");
    GraphForge::import_portable_v2(
        &target,
        &PortableV2ImportRequest {
            input: package,
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    graphforge_storage::execute_project_cleanup(
        &target,
        graphforge_storage::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        Default::default(),
    )
    .unwrap();
    let imported = GraphForge::new(target.to_str()).unwrap();
    let record = imported.research_version(projection).unwrap();
    let expected: Vec<_> = original_record
        .content
        .evidence
        .into_iter()
        .filter(|entry| match entry {
            graphforge_storage::research_versions::ResearchEvidenceReference::Local {
                artifact_uuid,
                ..
            }
            | graphforge_storage::research_versions::ResearchEvidenceReference::ExternalOnly {
                artifact_uuid,
                ..
            }
            | graphforge_storage::research_versions::ResearchEvidenceReference::Unverifiable {
                artifact_uuid,
            } => [local, external].contains(artifact_uuid),
        })
        .collect();
    assert_eq!(record.content.evidence, expected);
    assert_eq!(record.content.evidence.len(), 2);
    let view = imported.open_research_version(projection).unwrap();
    let imported_graph = crate::research_versions::materialize_version(&imported, &record).unwrap();
    let bytes = view.artifact_payload(local).unwrap();
    assert_eq!(
        bytes.batches[0]
            .column_by_name("payload")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        payload
    );
    assert_eq!(
        view.artifact_payload(external).unwrap_err().code(),
        original_external_error
    );
    assert!(view.artifact_payload(unrelated).is_err());
    let actual = crate::branches::fields::read(&imported_graph, &CancellationToken::new()).unwrap();
    assert!(actual.keys().all(|key| key.0 != "node"
        && key.0 != "edge"
        && ![unrelated, unrelated_source].contains(&key.1)));
    for (key, value) in fields.iter().filter(|(key, _)| selected.contains(&key.1)) {
        assert_eq!(actual.get(key), Some(value));
    }
    assert_eq!(
        imported_graph
            .persisted_workspace_ontology_composition()
            .unwrap(),
        original_graph
            .persisted_workspace_ontology_composition()
            .unwrap()
    );
    assert!(imported.open_research_version(version).is_err());
    assert_eq!(
        imported
            .research_version_retention()
            .unwrap()
            .versions
            .len(),
        1
    );
}

fn context() -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    }
}
fn generation(graph: &GraphForge) -> Uuid {
    graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}
fn install_ontology(graph: &mut GraphForge, branch_uuid: Uuid) -> Uuid {
    let doc = OntologyDoc {
        ontology_id: "https://example.invalid/interchange-evidence".into(),
        version: "1.0.0".into(),
        entity_types: vec![EntityTypeDef {
            name: "Character".into(),
            r#abstract: false,
            parent: None,
        }],
        relation_types: vec![],
        properties: vec![],
        constraints: vec![],
        migrations: vec![],
    };
    let module = AuthoredModule {
        id: OntologyModuleId {
            ontology_id: doc.ontology_id.clone(),
            authored_version: doc.version.clone(),
            canonical_digest: module_document_digest(&doc).unwrap(),
        },
        dependencies: vec![],
        doc,
        allow_projected_identity: false,
    };
    let compiled = compile_inventory(InventoryCompileRequest {
        modules: &[module],
        bridges: &[],
        activation: &[],
        profile_default: ActivationMode::Strict,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .unwrap();
    let version_uuid = Uuid::now_v7();
    graph
        .change_research_branch_ontology(
            &ChangeResearchBranchOntologyRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: generation(graph),
                branch_uuid,
                version_uuid,
                expected_composition_fingerprint: None,
                candidate: graphforge_storage::WorkspaceOntologyComposition::from_compiled(
                    &compiled,
                    vec![],
                ),
                data_disposition: CompositionDataDisposition::RequireConforming,
                created_at: 2,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    version_uuid
}
fn freeze_evidence(
    graph: &GraphForge,
    version_uuid: Uuid,
    source: Uuid,
    local: Uuid,
    external: Uuid,
) -> Vec<u8> {
    let frozen = graph
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version { version_uuid },
                selector: SliceSelector::Direct {
                    members: SliceMembers {
                        sources: [source].into(),
                        artifacts: [local, external].into(),
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
    let mut bytes = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &frozen.schema).unwrap();
        for batch in frozen.batches {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
    }
    bytes
}

fn assert_no_private_payload(root: &std::path::Path) {
    let secret = b"PRIVATE_ANCESTOR_ARTIFACT";
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            pending.extend(
                std::fs::read_dir(&path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path()),
            );
        } else {
            let bytes = std::fs::read(&path).unwrap();
            assert!(
                !bytes.windows(secret.len()).any(|window| window == secret),
                "unselected local Artifact payload leaked into {}",
                path.display()
            );
        }
    }
}
