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
                author: None,
                committer: None,
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
    GraphForge::import_portable(
        &target,
        &PortableImportRequest {
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

/// A bundled selected export whose Version holds one local evidence object.
fn evidence_bundle(owner: &std::path::Path, payload: &[u8]) -> (std::path::PathBuf, Vec<u8>) {
    let mut graph = GraphForge::new(owner.join("source").to_str()).unwrap();
    graph.execute("CREATE (:Character), (:Character)").unwrap();
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
    graph
        .register_source(RegisterSourceRequest {
            context: context(),
            source_uuid: source,
            label: "Selected".into(),
            source_kind: SourceKind::Other,
            identity_uri: None,
        })
        .unwrap();
    let artifact = Uuid::now_v7();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            source_uuid: source,
            artifact_uuid: artifact,
            artifact_kind: ArtifactKind::Other,
            media_type: "application/octet-stream".into(),
            payload: ArtifactPayloadRequest::LocalBytes(payload.to_vec()),
            derivation_inputs: vec![],
            run_uuid: None,
        })
        .unwrap();
    let branch = Uuid::now_v7();
    graph
        .create_research_branch(
            &CreateResearchBranchRequest {
                author: None,
                committer: None,
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
    let record = graph.research_version(version).unwrap();
    let materialized = crate::research_versions::materialize_version(&graph, &record).unwrap();
    let fields = crate::branches::fields::read(&materialized, &CancellationToken::new())
        .unwrap()
        .keys()
        .filter(|key| {
            [source, artifact].contains(&key.1) && matches!(key.0.as_str(), "source" | "artifact")
        })
        .map(|key| ResearchFieldIdentity {
            object_kind: key.0.clone(),
            object_uuid: key.1,
            field: key.2.clone(),
        })
        .collect();
    let package = owner.join("evidence.gfpb");
    graph
        .export_research(
            &ExportResearchRequest {
                version_uuid: version,
                output: package.clone(),
                bundled: true,
                projection: Some(ResearchExportProjection {
                    version_uuid: Uuid::now_v7(),
                    frozen_ipc: freeze_evidence(&graph, version, source, artifact, artifact),
                    fields,
                    created_at: 4,
                }),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let bytes = std::fs::read(&package).unwrap();
    (package, bytes)
}

/// Where the evidence object's payload starts in the bundle: its header names
/// the digest, and the payload follows in the next block.
fn evidence_header(bytes: &[u8], digest: &str) -> usize {
    let block = bytes
        .chunks(512)
        .position(|block| block.starts_with(digest.as_bytes()))
        .expect("the evidence object is in the bundle");
    block * 512
}

/// Evidence is checked as it streams and never staged, so it needs no disk and
/// does not count against the Version limit, whatever its size; a changed or
/// shortened evidence object is still refused, alike from a file and a stream.
#[test]
fn local_evidence_is_authenticated_in_the_stream_and_never_staged() {
    use graphforge_core::portable::{PortableErrorCode, PortableLimits, PortableMode};
    let owner = tempfile::tempdir().unwrap();
    let payload = vec![0x5a_u8; 9 * 1024 * 1024];
    let (package, bytes) = evidence_bundle(owner.path(), &payload);
    let by_file = |path: &std::path::Path, limits: PortableLimits| {
        crate::verify_portable(
            &crate::PortableVerifyRequest {
                input: path.to_path_buf(),
                mode: PortableMode::Full,
                limits,
            },
            None,
        )
    };
    let scratch = tempfile::tempdir().unwrap();
    let by_stream = |bytes: &[u8], limits: PortableLimits| {
        crate::verify_portable_stream(
            &mut std::io::Cursor::new(bytes),
            Some(scratch.path()),
            limits,
            None,
        )
    };
    let default = PortableLimits::default();
    let report = by_file(&package, default).unwrap();
    let evidence = report
        .research_entries
        .iter()
        .find(|entry| entry.length == payload.len() as u64)
        .expect("the evidence object is retained");
    let digest = evidence.sha256.clone();
    // A limit far below the evidence admits the package: nothing stages it.
    let limits = PortableLimits {
        max_research_version_bytes: 8 * 1024 * 1024,
        ..default
    };
    by_file(&package, limits).unwrap();
    by_stream(&bytes, limits).unwrap();
    // While a Version is validated, the scratch directory holds less than the
    // evidence object alone.
    let mut largest = 0;
    let mut check =
        |generation: &graphforge_storage::ResolvedProjectGeneration,
         version: &graphforge_storage::research_versions::ResearchVersionRecord,
         registry: &graphforge_storage::research_versions::ResearchRegistry| {
            largest = largest.max(directory_bytes(scratch.path()));
            crate::research_interchange::validation::validate(generation, version, registry)
        };
    graphforge_storage::validate_research_package_stream(
        &mut std::io::Cursor::new(&bytes),
        Some(scratch.path()),
        limits,
        None,
        &mut check,
    )
    .unwrap();
    assert!(
        0 < largest && largest < payload.len() as u64,
        "{largest} bytes were on disk during validation"
    );
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    // One changed byte, and one byte fewer, fail the digest alike.
    let header = evidence_header(&bytes, &digest);
    let mut changed = bytes.clone();
    changed[header + 512] ^= 1;
    let mut shortened = bytes[..header].to_vec();
    let mut block = bytes[header..header + 512].to_vec();
    block[124..136].copy_from_slice(format!("{:011o}\0", payload.len() - 1).as_bytes());
    block[148..156].fill(b' ');
    let sum: u64 = block.iter().map(|byte| u64::from(*byte)).sum();
    block[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    shortened.extend(block);
    shortened.extend(&bytes[header + 512..header + 512 + payload.len() - 1]);
    shortened.resize(shortened.len().div_ceil(512) * 512, 0);
    shortened.extend(&bytes[header + 512 + payload.len().div_ceil(512) * 512..]);
    for (name, tampered) in [("changed", changed), ("shortened", shortened)] {
        let path = owner.path().join(name);
        std::fs::write(&path, &tampered).unwrap();
        let file = by_file(&path, default).unwrap_err();
        let stream = by_stream(&tampered, default).unwrap_err();
        assert_eq!(file.code, PortableErrorCode::DigestMismatch, "{name}");
        assert_eq!(
            (stream.code, &stream.entry),
            (file.code, &file.entry),
            "{name}"
        );
    }
}

fn directory_bytes(root: &std::path::Path) -> u64 {
    let mut total = 0;
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(&path).into_iter().flatten().flatten() {
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                total += metadata.len();
            }
        }
    }
    total
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
                author: None,
                committer: None,
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
