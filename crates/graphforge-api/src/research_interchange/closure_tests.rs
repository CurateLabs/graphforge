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
fn evidence_bundle(
    owner: &std::path::Path,
    payload: impl FnOnce(&std::path::Path) -> Vec<u8>,
) -> (std::path::PathBuf, Vec<u8>) {
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
    let payload = payload(&owner.join("source"));
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            source_uuid: source,
            artifact_uuid: artifact,
            artifact_kind: ArtifactKind::Other,
            media_type: "application/octet-stream".into(),
            payload: ArtifactPayloadRequest::LocalBytes(payload),
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
    let (package, bytes) = evidence_bundle(owner.path(), |_| payload.clone());
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

/// Evidence bytes reach no decoder, so an evidence-only Parquet file with an
/// unfunded encoding verifies alike from a file and a stream.
#[test]
fn evidence_only_parquet_with_an_unfunded_encoding_is_never_decoded() {
    use graphforge_core::portable::{PortableLimits, PortableMode};
    let owner = tempfile::tempdir().unwrap();
    let blob = super::stream_parity::unfunded_parquet();
    let (package, bytes) = evidence_bundle(owner.path(), |_| blob.clone());
    let limits = PortableLimits::default();
    let file = crate::verify_portable(
        &crate::PortableVerifyRequest {
            input: package,
            mode: PortableMode::Full,
            limits,
        },
        None,
    )
    .unwrap();
    assert!(
        file.research_entries
            .iter()
            .any(|entry| entry.path.contains("/research-retained-evidence/")
                && entry.length == blob.len() as u64)
    );
    let scratch = tempfile::tempdir().unwrap();
    let stream = crate::verify_portable_stream(
        &mut std::io::Cursor::new(&bytes),
        Some(scratch.path()),
        limits,
        None,
    )
    .unwrap();
    assert_eq!(stream.package_digest, file.package_digest);
}

/// Bytes allocated under `scratch` right now: named files, and the unnamed
/// files this process holds open there (which no directory listing shows).
#[cfg(target_os = "linux")]
fn allocated_under(scratch: &std::path::Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    let mut total = 0;
    let mut pending = vec![scratch.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)
            .into_iter()
            .flatten()
            .flatten()
        {
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                total += metadata.blocks() * 512;
            }
        }
    }
    let prefix = scratch.to_string_lossy().into_owned();
    for descriptor in std::fs::read_dir("/proc/self/fd").unwrap().flatten() {
        let Ok(target) = std::fs::read_link(descriptor.path()) else {
            continue;
        };
        let target = target.to_string_lossy();
        if target.starts_with(&prefix) && target.ends_with("(deleted)") {
            if let Ok(metadata) = std::fs::metadata(descriptor.path()) {
                total += metadata.blocks() * 512;
            }
        }
    }
    total
}

/// Reads in small steps with a pause, so a sampling thread sees the transient
/// scratch an entry causes while it streams past.
struct Paced<R>(R);

impl<R: std::io::Read> std::io::Read for Paced<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        std::thread::sleep(std::time::Duration::from_micros(500));
        let step = buffer.len().min(64 * 1024);
        self.0.read(&mut buffer[..step])
    }
}

/// The page walk keeps a copy of a streamed entry that exceeds its in-memory
/// threshold, in an unnamed file. Evidence bytes reach no decoder, so no copy of
/// them is kept anywhere: allocated scratch stays below the evidence size.
#[cfg(target_os = "linux")]
#[test]
fn a_streamed_evidence_blob_is_not_copied_into_scratch() {
    use graphforge_core::portable::PortableLimits;
    let owner = tempfile::tempdir().unwrap();
    let payload = vec![0x5a_u8; 9 * 1024 * 1024];
    let (_, bytes) = evidence_bundle(owner.path(), |_| payload.clone());
    let scratch = tempfile::tempdir().unwrap();
    // The meter sees unnamed files: one that is written and never named.
    {
        let mut probe = tempfile::tempfile_in(scratch.path()).unwrap();
        std::io::Write::write_all(&mut probe, &vec![1_u8; 1024 * 1024]).unwrap();
        assert!(allocated_under(scratch.path()) >= 1024 * 1024);
    }
    assert_eq!(allocated_under(scratch.path()), 0);
    let limits = PortableLimits {
        max_parquet_walk_bytes: 1024 * 1024,
        ..PortableLimits::default()
    };
    let done = std::sync::atomic::AtomicBool::new(false);
    let peak = std::thread::scope(|scope| {
        let sampler = scope.spawn(|| {
            let mut peak = 0;
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                peak = peak.max(allocated_under(scratch.path()));
            }
            peak
        });
        crate::verify_portable_stream(
            &mut Paced(std::io::Cursor::new(&bytes)),
            Some(scratch.path()),
            limits,
            None,
        )
        .unwrap();
        done.store(true, std::sync::atomic::Ordering::Release);
        sampler.join().unwrap()
    });
    assert!(
        peak < payload.len() as u64,
        "{peak} bytes were allocated in scratch for a {} byte evidence blob",
        payload.len()
    );
}

/// The smallest Parquet file under a project: one of its graph files.
fn graph_file_bytes(root: &std::path::Path) -> Vec<u8> {
    let mut pending = vec![root.to_path_buf()];
    let mut smallest: Option<Vec<u8>> = None;
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if let Ok(bytes) = std::fs::read(&path) {
                if bytes.starts_with(b"PAR1")
                    && smallest
                        .as_ref()
                        .is_none_or(|best| bytes.len() < best.len())
                {
                    smallest = Some(bytes);
                }
            }
        }
    }
    smallest.expect("the project holds a graph file")
}

/// A valid Version can carry one object that is both a local evidence blob and
/// one of its graph files: identical bytes have one path. It is staged like any
/// closure member, so it verifies alike from a file and a stream.
#[test]
fn evidence_identical_to_a_graph_file_is_staged_and_verifies() {
    use graphforge_core::portable::{PortableLimits, PortableMode};
    let owner = tempfile::tempdir().unwrap();
    let (package, bytes) = evidence_bundle(owner.path(), graph_file_bytes);
    let limits = PortableLimits::default();
    let file = crate::verify_portable(
        &crate::PortableVerifyRequest {
            input: package,
            mode: PortableMode::Full,
            limits,
        },
        None,
    )
    .unwrap();
    assert!(file.research_interchange);
    assert!(
        file.research_entries
            .iter()
            .all(|entry| !entry.path.contains("/research-retained-evidence/")),
        "the evidence is also a graph file, so nothing is evidence-only"
    );
    let scratch = tempfile::tempdir().unwrap();
    let stream = crate::verify_portable_stream(
        &mut std::io::Cursor::new(&bytes),
        Some(scratch.path()),
        limits,
        None,
    )
    .unwrap();
    let strip = |report: &graphforge_core::portable::PortableReport| {
        let mut value = serde_json::to_value(report).unwrap();
        value.as_object_mut().unwrap().remove("source").unwrap();
        value
    };
    assert_eq!(strip(&file), strip(&stream));
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
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
