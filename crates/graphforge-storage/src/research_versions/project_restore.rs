//! Complete Project research replacement within the research publication.
use super::{
    BTreeMap, GfError, Path, ProjectCapability, ProjectGenerationRequest, ProjectParticipant,
    ProjectParticipantEncoding, ProjectStageOutcome, ResearchEvidenceReference, ResearchMutation,
    ResearchOperation, ResearchParticipantKey, ResearchRegistry, ResearchVersionRecord,
    ResolvedProjectGeneration, Uuid, hex, history, inspect_research_version, invalid,
    materialize_research_graph,
};

pub(super) fn prepare(
    root: &Path,
    operation: &ResearchOperation,
    registry: &ResearchRegistry,
    publication: &mut ProjectGenerationRequest,
) -> Result<Option<tempfile::TempDir>, GfError> {
    let ResearchMutation::RestoreProject { source_version, .. } = &operation.mutation else {
        return Ok(None);
    };
    let version = registry
        .versions
        .get(source_version)
        .ok_or_else(|| invalid("Project restore source is unavailable"))?;
    if version.content.source_version.is_some() {
        return Err(invalid("Project restore requires a complete Version"));
    }
    let snapshots = inspect_research_version(root, version)?;
    publication.participants.retain(|p| {
        history(&ResearchParticipantKey {
            capability: p.capability_id.clone(),
            family: p.record_family_id.clone(),
        })
    });
    let has_files = snapshots
        .iter()
        .any(|p| p.capability_id == "graph" && p.record_family_id == "files");
    for p in snapshots {
        publication.participants.push(ProjectParticipant {
            capability_id: p.capability_id,
            capability_version: p.capability_version,
            record_family_id: p.record_family_id,
            record_version: p.record_version,
            encoding: match p.encoding.as_str() {
                "json" => ProjectParticipantEncoding::Json,
                "arrow" => ProjectParticipantEncoding::Arrow,
                "parquet" => ProjectParticipantEncoding::Parquet,
                _ => return Err(invalid("unsupported retained participant encoding")),
            },
            schema_fingerprint: p.schema_fingerprint,
            row_count: p.row_count,
            bytes: p.bytes,
        });
    }
    let mut capabilities = BTreeMap::new();
    // Empty mandatory capabilities are valid in an initial empty Project.
    capabilities.insert("workspace".to_owned(), 1);
    capabilities.insert("graph".to_owned(), 1);
    for p in &publication.participants {
        if capabilities
            .insert(p.capability_id.clone(), p.capability_version)
            .is_some_and(|old| old != p.capability_version)
        {
            return Err(invalid("restored capability versions conflict"));
        }
    }
    publication.capabilities = capabilities
        .into_iter()
        .map(|(capability_id, capability_version)| ProjectCapability {
            capability_id,
            capability_version,
        })
        .collect();
    if !has_files {
        return Ok(None);
    }
    let graph =
        tempfile::tempdir().map_err(|_| invalid("cannot allocate historical graph workspace"))?;
    materialize_research_graph(root, version, graph.path())?;
    Ok(Some(graph))
}

/// Materialize exact retained research in an empty process-owned temporary Project.
/// Source authority is read-only and the returned container has no research history.
pub fn materialize_research_project(
    root: &Path,
    version: &ResearchVersionRecord,
    target: &Path,
) -> Result<ResolvedProjectGeneration, GfError> {
    let source_pin = crate::resolve_project_generation(root)?;
    validate_private_target(root, target)?;
    let snapshots = inspect_research_version(root, version)?;
    crate::open_or_initialize_ephemeral_project(target)?;
    let lease = crate::begin_graph_object_publication(target)?;
    let graph =
        tempfile::tempdir().map_err(|_| invalid("cannot allocate historical graph workspace"))?;
    let mut has_graph = false;
    for p in &snapshots {
        if p.capability_id == "graph" && p.record_family_id == "files" {
            has_graph = true;
            materialize_research_graph(root, version, graph.path())?;
            if matches!(
                crate::graph_files::decode_versioned_graph_files_participant(
                    p.record_version,
                    &p.bytes
                )?,
                crate::GraphFilesParticipant::V2(_)
            ) {
                let (files, nodes) =
                    super::retained_content::graph_closure(root, p.record_version, &p.bytes, None)?;
                for node in nodes {
                    let bytes = crate::read_graph_object_by_digest(
                        root,
                        &node,
                        crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
                    )?;
                    crate::graph_object_store::install_graph_object_bytes(target, &bytes)?;
                }
                for file in files {
                    crate::graph_object_store::install_graph_object_file(
                        target,
                        &crate::graph_object_path(root, &file.content_sha256)?,
                        &file.content_sha256,
                        file.byte_length,
                    )?;
                }
            }
        }
    }
    materialize_evidence(root, version, target)?;
    let mut capabilities = BTreeMap::from([("workspace".to_owned(), 1), ("graph".to_owned(), 1)]);
    let mut participants = Vec::new();
    for p in snapshots {
        capabilities.insert(p.capability_id.clone(), p.capability_version);
        participants.push(ProjectParticipant {
            capability_id: p.capability_id,
            capability_version: p.capability_version,
            record_family_id: p.record_family_id,
            record_version: p.record_version,
            encoding: match p.encoding.as_str() {
                "json" => ProjectParticipantEncoding::Json,
                "arrow" => ProjectParticipantEncoding::Arrow,
                "parquet" => ProjectParticipantEncoding::Parquet,
                _ => return Err(invalid("unsupported historical encoding")),
            },
            schema_fingerprint: p.schema_fingerprint,
            row_count: p.row_count,
            bytes: p.bytes,
        });
    }
    let publication = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities: capabilities
            .into_iter()
            .map(|(capability_id, capability_version)| ProjectCapability {
                capability_id,
                capability_version,
            })
            .collect(),
        participants,
    };
    let staged = crate::stage_project_generation_with_graph_tree_mode(
        target,
        &publication,
        has_graph.then_some(graph.path()),
        crate::filesystem_admission::ProjectLifecycleMode::Ephemeral,
    )?;
    if let ProjectStageOutcome::Staged(staged) = staged {
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))?
            .publish_with_graph_objects(&lease)?;
    }
    drop(source_pin);
    crate::resolve_project_generation(target)
}

fn validate_private_target(root: &Path, target: &Path) -> Result<(), GfError> {
    if target
        .canonicalize()
        .map_err(|_| invalid("private Project target is unavailable"))?
        .starts_with(
            root.canonicalize()
                .map_err(|_| invalid("source Project is unavailable"))?,
        )
        || std::fs::read_dir(target)
            .map_err(|_| invalid("private Project target is unavailable"))?
            .next()
            .is_some()
    {
        return Err(invalid(
            "historical Project requires an empty private target outside the source",
        ));
    }
    Ok(())
}

fn materialize_evidence(
    root: &Path,
    version: &ResearchVersionRecord,
    target: &Path,
) -> Result<(), GfError> {
    for evidence in &version.content.evidence {
        if let ResearchEvidenceReference::Local {
            sha256,
            byte_length,
            ..
        } = evidence
        {
            let digest = hex(sha256);
            crate::graph_object_store::install_graph_object_file(
                target,
                &crate::graph_object_path(root, &digest)?,
                &digest,
                *byte_length,
            )?;
        }
    }
    Ok(())
}
