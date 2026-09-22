//! Preserve exact immutable participant bytes independently of portable working layout.
use super::super::{
    BTreeMap, BTreeSet, GfError, Path, ProjectCapability, ProjectGenerationRequest,
    ProjectStageOutcome, RESEARCH_CAPABILITY, RESEARCH_VERSION, ResearchEvidenceReference,
    ResearchRegistry, ResearchRetentionRoot, ResearchRootKind, ResolvedProjectGeneration, Uuid,
    hex, invalid, retained_content,
};
use super::ResearchInterchangeManifest;

/// Materialize a selected research package in an empty private Project.
/// The source CURRENT and genealogy are never changed. Foreign branches stay historical.
pub fn materialize_research_interchange(
    source: &Path,
    manifest: &ResearchInterchangeManifest,
    target: &Path,
) -> Result<ResolvedProjectGeneration, GfError> {
    manifest.validate()?;
    let source_generation = crate::resolve_project_generation(source)?;
    let registry = super::super::read_research_registry(&source_generation)?;
    for (id, identity) in &manifest.identities {
        if registry
            .identities
            .get(id)
            .is_some_and(|known| known != identity)
        {
            return Err(invalid(
                "export conflicts with known immutable research identity",
            ));
        }
    }
    let selected = &manifest.versions[&manifest.selected_version_uuid];
    // Prepared projections are authenticated from CAS; registered content also
    // authenticates its permanent commitment and original generation when needed.
    let prepared = !registry.versions.contains_key(&selected.version_uuid);
    let current = super::super::project_restore::materialize(source, selected, target, prepared)?;
    let lease = crate::begin_graph_object_publication(target)?;
    for version in manifest.versions.values() {
        copy_version(source, target, version, &registry)?;
    }
    let imported = manifest.registry()?;
    let mut participants = current
        .participant_snapshots()?
        .into_iter()
        .map(|snapshot| {
            Ok(crate::ProjectParticipant {
                capability_id: snapshot.capability_id,
                capability_version: snapshot.capability_version,
                record_family_id: snapshot.record_family_id,
                record_version: snapshot.record_version,
                encoding: match snapshot.encoding.as_str() {
                    "json" => crate::ProjectParticipantEncoding::Json,
                    "arrow" => crate::ProjectParticipantEncoding::Arrow,
                    "parquet" => crate::ProjectParticipantEncoding::Parquet,
                    _ => return Err(invalid("unsupported research participant encoding")),
                },
                schema_fingerprint: snapshot.schema_fingerprint,
                row_count: snapshot.row_count,
                bytes: snapshot.bytes,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    participants.push(imported.participant()?);
    let mut capabilities: Vec<_> = current
        .capabilities()
        .into_iter()
        .map(|capability| ProjectCapability {
            capability_id: capability.capability_id,
            capability_version: capability.capability_version,
        })
        .collect();
    capabilities.push(ProjectCapability {
        capability_id: RESEARCH_CAPABILITY.into(),
        capability_version: RESEARCH_VERSION,
    });
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities,
        participants,
    };
    if let ProjectStageOutcome::Staged(staged) =
        crate::stage_project_generation_with_graph_tree_mode(
            target,
            &request,
            None,
            crate::filesystem_admission::ProjectLifecycleMode::Ephemeral,
        )?
    {
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))?
            .publish_with_graph_objects(&lease)?;
    }
    crate::resolve_project_generation(target)
}

impl ResearchInterchangeManifest {
    /// Construct the destination's headless registry from admitted historical content.
    pub fn registry(&self) -> Result<ResearchRegistry, GfError> {
        self.validate()?;
        let mut roots = BTreeMap::new();
        roots.insert(
            self.selected_version_uuid,
            ResearchRetentionRoot {
                root_uuid: self.selected_version_uuid,
                kind: ResearchRootKind::RetainedVersion,
                versions: BTreeSet::from([self.selected_version_uuid]),
            },
        );
        for (original, exported) in &self.proof_exports {
            if roots.contains_key(original) {
                return Err(invalid("acceptance root collides with selection root"));
            }
            roots.insert(
                *original,
                ResearchRetentionRoot {
                    root_uuid: *original,
                    kind: ResearchRootKind::AcceptedProvenance,
                    versions: BTreeSet::from([*exported]),
                },
            );
        }
        let registry = ResearchRegistry {
            versions: self.versions.clone(),
            identities: self.identities.clone(),
            materialized: self.versions.keys().copied().collect(),
            roots,
            interchange: BTreeMap::from([(self.selected_version_uuid, self.clone())]),
            ..ResearchRegistry::default()
        };
        registry.validate()?;
        Ok(registry)
    }
}

fn copy_version(
    source: &Path,
    target: &Path,
    version: &super::super::ResearchVersionRecord,
    registry: &ResearchRegistry,
) -> Result<(), GfError> {
    let snapshots = if registry.versions.contains_key(&version.version_uuid) {
        super::super::inspect_with_registry(source, version, registry)?
    } else {
        retained_content::inspect(source, version, None)?
    };
    for snapshot in snapshots {
        crate::graph_object_store::install_graph_object_bytes(target, &snapshot.bytes)?;
        if snapshot.capability_id == "graph" && snapshot.record_family_id == "files" {
            let (files, nodes) = retained_content::graph_closure(
                source,
                snapshot.record_version,
                &snapshot.bytes,
                None,
            )?;
            for node in nodes {
                let bytes = crate::read_graph_object_by_digest(
                    source,
                    &node,
                    crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
                )?;
                crate::graph_object_store::install_graph_object_bytes(target, &bytes)?;
            }
            let original = if !registry.materialized.contains(&version.version_uuid)
                && registry.versions.contains_key(&version.version_uuid)
                && matches!(
                    crate::graph_files::decode_versioned_graph_files_participant(
                        snapshot.record_version,
                        &snapshot.bytes
                    )?,
                    crate::GraphFilesParticipant::V1(_)
                ) {
                Some(crate::resolve_generation_by_uuid(
                    source,
                    version.content.generation_uuid,
                )?)
            } else {
                None
            };
            for file in files {
                let path = match &original {
                    Some(generation) => crate::graph_files::resolve_v1_inventory_entry(
                        &generation.graph_tree_root(),
                        &file,
                    )?,
                    None => crate::graph_object_path(source, &file.content_sha256)?,
                };
                crate::graph_object_store::install_graph_object_file(
                    target,
                    &path,
                    &file.content_sha256,
                    file.byte_length,
                )?;
            }
        }
    }
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
                &crate::graph_object_path(source, &digest)?,
                &digest,
                *byte_length,
            )?;
        }
    }
    retained_content::inspect(target, version, None)?;
    Ok(())
}
