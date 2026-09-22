//! Authenticate frozen membership against retained native source authority.
use super::{
    CancellationToken, GfError, GraphForge, SliceMembers, SliceRequest, SliceSelector, SliceSource,
    engine, frozen, invalid, unavailable,
};
use graphforge_storage::research_versions::{ResearchEvidenceReference, ResearchVersionRecord};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use uuid::Uuid;

pub(crate) struct BranchSelection {
    pub version: ResearchVersionRecord,
    pub prepared: graphforge_storage::research_versions::PreparedBranchContent,
    pub view: GraphForge,
    pub active: BTreeSet<(String, Uuid)>,
    pub required: BTreeSet<(String, Uuid)>,
    pub evidence: Vec<ResearchEvidenceReference>,
    pub selector_sha256: [u8; 32],
}

pub(crate) fn authenticate(
    owner: &GraphForge,
    ipc: &[u8],
    cancellation: &CancellationToken,
) -> Result<BranchSelection, GfError> {
    let (selection, context) = frozen::decode(ipc, Some(cancellation))?;
    let version = owner.research_version(context.version_uuid)?;
    let commitment: [u8; 32] = Sha256::digest(
        serde_json::to_vec(&version.content).map_err(|_| invalid("invalid Version content"))?,
    )
    .into();
    let ontology: Vec<_> = version
        .content
        .participants
        .iter()
        .filter(|p| p.key.capability == "workspace" && p.key.family.contains("ontology"))
        .cloned()
        .collect();
    if commitment != context.version_sha256
        || version.content.generation_uuid != context.generation_uuid
        || ontology != context.ontology
    {
        return Err(unavailable());
    }
    let mut members = SliceMembers::default();
    for object in selection.active.keys() {
        match object.kind.as_str() {
            "node" => &mut members.nodes,
            "edge" => &mut members.edges,
            "source" => &mut members.sources,
            "artifact" => &mut members.artifacts,
            "assertion" => &mut members.assertions,
            _ => return Err(invalid("unsupported active Branch membership")),
        }
        .insert(object.uuid);
    }
    let request = SliceRequest {
        request_uuid: context.request_uuid,
        source: SliceSource::Version {
            version_uuid: context.version_uuid,
        },
        selector: SliceSelector::Direct { members },
        include: SliceMembers::default(),
        exclude: SliceMembers::default(),
        limits: context.limits,
    };
    // Capsule fingerprints are not signatures. Re-derive the dependency closure
    // with the native owners; caller-supplied required rows cannot omit evidence.
    let (prepared, view) =
        selected_view(owner, &version, &selection, &context.evidence, cancellation)?;
    crate::branches::domain_bounds::preflight(&view.generation_for_read()?)?;
    let derived = engine::evaluate(&view, &request, Some(cancellation))?;
    if !derived.active.keys().eq(selection.active.keys())
        || !derived.required.keys().eq(selection.required.keys())
    {
        return Err(invalid(
            "frozen Branch membership differs from native dependency closure",
        ));
    }
    let evidence: Vec<_> = version
        .content
        .evidence
        .iter()
        .filter(|e| {
            let id = match e {
                ResearchEvidenceReference::Local { artifact_uuid, .. }
                | ResearchEvidenceReference::ExternalOnly { artifact_uuid, .. }
                | ResearchEvidenceReference::Unverifiable { artifact_uuid } => *artifact_uuid,
            };
            let object = super::Object::new("artifact", id);
            derived.active.contains_key(&object) || derived.required.contains_key(&object)
        })
        .cloned()
        .collect();
    if evidence != context.evidence {
        return Err(invalid(
            "frozen Branch evidence differs from retained source",
        ));
    }
    Ok(BranchSelection {
        version,
        prepared,
        view,
        active: derived
            .active
            .into_keys()
            .map(|o| (o.kind, o.uuid))
            .collect(),
        required: derived
            .required
            .into_keys()
            .map(|o| (o.kind, o.uuid))
            .collect(),
        evidence,
        selector_sha256: context.selector_sha256,
    })
}

fn selected_view(
    owner: &GraphForge,
    version: &ResearchVersionRecord,
    selection: &super::Selection,
    evidence: &[ResearchEvidenceReference],
    cancellation: &CancellationToken,
) -> Result<
    (
        graphforge_storage::research_versions::PreparedBranchContent,
        GraphForge,
    ),
    GfError,
> {
    use graphforge_storage::research_versions::{
        RegisterResearchVersion, ResearchGraphSelection, materialize_prepared_branch,
        prepare_branch_selection,
    };
    let graph = ResearchGraphSelection {
        nodes: selection
            .active
            .keys()
            .chain(selection.required.keys())
            .filter(|o| o.kind == "node")
            .map(|o| o.uuid)
            .collect(),
        edges: selection
            .active
            .keys()
            .chain(selection.required.keys())
            .filter(|o| o.kind == "edge")
            .map(|o| o.uuid)
            .collect(),
        induced_edges: false,
        exclude_properties: BTreeSet::new(),
    };
    let spec = RegisterResearchVersion {
        version_uuid: Uuid::now_v7(),
        context_uuid: Uuid::now_v7(),
        source_generation_uuid: version.content.generation_uuid,
        source_version: Some(version.version_uuid),
        selection: Some(
            version
                .content
                .participants
                .iter()
                .map(|p| p.key.clone())
                .collect(),
        ),
        required_versions: BTreeSet::new(),
        label: None,
        description: None,
        created_at: 0,
        evidence: evidence.to_vec(),
    };
    let root = owner.resolved_generation.container_root();
    let prepared =
        prepare_branch_selection(root, &spec, None, Some(&graph), &[], cancellation.flag())?;
    let directory =
        std::sync::Arc::new(tempfile::tempdir().map_err(|e| GfError::Storage(e.to_string()))?);
    let generation = materialize_prepared_branch(root, &prepared, directory.path())?;
    let mut view = GraphForge::open_resolved_with_options(
        directory.path().to_path_buf(),
        generation.clone(),
        true,
        owner.write_options.clone(),
        owner.resource_policy.clone(),
        graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
            generation.generation_uuid(),
        ),
    )?;
    view.lifecycle_mode = graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral;
    view.research_materialization = Some(directory);
    view.resource_policy.memory_budget_bytes = 64 * 1024 * 1024;
    view.resource_policy.batch_size = 256;
    view.resource_policy.target_partitions = 1;
    view.resource_policy.spill_enabled = false;
    Ok((prepared, view))
}
