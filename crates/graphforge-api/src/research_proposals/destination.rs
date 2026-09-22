//! Private destination preparation and native owner validation before publication.
use super::{ReviewResearchProposalRequest, identity, invalid, preview::Preview};
use crate::{
    CancellationToken, GfError, GraphForge,
    branches::{baseline, fields, publication},
};
use graphforge_storage::research_versions::{
    PreparedResearchContent, RegisterResearchVersion, ResearchProposalDestination,
    ResearchProposalItem, prepare_branch_content, prepare_project_draft,
    replace_prepared_branch_domains,
};
use std::collections::BTreeSet;

pub(super) fn prepare(
    owner: &GraphForge,
    command: &publication::Command,
    preview: &Preview,
    request: &ReviewResearchProposalRequest,
    proof: &PreparedResearchContent,
    items: &[ResearchProposalItem],
    cancellation: &CancellationToken,
) -> Result<PreparedResearchContent, GfError> {
    let source = crate::branches::private_view::open(owner, proof)?;
    let project_draft;
    let (mut graph, branch_version) = match preview.proposal.destination {
        ResearchProposalDestination::Project { project_uuid } => {
            let spec = RegisterResearchVersion {
                version_uuid: identity(request.operation_uuid, "destination"),
                context_uuid: project_uuid,
                source_generation_uuid: preview.generation,
                selection: None,
                source_version: None,
                required_versions: BTreeSet::new(),
                label: None,
                description: None,
                created_at: request.created_at,
                evidence: crate::research_versions::complete_evidence(
                    &preview.destination.generation_for_read()?,
                )?,
            };
            project_draft = Some(prepare_project_draft(
                &command.root,
                &spec,
                cancellation.flag(),
            )?);
            let draft = project_draft.as_ref().expect("Project draft");
            let generation = graphforge_storage::resolve_project_generation(draft.path())?;
            let mut graph = GraphForge::open_resolved_with_options(
                draft.path().to_path_buf(),
                generation.clone(),
                false,
                owner.write_options.clone(),
                owner.resource_policy.clone(),
                graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
                    generation.generation_uuid(),
                ),
            )?;
            graph.lifecycle_mode =
                graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral;
            (graph, None)
        }
        ResearchProposalDestination::Branch { branch_uuid } => {
            project_draft = None;
            let (graph, mut version) = crate::branches::edit::prepare(owner, command, branch_uuid)?;
            version.version_uuid = identity(request.operation_uuid, "destination");
            version.created_at = request.created_at;
            (graph, Some(version))
        }
    };
    super::ontology::apply(&mut graph, &source, request, items, cancellation)?;
    let domains = crate::branches::merge_domains::merge(&graph, &source)?;
    super::apply_graph::apply(&graph, &source, items, cancellation)?;
    let generation = graph.generation_for_read()?;
    let mut prepared = if let Some(version) = branch_version {
        prepare_branch_content(&command.root, &generation, version, cancellation.flag())?
    } else {
        project_draft.as_ref().expect("Project draft").finish(
            crate::research_versions::complete_evidence(&generation)?,
            cancellation.flag(),
        )?
    };
    for evidence in &proof.version.content.evidence {
        if !prepared.version.content.evidence.contains(evidence) {
            prepared.version.content.evidence.push(evidence.clone());
        }
    }
    let keep = prepared
        .version
        .content
        .participants
        .iter()
        .map(|p| p.key.clone())
        .collect();
    replace_prepared_branch_domains(
        &command.root,
        &mut prepared,
        &keep,
        &domains,
        cancellation.flag(),
    )?;
    if matches!(
        preview.proposal.destination,
        ResearchProposalDestination::Branch { .. }
    ) {
        preserve_contributions(
            owner,
            &source,
            &preview.destination,
            &mut prepared,
            items,
            cancellation,
        )?;
    }
    verify_destination(owner, &prepared, items, cancellation)?;
    Ok(prepared)
}

fn verify_destination(
    owner: &GraphForge,
    prepared: &PreparedResearchContent,
    items: &[ResearchProposalItem],
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let finished = crate::branches::private_view::open(owner, prepared)?;
    let values = fields::read_selected(
        &finished,
        Some(
            &items
                .iter()
                .map(|item| (item.unit.object_kind.clone(), item.unit.object_uuid))
                .collect(),
        ),
        cancellation,
    )?;
    for item in items {
        if values
            .get(&(
                item.unit.object_kind.clone(),
                item.unit.object_uuid,
                item.unit.field.clone(),
            ))
            .copied()
            != item.value_sha256
        {
            return Err(invalid(
                "prepared destination does not contain the exact accepted value",
            ));
        }
    }
    Ok(())
}

fn preserve_contributions(
    owner: &GraphForge,
    source: &GraphForge,
    prior: &GraphForge,
    prepared: &mut PreparedResearchContent,
    items: &[ResearchProposalItem],
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let view = crate::branches::private_view::open(owner, prepared)?;
    let current = fields::read(&view, cancellation)?;
    let mut rows = baseline::read(prior)?;
    let incoming = baseline::read(source)?;
    for row in rows.values_mut() {
        row.current = current
            .get(&row.key)
            .map(super::output::hex)
            .unwrap_or_default();
    }
    for item in items {
        let key = (
            item.unit.object_kind.clone(),
            item.unit.object_uuid,
            item.unit.field.clone(),
        );
        let origin = incoming
            .get(&key)
            .ok_or_else(|| invalid("accepted contribution baseline is unavailable"))?;
        let row = rows.entry(key.clone()).or_insert_with(|| baseline::Row {
            key: key.clone(),
            origin: origin.origin,
            incorporated: None,
            original: origin.original.clone(),
            baseline: String::new(),
            current: String::new(),
            contribution: origin.contribution,
            role: "active".into(),
        });
        row.current = current
            .get(&key)
            .map(super::output::hex)
            .unwrap_or_default();
        row.contribution = origin.contribution;
        row.role = "active".into();
    }
    baseline::install(
        owner.resolved_generation.container_root(),
        prepared,
        &rows,
        cancellation,
    )
}
