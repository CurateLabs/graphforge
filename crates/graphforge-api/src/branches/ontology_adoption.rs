//! Retain verified adoption in workspace and selected Branch in one CURRENT transition.
use super::{edit, publication};
use crate::{
    CancellationToken, CompositionChangeRequest, CompositionDataDisposition, GfError, GraphForge,
    OntologyAdoptionReceipt, ResolvedOntologyModule, WriteContext,
};
use graphforge_storage::research_versions::{
    RegisterResearchVersion, ResearchMutation, prepare_branch_content, prepare_project_draft,
    read_research_registry,
};
use uuid::Uuid;

impl GraphForge {
    /// Adopt an authenticated closure for ordinary publication. An existing selected
    /// Branch receives a fresh immutable head in the same transaction as workspace
    /// authority. Their independent graph rows and unrelated metadata are preserved.
    /// Without Branches this keeps workspace-only adoption semantics. Exact completed
    /// repeats do not advance CURRENT; a workspace-only adoption is completed on retry.
    #[allow(clippy::too_many_lines)] // two private preparations share one guarded publication
    pub fn adopt_resolved_ontology_module_for_publication(
        &mut self,
        resolved: &ResolvedOntologyModule,
        context: WriteContext,
        cancellation: Option<&CancellationToken>,
    ) -> Result<OntologyAdoptionReceipt, GfError> {
        let token = cancellation.cloned().unwrap_or_default();
        token.checkpoint()?;
        self.require_private_workspace()?;
        let source = self.generation_for_read()?;
        if source.generation_uuid()
            != *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned")
        {
            return Err(GfError::Validation(
                "Project changed; reopen before ontology adoption".into(),
            ));
        }
        let registry = read_research_registry(&source)?;
        if registry.branches.is_empty() {
            return self.adopt_resolved_ontology_module(resolved, context, Some(&token));
        }
        let branch = registry.current_branch.or_else(|| {
            (registry.branches.len() == 1).then(|| *registry.branches.keys().next().expect("one Branch"))
        }).ok_or_else(|| GfError::Validation("GF_RESEARCH_BRANCH_UNSELECTED: select a Branch with `gf switch <ref>` before adoption".into()))?;
        // Bind the authenticated source, not random private preparation identities.
        let command = publication::begin(
            self,
            context.operation_uuid.0,
            source.generation_uuid(),
            &(
                "ontology.adopt",
                context.operation_uuid.0,
                context.actor_uuid,
                resolved.module(),
                resolved.repository().canonical_name(),
                resolved.package_digest(),
            ),
            &token,
        )?;
        // Native recovery distinguishes an abandoned physical preparation from a
        // live writer before the same logical operation prepares fresh bytes.
        graphforge_storage::recover_project_transactions_with_mode(
            &command.root,
            self.lifecycle_mode,
        )?;
        token.checkpoint()?;
        let replay = command.replay(self)?.is_some();
        let (mut head, mut version) = edit::prepare(self, &command, branch)?;
        let spec = RegisterResearchVersion {
            version_uuid: Uuid::now_v7(),
            context_uuid: crate::research_claims::authority::project_uuid(&source)?,
            source_generation_uuid: source.generation_uuid(),
            selection: None,
            source_version: None,
            required_versions: Default::default(),
            label: None,
            description: None,
            created_at: (self.clock.lock().expect("clock lock poisoned"))()?,
            evidence: crate::research_versions::complete_evidence(&source)?,
            author: None,
            committer: None,
        };
        let draft = prepare_project_draft(&command.root, &spec, token.flag())?;
        let generation = graphforge_storage::resolve_project_generation(draft.path())?;
        let mut workspace = GraphForge::open_resolved_with_options(
            draft.path().to_path_buf(),
            generation.clone(),
            false,
            self.write_options.clone(),
            self.resource_policy.clone(),
            graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
                generation.generation_uuid(),
            ),
        )?;
        workspace.lifecycle_mode =
            graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral;
        // Intermediate publications affect only the process-owned draft.
        let mut receipt = workspace.adopt_resolved_ontology_module(
            resolved,
            WriteContext {
                operation_uuid: crate::OperationId(Uuid::now_v7()),
                actor_uuid: context.actor_uuid,
            },
            Some(&token),
        )?;
        let candidate = workspace
            .workspace_ontology_composition()?
            .ok_or_else(conflict)?;
        let prior = head.workspace_ontology_composition()?;
        // Workspace-only changes may add authority, but never erase or replace
        // independent Branch modules, bridges or enforcement.
        if !retains_authority(prior.as_ref(), &candidate)? {
            return Err(conflict());
        }
        let workspace_metadata = workspace.research_project_metadata()?;
        let mut metadata = head.research_project_metadata()?;
        let prefix = crate::research_project::ONTOLOGY_ADOPTION_EXTENSION_PREFIX;
        for (key, value) in &workspace_metadata.extensions {
            if key.starts_with(prefix) {
                if metadata
                    .extensions
                    .get(key)
                    .is_some_and(|prior| prior != value)
                {
                    return Err(conflict());
                }
                metadata.extensions.insert(key.clone(), value.clone());
            }
        }
        // A Branch-only adoption record cannot be silently dropped or substituted.
        if metadata
            .extensions
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .any(|(key, value)| workspace_metadata.extensions.get(key) != Some(value))
        {
            return Err(conflict());
        }
        if receipt.already_adopted
            && prior.as_ref() == Some(&candidate)
            && head.research_project_metadata()? == metadata
        {
            token.checkpoint()?;
            if graphforge_storage::resolve_project_generation(&command.root)?.generation_uuid()
                != source.generation_uuid()
            {
                return Err(GfError::Project {
                    code: crate::ProjectErrorCode::WriteConflict,
                    message: "Project changed during ontology adoption; reopen before retry".into(),
                });
            }
            receipt.project_generation_uuid = source.generation_uuid();
            return Ok(receipt);
        }
        if replay {
            return Err(GfError::Validation(
                "adoption operation was already published; retry with a new operation UUID".into(),
            ));
        }
        let change = CompositionChangeRequest {
            context: WriteContext {
                operation_uuid: crate::OperationId(Uuid::now_v7()),
                actor_uuid: context.actor_uuid,
            },
            expected_project_generation_uuid: head.generation_for_read()?.generation_uuid(),
            expected_composition_fingerprint: prior
                .map(|composition| composition.composition_fingerprint),
            candidate,
            data_disposition: CompositionDataDisposition::RequireConformingOperation {
                operation: format!(
                    "ontology.adopt:{}",
                    String::from_utf8(metadata.to_canonical_json()?).map_err(|_| conflict())?
                ),
            },
        };
        let preview = head.preview_ontology_composition_change(&change, Some(&token))?;
        head.publish_ontology_composition_with_metadata(
            &change,
            &preview,
            Some(&token),
            Some(&metadata),
        )?;
        version.version_uuid = Uuid::now_v7();
        version.created_at = spec.created_at;
        edit::sign(&mut version, None, None)?;
        let generation = head.generation_for_read()?;
        version.content.evidence = crate::research_versions::complete_evidence(&generation)?;
        let mut prepared_head =
            prepare_branch_content(&command.root, &generation, version, token.flag())?;
        super::baseline::update(
            self,
            &head,
            &mut prepared_head,
            context.operation_uuid.0,
            &token,
        )?;
        let prepared_workspace = draft.finish(
            crate::research_versions::complete_evidence(&workspace.generation_for_read()?)?,
            token.flag(),
        )?;
        let mutation = ResearchMutation::AdoptBranchOntology {
            intent_sha256: command.intent,
            version: Box::new(prepared_head.version.clone()),
            workspace: Box::new(prepared_workspace.version.clone()),
        };
        let published = command.publish(self, mutation, &token)?;
        receipt.project_generation_uuid = published.generation_uuid;
        receipt.already_adopted = false;
        Ok(receipt)
    }
}

fn conflict() -> GfError {
    GfError::Validation("workspace and selected Branch ontology authority or adoption provenance conflict; reconcile before adoption".into())
}

// Adding workspace authority must not discard an independent Branch declaration
// or change enforcement on any existing subject.
fn retains_authority(
    previous: Option<&graphforge_storage::WorkspaceOntologyComposition>,
    candidate: &graphforge_storage::WorkspaceOntologyComposition,
) -> Result<bool, GfError> {
    let Some(previous) = previous else {
        return Ok(true);
    };
    if previous.profile_default != candidate.profile_default
        || previous
            .modules
            .iter()
            .any(|module| !candidate.modules.contains(module))
        || previous
            .bridges
            .iter()
            .any(|bridge| !candidate.bridges.contains(bridge))
    {
        return Ok(false);
    }
    let before = previous.compile()?;
    let after = candidate.compile()?;
    let mode = |composition: &graphforge_ontology::CompiledComposition, scope, subject: &str| {
        composition
            .activation
            .iter()
            .find(|record| record.scope == scope && record.subject == subject)
            .map_or(composition.profile_default, |record| record.mode)
    };
    Ok(before.modules.iter().all(|module| {
        before.effective_module_mode(&module.id) == after.effective_module_mode(&module.id)
    }) && before.bridges.iter().all(|bridge| {
        mode(
            &before,
            graphforge_ontology::ActivationScope::Bridge,
            &bridge.display_ref(),
        ) == mode(
            &after,
            graphforge_ontology::ActivationScope::Bridge,
            &bridge.display_ref(),
        )
    }))
}
