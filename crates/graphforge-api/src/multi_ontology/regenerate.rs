//! Receipt-authorized identity rewriting, prepared privately and published once.
use super::candidate::{bridge_id, cascade_bridge_identities, compile_candidate};
use crate::{
    CancellationToken, GfError, GraphForge, ItemIdRegeneration, ModuleMigrationRequest,
    ModuleSelector, OntologyAuthorityExpectation, OperationId, UpdateResearchMetadataRequest,
    WriteContext,
    item_refusals::{RegenerationBinding, RegenerationGuard},
};
use graphforge_discovery::{ItemId, ItemKind};
use graphforge_storage::{
    WorkspaceOntologyComposition,
    research_versions::{
        RegisterResearchVersion, ResearchMutation, ResearchOperation, prepare_project_draft,
        read_research_registry,
    },
};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

fn invalid(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}
fn ontology_error(error: super::MultiOntologyError) -> GfError {
    invalid(error.code + ": " + &error.message)
}
fn stale() -> GfError {
    GfError::Project {
        code: graphforge_core::ProjectErrorCode::WriteConflict,
        message: "ID regeneration source generation changed; the bound operation cannot be rebased; retry explicitly with a new operation UUID"
            .into(),
    }
}

impl GraphForge {
    /// Replace a GraphForge item ID only after a locally recorded `id_taken`.
    /// Rust generates a same-kind ID and atomically publishes all references,
    /// stored routes, a new Version and the selected Branch head. History is
    /// immutable. Reuse the operation UUID to replay the same ID and Version,
    /// including after reopen, later work or restoration of older content.
    #[allow(clippy::too_many_lines)] // authorization, native publication and error reconciliation form one operation
    pub fn regenerate_item_id(
        &mut self,
        id: &str,
        operation_uuid: Uuid,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ItemIdRegeneration, GfError> {
        let id = ItemId::parse_reference(id)
            .map_err(|_| invalid("a permanent GraphForge item ID is required"))?;
        if operation_uuid.is_nil() {
            return Err(invalid("ID regeneration operation UUID must be non-nil"));
        }
        if self.read_only {
            return Err(GfError::Project {
                code: graphforge_core::ProjectErrorCode::ReadOnlyView,
                message: "historical views cannot regenerate IDs".into(),
            });
        }
        let mut guard = RegenerationGuard::begin(self, &id, operation_uuid, |receipt| {
            cancellation.map_or(Ok(()), CancellationToken::checkpoint)?;
            self.prepare_regeneration_binding(&id, operation_uuid, receipt)
        })?;
        // Replay before reading current composition, Branch selection or source.
        if let Some(result) = guard.replay(self)? {
            return Ok(result);
        }
        self.graph_visibility.health.check()?;
        let token = cancellation.cloned().unwrap_or_default();
        token.checkpoint()?;
        let root = self.resolved_generation.container_root().to_path_buf();
        let source = graphforge_storage::resolve_project_generation(&root)?;
        let binding = guard.binding.clone();
        if source.generation_uuid() != binding.source_generation {
            return Err(stale());
        }
        let registry = read_research_registry(&source)?;
        let prior = registry
            .versions
            .get(&binding.source_version)
            .ok_or_else(|| invalid("selected Branch head is unavailable"))?;
        let spec = RegisterResearchVersion {
            version_uuid: binding.replacement.version_uuid,
            context_uuid: crate::research_claims::authority::project_uuid(&source)?,
            source_generation_uuid: binding.source_generation,
            selection: None,
            source_version: None,
            required_versions: BTreeSet::new(),
            label: None,
            description: None,
            created_at: binding.created_at,
            evidence: crate::research_versions::complete_evidence(&source)?,
            author: None,
            committer: None,
        };
        let draft = prepare_project_draft(&root, &spec, token.flag())?;
        let generation = graphforge_storage::resolve_project_generation(draft.path())?;
        let mut private = GraphForge::open_resolved_with_options(
            draft.path().to_path_buf(),
            generation.clone(),
            false,
            self.write_options.clone(),
            self.resource_policy.clone(),
            graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
                generation.generation_uuid(),
            ),
        )?;
        private.lifecycle_mode =
            graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral;
        let previous = private
            .workspace_ontology_composition()?
            .ok_or_else(|| invalid("item composition is missing"))?;
        let candidate = private.regenerate_composition(
            &previous,
            &id,
            &binding.replacement.id,
            operation_uuid,
        )?;
        private.materialize_regeneration(
            &previous,
            &candidate,
            &id,
            &binding.replacement.id,
            &token,
        )?;
        let private_generation = private.generation_for_read()?;
        let mut prepared = draft.finish(
            crate::research_versions::complete_evidence(&private_generation)?,
            token.flag(),
        )?;
        prepared.version.context_uuid = binding.branch_uuid;
        prepared.version.content.source_version = prior.content.source_version;
        prepared
            .version
            .content
            .required_versions
            .clone_from(&prior.content.required_versions);
        crate::branches::publication::commit(
            &mut prepared.version,
            vec![binding.source_version],
            None,
            None,
            None,
        )?;
        preserve_field_history(
            self,
            &private,
            prior,
            &previous,
            &candidate,
            &id,
            &binding.replacement.id,
            &mut prepared,
            &token,
        )?;
        let operation = ResearchOperation {
            operation_uuid,
            expected_generation_uuid: binding.source_generation,
            mutation: ResearchMutation::RegenerateItemId {
                intent_sha256: binding.digest()?,
                version: Box::new(prepared.version.clone()),
            },
        };
        guard.bind_request(&operation)?;
        token.checkpoint()?;
        #[cfg(test)]
        tests::before_publication(self, &binding, &token)?;
        let outcome = graphforge_storage::research_versions::publish_research_operation_with_mode(
            &root,
            &operation,
            token.flag(),
            self.lifecycle_mode,
        );
        // Errors after CURRENT linearization remain errors. Reconcile live readers
        // in either case; the authenticated receipt makes the next retry a replay.
        if let Err(refresh) =
            self.refresh_research_authority(&root, &outcome, None, binding.source_generation, false)
        {
            self.graph_visibility.health.fail(&refresh);
            return Err(outcome.err().unwrap_or(refresh));
        }
        outcome?;
        #[cfg(test)]
        tests::after_publication(&binding, &token)?;
        guard.acknowledge(self)?;
        Ok(binding.replacement)
    }

    fn prepare_regeneration_binding(
        &self,
        id: &ItemId,
        operation: Uuid,
        refusal: &crate::ItemIdRefusalReceipt,
    ) -> Result<RegenerationBinding, GfError> {
        let source = graphforge_storage::resolve_project_generation(
            self.resolved_generation.container_root(),
        )?;
        if source.generation_uuid() != self.generation_for_read()?.generation_uuid() {
            return Err(stale());
        }
        let registry = read_research_registry(&source)?;
        let branch = registry
            .current_branch
            .filter(|id| registry.branches.contains_key(id))
            .or_else(|| {
                (registry.branches.len() == 1)
                    .then(|| *registry.branches.keys().next().expect("one Branch"))
            })
            .ok_or_else(|| {
                invalid(
                    "GF_RESEARCH_BRANCH_UNSELECTED: no Branch is selected; run `gf switch <ref>`",
                )
            })?;
        let head = *registry
            .heads
            .get(&branch)
            .ok_or_else(|| invalid("selected Branch has no head"))?;
        let composition = self
            .workspace_ontology_composition()?
            .ok_or_else(|| invalid("item composition is missing"))?;
        require_item(&composition, id)?;
        let replacement = ItemId::generate(id.kind())
            .map_err(|_| invalid("permanent item ID randomness is unavailable"))?;
        if all_item_ids(&composition).any(|existing| {
            ItemId::parse_reference(existing).is_ok_and(|existing| existing == replacement)
        }) {
            return Err(invalid("generated item ID already exists"));
        }
        Ok(RegenerationBinding {
            operation_uuid: operation,
            old_id: id.as_str().into(),
            refusal: refusal.clone(),
            replacement: ItemIdRegeneration {
                id: replacement.as_str().into(),
                version_uuid: Uuid::now_v7(),
            },
            source_generation: source.generation_uuid(),
            branch_uuid: branch,
            source_version: head,
            created_at: (self.clock.lock().expect("clock lock poisoned"))()?,
            request_sha256: None,
        })
    }

    fn regenerate_composition(
        &self,
        previous: &WorkspaceOntologyComposition,
        id: &ItemId,
        next: &str,
        operation: Uuid,
    ) -> Result<WorkspaceOntologyComposition, GfError> {
        let next =
            ItemId::parse_reference(next).map_err(|_| invalid("replacement ID is invalid"))?;
        match id.kind() {
            ItemKind::OntologyModule => {
                let module = previous
                    .modules
                    .iter()
                    .find(|module| same_id(&module.id.ontology_id, id))
                    .ok_or_else(|| invalid("module not found"))?;
                let mut document = module.document.clone();
                document.ontology_id = next.to_uri();
                let authority = self.ontology_authority_state().map_err(ontology_error)?;
                let request = ModuleMigrationRequest {
                    authority: OntologyAuthorityExpectation {
                        context: WriteContext {
                            operation_uuid: OperationId(operation),
                            actor_uuid: None,
                        },
                        expected_project_generation_uuid: authority.project_generation_uuid,
                        expected_composition_fingerprint: authority.composition_fingerprint,
                    },
                    selector: ModuleSelector::Exact(module.id.clone()),
                    document,
                    dependencies: module.dependencies.clone(),
                    enforcement: previous
                        .activation
                        .iter()
                        .find(|record| record.subject == module.id.display_ref())
                        .map(|record| record.mode),
                };
                self.module_migration_candidate_for_identity(&request, true)
                    .map(|value| value.0)
                    .map_err(ontology_error)
            }
            ItemKind::BridgeSet => {
                let mut candidate = previous.clone();
                let old = candidate
                    .bridges
                    .iter()
                    .map(bridge_id)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(ontology_error)?;
                let bridge = candidate
                    .bridges
                    .iter_mut()
                    .find(|bridge| same_id(&bridge.bridge_id, id))
                    .ok_or_else(|| invalid("bridge not found"))?;
                bridge.bridge_id = next.to_uri();
                cascade_bridge_identities(&mut candidate, old).map_err(ontology_error)?;
                let compiled = compile_candidate(&candidate).map_err(ontology_error)?;
                Ok(WorkspaceOntologyComposition::from_compiled(
                    &compiled,
                    candidate.bridges,
                ))
            }
        }
    }

    fn materialize_regeneration(
        &mut self,
        previous: &WorkspaceOntologyComposition,
        candidate: &WorkspaceOntologyComposition,
        id: &ItemId,
        next: &str,
        token: &CancellationToken,
    ) -> Result<(), GfError> {
        let source = self.generation_for_read()?;
        let before = previous.compile()?;
        let after = candidate.compile()?;
        let replacement =
            ItemId::parse_reference(next).map_err(|_| invalid("invalid replacement ID"))?;
        let mut pairs = Vec::new();
        for old in &before.modules {
            let expected = if same_id(&old.id.ontology_id, id) {
                let mut document = old.doc.clone();
                document.ontology_id = replacement.to_uri();
                crate::OntologyModuleId {
                    ontology_id: replacement.to_uri(),
                    authored_version: old.id.authored_version.clone(),
                    canonical_digest: graphforge_ontology::module_document_digest(&document)
                        .map_err(invalid)?,
                }
            } else {
                old.id.clone()
            };
            let new = after
                .modules
                .iter()
                .find(|module| module.id == expected)
                .ok_or_else(|| invalid("regeneration target exact module identity is missing"))?;
            if old.id != new.id {
                pairs.push((old.id.clone(), new.id.clone()));
            }
        }
        let bindings = graphforge_storage::semantic_storage_bindings(&source)?
            .ok_or_else(|| invalid("semantic storage bindings are missing"))?;
        let (tree, _guard, _) = crate::hydrate_graph_workspace(&source, true)?;
        let inventory = crate::property_inventory_for_hydrated_generation(
            &source,
            &tree,
            self.resource_policy.memory_budget_bytes,
        )?;
        let authority =
            graphforge_storage::TopologyFileAuthority::from_inventory(&tree, &inventory)?;
        let topology = graphforge_storage::enumerate_topology_files(&authority, None)?;
        let plan =
            graphforge_storage::SemanticStorageBindings::plan_retained_data_migration_from_files(
                &before, &after, &bindings, &tree, &pairs, &topology,
            )?;
        let staging = tempfile::tempdir().map_err(|_| invalid("cannot prepare ID regeneration"))?;
        let candidate_graph = staging.path().join("graph");
        let (evidence, migrated) = graphforge_storage::materialize_semantic_migration_from_files(
            &plan,
            &tree,
            &candidate_graph,
            graphforge_storage::SemanticMigrationLimits::default(),
            || token.checkpoint(),
            &topology,
        )?;
        if evidence.plan_digest != plan.plan_digest {
            return Err(invalid("ID regeneration migration plan changed"));
        }
        crate::workspace_ontology::publish_workspace_records_with_graph_tree(
            self,
            Uuid::now_v7(),
            None,
            &self.workspace_ontology()?,
            &self.workspace_configuration()?,
            candidate,
            &plan.bindings,
            Uuid::now_v7(),
            &candidate_graph,
            &migrated,
            Some(token),
        )?;
        // Metadata is another private edit; it becomes visible only with the final
        // complete Version, never as an intermediate authoritative generation.
        let mut metadata = self.research_project_metadata()?;
        for ontology in &mut metadata.ontologies {
            if same_id(ontology, id) {
                *ontology = ItemId::parse_reference(next)
                    .map_err(|_| invalid("invalid replacement ID"))?
                    .to_uri();
            }
        }
        metadata.ontologies.sort();
        metadata.ontologies.dedup();
        self.update_research_metadata(UpdateResearchMetadataRequest {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            metadata,
        })?;
        Ok(())
    }
}

fn same_id(value: &str, expected: &ItemId) -> bool {
    ItemId::parse_reference(value).is_ok_and(|value| value == *expected)
}
fn all_item_ids(composition: &WorkspaceOntologyComposition) -> impl Iterator<Item = &str> {
    composition
        .modules
        .iter()
        .map(|module| module.id.ontology_id.as_str())
        .chain(
            composition
                .bridges
                .iter()
                .map(|bridge| bridge.bridge_id.as_str()),
        )
}
fn require_item(composition: &WorkspaceOntologyComposition, id: &ItemId) -> Result<(), GfError> {
    let count = match id.kind() {
        ItemKind::OntologyModule => composition
            .modules
            .iter()
            .filter(|module| same_id(&module.id.ontology_id, id))
            .count(),
        ItemKind::BridgeSet => composition
            .bridges
            .iter()
            .filter(|bridge| same_id(&bridge.bridge_id, id))
            .count(),
    };
    if count != 1 {
        return Err(invalid(
            "regeneration requires exactly one adopted item under the refused ID",
        ));
    }
    Ok(())
}

// Carry immutable field origins and contribution identities across context-key
// changes. Updated current hashes describe the rewrite, without resetting credit.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // remap context keys while preserving immutable field credit
fn preserve_field_history(
    owner: &GraphForge,
    private: &GraphForge,
    prior: &graphforge_storage::research_versions::ResearchVersionRecord,
    previous: &WorkspaceOntologyComposition,
    candidate: &WorkspaceOntologyComposition,
    id: &ItemId,
    next: &str,
    prepared: &mut graphforge_storage::research_versions::PreparedResearchContent,
    token: &CancellationToken,
) -> Result<(), GfError> {
    let next = ItemId::parse_reference(next).map_err(|_| invalid("invalid replacement ID"))?;
    let mut remap = BTreeMap::new();
    let mut subjects = BTreeMap::new();
    let mut pair = |kind: &str, old: &str, new: &str| {
        remap.insert(
            (
                kind.to_owned(),
                crate::branches::context_fields::context_identity(kind, old),
            ),
            crate::branches::context_fields::context_identity(kind, new),
        );
    };
    for old in &previous.modules {
        let new = candidate
            .modules
            .iter()
            .find(|new| {
                new.id.authored_version == old.id.authored_version
                    && if same_id(&old.id.ontology_id, id) {
                        same_id(&new.id.ontology_id, &next)
                    } else {
                        new.id.ontology_id == old.id.ontology_id
                    }
            })
            .ok_or_else(|| invalid("regenerated module identity is missing"))?;
        pair("ontology_module", &old.id.ontology_id, &new.id.ontology_id);
        subjects.insert(old.id.display_ref(), new.id.display_ref());
    }
    for old in &previous.bridges {
        let new = candidate
            .bridges
            .iter()
            .find(|new| {
                new.authored_version == old.authored_version
                    && if same_id(&old.bridge_id, id) {
                        same_id(&new.bridge_id, &next)
                    } else {
                        new.bridge_id == old.bridge_id
                    }
            })
            .ok_or_else(|| invalid("regenerated bridge identity is missing"))?;
        pair("ontology_bridge", &old.bridge_id, &new.bridge_id);
        subjects.insert(
            bridge_id(old).map_err(ontology_error)?.display_ref(),
            bridge_id(new).map_err(ontology_error)?.display_ref(),
        );
    }
    for old in &previous.activation {
        let new = subjects.get(&old.subject).unwrap_or(&old.subject);
        pair(
            "ontology_activation",
            &format!("{:?}:{}", old.scope, old.subject),
            &format!("{:?}:{}", old.scope, new),
        );
    }
    let generation = private.generation_for_read()?;
    let graph = GraphForge::open_resolved_with_options(
        generation.container_root().to_path_buf(),
        generation.clone(),
        true,
        owner.write_options.clone(),
        owner.resource_policy.clone(),
        graphforge_storage::ProjectOpenRecoveryEvidence::checkpoint_view(
            generation.generation_uuid(),
        ),
    )?;
    let current = crate::branches::fields::read(&graph, token)?;
    let prior_graph = crate::research_versions::materialize_version(owner, prior)?;
    let mut rows = BTreeMap::new();
    for (_, mut row) in crate::branches::baseline::read(&prior_graph)? {
        if let Some(id) = remap.get(&(row.key.0.clone(), row.key.1)) {
            row.key.1 = *id;
        }
        row.current = current.get(&row.key).map(hex).unwrap_or_default();
        if rows.insert(row.key.clone(), row).is_some() {
            return Err(invalid("regeneration field identity collision"));
        }
    }
    for (key, digest) in current {
        rows.entry(key.clone())
            .or_insert_with(|| crate::branches::baseline::Row {
                key: key.clone(),
                origin: prepared.version.version_uuid,
                incorporated: None,
                original: hex(&digest),
                baseline: String::new(),
                current: hex(&digest),
                contribution: crate::branches::baseline::contribution(
                    prepared.version.context_uuid,
                    prepared.version.version_uuid,
                    &key,
                ),
                role: "active".into(),
            });
    }
    crate::branches::baseline::install(
        owner.resolved_generation.container_root(),
        prepared,
        &rows,
        token,
    )
}
fn hex(digest: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    digest.iter().fold(String::new(), |mut text, byte| {
        write!(text, "{byte:02x}").expect("string");
        text
    })
}

#[cfg(test)]
mod tests;
