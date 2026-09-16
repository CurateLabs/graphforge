//! Candidate compilation and module/bridge identity transformation.

use super::diagnostics::composition_error;
use super::{GfError, ModuleMigrationPreview, ModuleMigrationRequest};
use graphforge_ontology::{
    ActivationMode, ActivationRecord, ActivationScope, BridgeDocument, BridgeInventory,
    BridgeSelector, BridgeSetId, CompositionLimits, ImportFormatHint, InventorySnapshot,
    ModuleSelector, OntologyDoc, OntologyInventory, OntologyModuleId, SymbolKind,
};
use graphforge_storage::WorkspaceOntologyComposition;
use std::collections::HashSet;
use uuid::Uuid;

pub(super) fn module_inventory(
    composition: &WorkspaceOntologyComposition,
) -> Result<OntologyInventory, GfError> {
    if let [module] = composition.modules.as_slice()
        && module.allow_projected_identity
        && module.dependencies.is_empty()
        && composition.bridges.is_empty()
        && composition.activation.is_empty()
    {
        return OntologyInventory::from_legacy_single(
            module.document.clone(),
            true,
            composition.profile_default,
        )
        .map_err(composition_error);
    }
    OntologyInventory::reopen(InventorySnapshot {
        schema_version: 1,
        generation: 0,
        profile_default: composition.profile_default,
        activation: composition.activation.clone(),
        bridges: composition
            .bridges
            .iter()
            .map(bridge_id)
            .collect::<Result<Vec<_>, _>>()?,
        adopted: composition
            .modules
            .iter()
            .map(|m| graphforge_ontology::SnapshotModule {
                id: m.id.clone(),
                dependencies: m.dependencies.clone(),
                doc: m.document.clone(),
                enforcement: None,
            })
            .collect(),
        composition_fingerprint: composition.composition_fingerprint.clone(),
        receipts: Vec::new(),
    })
    .map_err(composition_error)
}

pub(super) fn bridge_inventory(
    composition: &WorkspaceOntologyComposition,
) -> Result<BridgeInventory, GfError> {
    let compiled = composition.compile()?;
    let modules = compiled
        .modules
        .iter()
        .map(|module| graphforge_ontology::SnapshotModuleSymbols {
            id: module.id.clone(),
            entities: module
                .symbols
                .iter()
                .filter(|s| s.kind == SymbolKind::Entity)
                .map(|s| s.local_id.clone())
                .collect(),
            relations: module
                .symbols
                .iter()
                .filter(|s| s.kind == SymbolKind::Relation)
                .map(|s| s.local_id.clone())
                .collect(),
            properties: module
                .symbols
                .iter()
                .filter(|s| s.kind == SymbolKind::Property)
                .map(|s| s.local_id.clone())
                .collect(),
        })
        .collect();
    let adopted = composition
        .bridges
        .iter()
        .map(|document| {
            Ok(graphforge_ontology::SnapshotBridge {
                id: bridge_id(document)?,
                dependencies: document.dependencies.clone(),
                doc: document.clone(),
            })
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    let known = adopted
        .iter()
        .map(|bridge| bridge.id.display_ref())
        .collect::<HashSet<_>>();
    let activation_subjects = composition
        .activation
        .iter()
        .filter(|record| record.scope == ActivationScope::Bridge && known.contains(&record.subject))
        .map(|record| record.subject.clone())
        .collect();
    BridgeInventory::reopen(graphforge_ontology::BridgeSnapshot {
        schema_version: 1,
        generation: 0,
        profile_default: composition.profile_default,
        activation_subjects,
        modules,
        adopted,
        receipts: Vec::new(),
    })
    .map_err(composition_error)
}

pub(super) fn enrich_module_delete_preview(
    mut preview: graphforge_ontology::DeletePreview,
    composition: &WorkspaceOntologyComposition,
) -> Result<graphforge_ontology::DeletePreview, GfError> {
    preview.bridge_refs = composition
        .bridges
        .iter()
        .filter(|bridge| bridge_references_module(bridge, &preview.target))
        .map(bridge_id)
        .collect::<Result<Vec<_>, _>>()?;
    preview.bridge_refs.sort_by_key(BridgeSetId::sort_key);
    preview.bridge_refs.dedup();
    preview.safe = preview.safe && preview.bridge_refs.is_empty();
    Ok(preview)
}

fn bridge_references_module(bridge: &BridgeDocument, module: &OntologyModuleId) -> bool {
    bridge.source_modules.iter().any(|id| id == module)
        || bridge.target_modules.iter().any(|id| id == module)
        || bridge.assertions.iter().any(|assertion| {
            assertion.source.module == *module || assertion.target.module == *module
        })
}

pub(super) fn compile_candidate(
    candidate: &WorkspaceOntologyComposition,
) -> Result<graphforge_ontology::CompiledComposition, GfError> {
    let modules = candidate
        .modules
        .iter()
        .map(|m| graphforge_ontology::AuthoredModule {
            id: m.id.clone(),
            dependencies: m.dependencies.clone(),
            doc: m.document.clone(),
            allow_projected_identity: m.allow_projected_identity,
        })
        .collect::<Vec<_>>();
    let bridges = candidate
        .bridges
        .iter()
        .map(bridge_id)
        .collect::<Result<Vec<_>, _>>()?;
    graphforge_ontology::compile_inventory(graphforge_ontology::InventoryCompileRequest {
        modules: &modules,
        bridges: &bridges,
        activation: &candidate.activation,
        profile_default: candidate.profile_default,
        limits: CompositionLimits::default(),
        cancelled: None,
    })
    .map_err(composition_error)
}

pub(super) fn empty_composition() -> Result<WorkspaceOntologyComposition, GfError> {
    let compiled =
        graphforge_ontology::compile_inventory(graphforge_ontology::InventoryCompileRequest {
            modules: &[],
            bridges: &[],
            activation: &[],
            profile_default: ActivationMode::Exploratory,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .map_err(composition_error)?;
    Ok(WorkspaceOntologyComposition::from_compiled(
        &compiled,
        Vec::new(),
    ))
}

pub(super) fn composition_from_generation(
    generation: &graphforge_storage::ResolvedProjectGeneration,
) -> Result<Option<WorkspaceOntologyComposition>, GfError> {
    if let Some(snapshot) = generation.participant_snapshot(
        graphforge_storage::WORKSPACE_CAPABILITY_ID,
        graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY,
    )? {
        return Ok(Some(WorkspaceOntologyComposition::from_canonical_json(
            &snapshot.bytes,
        )?));
    }
    let ontology = generation
        .participant_snapshot(
            graphforge_storage::WORKSPACE_CAPABILITY_ID,
            graphforge_storage::WORKSPACE_ONTOLOGY_FAMILY,
        )?
        .ok_or_else(|| GfError::Validation("workspace ontology authority is missing"))?;
    let ontology = graphforge_storage::WorkspaceOntology::from_canonical_json(&ontology.bytes)?;
    Ok(WorkspaceOntologyComposition::virtual_legacy(&ontology)?)
}

pub(super) fn bridge_id(document: &BridgeDocument) -> Result<BridgeSetId, GfError> {
    Ok(BridgeSetId {
        bridge_id: document.bridge_id.clone(),
        authored_version: document.authored_version.clone(),
        canonical_digest: graphforge_ontology::bridge_document_digest(document)
            .map_err(GfError::Validation)?,
    })
}

pub(super) fn migration_generation_uuid(
    request: &ModuleMigrationRequest,
    preview: &ModuleMigrationPreview,
) -> Uuid {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-retained-data-migration-generation/1");
    hasher.update(request.authority.context.operation_uuid.0.as_bytes());
    hasher.update(preview.plan.plan_digest.as_bytes());
    hasher.update(preview.next_module.display_ref().as_bytes());
    graphforge_core::canonical::uuid_v8(hasher.finalize().into())
}

pub(super) fn rewrite_bridge_symbols_for_migration(
    candidate: &mut WorkspaceOntologyComposition,
    previous: &OntologyModuleId,
    next: &OntologyModuleId,
    document: &OntologyDoc,
) -> Result<(), GfError> {
    let steps = graphforge_ontology::MigrationEngine::plan(
        &previous.authored_version,
        &next.authored_version,
        &document.migrations,
    )
    .map_err(|error| GfError::Validation(error.to_string()))?;
    for bridge in &mut candidate.bridges {
        for assertion in &mut bridge.assertions {
            for symbol in [&mut assertion.source, &mut assertion.target] {
                if symbol.module != *previous {
                    continue;
                }
                for step in &steps {
                    match &step.transform_kind {
                        graphforge_ontology::TransformKind::RenameType { old_name, new_name }
                            if symbol.kind == SymbolKind::Entity
                                && symbol.local_id == *old_name =>
                        {
                            symbol.local_id.clone_from(new_name);
                        }
                        graphforge_ontology::TransformKind::RenameProperty {
                            owner,
                            old_name,
                            new_name,
                        } if symbol.kind == SymbolKind::Property
                            && symbol.local_id == format!("{owner}:{old_name}") =>
                        {
                            symbol.local_id = format!("{owner}:{new_name}");
                        }
                        graphforge_ontology::TransformKind::RenameType { old_name, new_name }
                            if symbol.kind == SymbolKind::Property
                                && symbol.local_id.starts_with(&format!("{old_name}:")) =>
                        {
                            symbol.local_id = format!(
                                "{new_name}:{}",
                                symbol.local_id.split_once(':').map_or("", |(_, name)| name)
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    Ok(())
}
pub(super) fn staged_module_document(
    text: &str,
    format: ImportFormatHint,
) -> Result<OntologyDoc, GfError> {
    match format {
        ImportFormatHint::Json => graphforge_ontology::OntologyLoader::load_json(text.as_bytes()),
        ImportFormatHint::Auto if text.trim_start().starts_with('{') => {
            graphforge_ontology::OntologyLoader::load_json(text.as_bytes())
        }
        ImportFormatHint::Yaml | ImportFormatHint::Auto => {
            graphforge_ontology::OntologyLoader::load_yaml(text.as_bytes())
        }
    }
    .map_err(|_| GfError::Validation("ontology module import is malformed"))
}

pub(super) fn reject_duplicate_module(
    candidate: &WorkspaceOntologyComposition,
    id: &OntologyModuleId,
) -> Result<(), GfError> {
    if candidate.modules.iter().any(|m| m.id == *id) {
        Err(GfError::Validation("module identity already adopted"))
    } else {
        Ok(())
    }
}
pub(super) fn reject_duplicate_bridge(
    candidate: &WorkspaceOntologyComposition,
    id: &BridgeSetId,
) -> Result<(), GfError> {
    if candidate
        .bridges
        .iter()
        .filter_map(|document| bridge_id(document).ok())
        .any(|value| value == *id)
    {
        Err(GfError::Validation("bridge identity already adopted"))
    } else {
        Ok(())
    }
}
pub(super) fn resolve_module_index(
    candidate: &WorkspaceOntologyComposition,
    selector: &ModuleSelector,
) -> Result<usize, GfError> {
    let matches = candidate
        .modules
        .iter()
        .enumerate()
        .filter(|(_, m)| match selector {
            ModuleSelector::Exact(id) => m.id == *id,
            ModuleSelector::OntologyId(id) => m.id.ontology_id == *id,
        })
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [index] => Ok(*index),
        [] => Err(GfError::Validation("module not found")),
        _ => Err(GfError::Validation("module selector is ambiguous")),
    }
}
pub(super) fn resolve_bridge_index(
    candidate: &WorkspaceOntologyComposition,
    selector: &BridgeSelector,
) -> Result<usize, GfError> {
    let matches = candidate
        .bridges
        .iter()
        .enumerate()
        .filter(|(_, d)| match selector {
            BridgeSelector::Exact(id) => bridge_id(d).is_ok_and(|value| value == *id),
            BridgeSelector::BridgeId(id) => d.bridge_id == *id,
        })
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [index] => Ok(*index),
        [] => Err(GfError::Validation("bridge not found")),
        _ => Err(GfError::Validation("bridge selector is ambiguous")),
    }
}
pub(super) fn rewrite_module_identity(
    candidate: &mut WorkspaceOntologyComposition,
    prior: &OntologyModuleId,
    next: &OntologyModuleId,
) -> Result<(), GfError> {
    let prior_bridge_ids = candidate
        .bridges
        .iter()
        .map(bridge_id)
        .collect::<Result<Vec<_>, _>>()?;
    for module in &mut candidate.modules {
        for dep in &mut module.dependencies {
            if dep == prior {
                *dep = next.clone();
            }
        }
    }
    for bridge in &mut candidate.bridges {
        for id in bridge
            .source_modules
            .iter_mut()
            .chain(bridge.target_modules.iter_mut())
        {
            if id == prior {
                *id = next.clone();
            }
        }
        for assertion in &mut bridge.assertions {
            if assertion.source.module == *prior {
                assertion.source.module = next.clone();
            }
            if assertion.target.module == *prior {
                assertion.target.module = next.clone();
            }
        }
    }
    for record in &mut candidate.activation {
        if record.subject == prior.display_ref() {
            record.subject = next.display_ref();
        }
    }
    cascade_bridge_identities(candidate, prior_bridge_ids)
}

pub(super) fn cascade_bridge_identities(
    candidate: &mut WorkspaceOntologyComposition,
    mut prior_ids: Vec<BridgeSetId>,
) -> Result<(), GfError> {
    for _ in 0..=candidate.bridges.len() {
        let current_ids = candidate
            .bridges
            .iter()
            .map(bridge_id)
            .collect::<Result<Vec<_>, _>>()?;
        let changes = prior_ids
            .iter()
            .zip(&current_ids)
            .filter(|(prior, current)| prior != current)
            .map(|(prior, current)| (prior.clone(), current.clone()))
            .collect::<Vec<_>>();
        if changes.is_empty() {
            return Ok(());
        }
        for bridge in &mut candidate.bridges {
            for dependency in &mut bridge.dependencies {
                if let Some((_, replacement)) =
                    changes.iter().find(|(prior, _)| dependency == prior)
                {
                    *dependency = replacement.clone();
                }
            }
        }
        for record in &mut candidate.activation {
            if let Some((_, replacement)) = changes
                .iter()
                .find(|(prior, _)| record.subject == prior.display_ref())
            {
                record.subject = replacement.display_ref();
            }
        }
        prior_ids = current_ids;
    }
    Err(GfError::Validation(
        "bridge identity cascade did not converge",
    ))
}
pub(super) fn set_module_activation(
    candidate: &mut WorkspaceOntologyComposition,
    id: &OntologyModuleId,
    mode: Option<ActivationMode>,
) {
    candidate
        .activation
        .retain(|r| !(r.scope == ActivationScope::Module && r.subject == id.display_ref()));
    if let Some(mode) = mode {
        candidate.activation.push(ActivationRecord {
            scope: ActivationScope::Module,
            subject: id.display_ref(),
            mode,
        });
    }
}

#[cfg(test)]
mod tests;
