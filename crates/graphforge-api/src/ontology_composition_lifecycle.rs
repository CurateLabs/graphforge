//! Atomic project-generation lifecycle for ontology composition authority.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};

use arrow::array::{Array, ListArray, UInt32Array};

use graphforge_core::{GfError, ProjectErrorCode};
use graphforge_ontology::{
    ActivationMode, MigrationEngine, ResolveRequest, SymbolKind, TransformKind,
};
use graphforge_storage::{
    WORKSPACE_CAPABILITY_ID, WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY, WorkspaceOntologyComposition,
    WorkspaceOntologyMode,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{CancellationToken, GraphForge, WriteContext};

/// Explicit disposition for runtime observations encountered by strict promotion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompositionDataDisposition {
    /// No runtime-only observations may remain.
    RequireConforming,
}

/// Request bound to one exact current generation and composition identity.
#[derive(Debug, Clone)]
pub struct CompositionChangeRequest {
    /// Idempotency and actor identity.
    pub context: WriteContext,
    /// Project generation previewed by the caller.
    pub expected_project_generation_uuid: Uuid,
    /// Current composition fingerprint, or `None` for an empty legacy project.
    pub expected_composition_fingerprint: Option<String>,
    /// Complete replacement authority; deltas are never durable authority.
    pub candidate: WorkspaceOntologyComposition,
    /// Explicit stored-data disposition.
    pub data_disposition: CompositionDataDisposition,
}

/// Stable attributable lifecycle diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositionChangeDiagnostic {
    /// Stable machine code.
    pub code: String,
    /// Exact bounded subject.
    pub subject: String,
    /// Actionable remediation.
    pub remediation: String,
}

/// Deterministic preview bound to both authority identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositionChangePreview {
    /// Expected current project generation.
    pub expected_project_generation_uuid: Uuid,
    /// Expected current composition.
    pub expected_composition_fingerprint: Option<String>,
    /// Candidate exact composition.
    pub candidate_composition_fingerprint: String,
    /// Canonical candidate content hash.
    pub candidate_sha256: String,
    /// Identity-sorted affected modules.
    pub affected_modules: Vec<String>,
    /// Identity-sorted affected bridges.
    pub affected_bridges: Vec<String>,
    /// Portable-v2 disposition until #841 adds authenticated package support.
    pub portable_compatibility: CompositionPortableCompatibility,
    /// Bounded diagnostics; empty means publishable.
    pub diagnostics: Vec<CompositionChangeDiagnostic>,
}

/// Semantic portable-v2 verdict for a candidate composition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompositionPortableCompatibility {
    /// Canonical authority is representable, while current package readers
    /// deliberately fail closed on the required `ontology-composition@1` token.
    RepresentableReaderUnsupported,
}

/// Durable publication receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositionChangeReceipt {
    /// Published project generation.
    pub project_generation_uuid: Uuid,
    /// Published exact composition identity.
    pub composition_fingerprint: String,
    /// Canonical request content hash.
    pub candidate_sha256: String,
}

impl GraphForge {
    /// Read exact composition authority, or a virtual legacy projection without mutation.
    pub fn workspace_ontology_composition(
        &self,
    ) -> Result<Option<WorkspaceOntologyComposition>, GfError> {
        if let Some(composition) = self.persisted_workspace_ontology_composition()? {
            return Ok(Some(composition));
        }
        WorkspaceOntologyComposition::virtual_legacy(&self.workspace_ontology()?)
    }

    pub(crate) fn persisted_workspace_ontology_composition(
        &self,
    ) -> Result<Option<WorkspaceOntologyComposition>, GfError> {
        let current = self.generation_for_read()?;
        if let Some(snapshot) = current.participant_snapshot(
            WORKSPACE_CAPABILITY_ID,
            WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY,
        )? {
            return WorkspaceOntologyComposition::from_canonical_json(&snapshot.bytes).map(Some);
        }
        Ok(None)
    }

    /// Preflight a complete replacement against exact current authority.
    #[allow(clippy::too_many_lines)] // keeps the bounded aggregate preflight in one ordered pass
    pub fn preview_ontology_composition_change(
        &self,
        request: &CompositionChangeRequest,
        cancellation: Option<&CancellationToken>,
    ) -> Result<CompositionChangePreview, GfError> {
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let current = self.generation_for_read()?;
        if current.generation_uuid() != request.expected_project_generation_uuid {
            return Err(GfError::Validation(
                "composition preview project generation is stale".into(),
            ));
        }
        let existing = self.workspace_ontology_composition()?;
        let existing_fingerprint = existing
            .as_ref()
            .map(|composition| composition.composition_fingerprint.clone());
        if existing_fingerprint != request.expected_composition_fingerprint {
            return Err(GfError::Validation(
                "composition preview fingerprint is stale".into(),
            ));
        }
        let (compiled, compile_error) = match request.candidate.compile() {
            Ok(compiled) => (Some(compiled), None),
            Err(error) => (None, Some(error.to_string())),
        };
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let candidate_bytes = request.candidate.to_canonical_json()?;
        let (runtime_symbols, catalog_drift) = runtime_symbols(self, cancellation)?;
        let mut diagnostics = Vec::new();
        if compiled.is_none() {
            diagnostics.push(CompositionChangeDiagnostic {
                code: "candidate_composition_invalid".into(),
                subject: request.candidate.composition_fingerprint.clone(),
                remediation: compile_error.unwrap_or_else(|| "repair candidate authority".into()),
            });
        }
        if catalog_drift {
            diagnostics.push(CompositionChangeDiagnostic {
                code: "runtime_catalog_generation_drift".into(),
                subject: current.generation_uuid().hyphenated().to_string(),
                remediation: "reopen the exact current generation before composition preflight"
                    .into(),
            });
        }
        diagnostics.extend(migration_diagnostics(
            existing.as_ref(),
            &request.candidate,
            &runtime_symbols,
            cancellation,
        )?);
        diagnostics.extend(removed_authority_diagnostics(
            existing.as_ref(),
            &request.candidate,
            &runtime_symbols,
        ));
        let has_strict_scope = compiled.as_ref().is_some_and(|compiled| {
            compiled.profile_default == ActivationMode::Strict
                || compiled.modules.iter().any(|module| {
                    compiled.effective_module_mode(&module.id) == ActivationMode::Strict
                })
        });
        if has_strict_scope {
            let nonconforming = runtime_symbols.iter().filter(|symbol| {
                compiled
                    .as_ref()
                    .is_none_or(|compiled| !symbol_declared(compiled, symbol))
            });
            for symbol in nonconforming.take(64usize.saturating_sub(diagnostics.len())) {
                diagnostics.push(CompositionChangeDiagnostic {
                    code: "strict_runtime_observation".into(),
                    subject: symbol.clone(),
                    remediation: "declare the symbol before strict publication".into(),
                });
            }
        }
        let old_modules = existing
            .as_ref()
            .into_iter()
            .flat_map(|composition| composition.modules.iter())
            .map(|module| module.id.display_ref())
            .collect::<BTreeSet<_>>();
        let new_modules = request
            .candidate
            .modules
            .iter()
            .map(|module| module.id.display_ref())
            .collect::<BTreeSet<_>>();
        let old_bridges = existing
            .as_ref()
            .into_iter()
            .flat_map(|composition| composition.bridges.iter())
            .map(|bridge| {
                (
                    format!("{}@{}", bridge.bridge_id, bridge.authored_version),
                    serde_json::to_vec(bridge).unwrap_or_default(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let new_bridges = request
            .candidate
            .bridges
            .iter()
            .map(|bridge| {
                (
                    format!("{}@{}", bridge.bridge_id, bridge.authored_version),
                    serde_json::to_vec(bridge).unwrap_or_default(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let old_activation = existing
            .as_ref()
            .into_iter()
            .flat_map(|composition| composition.activation.iter())
            .map(|record| (record.subject.clone(), record.mode))
            .collect::<BTreeMap<_, _>>();
        let new_activation = request
            .candidate
            .activation
            .iter()
            .map(|record| (record.subject.clone(), record.mode))
            .collect::<BTreeMap<_, _>>();
        let mut affected_modules = old_modules
            .symmetric_difference(&new_modules)
            .cloned()
            .collect::<BTreeSet<_>>();
        for subject in old_activation.keys().chain(new_activation.keys()) {
            if old_activation.get(subject) != new_activation.get(subject) {
                affected_modules.insert(subject.clone());
            }
        }
        let affected_bridges = old_bridges
            .keys()
            .chain(new_bridges.keys())
            .filter(|identity| old_bridges.get(*identity) != new_bridges.get(*identity))
            .cloned()
            .collect::<BTreeSet<_>>();
        diagnostics
            .sort_by(|left, right| (&left.code, &left.subject).cmp(&(&right.code, &right.subject)));
        diagnostics.truncate(64);
        Ok(CompositionChangePreview {
            expected_project_generation_uuid: request.expected_project_generation_uuid,
            expected_composition_fingerprint: existing_fingerprint,
            candidate_composition_fingerprint: compiled.as_ref().map_or_else(
                || request.candidate.composition_fingerprint.clone(),
                |value| value.fingerprint.clone(),
            ),
            candidate_sha256: hex(&Sha256::digest(candidate_bytes)),
            affected_modules: affected_modules.into_iter().collect(),
            affected_bridges: affected_bridges.into_iter().collect(),
            portable_compatibility:
                CompositionPortableCompatibility::RepresentableReaderUnsupported,
            diagnostics,
        })
    }

    /// Revalidate and atomically publish one complete composition replacement.
    pub fn publish_ontology_composition_change(
        &mut self,
        request: &CompositionChangeRequest,
        preview: &CompositionChangePreview,
        cancellation: Option<&CancellationToken>,
    ) -> Result<CompositionChangeReceipt, GfError> {
        let request_fingerprint = composition_request_fingerprint(request)?;
        let mut generation_hasher = Sha256::new();
        generation_hasher.update(b"graphforge-composition-generation/1");
        generation_hasher.update(request.context.operation_uuid.0.as_bytes());
        generation_hasher.update(request_fingerprint);
        let expected_generation_uuid =
            graphforge_core::canonical::uuid_v8(generation_hasher.finalize().into());
        let root = self.resolved_generation.container_root();
        if let Some(published) = graphforge_storage::published_project_transaction(
            root,
            request.context.operation_uuid.0,
        )? {
            if published.generation_uuid != expected_generation_uuid {
                return Err(GfError::Project {
                    code: ProjectErrorCode::TransactionConflict,
                    message: "composition operation UUID was reused with different content".into(),
                });
            }
            let generation = graphforge_storage::resolve_verified_generation(
                root,
                published.generation_uuid,
                published.generation_manifest_sha256,
            )?;
            let snapshot = generation
                .participant_snapshot(
                    WORKSPACE_CAPABILITY_ID,
                    WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY,
                )?
                .ok_or_else(|| GfError::Validation("published composition is missing".into()))?;
            let composition = WorkspaceOntologyComposition::from_canonical_json(&snapshot.bytes)?;
            if composition != request.candidate {
                return Err(GfError::Project {
                    code: ProjectErrorCode::TransactionConflict,
                    message: "published composition content does not match retry".into(),
                });
            }
            return Ok(CompositionChangeReceipt {
                project_generation_uuid: published.generation_uuid,
                composition_fingerprint: composition.composition_fingerprint,
                candidate_sha256: hex(&Sha256::digest(request.candidate.to_canonical_json()?)),
            });
        }
        let fresh = self.preview_ontology_composition_change(request, cancellation)?;
        if &fresh != preview {
            return Err(GfError::Validation(
                "composition preview does not match request".into(),
            ));
        }
        if !fresh.diagnostics.is_empty() {
            return Err(GfError::Validation(
                "composition preflight contains unresolved diagnostics".into(),
            ));
        }
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let ontology = self.workspace_ontology()?;
        let mut configuration = self.workspace_configuration()?;
        configuration.ontology_mode = match request.candidate.profile_default {
            ActivationMode::Exploratory => WorkspaceOntologyMode::None,
            ActivationMode::Advisory => WorkspaceOntologyMode::Advisory,
            ActivationMode::Strict => WorkspaceOntologyMode::Strict,
        };
        let published_binding = graphforge_ir::CompositionBindingContext::new(
            std::sync::Arc::new(request.candidate.compile()?),
            request.candidate.bridges.clone(),
            graphforge_ir::CompositionBindingLimits::default(),
        );
        crate::workspace_ontology::publish_workspace_records(
            self,
            request.context.operation_uuid.0,
            request.context.actor_uuid,
            &ontology,
            &configuration,
            Some(&request.candidate),
            Some(expected_generation_uuid),
            cancellation,
        )?;
        *self
            .composition_binding
            .lock()
            .expect("composition binding lock poisoned") =
            Some(std::sync::Arc::new(published_binding));
        self.ontology_mode = configuration.ontology_mode.execution_mode();
        self.adjacency_provider = std::sync::Arc::new(
            graphforge_exec::PersistentAdjacencyProvider::new(self.dir.clone(), self.ontology_mode),
        );
        Ok(CompositionChangeReceipt {
            project_generation_uuid: *self
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            composition_fingerprint: fresh.candidate_composition_fingerprint,
            candidate_sha256: fresh.candidate_sha256,
        })
    }
}

fn removed_authority_diagnostics(
    existing: Option<&WorkspaceOntologyComposition>,
    candidate: &WorkspaceOntologyComposition,
    runtime_symbols: &BTreeSet<String>,
) -> Vec<CompositionChangeDiagnostic> {
    let Some(existing) = existing else {
        return Vec::new();
    };
    let new_ids = candidate
        .modules
        .iter()
        .map(|module| module.id.ontology_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut diagnostics = Vec::new();
    for module in existing
        .modules
        .iter()
        .filter(|module| !new_ids.contains(module.id.ontology_id.as_str()))
    {
        let symbols = module
            .document
            .entity_types
            .iter()
            .map(|item| format!("entity:{}", item.name))
            .chain(
                module
                    .document
                    .relation_types
                    .iter()
                    .map(|item| format!("relation:{}", item.name)),
            )
            .chain(
                module
                    .document
                    .properties
                    .iter()
                    .map(|item| format!("property:{}", item.name)),
            );
        for subject in symbols.filter(|symbol| runtime_symbols.contains(symbol)) {
            diagnostics.push(CompositionChangeDiagnostic {
                code: "removed_authority_has_stored_data".into(),
                subject,
                remediation: "retain the module or remove the affected stored data before publication".into(),
            });
        }
    }
    diagnostics
}

fn migration_diagnostics(
    existing: Option<&WorkspaceOntologyComposition>,
    candidate: &WorkspaceOntologyComposition,
    runtime_symbols: &BTreeSet<String>,
    cancellation: Option<&CancellationToken>,
) -> Result<Vec<CompositionChangeDiagnostic>, GfError> {
    let Some(existing) = existing else {
        return Ok(Vec::new());
    };
    let mut diagnostics = Vec::new();
    for old in &existing.modules {
        if let Some(token) = cancellation {
            token.checkpoint()?;
        }
        let Some(new) = candidate
            .modules
            .iter()
            .find(|module| module.id.ontology_id == old.id.ontology_id)
        else {
            continue;
        };
        if old.id.authored_version == new.id.authored_version {
            continue;
        }
        let migrations = new
            .document
            .migrations
            .iter()
            .chain(old.document.migrations.iter())
            .cloned()
            .collect::<Vec<_>>();
        match MigrationEngine::plan(
            &old.id.authored_version,
            &new.id.authored_version,
            &migrations,
        ) {
            Err(_) => diagnostics.push(CompositionChangeDiagnostic {
                code: "migration_unreachable".into(),
                subject: new.id.ontology_id.clone(),
                remediation: format!(
                    "author a migration path from {} to {}",
                    old.id.authored_version, new.id.authored_version
                ),
            }),
            Ok(steps) => {
                for step in &steps {
                    if let Some(token) = cancellation {
                        token.checkpoint()?;
                    }
                    if let TransformKind::Unknown { raw } = &step.transform_kind {
                        diagnostics.push(CompositionChangeDiagnostic {
                            code: "migration_transform_unknown".into(),
                            subject: format!(
                                "{}:{}->{}",
                                new.id.ontology_id, step.from_version, step.to_version
                            ),
                            remediation: format!(
                                "replace unsupported migration transform `{raw}` with a known transform"
                            ),
                        });
                    }
                    if let Some(subject) = migration_runtime_subject(&step.transform_kind) {
                        if runtime_symbols.contains(&subject) {
                            diagnostics.push(CompositionChangeDiagnostic {
                                code: "migration_requires_data_rewrite".into(),
                                subject,
                                remediation: "publish a migration that stages and validates the affected graph/catalog data".into(),
                            });
                        }
                    }
                }
                if !steps
                    .iter()
                    .any(|step| matches!(step.transform_kind, TransformKind::Unknown { .. }))
                {
                    match MigrationEngine::apply_document(old.document.clone(), &steps) {
                        Ok(mut migrated) => {
                            migrated.migrations.clone_from(&new.document.migrations);
                            if migrated != new.document {
                                diagnostics.push(CompositionChangeDiagnostic {
                                    code: "migration_result_mismatch".into(),
                                    subject: new.id.ontology_id.clone(),
                                    remediation: "make the authored migration result exactly match the candidate ontology document".into(),
                                });
                            }
                        }
                        Err(error) => diagnostics.push(CompositionChangeDiagnostic {
                            code: "migration_apply_failed".into(),
                            subject: new.id.ontology_id.clone(),
                            remediation: error.to_string(),
                        }),
                    }
                }
            }
        }
        if diagnostics.len() == 64 {
            break;
        }
    }
    Ok(diagnostics)
}

fn migration_runtime_subject(transform: &TransformKind) -> Option<String> {
    match transform {
        TransformKind::RenameType { old_name, .. }
        | TransformKind::RemoveType { name: old_name } => {
            Some(format!("{}:{old_name}", SymbolKind::Entity.as_str()))
        }
        TransformKind::RenameProperty { old_name, .. }
        | TransformKind::RemoveProperty { name: old_name, .. } => {
            Some(format!("{}:{old_name}", SymbolKind::Property.as_str()))
        }
        TransformKind::AddProperty { .. }
        | TransformKind::AddType { .. }
        | TransformKind::Unknown { .. } => None,
    }
}

fn composition_request_fingerprint(
    request: &CompositionChangeRequest,
) -> Result<[u8; 32], GfError> {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-composition-request/1");
    hasher.update(request.expected_project_generation_uuid.as_bytes());
    match &request.expected_composition_fingerprint {
        Some(fingerprint) => {
            hasher.update([1]);
            hasher.update(fingerprint.as_bytes());
        }
        None => hasher.update([0]),
    }
    match request.context.actor_uuid {
        Some(actor) => {
            hasher.update([1]);
            hasher.update(actor.as_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update(request.candidate.to_canonical_json()?);
    hasher.update(b"require_conforming");
    Ok(hasher.finalize().into())
}

fn symbol_declared(composition: &graphforge_ontology::CompiledComposition, encoded: &str) -> bool {
    let Some((kind, local_id)) = encoded.split_once(':') else {
        return false;
    };
    let kind = match kind {
        "entity" => SymbolKind::Entity,
        "relation" => SymbolKind::Relation,
        "property" => SymbolKind::Property,
        _ => return false,
    };
    composition
        .resolve(&ResolveRequest {
            module: None,
            kind,
            local_id,
            max_candidates: 2,
        })
        .is_ok()
}

fn runtime_symbols(
    graph: &GraphForge,
    cancellation: Option<&CancellationToken>,
) -> Result<(BTreeSet<String>, bool), GfError> {
    let current = graph.generation_for_read()?;
    let persisted = crate::load_runtime_catalog(&current.graph_tree_root())?;
    let live = graph
        .runtime_catalog
        .lock()
        .expect("runtime catalog poisoned")
        .clone();
    let catalog_symbols = persisted
        .entity_types()
        .into_iter()
        .map(|name| format!("{}:{name}", SymbolKind::Entity.as_str()))
        .chain(
            persisted
                .relation_types()
                .into_iter()
                .map(|name| format!("{}:{name}", SymbolKind::Relation.as_str())),
        )
        .chain(
            persisted
                .property_names()
                .map(|(_, name)| format!("{}:{name}", SymbolKind::Property.as_str())),
        )
        .collect::<BTreeSet<_>>();
    let symbols = pinned_data_symbols(&current.graph_tree_root(), &persisted, cancellation)?;
    let live_symbols = live
        .entity_types()
        .into_iter()
        .map(|name| format!("{}:{name}", SymbolKind::Entity.as_str()))
        .chain(
            live.relation_types()
                .into_iter()
                .map(|name| format!("{}:{name}", SymbolKind::Relation.as_str())),
        )
        .chain(
            live.property_names()
                .map(|(_, name)| format!("{}:{name}", SymbolKind::Property.as_str())),
        )
        .collect::<BTreeSet<_>>();
    Ok((
        symbols.clone(),
        catalog_symbols != live_symbols || !symbols.is_subset(&catalog_symbols),
    ))
}

fn pinned_data_symbols(
    root: &std::path::Path,
    catalog: &graphforge_ir::RuntimeCatalog,
    cancellation: Option<&CancellationToken>,
) -> Result<BTreeSet<String>, GfError> {
    let mut symbols = BTreeSet::new();
    let scan = |path: &std::path::Path| -> Result<Vec<arrow::record_batch::RecordBatch>, GfError> {
        if !path.is_file() {
            return Ok(Vec::new());
        }
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            File::open(path).map_err(|e| GfError::Storage(e.to_string()))?,
        )
        .map_err(|e| GfError::Storage(e.to_string()))?
        .with_batch_size(1024)
        .build()
        .map_err(|e| GfError::Storage(e.to_string()))?;
        let mut batches = Vec::new();
        for batch in reader {
            if let Some(token) = cancellation {
                token.checkpoint()?;
            }
            batches.push(batch.map_err(|e| GfError::Storage(e.to_string()))?);
        }
        Ok(batches)
    };
    for batch in scan(&root.join("topology/nodes.parquet"))? {
        let lists = batch
            .column_by_name("type_ids")
            .and_then(|v| v.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| GfError::Validation("nodes type_ids schema drift".into()))?;
        for row in 0..lists.len() {
            let values = lists.value(row);
            let ids = values
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| GfError::Validation("nodes type_ids value drift".into()))?;
            for id in ids.values() {
                let name = catalog
                    .entity_type_name(graphforge_ir::RuntimeTypeId(*id))
                    .ok_or_else(|| {
                        GfError::Validation("node type id missing from pinned catalog".into())
                    })?;
                symbols.insert(format!("entity:{name}"));
            }
        }
    }
    for (directory, kind) in [
        ("properties", "property"),
        ("edge_properties", "property"),
        ("topology/edges", "relation"),
    ] {
        let path = root.join(directory);
        if !path.is_dir() {
            continue;
        }
        let mut entries = fs::read_dir(path)
            .map_err(|e| GfError::Storage(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| GfError::Storage(e.to_string()))?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            let stem = path
                .file_stem()
                .and_then(|v| v.to_str())
                .unwrap_or_default()
                .to_owned();
            if stem.starts_with('_') {
                continue;
            }
            for batch in scan(&path)? {
                if batch.num_rows() > 0 {
                    if kind == "relation" {
                        symbols.insert(format!("relation:{stem}"));
                    } else {
                        for field in batch.schema().fields().iter().skip(1) {
                            symbols.insert(format!("property:{}", field.name()));
                        }
                    }
                }
            }
        }
    }
    Ok(symbols)
}

fn hex(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(TABLE[(byte >> 4) as usize] as char);
        output.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use graphforge_ontology::{
        AuthoredModule, CompositionLimits, EntityTypeDef, InventoryCompileRequest, MigrationDef,
        OntologyDoc, OntologyModuleId, compile_inventory, module_document_digest,
    };

    use super::*;
    use crate::{CheckpointRequest, OperationId, RevertCheckpointRequest};

    fn composition(
        version: &str,
        mode: ActivationMode,
        entity_names: &[&str],
        migrations: Vec<MigrationDef>,
    ) -> WorkspaceOntologyComposition {
        let document = OntologyDoc {
            ontology_id: "acme".into(),
            version: version.into(),
            entity_types: entity_names
                .iter()
                .map(|name| EntityTypeDef {
                    name: (*name).into(),
                    r#abstract: false,
                    parent: None,
                })
                .collect(),
            relation_types: Vec::new(),
            properties: Vec::new(),
            constraints: Vec::new(),
            migrations,
        };
        let authored = AuthoredModule {
            id: OntologyModuleId {
                ontology_id: document.ontology_id.clone(),
                authored_version: document.version.clone(),
                canonical_digest: module_document_digest(&document).unwrap(),
            },
            dependencies: Vec::new(),
            doc: document,
            allow_projected_identity: false,
        };
        let compiled = compile_inventory(InventoryCompileRequest {
            modules: std::slice::from_ref(&authored),
            bridges: &[],
            activation: &[],
            profile_default: mode,
            limits: CompositionLimits::default(),
            cancelled: None,
        })
        .unwrap();
        WorkspaceOntologyComposition::from_compiled(&compiled, Vec::new())
    }

    fn context(seed: u128) -> WriteContext {
        WriteContext {
            operation_uuid: OperationId(Uuid::from_u128(seed)),
            actor_uuid: None,
        }
    }

    fn request(
        graph: &GraphForge,
        seed: u128,
        candidate: WorkspaceOntologyComposition,
    ) -> CompositionChangeRequest {
        CompositionChangeRequest {
            context: context(seed),
            expected_project_generation_uuid: *graph.current_generation_uuid.lock().unwrap(),
            expected_composition_fingerprint: graph
                .workspace_ontology_composition()
                .unwrap()
                .map(|value| value.composition_fingerprint),
            candidate,
            data_disposition: CompositionDataDisposition::RequireConforming,
        }
    }

    #[test]
    fn publish_reopen_retry_and_rollback_preserve_exact_authority() {
        let project = tempfile::tempdir().unwrap();
        let path = project.path().to_str().unwrap();
        let mut graph = GraphForge::new(Some(path)).unwrap();
        let first = request(
            &graph,
            8401,
            composition("1", ActivationMode::Strict, &["Person"], Vec::new()),
        );
        let first_preview = graph
            .preview_ontology_composition_change(&first, None)
            .unwrap();
        let first_receipt = graph
            .publish_ontology_composition_change(&first, &first_preview, None)
            .unwrap();
        graph
            .checkpoint(CheckpointRequest {
                name: "composition-v1".into(),
                description: Some("retained exact composition generation".into()),
                idempotency_key: OperationId(Uuid::from_u128(8404)),
                actor_uuid: None,
            })
            .unwrap();
        graph
            .execute("MATCH (n:Person) RETURN n")
            .expect("normal execution consumes published composition authority");
        assert!(graph.execute("MATCH (n:Unknown) RETURN n").is_err());
        graph.set_graph_directedness(&context(8499), None).unwrap();
        assert_eq!(
            graph
                .publish_ontology_composition_change(&first, &first_preview, None)
                .unwrap(),
            first_receipt
        );
        drop(graph);

        let mut reopened = GraphForge::new(Some(path)).unwrap();
        let observed = reopened.workspace_ontology_composition().unwrap().unwrap();
        assert_eq!(observed.profile_default, ActivationMode::Strict);
        assert_eq!(observed.modules[0].document.version, "1");
        reopened
            .execute("MATCH (n:Person) RETURN n")
            .expect("reopen hydrates normal composition binding authority");

        let upgraded = request(
            &reopened,
            8402,
            composition(
                "2",
                ActivationMode::Strict,
                &["Person", "Company"],
                vec![MigrationDef {
                    from_version: "1".into(),
                    to_version: "2".into(),
                    transform_kind: "add_type:Company".into(),
                    script_ref: None,
                    checksum: None,
                }],
            ),
        );
        let upgraded_preview = reopened
            .preview_ontology_composition_change(&upgraded, None)
            .unwrap();
        assert!(upgraded_preview.diagnostics.is_empty());
        reopened
            .publish_ontology_composition_change(&upgraded, &upgraded_preview, None)
            .unwrap();

        let downgrade = request(&reopened, 8403, observed.clone());
        assert_eq!(
            reopened
                .preview_ontology_composition_change(&downgrade, None)
                .unwrap()
                .diagnostics[0]
                .code,
            "migration_unreachable"
        );
        reopened
            .revert_to_checkpoint(RevertCheckpointRequest {
                name: "composition-v1".into(),
                reason: "restore retained validated authority".into(),
                idempotency_key: OperationId(Uuid::from_u128(8405)),
                actor_uuid: None,
            })
            .unwrap();
        drop(reopened);
        assert_eq!(
            GraphForge::new(Some(path))
                .unwrap()
                .workspace_ontology_composition()
                .unwrap(),
            Some(observed)
        );
    }

    #[test]
    fn stale_cancelled_and_conflicting_replay_leave_generation_unchanged() {
        let mut graph = GraphForge::new(None).unwrap();
        let request = request(
            &graph,
            8410,
            composition("1", ActivationMode::Advisory, &["Person"], Vec::new()),
        );
        let preview = graph
            .preview_ontology_composition_change(&request, None)
            .unwrap();
        let before = *graph.current_generation_uuid.lock().unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(
            graph
                .publish_ontology_composition_change(&request, &preview, Some(&cancellation))
                .is_err()
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), before);
        graph
            .publish_ontology_composition_change(&request, &preview, None)
            .unwrap();
        let published = *graph.current_generation_uuid.lock().unwrap();

        let mut conflicting = request;
        conflicting.candidate =
            composition("1", ActivationMode::Advisory, &["Company"], Vec::new());
        assert!(
            graph
                .publish_ontology_composition_change(&conflicting, &preview, None)
                .is_err()
        );
        assert_eq!(*graph.current_generation_uuid.lock().unwrap(), published);
    }

    #[test]
    fn preflight_rejects_live_catalog_drift_from_bound_generation() {
        let graph = GraphForge::new(None).unwrap();
        {
            let mut catalog = graph.runtime_catalog.lock().unwrap();
            for index in 0..80 {
                catalog.intern_label(&format!("Exploratory{index:02}"));
            }
        }
        let request = request(
            &graph,
            8420,
            composition("1", ActivationMode::Strict, &["Person"], Vec::new()),
        );
        let preview = graph
            .preview_ontology_composition_change(&request, None)
            .unwrap();
        assert_eq!(preview.diagnostics.len(), 1);
        assert_eq!(
            preview.diagnostics[0].code,
            "runtime_catalog_generation_drift"
        );
    }

    #[test]
    fn migration_preflight_is_reachable_ordered_and_fail_closed() {
        let mut graph = GraphForge::new(None).unwrap();
        let initial = request(
            &graph,
            8430,
            composition("1", ActivationMode::Advisory, &["Person"], Vec::new()),
        );
        let preview = graph
            .preview_ontology_composition_change(&initial, None)
            .unwrap();
        graph
            .publish_ontology_composition_change(&initial, &preview, None)
            .unwrap();

        let unreachable = request(
            &graph,
            8431,
            composition("3", ActivationMode::Advisory, &["Person"], Vec::new()),
        );
        assert_eq!(
            graph
                .preview_ontology_composition_change(&unreachable, None)
                .unwrap()
                .diagnostics[0]
                .code,
            "migration_unreachable"
        );
        let unknown = request(
            &graph,
            8432,
            composition(
                "2",
                ActivationMode::Advisory,
                &["Person"],
                vec![MigrationDef {
                    from_version: "1".into(),
                    to_version: "2".into(),
                    transform_kind: "execute_magic".into(),
                    script_ref: None,
                    checksum: None,
                }],
            ),
        );
        assert_eq!(
            graph
                .preview_ontology_composition_change(&unknown, None)
                .unwrap()
                .diagnostics[0]
                .code,
            "migration_transform_unknown"
        );
    }
}
